use std::io::IoSlice;
use std::io::{self, Read, Write};
use std::os::unix::fs::FileExt;

#[cfg(feature = "shuttle")]
use shuttle::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
#[cfg(not(feature = "shuttle"))]
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

const CHECKSUM_SIZE: usize = std::mem::size_of::<u32>();
const LENGTH_SIZE: usize = std::mem::size_of::<u64>();
const HEADER_SIZE: usize = CHECKSUM_SIZE + LENGTH_SIZE;

const CHECKSUM_BUFFER_SIZE: usize = 4096;

pub struct Wal {
    ok: AtomicBool,

    file: std::fs::File,

    entries: Mutex<Vec<(u64, u64)>>,
    written: AtomicU64,
    durable: AtomicU64,

    sync_lock: Mutex<()>,
}

pub struct WalIterator<'a> {
    wal: &'a Wal,
    index: u64,
    end: u64,
}

impl Wal {
    pub fn open(path: impl AsRef<std::path::Path>) -> io::Result<Self> {
        let ok = AtomicBool::new(true);
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        file.try_lock()?;

        // On linux, sync the file and its parent directory
        // to ensure that the file is created on disk.
        file.sync_all()?;
        let parent = std::path::Path::new(path.as_ref())
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| std::path::Path::new("."));
        std::fs::File::open(parent)?.sync_all()?;

        let file_len = file.metadata()?.len();

        let mut entries = Vec::new();
        let mut offset = 0;

        while offset < file_len {
            let mut header = [0u8; HEADER_SIZE];
            match file.read_exact(&mut header) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    // unexpected EOF
                    ok.store(false, Ordering::SeqCst);
                    break;
                }
                Err(e) => return Err(e),
            }
            let checksum = u32::from_le_bytes(header[0..CHECKSUM_SIZE].try_into().unwrap());
            let length = u64::from_le_bytes(header[CHECKSUM_SIZE..HEADER_SIZE].try_into().unwrap());
            let Some(end) = offset
                .checked_add(header.len() as u64)
                .and_then(|v| v.checked_add(length))
                .filter(|&v| v <= file_len)
            else {
                // invalid length
                ok.store(false, Ordering::SeqCst);
                break;
            };
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&header[CHECKSUM_SIZE..HEADER_SIZE]);
            let buffer = &mut [0u8; CHECKSUM_BUFFER_SIZE];

            let mut remaining = length;
            while remaining > 0 {
                let read_len = usize::try_from(std::cmp::min(remaining, buffer.len() as u64))
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "length too large"))?;
                match file.read_exact(&mut buffer[..read_len]) {
                    Ok(_) => {}
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                        break;
                    }
                    Err(e) => return Err(e),
                }
                hasher.update(&buffer[..read_len]);
                remaining -= read_len as u64;
            }
            if remaining > 0 {
                // unexpected EOF
                ok.store(false, Ordering::SeqCst);
                break;
            }
            if hasher.finalize() != checksum {
                // checksum mismatch
                ok.store(false, Ordering::SeqCst);
                break;
            }
            entries.push((offset, length));
            offset = end;
        }

        let durable = AtomicU64::new(entries.len() as u64);
        let written = AtomicU64::new(entries.len() as u64);
        let entries = Mutex::new(entries);
        let sync_lock = Mutex::new(());

        Ok(Wal {
            ok,
            file,
            entries,
            durable,
            written,
            sync_lock,
        })
    }

    pub fn is_ok(&self) -> bool {
        self.ok.load(Ordering::SeqCst)
    }

    pub fn write(&self, data: &[u8]) -> io::Result<u64> {
        let length = data.len() as u64;
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&length.to_le_bytes());
        hasher.update(data);
        let checksum = hasher.finalize();

        let mut header = [0u8; HEADER_SIZE];
        header[0..CHECKSUM_SIZE].copy_from_slice(&checksum.to_le_bytes());
        header[CHECKSUM_SIZE..HEADER_SIZE].copy_from_slice(&length.to_le_bytes());

        let mut entries = self.entries.lock().unwrap();
        if !self.is_ok() {
            return Err(io::Error::other("WAL is corrupted, cannot write"));
        }
        let mut file = &self.file;
        let offset = match entries.last() {
            Some((last_offset, last_length)) => last_offset + HEADER_SIZE as u64 + last_length,
            None => 0,
        };
        let mut buffers = [IoSlice::new(&header), IoSlice::new(data)];
        let mut remaining = &mut buffers[..];
        while !remaining.is_empty() {
            match file.write_vectored(remaining) {
                Ok(0) => {
                    self.ok.store(false, Ordering::SeqCst);
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write all data",
                    ));
                }
                Ok(n) => IoSlice::advance_slices(&mut remaining, n),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    self.ok.store(false, Ordering::SeqCst);
                    return Err(e);
                }
            }
        }
        entries.push((offset, length));
        let index = entries.len() as u64 - 1;
        self.written.store(index + 1, Ordering::SeqCst);

        Ok(index)
    }

    pub fn sync(&self, index: u64) -> io::Result<u64> {
        let _lock = self.sync_lock.lock().unwrap();
        if index < self.durable.load(Ordering::SeqCst) {
            return Ok(index);
        }
        if !self.is_ok() {
            return Err(io::Error::other("WAL is unhealthy, cannot sync"));
        }
        let current = self.written.load(Ordering::SeqCst);
        self.file.sync_all().inspect_err(|_| {
            self.ok.store(false, Ordering::SeqCst);
        })?;
        self.durable.store(current, Ordering::SeqCst);
        Ok(index)
    }

    pub fn append(&self, data: &[u8]) -> io::Result<u64> {
        let index = self.write(data)?;
        self.sync(index)?;
        Ok(index)
    }

    pub fn read(&self, index: u64) -> io::Result<Vec<u8>> {
        let index = usize::try_from(index)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "index too large"))?;
        let (offset, expected_length) = {
            let entries = self.entries.lock().unwrap();
            entries
                .get(index)
                .copied()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "index out of bounds"))?
        };
        let mut header = [0u8; HEADER_SIZE];
        self.file.read_exact_at(&mut header, offset)?;
        let checksum = u32::from_le_bytes(header[0..CHECKSUM_SIZE].try_into().unwrap());
        let length = u64::from_le_bytes(header[CHECKSUM_SIZE..HEADER_SIZE].try_into().unwrap());
        if length != expected_length {
            self.ok.store(false, Ordering::SeqCst);
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "length mismatch",
            ));
        }
        let data_length = usize::try_from(length)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "length too large"))?;
        let mut data = vec![0u8; data_length];
        self.file
            .read_exact_at(&mut data, offset + HEADER_SIZE as u64)?;
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&length.to_le_bytes());
        hasher.update(&data);
        if hasher.finalize() != checksum {
            self.ok.store(false, Ordering::SeqCst);
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "checksum mismatch",
            ));
        }
        Ok(data)
    }

    pub fn iter(&self) -> WalIterator<'_> {
        let end = self.durable.load(Ordering::SeqCst);
        WalIterator {
            wal: self,
            index: 0,
            end,
        }
    }
}

impl Iterator for WalIterator<'_> {
    type Item = io::Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.end {
            return None;
        }
        match self.wal.read(self.index) {
            Ok(item) => {
                self.index += 1;
                Some(Ok(item))
            }
            Err(e) => {
                self.index = self.end; // Stop iteration on error
                Some(Err(e))
            }
        }
    }
}
impl std::iter::FusedIterator for WalIterator<'_> {}

impl<'a> IntoIterator for &'a Wal {
    type Item = io::Result<Vec<u8>>;
    type IntoIter = WalIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use proptest::test_runner::{RngAlgorithm, TestRng};
    use rand::Rng;

    #[derive(Debug, Clone)]
    enum CorruptionKind {
        Zero,
        Flip,
        Random,
    }

    #[derive(Debug, Clone)]
    enum Op {
        Write(Vec<u8>),
        Sync(usize),
        Read(usize),
        Reopen,
        Crash(usize),
        Corrupt(CorruptionKind, usize, usize),
    }

    fn operation() -> impl Strategy<Value = Op> {
        prop_oneof![
            proptest::collection::vec(any::<u8>(), 0..128).prop_map(Op::Write),
            (0usize..100).prop_map(Op::Sync),
            (0usize..100).prop_map(Op::Read),
            Just(Op::Reopen),
            any::<usize>().prop_map(Op::Crash),
            (
                prop_oneof![
                    Just(CorruptionKind::Zero),
                    Just(CorruptionKind::Flip),
                    Just(CorruptionKind::Random),
                ],
                any::<usize>(),
                any::<usize>(),
            )
                .prop_map(|(kind, offset, length)| Op::Corrupt(kind, offset, length)),
        ]
    }

    proptest! {
        #[test]
        fn matches_model(
            seed in any::<[u8; 32]>(),
            operations in proptest::collection::vec(operation(), 0..50),
        ) {
            let mut rng = TestRng::from_seed(RngAlgorithm::ChaCha, &seed);
            // let directory = tempfile::tempdir().unwrap();
            let directory = tempfile::tempdir_in("/dev/shm")?;
            let path = directory.path().join("test.wal");
            let mut wal = Wal::open(&path).unwrap();

            let mut entries: Vec<Vec<u8>> = Vec::new();
            let mut durable = 0usize;

            for operation in operations {
                match operation {
                    Op::Write(data) => {
                        let index = wal.write(&data).unwrap();
                        prop_assert_eq!(index, entries.len() as u64);
                        entries.push(data);
                    }

                    Op::Sync(selector) => {
                        if !entries.is_empty() {
                            // Select an existing entry.
                            let index = selector % entries.len();

                            let returned = wal.sync(index as u64).unwrap();
                            prop_assert_eq!(returned, index as u64);

                            // Syncing an already-durable entry is a no-op.
                            if index >= durable {
                                durable = entries.len();
                            }
                        }
                    }

                    Op::Read(selector) => {
                        // Include one out-of-bounds position.
                        let index = selector % (entries.len() + 1);
                        let actual = wal.read(index as u64);

                        match entries.get(index) {
                            Some(expected) => {
                                prop_assert_eq!(
                                    actual.unwrap(),
                                    expected.clone(),
                                );
                            }
                            None => {
                                prop_assert_eq!(
                                    actual.unwrap_err().kind(),
                                    io::ErrorKind::InvalidInput,
                                );
                            }
                        }
                    }

                    Op::Reopen => {
                        drop(wal);
                        durable = entries.len();

                        wal = Wal::open(&path).unwrap();

                        // The durable entries should be intact.
                        let actual = wal.iter()
                            .collect::<io::Result<Vec<_>>>()
                            .unwrap();
                        prop_assert_eq!(actual.as_slice(), &entries[..durable]);
                    }

                    Op::Corrupt(kind, offset_selector, length_selector) => {
                        drop(wal);
                        let mut bytes = std::fs::read(&path).unwrap();
                        let file_len: usize = entries.iter()
                            .map(|data| HEADER_SIZE + data.len())
                            .sum();
                        prop_assert_eq!(bytes.len(), file_len);
                        let offset = offset_selector % (file_len + 1);
                        let length = length_selector % (file_len - offset + 1);
                        let mut random_bytes = vec![0u8; length];
                        if matches!(kind, CorruptionKind::Random) {
                            rng.fill_bytes(&mut random_bytes);
                        }
                        let mut first_changed = None;
                        for (i, byte) in bytes[offset..offset + length].iter_mut().enumerate() {
                            let replacement = match &kind {
                                CorruptionKind::Zero => 0,
                                CorruptionKind::Flip => !*byte,
                                CorruptionKind::Random => random_bytes[i],
                            };
                            if replacement != *byte {
                                first_changed.get_or_insert(offset + i);
                            }
                            *byte = replacement;
                        }

                        // Derive the intact prefix from original record boundaries.
                        // Like CRC-based recovery, this assumes no checksum collision.
                        let mut record_end = 0;
                        let expected_count = match first_changed {
                            Some(changed) => entries.iter().position(|data| {
                                record_end += HEADER_SIZE + data.len();
                                changed < record_end
                            }).unwrap(),
                            None => entries.len(),
                        };
                        std::fs::write(&path, &bytes).unwrap();
                        wal = Wal::open(&path).unwrap();
                        let actual = wal.iter().collect::<io::Result<Vec<_>>>().unwrap();
                        prop_assert_eq!(actual.as_slice(), &entries[..expected_count]);
                        prop_assert_eq!(wal.is_ok(), first_changed.is_none());

                        if !wal.is_ok() {
                            let tmp_path = directory.path().join("wal.log.tmp");
                            let tmp_wal = Wal::open(&tmp_path).unwrap();
                            for entry in wal.iter() {
                                tmp_wal.append(&entry.unwrap()).unwrap();
                            }
                            std::fs::rename(&tmp_path, &path).unwrap();
                            std::fs::File::open(directory.path()).unwrap().sync_all().unwrap();
                            wal = tmp_wal;
                        }
                        entries.truncate(expected_count);
                        durable = expected_count;
                    }

                    Op::Crash(selector) => {
                        // Model a crash that loses only a suffix of unsynced bytes.
                        let file_len: usize = entries.iter()
                            .map(|data| HEADER_SIZE + data.len())
                            .sum();
                        let durable_end: usize = entries[..durable].iter()
                            .map(|data| HEADER_SIZE + data.len())
                            .sum();
                        let unsynced_bytes = file_len - durable_end;
                        let lost_bytes = selector % (unsynced_bytes + 1);
                        let truncate_len = file_len - lost_bytes;
                        let mut valid_end = 0;
                        let mut expected_count = 0;
                        for data in &entries {
                            let end = valid_end + HEADER_SIZE + data.len();
                            if end > truncate_len {
                                break;
                            }
                            valid_end = end;
                            expected_count += 1;
                        }

                        drop(wal);
                        std::fs::OpenOptions::new()
                            .write(true)
                            .open(&path)
                            .unwrap()
                            .set_len(truncate_len as u64)
                            .unwrap();
                        wal = Wal::open(&path).unwrap();

                        // Recover exactly the complete prefix, including all synced entries.
                        let actual = wal.iter()
                            .collect::<io::Result<Vec<_>>>()
                            .unwrap();
                        prop_assert_eq!(actual.as_slice(), &entries[..expected_count]);
                        prop_assert!(expected_count >= durable);
                        prop_assert_eq!(wal.is_ok(), truncate_len == valid_end);

                        // if wal is broken, recover
                        if !wal.is_ok() {
                            let tmp_path = directory.path().join("wal.log.tmp");
                            let tmp_wal = Wal::open(&tmp_path).unwrap();
                            for entry in wal.iter() {
                                let data = entry.unwrap();
                                tmp_wal.append(&data).unwrap();
                            }
                            drop(wal);
                            drop(tmp_wal);
                            std::fs::rename(&tmp_path, &path).unwrap();
                            wal = Wal::open(&path).unwrap();
                        }
                        entries.truncate(expected_count);
                        durable = expected_count;
                    }
                }

                let actual = wal.iter()
                    .collect::<io::Result<Vec<_>>>()
                    .unwrap();

                prop_assert_eq!(actual.as_slice(), &entries[..durable]);
                prop_assert!(wal.is_ok());
            }
        }
    }
}

#[cfg(all(test, feature = "shuttle"))]
mod shuttle_tests {
    use super::*;
    use shuttle::{sync::Arc, thread};

    #[test]
    fn concurrent_writes_syncs_and_snapshots() {
        shuttle::check_random(
            || {
                let dir = tempfile::tempdir_in("/dev/shm").unwrap();
                let path = dir.path().join("test.wal");
                let wal = Arc::new(Wal::open(&path).unwrap());
                assert_eq!(wal.append(b"").unwrap(), 0);
                let initial_snapshot = wal.iter();

                let writers: Vec<_> = (0u8..2).map(|writer_id| {
                    let wal = Arc::clone(&wal);
                    thread::spawn(move || {
                        let mut completed = Vec::new();
                        for sequence in 0u8..3 {
                            let payload = vec![writer_id, sequence];
                            let index = wal.write(&payload).unwrap();
                            // A completed write must be readable even before this caller syncs.
                            assert_eq!(wal.read(index).unwrap(), payload);
                            thread::yield_now();
                            assert_eq!(wal.sync(index).unwrap(), index);
                            let durable = wal.iter().collect::<io::Result<Vec<_>>>().unwrap();
                            assert_eq!(durable.get(usize::try_from(index).unwrap()), Some(&payload));
                            // Exercise the already-durable path while other writes proceed.
                            wal.sync(index).unwrap();
                            completed.push((index, payload));
                        }
                        completed
                    })
                }).collect();

                let reader_wal = Arc::clone(&wal);
                let reader = thread::spawn(move || {
                    let mut snapshots = Vec::new();
                    for _ in 0..4 {
                        let mut snapshot = reader_wal.iter();
                        let end = snapshot.end;
                        thread::yield_now();
                        let mut records = Vec::new();
                        for record in snapshot.by_ref() {
                            records.push(record.unwrap());
                            thread::yield_now();
                        }
                        assert_eq!(records.len() as u64, end);
                        assert!(snapshot.next().is_none());
                        snapshots.push(records);
                    }
                    snapshots
                });

                let mut completed = vec![(0, Vec::new())];
                for writer in writers {
                    completed.extend(writer.join().unwrap());
                }
                let snapshots = reader.join().unwrap();
                completed.sort_by_key(|(index, _)| *index);
                for (expected_index, (index, _)) in completed.iter().enumerate() {
                    assert_eq!(*index, expected_index as u64);
                }
                let expected: Vec<_> = completed.into_iter().map(|(_, data)| data).collect();
                for snapshot in snapshots {
                    assert_eq!(snapshot.as_slice(), &expected[..snapshot.len()]);
                }
                assert_eq!(
                    initial_snapshot.collect::<io::Result<Vec<_>>>().unwrap(),
                    vec![Vec::<u8>::new()],
                );
                assert_eq!(wal.iter().collect::<io::Result<Vec<_>>>().unwrap(), expected);
                assert!(wal.is_ok());

                // Reopening checks the resulting file, not simulated crash persistence.
                drop(wal);
                let reopened = Wal::open(&path).unwrap();
                assert!(reopened.is_ok());
                assert_eq!(reopened.iter().collect::<io::Result<Vec<_>>>().unwrap(), expected);
            },
            1000,
        );
    }

    #[test]
    fn concurrent_appends() {
        shuttle::check_random(
            || {
                // let dir = tempfile::tempdir().unwrap();
                let dir = tempfile::tempdir_in("/dev/shm").unwrap();
                let wal = Arc::new(Wal::open(dir.path().join("test.wal")).unwrap());

                let other = Arc::clone(&wal);
                let writer = thread::spawn(move || other.append(b"first"));

                let second_index = wal.append(b"second").unwrap();
                let first_index = writer.join().unwrap().unwrap();

                assert_ne!(first_index, second_index);
                assert_eq!(wal.read(first_index).unwrap(), b"first");
                assert_eq!(wal.read(second_index).unwrap(), b"second");

                let mut expected = vec![
                    (first_index, b"first".to_vec()),
                    (second_index, b"second".to_vec()),
                ];
                expected.sort_by_key(|(index, _)| *index);
                assert_eq!(expected[0].0, 0);
                assert_eq!(expected[1].0, 1);
                let expected: Vec<_> = expected.into_iter().map(|(_, data)| data).collect();
                let actual = wal.iter().collect::<io::Result<Vec<_>>>().unwrap();
                assert_eq!(actual, expected);

                assert_eq!(wal.read(first_index).unwrap(), b"first");
                assert_eq!(wal.read(second_index).unwrap(), b"second");
            },
            100,
        );
    }
}
