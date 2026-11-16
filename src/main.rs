use formal_wal::Wal;
use std::sync::Arc;

fn main() {
    let wal = match Wal::open("wal.log") {
        Ok(wal) if wal.is_ok() => Arc::new(wal),
        Ok(_) => {
            eprintln!("WAL is corrupted");
            return;
        }
        Err(e) => {
            eprintln!("Failed to open WAL: {}", e);
            return;
        }
    };

    std::thread::scope(|s| {
        s.spawn(|| wal.append(b"first"));
        s.spawn(|| wal.append(b"second"));
    });

    for entry in wal.iter() {
        let data = entry.unwrap();
        println!("Data: {:?}", String::from_utf8_lossy(&data));
    }
}
