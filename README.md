# Disky: Riegeli Format for Rust

Disky is a Rust implementation of the [Riegeli file format](https://github.com/google/riegeli). Riegeli is a file format for storing records, where a record is conceptually a binary string, typically a serialized protocol buffer message.

## Features

- Read and write Riegeli files
- Simple record encoding/decoding
- Position tracking for records
- Support for seeking to specific records

## Basic Example

```rust
use std::fs::File;
use disky::{RecordReader, RecordWriter};

// Writing records
let file = File::create("data.riegeli").unwrap();
let mut writer = RecordWriter::new(file).unwrap();

for i in 0..100 {
    let record = format!("Record #{}", i).into_bytes();
    writer.write_record(&record).unwrap();
}

writer.close().unwrap();

// Reading records
let file = File::open("data.riegeli").unwrap();
let mut reader = RecordReader::new(file).unwrap();

while let Ok(record) = reader.read_record() {
    let text = String::from_utf8_lossy(&record);
    println!("Read: {}", text);
}
```

## Current Status

This is a basic implementation with the following limitations:

- Only supports uncompressed records for now
- Limited error recovery
- No support for transposed records
- No proto message handling (though you can manually serialize them)

## Future Work

- Add compression support (Brotli, Zstd, Snappy)
- Implement transposed record format for better proto compression
- Add better error recovery
- Add proto message integration
- Improve performance with vectorized operations
- Add more testing and benchmarks

## License

Licensed under the Apache License, Version 2.0.