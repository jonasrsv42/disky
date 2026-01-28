# Disky: Streaming File Format

Disky is a Rust reader/writer for a variation of the [Riegeli file format](https://github.com/google/riegeli), designed for high-performance streaming of record-based data. It provides both single-threaded and multi-threaded APIs with features like corruption recovery, and configurable compression.

## Features

- **High-performance streaming** record I/O with minimal memory overhead
- **Compression** optionally compress/decompress records trading off I/O and CPU
- **Corruption detection and recovery** capabilities

## Quick Start

### Basic Writing and Reading

```rust
use std::fs::File;
use disky::reader::{RecordReaderConfig, DiskyPiece};
use disky::writer::RecordWriterConfig;

// Writing records
let file = File::create("data.disky")?;
let mut writer = RecordWriterConfig::new(file).build()?;

// Write some records
writer.write_record(b"Record 1")?;
writer.write_record(b"Record 2")?;
writer.write_record(b"Record 3")?;

// Ensure all data is written
writer.close()?;

// Reading records
let file = File::open("data.disky")?;
let mut reader = RecordReaderConfig::new(file).build()?;

// Read all records
loop {
    match reader.next_record()? {
        DiskyPiece::Record(bytes) => {
            println!("Record: {}", String::from_utf8_lossy(&bytes));
        }
        DiskyPiece::EOF => break,
    }
}
```

### Using Iterator Interface

```rust
use std::fs::File;
use disky::reader::RecordReaderConfig;

let file = File::open("data.disky")?;
let reader = RecordReaderConfig::new(file).build()?;

// Iterate over all records
for record_result in reader {
    let record = record_result?;
    println!("Record: {}", String::from_utf8_lossy(&record));
}
```

### Appending to Existing Files

```rust
use std::fs::File;
use disky::writer::RecordWriterConfig;

// Open existing file for appending
let file = File::options().read(true).write(true).open("data.disky")?;
let file_size = file.metadata()?.len();

// Create a writer in append mode
let mut writer = RecordWriterConfig::new(file)
    .for_append(file_size)
    .build()?;

// Append records
writer.write_record(b"Appended Record")?;
writer.close()?;
```

## Parallel Processing with Multi-Threading

For high-throughput scenarios, Disky provides a powerful multi-threaded API that scales with your available CPU cores. Enable the `parallel` feature in your Cargo.toml:

```toml
[dependencies]
disky = { version = "0.1.0", features = ["parallel"] }
```

Then you can use the multi-threaded API:

```rust
use bytes::Bytes;
use disky::parallel::multi_threaded_writer::{MultiThreadedWriter, MultiThreadedWriterConfig};
use disky::parallel::writer::{ShardingConfig, ParallelWriterConfig};
use disky::shard::sink::FileShardsBuilder;

// Create a shard factory for multiple output files
let file_shards = FileShardsBuilder::new("/tmp/output", "shard").build()?;

// Configure with 3 shards and 4 worker threads
let sharding_config = ShardingConfig::new(Box::new(file_shards), 3);
let config = MultiThreadedWriterConfig {
    writer_config: ParallelWriterConfig::default(),
    worker_threads: 4,
};

// Create the multi-threaded writer
let writer = MultiThreadedWriter::new(sharding_config, config)?;

// Write records asynchronously
for i in 0..1000 {
    let data = format!("Record #{}", i);
    let bytes = Bytes::from(data.into_bytes());
    
    // Write asynchronously (returns a Promise)
    let promise = writer.write_record(bytes)?;
    
    // Optionally wait for completion
    let _ = promise.wait()?;
}

// Flush and close when done
writer.flush()?;
writer.close()?;
```

Reading with the multi-threaded API is just as easy:

```rust
use std::path::PathBuf;
use disky::parallel::multi_threaded_reader::{MultiThreadedReader, MultiThreadedReaderConfig};
use disky::parallel::reader::{ShardingConfig, ParallelReaderConfig, DiskyParallelPiece};
use disky::shard::source::{FileShards, SequentialShardSource};

// Discover sharded files and create a sequential source
let file_shards = FileShards::from_pattern(PathBuf::from("/tmp/output"), "shard")?;
let source = SequentialShardSource::new(file_shards);
let sharding_config = ShardingConfig::new(Box::new(source), 3);

// Create the multi-threaded reader
let reader = MultiThreadedReader::new(sharding_config, MultiThreadedReaderConfig::default())?;

// Use iterator interface
for record_result in reader {
    let record = record_result?;
    println!("Record: {}", String::from_utf8_lossy(&record));
}
```


## Benchmarks

To run the benchmarks:

```bash
# Run all benchmarks (including parallel with appropriate feature)
cargo bench

# Run only single-threaded benchmarks
cargo bench --bench record_io_bench

# Run only parallel benchmarks
cargo bench --bench parallel_reader_bench --features parallel
cargo bench --bench parallel_writer_bench --features parallel
```

## Testing

```bash
# Run unit tests
cargo test

# Run integration tests
cargo test --test '*'

# Run tests with parallel features enabled
cargo test --features parallel

# Run examples
cargo run --example single_threaded_examples
cargo run --example multi_threaded_examples --features parallel
```

## Format Overview

Disky implements a version of the Riegeli file format, which has the following structure:

- Efficient streaming of sequential data
- Effective compression of multiple records together
- Corruption detection and optional recovery
- Optimized for high-throughput operations

## License

Licensed under the Apache License, Version 2.0.
