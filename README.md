# Disky: High-Performance Streaming File Format for Rust

Disky is a Rust implementation of the [Riegeli file format](https://github.com/google/riegeli), designed for high-performance streaming of record-based data. It provides both single-threaded and multi-threaded APIs for efficient reading and writing of record data with advanced features like parallel processing, corruption recovery, and configurable compression.

## Features

- **High-performance streaming** record I/O with minimal memory overhead
- **Thread-safe parallel APIs** for multi-core scaling
- **Auto-sharding** for distributing data across multiple files
- **Corruption detection and recovery** capabilities
- **Block-based architecture** for efficient data access
- **Chunked record storage** for optimized I/O operations
- **Configurable compression** options
- **Memory efficient** with minimal allocations
- **Append support** for adding records to existing files

## Quick Start

### Basic Writing and Reading

```rust
use std::fs::File;
use disky::reader::{RecordReader, DiskyPiece};
use disky::writer::RecordWriter;

// Writing records
let file = File::create("data.disky")?;
let mut writer = RecordWriter::new(file)?;

// Write some records
writer.write_record(b"Record 1")?;
writer.write_record(b"Record 2")?;
writer.write_record(b"Record 3")?;

// Ensure all data is written
writer.close()?;

// Reading records
let file = File::open("data.disky")?;
let mut reader = RecordReader::new(file)?;

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
use disky::reader::RecordReader;

let file = File::open("data.disky")?;
let reader = RecordReader::new(file)?;

// Iterate over all records
for record_result in reader {
    let record = record_result?;
    println!("Record: {}", String::from_utf8_lossy(&record));
}
```

### Appending to Existing Files

```rust
use std::fs::File;
use disky::writer::RecordWriter;

// Open existing file for appending
let file = File::options().read(true).write(true).open("data.riegeli")?;
let file_size = file.metadata()?.len();

// Create a writer in append mode
let mut writer = RecordWriter::for_append(file, file_size)?;

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
use std::path::PathBuf;
use bytes::Bytes;
use disky::parallel::multi_threaded_writer::{MultiThreadedWriter, MultiThreadedWriterConfig};
use disky::parallel::writer::{ShardingConfig, ParallelWriterConfig};
use disky::parallel::sharding::FileSharder;

// Create a sharder for multiple output files
let sharder = FileSharder::with_prefix(PathBuf::from("/tmp/output"), "shard");

// Configure with 3 shards and 4 worker threads
let sharding_config = ShardingConfig::new(Box::new(sharder), 3);
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
use disky::parallel::sharding::FileShardLocator;

// Create a locator for finding sharded files
let shard_locator = FileShardLocator::new(PathBuf::from("/tmp/output"), "shard")?;
let sharding_config = ShardingConfig::new(Box::new(shard_locator), 3);

// Create the multi-threaded reader
let reader = MultiThreadedReader::new(sharding_config, MultiThreadedReaderConfig::default())?;

// Use iterator interface
for record_result in reader {
    let record = record_result?;
    println!("Record: {}", String::from_utf8_lossy(&record));
}
```

## Example Applications

- **Data processing pipelines** for ETL workflows
- **Log aggregation** and analysis systems
- **High-throughput event storage**
- **Machine learning data preparation**
- **Archival of structured data**
- **Inter-service data exchange**

## Performance

Disky is designed for high-performance scenarios. The multi-threaded implementation can achieve significantly higher throughput by parallelizing both I/O and processing operations.

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

Disky includes comprehensive test suites to ensure correctness and reliability:

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

Disky implements the Riegeli file format, which has the following structure:

1. **File Signature**: Every file begins with a standard signature
2. **Blocks**: The file is divided into fixed-size blocks (default 64KB)
3. **Chunks**: Records are grouped into chunks for efficient I/O
4. **Records**: Individual binary data units stored within chunks

This architecture provides several benefits:
- Efficient streaming of sequential data
- Effective compression of multiple records together
- Corruption detection and optional recovery
- Optimized for high-throughput operations

## License

Licensed under the Apache License, Version 2.0.