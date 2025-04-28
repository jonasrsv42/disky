// Copyright 2024
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Benchmark comparing the performance of:
//! - Single-threaded RecordReader on a single large file (10GB)
//! - MultiThreadedReader on multiple smaller files (10 x 1GB)
//!
//! This benchmark tests how well the multi-threaded reader handles
//! parallelism across multiple shards compared to a single-threaded
//! reader on a single large file.

#![cfg_attr(not(feature = "parallel"), allow(dead_code, unused_imports))]

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::time::Duration;
use tempfile::{tempdir, NamedTempFile, TempDir};

use disky::error::Result;
use disky::reader::{DiskyPiece, RecordReader};
use disky::writer::RecordWriter;

/// Constants for benchmark sizes
///
/// This benchmark compares reading a large single file vs multiple smaller files in parallel.
/// Adjust these constants to increase or decrease the benchmark size.
const RECORD_SIZE: usize = 1_000_000; // 1MB records
const RECORDS_PER_SHARD: usize = 1_000; // 1,000 records per shard = ~1GB
const SHARD_COUNT: usize = 10; // 10 shards = ~10GB total

// Number of threads to use for multi-threaded reader and writer
const DEFAULT_THREAD_COUNT: usize = SHARD_COUNT;

/// Generate test record data of a specific size
fn generate_test_record(record_index: usize, record_size: usize) -> Vec<u8> {
    // Fill with sequential bytes to make it a bit more realistic than all zeros
    let mut record = Vec::with_capacity(record_size);
    for j in 0..record_size {
        record.push(((record_index + j) % 256) as u8);
    }
    record
}

/// Write records to a single file and return the temp file
fn write_single_file(record_count: usize, record_size: usize) -> Result<NamedTempFile> {
    // Create a temporary file
    let file = NamedTempFile::new().expect("Failed to create temp file");

    // Create a writer
    let mut writer = RecordWriter::new(file.reopen().unwrap())?;

    // Write all records
    for i in 0..record_count {
        let record = generate_test_record(i, record_size);
        writer.write_record(&record)?;
    }

    // Close the writer to flush all data
    writer.close()?;

    Ok(file)
}

/// Read all records using a standard RecordReader
fn read_with_record_reader(file: &NamedTempFile) -> Result<(usize, usize)> {
    let reader_file = file.reopen().unwrap();
    let mut reader = RecordReader::new(reader_file)?;

    let mut record_count = 0;
    let mut total_size = 0;

    loop {
        match reader.next_record()? {
            DiskyPiece::Record(bytes) => {
                record_count += 1;
                total_size += bytes.len();
            }
            DiskyPiece::EOF => break,
        }
    }

    Ok((record_count, total_size))
}

// The modules below are only compiled when the "parallel" feature is enabled
#[cfg(feature = "parallel")]
mod parallel_benchmarks {
    use super::*;

    use disky::parallel::multi_threaded_reader::{MultiThreadedReader, MultiThreadedReaderConfig};
    use disky::parallel::reader::{DiskyParallelPiece, ParallelReaderConfig, ShardingConfig};
    use disky::parallel::sharding::{FileShardLocator, FileSharder};
    use disky::parallel::writer::{ParallelWriter, ParallelWriterConfig};

    /// Write records to multiple shard files using the parallel writer
    pub fn write_sharded_files(
        record_count: usize,
        record_size: usize,
        shard_count: usize,
    ) -> Result<TempDir> {
        // Create a temporary directory
        let dir = tempdir().expect("Failed to create temp directory");

        // Create a FileSharder for the parallel writer
        let sharder = FileSharder::new(dir.path().to_path_buf(), "shard");

        // Create sharding config for the parallel writer
        let sharding_config =
            disky::parallel::writer::ShardingConfig::new(Box::new(sharder), shard_count);

        // Create parallel writer config
        let writer_config = ParallelWriterConfig::default();

        // Create the parallel writer
        let writer = ParallelWriter::new(sharding_config, writer_config)?;

        // Write records using the parallel writer
        for i in 0..record_count {
            let record = generate_test_record(i, record_size);
            writer.write_record(&record)?;
        }

        // Close the writer
        writer.close()?;

        Ok(dir)
    }

    /// Read all records using MultiThreadedReader
    pub fn read_with_multi_threaded_reader(
        dir: &TempDir,
        thread_count: usize,
    ) -> Result<(usize, usize)> {
        // Create a shard locator
        let locator = FileShardLocator::new(dir.path().to_path_buf(), "shard")?;

        // Create the sharding config - allow reading multiple shards concurrently
        let sharding_config = ShardingConfig::new(Box::new(locator), thread_count);

        // Create the reader config
        let reader_config = MultiThreadedReaderConfig::new(
            ParallelReaderConfig::default(),
            thread_count,
            10 * 1024 * 1024 * 1024, // 10 GB queue
        );

        // Create the multi-threaded reader
        let reader = MultiThreadedReader::new(sharding_config, reader_config)?;

        let mut record_count = 0;
        let mut total_size = 0;

        loop {
            match reader.read()? {
                DiskyParallelPiece::Record(bytes) => {
                    record_count += 1;
                    total_size += bytes.len();
                }
                DiskyParallelPiece::EOF => break,
                DiskyParallelPiece::ShardFinished => continue,
            }
        }


        Ok((record_count, total_size))
    }

    /// Benchmark a single-file vs multi-file comparison
    pub fn bench_single_vs_multi_file(c: &mut Criterion) {
        // Determine total number of records
        let total_records = RECORDS_PER_SHARD * SHARD_COUNT;

        // Set up benchmark group
        let mut group = c.benchmark_group("single_vs_multi_file");

        // Configure benchmark parameters for large data benchmarks
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(15));

        println!("Preparing benchmark data...");
        println!(
            "Total records: {}, Record size: {} bytes",
            total_records, RECORD_SIZE
        );

        println!("Writing single file with {} records...", total_records);

        // Prepare single file for standard reader
        let single_file = write_single_file(total_records, RECORD_SIZE).unwrap();

        println!(
            "Writing {} sharded files using parallel writer...",
            SHARD_COUNT
        );

        // Prepare shard directory using parallel writer
        let sharded_dir = write_sharded_files(total_records, RECORD_SIZE, SHARD_COUNT).unwrap();

        println!("Starting benchmarks...");

        // Benchmark single-threaded reader on the single large file
        group.bench_function(BenchmarkId::new("single_threaded", "single_file"), |b| {
            b.iter(|| read_with_record_reader(&single_file).unwrap())
        });

        // Benchmark multi-threaded reader with several threads
        group.bench_function(
            BenchmarkId::new("multi_threaded", format!("{}_shards", SHARD_COUNT)),
            |b| {
                b.iter(|| {
                    read_with_multi_threaded_reader(&sharded_dir, DEFAULT_THREAD_COUNT).unwrap()
                })
            },
        );

        // Finish the group to write results
        group.finish();
    }
}

// Define a dummy function for when parallel feature is disabled
#[cfg(not(feature = "parallel"))]
fn dummy_benchmark(_: &mut Criterion) {
    println!("Parallel feature is not enabled. Enable with --features parallel");
}

// Select the appropriate benchmark function based on feature flag
#[cfg(feature = "parallel")]
criterion_group!(benches, parallel_benchmarks::bench_single_vs_multi_file);

#[cfg(not(feature = "parallel"))]
criterion_group!(benches, dummy_benchmark);

criterion_main!(benches);
