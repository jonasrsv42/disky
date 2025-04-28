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
//! - Single-threaded RecordWriter on a single large file (10GB)
//! - MultiThreadedWriter on multiple smaller files (10 x 1GB)
//! - MultiThreadedWriter with different task queue capacity limits
//!
//! This benchmark tests how well the multi-threaded writer handles
//! parallelism across multiple shards compared to a single-threaded
//! writer on a single large file, and how different queue capacity
//! settings affect performance.

#![cfg_attr(not(feature = "parallel"), allow(dead_code, unused_imports))]

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::time::Duration;
use tempfile::{tempdir, NamedTempFile, TempDir};

use disky::error::Result;
use disky::writer::RecordWriter;

/// Constants for benchmark sizes
///
/// This benchmark compares writing a large single file vs multiple smaller files in parallel.
/// Adjust these constants to increase or decrease the benchmark size.
const RECORD_SIZE: usize = 1_000_000; // 1MB records
const RECORDS_PER_SHARD: usize = 1_000; // 1,000 records per shard = ~1GB
const SHARD_COUNT: usize = 10; // 10 shards = ~10GB total

// Number of threads to use for multi-threaded writer
const DEFAULT_THREAD_COUNT: usize = 4;

/// Generate test record data of a specific size
fn generate_test_record(record_index: usize, record_size: usize) -> Vec<u8> {
    // Fill with sequential bytes to make it a bit more realistic than all zeros
    let mut record = Vec::with_capacity(record_size);
    for j in 0..record_size {
        record.push(((record_index + j) % 256) as u8);
    }
    record
}

/// Write records to a single file and return the file
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

// The modules below are only compiled when the "parallel" feature is enabled
#[cfg(feature = "parallel")]
mod parallel_benchmarks {
    use super::*;
    use bytes::Bytes;

    use disky::parallel::multi_threaded_writer::{MultiThreadedWriter, MultiThreadedWriterConfig};
    use disky::parallel::sharding::FileSharder;
    use disky::parallel::writer::{ParallelWriterConfig, ShardingConfig};

    /// Write records to multiple shard files using the multi-threaded writer
    pub fn write_with_multi_threaded_writer(
        record_count: usize,
        record_size: usize,
        shard_count: usize,
        thread_count: usize,
        queue_capacity: Option<usize>,
    ) -> Result<TempDir> {
        // Create a temporary directory
        let dir = tempdir().expect("Failed to create temp directory");

        // Create a FileSharder for the multi-threaded writer
        let sharder = FileSharder::new(dir.path().to_path_buf(), "shard");

        // Create sharding config for the multi-threaded writer
        let sharding_config = ShardingConfig::new(Box::new(sharder), shard_count);

        // Create multi-threaded writer config with specified thread count and queue capacity
        let mut writer_config = ParallelWriterConfig::default();
        if let Some(capacity) = queue_capacity {
            writer_config = writer_config.with_task_queue_capacity(capacity);
        }

        let config = MultiThreadedWriterConfig::new(writer_config, thread_count);

        // Create the multi-threaded writer
        let writer = MultiThreadedWriter::new(sharding_config, config)?;

        // Write records using the multi-threaded writer
        for i in 0..record_count {
            let record = generate_test_record(i, record_size);
            let bytes = Bytes::from(record);
            writer.write_record(bytes)?;
        }

        // Close the writer
        writer.close()?;

        Ok(dir)
    }

    /// Benchmark single-threaded writer vs multi-threaded writer with various configurations
    pub fn bench_writer_performance(c: &mut Criterion) {
        // Determine total number of records
        let total_records = RECORDS_PER_SHARD * SHARD_COUNT;

        //let _ = env_logger::builder()
        //    .filter_level(log::LevelFilter::Debug)
        //    .is_test(true)
        //    .try_init();

        // Set up benchmark group
        let mut group = c.benchmark_group("writer_performance");

        // Configure benchmark parameters for large data benchmarks
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(15));

        println!("Preparing benchmark data...");
        println!(
            "Total records: {}, Record size: {} bytes",
            total_records, RECORD_SIZE
        );

        // Benchmark single-threaded writer
        //group.bench_function(BenchmarkId::new("single_threaded", "single_file"), |b| {
        //    b.iter(|| write_single_file(total_records, RECORD_SIZE).unwrap())
        //});

        // Benchmark multi-threaded writer with default settings (no queue capacity limit)
        group.bench_function(
            BenchmarkId::new(
                "multi_threaded",
                format!("{}_threads_unlimited_queue", DEFAULT_THREAD_COUNT),
            ),
            |b| {
                b.iter(|| {
                    write_with_multi_threaded_writer(
                        total_records,
                        RECORD_SIZE,
                        SHARD_COUNT,
                        DEFAULT_THREAD_COUNT,
                        None, // No queue capacity limit
                    )
                    .unwrap()
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
criterion_group!(benches, parallel_benchmarks::bench_writer_performance);

#[cfg(not(feature = "parallel"))]
criterion_group!(benches, dummy_benchmark);

criterion_main!(benches);

