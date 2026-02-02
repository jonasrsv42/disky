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
//! - SequentialShardReader on multiple smaller files (10 x 1GB)
//! - Tree-based reader with ThreadedNodeConfig per shard (10 x 1GB)
//!
//! This benchmark tests how well the multi-threaded reader and the
//! sequential shard reader handle reading across multiple shards
//! compared to a single-threaded reader on a single large file.
//!
//! The tree-based benchmark compares the composable tree API (where each
//! shard gets its own dedicated thread via ThreadedNodeConfig) against
//! the purpose-built MultiThreadedReader.

#![cfg_attr(not(feature = "parallel"), allow(dead_code, unused_imports))]

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use std::time::Duration;
use tempfile::{NamedTempFile, TempDir, tempdir};

use disky::error::Result;
use disky::reader::{DiskyPiece, RecordReaderConfig};
use disky::writer::RecordWriterConfig;

/// Constants for benchmark sizes
///
/// This benchmark compares reading a large single file vs multiple smaller files in parallel.
/// Adjust these constants to increase or decrease the benchmark size.
const RECORD_SIZE: usize = 1_000_000; // 1MB records
const RECORDS_PER_SHARD: usize = 1_000; // 1,000 records per shard = ~1GB
const SHARD_COUNT: usize = 10; // 10 shards = ~10GB total

// Number of threads to use for multi-threaded reader and writer
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

/// Write records to a single file and return the temp file
fn write_single_file(record_count: usize, record_size: usize) -> Result<NamedTempFile> {
    // Create a temporary file
    let file = NamedTempFile::new().expect("Failed to create temp file");

    // Create a writer
    let mut writer = RecordWriterConfig::new(file.reopen().unwrap()).build()?;

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
    let mut reader = RecordReaderConfig::new(reader_file).build()?;

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

    use disky::parallel::multi_threaded_reader::MultiThreadedReaderConfig;
    use disky::parallel::node::ThreadedNodeConfig;
    use disky::parallel::reader::{DiskyParallelPiece, ShardingConfig};
    use disky::parallel::writer::{ParallelWriter, ParallelWriterConfig};
    use disky::reader::RecordReaderConfig;
    use disky::shard::reader::SequentialShardReaderConfig;
    use disky::shard::sink::FileShardsBuilder;
    use disky::shard::source::{FileShards, SequentialShardSource};
    use disky::tree::reader::RoundRobinNode;

    /// Write records to multiple shard files using the parallel writer
    pub fn write_sharded_files(
        record_count: usize,
        record_size: usize,
        shard_count: usize,
    ) -> Result<TempDir> {
        // Create a temporary directory
        let dir = tempdir().expect("Failed to create temp directory");

        // Create a shard factory for the parallel writer
        let sharder = FileShardsBuilder::new(dir.path(), "shard").build().unwrap();

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

    /// Build a MultiThreadedReader (setup only, for benchmarking)
    pub fn build_multi_threaded_reader(
        dir: &TempDir,
        thread_count: usize,
    ) -> Result<disky::parallel::multi_threaded_reader::MultiThreadedReader> {
        let file_shards = FileShards::from_pattern(dir.path().to_path_buf(), "shard")?;
        let source = SequentialShardSource::new(file_shards);
        let sharding_config = ShardingConfig::new(Box::new(source), thread_count);

        MultiThreadedReaderConfig::new(sharding_config)
            .with_worker_threads(thread_count)
            .with_queue_size_bytes(10 * 1024 * 1024 * 1024) // 10 GB queue
            .build()
    }

    /// Iterate a MultiThreadedReader (read only, for benchmarking)
    pub fn iterate_multi_threaded_reader(
        reader: disky::parallel::multi_threaded_reader::MultiThreadedReader,
    ) -> Result<(usize, usize)> {
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

    /// Build a SequentialShardReader as a boxed iterator (setup only)
    pub fn build_sequential_shard_reader(
        dir: &TempDir,
    ) -> Result<Box<dyn Iterator<Item = Result<bytes::Bytes>>>> {
        let file_shards = FileShards::from_pattern(dir.path().to_path_buf(), "shard")?;
        Ok(Box::new(
            SequentialShardReaderConfig::new(SequentialShardSource::new(file_shards)).build(),
        ))
    }

    /// Iterate any boxed iterator (read only)
    pub fn iterate_boxed_reader(
        reader: Box<dyn Iterator<Item = Result<bytes::Bytes>>>,
    ) -> Result<(usize, usize)> {
        let mut record_count = 0;
        let mut total_size = 0;

        for result in reader {
            let bytes = result?;
            record_count += 1;
            total_size += bytes.len();
        }

        Ok((record_count, total_size))
    }

    /// Build a tree-based reader with one ThreadedNodeConfig per shard (setup only).
    ///
    /// Architecture:
    /// ```text
    ///     RoundRobinNode
    ///        /    |    \
    ///   Thread  Thread  Thread  ...
    ///      |      |       |
    ///   Reader  Reader  Reader  (one per shard file)
    /// ```
    pub fn build_tree_threaded_per_shard(
        dir: &TempDir,
        buffer_size: usize,
    ) -> Result<disky::tree::reader::Reader> {
        use disky::shard::source::Shards;

        let file_shards = FileShards::from_pattern(dir.path().to_path_buf(), "shard")?;
        let shard_count = file_shards.count();

        let mut tree = RoundRobinNode::new();

        for i in 0..shard_count {
            let shard = file_shards.open(i)?;
            // Open the file directly from the shard id (path)
            let file = std::fs::File::open(&shard.id)?;
            let threaded_reader = ThreadedNodeConfig::new(RecordReaderConfig::new(file))
                .with_buffer_size(buffer_size);
            tree = tree.append(threaded_reader);
        }

        tree.build()
    }

    /// Iterate any tree Reader (read only)
    pub fn iterate_tree_reader(reader: disky::tree::reader::Reader) -> Result<(usize, usize)> {
        let mut record_count = 0;
        let mut total_size = 0;

        for result in reader {
            let bytes = result?;
            record_count += 1;
            total_size += bytes.len();
        }

        Ok((record_count, total_size))
    }

    /// Build a tree-based reader with fewer threads, each reading multiple shards (setup only).
    ///
    /// Architecture:
    /// ```text
    ///        RoundRobinNode
    ///        /      |      \
    ///   Thread   Thread   Thread   (num_threads total)
    ///      |        |        |
    ///   SeqShard  SeqShard  SeqShard  (each reads ~N/num_threads shards)
    /// ```
    pub fn build_tree_grouped_shards(
        dir: &TempDir,
        num_threads: usize,
        buffer_size: usize,
    ) -> Result<disky::tree::reader::Reader> {
        use disky::shard::source::Shards;

        let file_shards = FileShards::from_pattern(dir.path().to_path_buf(), "shard")?;
        let shard_count = file_shards.count();
        let shards_per_thread = (shard_count + num_threads - 1) / num_threads;

        let mut tree = RoundRobinNode::new();

        for thread_idx in 0..num_threads {
            let start = thread_idx * shards_per_thread;
            let end = ((thread_idx + 1) * shards_per_thread).min(shard_count);

            if start >= shard_count {
                break;
            }

            let mut paths = Vec::new();
            for i in start..end {
                let shard = file_shards.open(i)?;
                paths.push(std::path::PathBuf::from(&shard.id));
            }

            let subset_shards = FileShards::new(paths)?;
            let shard_reader =
                SequentialShardReaderConfig::new(SequentialShardSource::new(subset_shards));

            let threaded_reader =
                ThreadedNodeConfig::new(shard_reader).with_buffer_size(buffer_size);
            tree = tree.append(threaded_reader);
        }

        tree.build()
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
        // Setup (thread spawning, etc.) is NOT measured - only iteration
        group.bench_function(
            BenchmarkId::new("multi_threaded", format!("{}_shards", SHARD_COUNT)),
            |b| {
                b.iter_batched(
                    || build_multi_threaded_reader(&sharded_dir, DEFAULT_THREAD_COUNT).unwrap(),
                    |reader| iterate_multi_threaded_reader(reader).unwrap(),
                    BatchSize::PerIteration,
                )
            },
        );

        // Benchmark sequential shard reader (single-threaded, reads across shards)
        group.bench_function(
            BenchmarkId::new("sequential_shard", format!("{}_shards", SHARD_COUNT)),
            |b| {
                b.iter_batched(
                    || build_sequential_shard_reader(&sharded_dir).unwrap(),
                    |reader| iterate_boxed_reader(reader).unwrap(),
                    BatchSize::PerIteration,
                )
            },
        );

        // Benchmark tree-based reader with one thread per shard
        group.bench_function(
            BenchmarkId::new("tree_10_threads", format!("{}_shards", SHARD_COUNT)),
            |b| {
                b.iter_batched(
                    || build_tree_threaded_per_shard(&sharded_dir, 64).unwrap(),
                    |reader| iterate_tree_reader(reader).unwrap(),
                    BatchSize::PerIteration,
                )
            },
        );

        // Benchmark tree-based reader with 4 threads, each reading ~2-3 shards sequentially
        group.bench_function(
            BenchmarkId::new("tree_4_threads", format!("{}_shards", SHARD_COUNT)),
            |b| {
                b.iter_batched(
                    || build_tree_grouped_shards(&sharded_dir, 4, 64).unwrap(),
                    |reader| iterate_tree_reader(reader).unwrap(),
                    BatchSize::PerIteration,
                )
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
