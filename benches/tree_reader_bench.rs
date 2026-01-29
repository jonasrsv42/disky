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
//! - Single RecordReader on one large file
//! - RoundRobinShardReader on multiple files (shard-based)
//! - Tree-based RoundRobinNode with multiple RecordReaderConfig nodes
//!
//! This benchmark tests the overhead of different reader composition strategies.

use std::fs::File;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use tempfile::{NamedTempFile, TempDir, tempdir};

use disky::error::Result;
use disky::reader::{DiskyPiece, RecordReaderConfig};
use disky::shard::reader::RoundRobinShardReaderConfig;
use disky::shard::source::{FileShards, SequentialShardSource};
use disky::tree::reader::{Node, interleave};
use disky::writer::RecordWriterConfig;

/// Benchmark configuration
const RECORD_SIZE: usize = 1_000; // 1KB records
const RECORDS_PER_FILE: usize = 10_000; // 10K records per file
const FILE_COUNT: usize = 10; // 10 files

/// Generate test record data
fn generate_record(index: usize, size: usize) -> Vec<u8> {
    let mut record = Vec::with_capacity(size);
    for j in 0..size {
        record.push(((index + j) % 256) as u8);
    }
    record
}

/// Write a single file with records
fn write_single_file(record_count: usize, record_size: usize) -> Result<NamedTempFile> {
    let file = NamedTempFile::new().expect("Failed to create temp file");
    let mut writer = RecordWriterConfig::new(file.reopen().unwrap()).build()?;

    for i in 0..record_count {
        let record = generate_record(i, record_size);
        writer.write_record(&record)?;
    }
    writer.close()?;

    Ok(file)
}

/// Write multiple files (for shard/tree comparison)
fn write_multiple_files(
    records_per_file: usize,
    record_size: usize,
    file_count: usize,
) -> Result<TempDir> {
    let dir = tempdir().expect("Failed to create temp directory");

    for shard_idx in 0..file_count {
        let path = dir.path().join(format!("shard-{:05}.disky", shard_idx));
        let file = File::create(&path).expect("Failed to create shard file");
        let mut writer = RecordWriterConfig::new(file).build()?;

        for i in 0..records_per_file {
            let record = generate_record(shard_idx * records_per_file + i, record_size);
            writer.write_record(&record)?;
        }
        writer.close()?;
    }

    Ok(dir)
}

/// Read using a single RecordReader
fn read_single_reader(file: &NamedTempFile) -> Result<(usize, usize)> {
    let mut reader = RecordReaderConfig::new(file.reopen().unwrap()).build()?;

    let mut count = 0;
    let mut total_size = 0;

    loop {
        match reader.next_record()? {
            DiskyPiece::Record(bytes) => {
                count += 1;
                total_size += bytes.len();
            }
            DiskyPiece::EOF => break,
        }
    }

    Ok((count, total_size))
}

/// Read using RoundRobinShardReader (shard-based)
fn read_shard_reader(dir: &TempDir, max_active: usize) -> Result<(usize, usize)> {
    let file_shards = FileShards::from_pattern(dir.path().to_path_buf(), "shard")?;
    let source = SequentialShardSource::new(file_shards);
    let reader = RoundRobinShardReaderConfig::new(source)
        .max_active(max_active)
        .build()?;

    let mut count = 0;
    let mut total_size = 0;

    for result in reader {
        let bytes = result?;
        count += 1;
        total_size += bytes.len();
    }

    Ok((count, total_size))
}

/// Read using tree-based RoundRobinNode with RecordReaderConfig nodes
fn read_tree_reader(dir: &TempDir) -> Result<(usize, usize)> {
    // Collect all shard files
    let file_shards = FileShards::from_pattern(dir.path().to_path_buf(), "shard")?;
    let source = SequentialShardSource::new(file_shards);

    // Build tree nodes - one RecordReaderConfig per file
    let nodes: Vec<Box<dyn Node>> = source
        .map(|shard_result| {
            let shard = shard_result.unwrap();
            Box::new(RecordReaderConfig::new(shard.source)) as Box<dyn Node>
        })
        .collect();

    // Create interleaved tree and build
    let tree = interleave(nodes);
    let reader = tree.make()?;

    let mut count = 0;
    let mut total_size = 0;

    for result in reader {
        let bytes = result?;
        count += 1;
        total_size += bytes.len();
    }

    Ok((count, total_size))
}

fn bench_reader_comparison(c: &mut Criterion) {
    let total_records = RECORDS_PER_FILE * FILE_COUNT;

    let mut group = c.benchmark_group("reader_comparison");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    println!("Preparing benchmark data...");
    println!(
        "Total records: {}, Record size: {} bytes, Files: {}",
        total_records, RECORD_SIZE, FILE_COUNT
    );

    // Prepare single large file
    println!("Writing single file with {} records...", total_records);
    let single_file = write_single_file(total_records, RECORD_SIZE).unwrap();

    // Prepare multiple files
    println!(
        "Writing {} files with {} records each...",
        FILE_COUNT, RECORDS_PER_FILE
    );
    let multi_dir = write_multiple_files(RECORDS_PER_FILE, RECORD_SIZE, FILE_COUNT).unwrap();

    println!("Starting benchmarks...");

    // Single RecordReader benchmark
    group.bench_function(BenchmarkId::new("single_reader", "1_file"), |b| {
        b.iter(|| read_single_reader(&single_file).unwrap())
    });

    // Shard reader with max_active=1 (sequential)
    group.bench_function(
        BenchmarkId::new("shard_reader_sequential", format!("{}_files", FILE_COUNT)),
        |b| b.iter(|| read_shard_reader(&multi_dir, 1).unwrap()),
    );

    // Shard reader with max_active=all (round-robin)
    group.bench_function(
        BenchmarkId::new("shard_reader_interleaved", format!("{}_files", FILE_COUNT)),
        |b| b.iter(|| read_shard_reader(&multi_dir, FILE_COUNT).unwrap()),
    );

    // Tree reader (round-robin via tree nodes)
    group.bench_function(
        BenchmarkId::new("tree_reader_interleaved", format!("{}_files", FILE_COUNT)),
        |b| b.iter(|| read_tree_reader(&multi_dir).unwrap()),
    );

    group.finish();
}

criterion_group!(benches, bench_reader_comparison);
criterion_main!(benches);
