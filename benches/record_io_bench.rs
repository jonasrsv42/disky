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

//! Benchmark for Disky record writing and reading using Criterion.
//!
//! This benchmark measures the performance of:
//! - Writing records of different sizes
//! - Reading records of different sizes
//! - Stream processing records

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::time::Duration;
use tempfile::NamedTempFile;

use disky::blocks::reader::{BlockReader, BlocksPiece};
use disky::chunks::chunks_parser::{ChunksParser, ChunkPiece};
use disky::reader::{DiskyPiece, RecordReader};
use disky::writer::RecordWriter;

/// Generate test records of a specific size
fn generate_test_records(num_records: usize, record_size: usize) -> Vec<Vec<u8>> {
    let mut records = Vec::with_capacity(num_records);

    for i in 0..num_records {
        // Fill with sequential bytes to make it a bit more realistic than all zeros
        let mut record = Vec::with_capacity(record_size);
        for j in 0..record_size {
            record.push(((i + j) % 256) as u8);
        }
        records.push(record);
    }

    records
}

/// Write records to a file and return the temp file
fn write_records_to_file(records: &[Vec<u8>]) -> NamedTempFile {
    // Create a temporary file
    let file = NamedTempFile::new().expect("Failed to create temp file");

    // Create a writer
    let mut writer = RecordWriter::new(file.reopen().unwrap()).unwrap();

    // Write all records
    for record in records {
        writer.write_record(record).unwrap();
    }

    // Close the writer to flush all data
    writer.close().unwrap();

    file
}

/// Read all records from a file and return the count and total size
fn read_all_records(file: &NamedTempFile) -> (usize, usize) {
    let reader_file = file.reopen().unwrap();
    let mut reader = RecordReader::new(reader_file).unwrap();
    
    let mut record_count = 0;
    let mut total_size = 0;
    
    loop {
        match reader.next_record().unwrap() {
            DiskyPiece::Record(bytes) => {
                record_count += 1;
                total_size += bytes.len();
            }
            DiskyPiece::EOF => break,
        }
    }
    
    (record_count, total_size)
}

/// Read all records from a file using the iterator API
fn read_all_records_iterator(file: &NamedTempFile) -> (usize, usize) {
    let reader_file = file.reopen().unwrap();
    let reader = RecordReader::new(reader_file).unwrap();
    
    let mut record_count = 0;
    let mut total_size = 0;
    
    for result in reader {
        let bytes = result.unwrap();
        record_count += 1;
        total_size += bytes.len();
    }
    
    (record_count, total_size)
}

/// Stream process records (manual iteration)
fn stream_process_records(file: &NamedTempFile) -> u64 {
    let reader_file = file.reopen().unwrap();
    let mut reader = RecordReader::new(reader_file).unwrap();

    let mut checksum: u64 = 0;

    loop {
        match reader.next_record().unwrap() {
            DiskyPiece::Record(bytes) => {
                // Simple operation: sum the first byte of each record
                if let Some(first_byte) = bytes.first() {
                    checksum = checksum.wrapping_add(*first_byte as u64);
                }
            }
            DiskyPiece::EOF => break,
        }
    }

    checksum
}

/// Stream process records using iterator
fn iterator_process_records(file: &NamedTempFile) -> u64 {
    let reader_file = file.reopen().unwrap();
    let reader = RecordReader::new(reader_file).unwrap();

    // Calculate a checksum using the iterator
    reader
        .filter_map(|result| result.ok())
        .filter_map(|bytes| bytes.first().copied())
        .map(|byte| byte as u64)
        .fold(0, |acc, val| acc.wrapping_add(val))
}

/// Benchmark writing records of different sizes
fn bench_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_records");
    group.sample_size(10);

    // Small records: 1000 records of 100 bytes each
    let small_records = generate_test_records(1000, 100);
    group.bench_function(BenchmarkId::new("small", "1000×100B"), |b| {
        b.iter(|| write_records_to_file(&small_records))
    });

    // Medium records: 100 records of 10 KB each
    let medium_records = generate_test_records(100, 10_000);
    group.bench_function(BenchmarkId::new("medium", "100×10KB"), |b| {
        b.iter(|| write_records_to_file(&medium_records))
    });

    // Large records: 10 records of 100 KB each
    let large_records = generate_test_records(10, 100_000);
    group.bench_function(BenchmarkId::new("large", "10×100KB"), |b| {
        b.iter(|| write_records_to_file(&large_records))
    });

    group.finish();
}

fn bench_write_audio_dataset(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_audio_dataset");
    group.measurement_time(Duration::from_secs(30)); // Increase target time to 30 seconds

    // Large writes, 100 is very slow.
    group.sample_size(10);

    // Mega record: 5 records of 1 MB each
    let audio_records = generate_test_records(2000, 2000_000);
    group.bench_function(BenchmarkId::new("audio_dataset", "2000×2MB"), |b| {
        b.iter(|| write_records_to_file(&audio_records))
    });

    group.finish();
}

/// Benchmark reading records of different sizes
fn bench_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_records");
    group.sample_size(10);

    // Small records: 1000 records of 100 bytes each
    let small_records = generate_test_records(1000, 100);
    let small_file = write_records_to_file(&small_records);
    group.bench_function(BenchmarkId::new("small", "1000×100B"), |b| {
        b.iter(|| read_all_records(&small_file))
    });

    // Medium records: 100 records of 10 KB each
    let medium_records = generate_test_records(100, 10_000);
    let medium_file = write_records_to_file(&medium_records);
    group.bench_function(BenchmarkId::new("medium", "100×10KB"), |b| {
        b.iter(|| read_all_records(&medium_file))
    });

    // Large records: 10 records of 100 KB each
    let large_records = generate_test_records(10, 100_000);
    let large_file = write_records_to_file(&large_records);
    group.bench_function(BenchmarkId::new("large", "10×100KB"), |b| {
        b.iter(|| read_all_records(&large_file))
    });

    group.finish();
}

fn bench_read_audio_dataset(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_audio_dataset");
    group.measurement_time(Duration::from_secs(30)); // Increase target time to 30 seconds

    // Large writes, 100 is very slow.
    group.sample_size(10);

    // This is realistic for reading a dataset of audio files while training,
    // typically an audio file may be ~ 2 MB and we may have a few thousand in a shard
    //
    // This should amount to about 4 GB of data.

    let audio_dataset_records = generate_test_records(2000, 2000_000);
    let audio_dataset_file = write_records_to_file(&audio_dataset_records);
    group.bench_function(BenchmarkId::new("audio_dataset", "2000x2MB"), |b| {
        b.iter(|| read_all_records(&audio_dataset_file))
    });

    group.finish();
}

/// Benchmark the iterator API with small records
fn bench_read_iterator(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_records_iterator");
    group.sample_size(10);

    // Small records: 1000 records of 100 bytes each
    let small_records = generate_test_records(1000, 100);
    let small_file = write_records_to_file(&small_records);
    group.bench_function(BenchmarkId::new("small", "1000×100B"), |b| {
        b.iter(|| read_all_records_iterator(&small_file))
    });

    // Medium records: 100 records of 10 KB each
    let medium_records = generate_test_records(100, 10_000);
    let medium_file = write_records_to_file(&medium_records);
    group.bench_function(BenchmarkId::new("medium", "100×10KB"), |b| {
        b.iter(|| read_all_records_iterator(&medium_file))
    });

    // Large records: 10 records of 100 KB each
    let large_records = generate_test_records(10, 100_000);
    let large_file = write_records_to_file(&large_records);
    group.bench_function(BenchmarkId::new("large", "10×100KB"), |b| {
        b.iter(|| read_all_records_iterator(&large_file))
    });

    group.finish();
}

/// Benchmark stream processing of records
fn bench_stream_processing(c: &mut Criterion) {
    let mut group = c.benchmark_group("stream_processing");

    group.sample_size(10);

    // Small records: 1000 records of 100 bytes each
    let small_records = generate_test_records(1000, 100);
    let small_file = write_records_to_file(&small_records);

    // Manual iteration
    group.bench_function(BenchmarkId::new("manual", "1000×100B"), |b| {
        b.iter(|| stream_process_records(&small_file))
    });

    // Iterator approach
    group.bench_function(BenchmarkId::new("iterator", "1000×100B"), |b| {
        b.iter(|| iterator_process_records(&small_file))
    });

    group.finish();
}

/// Benchmark just the BlockWriter.write_chunk method with large chunks.
/// This helps identify if the bottleneck is in the block writing process
/// rather than the higher-level record writing logic.
fn bench_block_write_chunks(c: &mut Criterion) {
    use disky::blocks::writer::BlockWriter;
    
    let mut group = c.benchmark_group("block_write_chunks");
    group.measurement_time(Duration::from_secs(30)); // Increase target time to 30 seconds
    group.sample_size(10);
    
    // Create 2MB chunk similar to audio dataset benchmark
    let chunk_size = 2_000_000;
    let chunk_data = vec![0u8; chunk_size];
    
    // Write 2000 chunks directly through BlockWriter
    group.bench_function(BenchmarkId::new("large_chunks", "2000×2MB"), |b| {
        b.iter(|| {
            // Use a temporary file to match other benchmarks
            let file = NamedTempFile::new().expect("Failed to create temp file");
            let mut writer = BlockWriter::new(file.reopen().unwrap()).unwrap();
            
            // Write 2000 chunks directly to simulate audio dataset workload
            for _ in 0..2000 {
                writer.write_chunk(&chunk_data).unwrap();
            }
            
            writer.flush().unwrap()
        })
    });
    
    group.finish();
}

/// Benchmark the BlockReader.read_chunks method with large chunks.
/// This helps identify if the bottleneck is in the low-level block reading process.
fn bench_block_read_chunks(c: &mut Criterion) {
    use disky::blocks::writer::BlockWriter;
    
    let mut group = c.benchmark_group("block_read_chunks");
    group.measurement_time(Duration::from_secs(30)); // Increase target time to 30 seconds
    group.sample_size(10);
    
    // Create 2MB chunk similar to audio dataset benchmark
    let chunk_size = 2_000_000;
    let chunk_data = vec![0u8; chunk_size];
    
    // Create a temporary file with 2000 chunks
    let file = NamedTempFile::new().expect("Failed to create temp file");
    {
        let mut writer = BlockWriter::new(file.reopen().unwrap()).unwrap();
        
        // Write 2000 chunks directly to simulate audio dataset workload
        for _ in 0..2000 {
            writer.write_chunk(&chunk_data).unwrap();
        }
        
        writer.flush().unwrap();
    }
    
    // Read chunks directly through BlockReader
    group.bench_function(BenchmarkId::new("large_chunks", "2000×2MB"), |b| {
        b.iter(|| {
            // Open a fresh reader for each iteration
            let mut reader = BlockReader::new(file.reopen().unwrap()).unwrap();
            
            // Read all chunks
            let mut chunk_count = 0;
            let mut total_size = 0;
            
            loop {
                match reader.read_chunks().unwrap() {
                    BlocksPiece::Chunks(data) => {
                        chunk_count += 1;
                        total_size += data.len();
                    }
                    BlocksPiece::EOF => break,
                }
            }
            
            (chunk_count, total_size)
        })
    });
    
    group.finish();
}

/// Benchmark the ChunksParser processing.
/// This helps identify if the bottleneck is in the chunk parsing process.
fn bench_chunks_parser(c: &mut Criterion) {
    let mut group = c.benchmark_group("chunks_parser");
    group.measurement_time(Duration::from_secs(30)); // Increase target time to 30 seconds
    group.sample_size(10);
    
    // Create a temporary file with the audio dataset
    let audio_records = generate_test_records(2000, 2000_000);
    let audio_file = write_records_to_file(&audio_records);
    
    // First, read all chunks with BlockReader to collect them
    let mut chunks_data = Vec::new();
    {
        let mut reader = BlockReader::new(audio_file.reopen().unwrap()).unwrap();
        
        loop {
            match reader.read_chunks().unwrap() {
                BlocksPiece::Chunks(data) => {
                    chunks_data.push(data);
                },
                BlocksPiece::EOF => break,
            }
        }
    }
    
    // Now benchmark just the ChunksParser on the collected chunks
    group.bench_function(BenchmarkId::new("parse_chunks", "2000×2MB"), |b| {
        b.iter(|| {
            let mut record_count = 0;
            let mut total_size = 0;
            
            // Process each chunk with ChunksParser
            for chunk_data in &chunks_data {
                let mut parser = ChunksParser::new(chunk_data.clone());
                
                // Parse all pieces in this chunk
                loop {
                    match parser.next() {
                        Ok(ChunkPiece::Record(data)) => {
                            record_count += 1;
                            total_size += data.len();
                        },
                        Ok(ChunkPiece::ChunksEnd) => break,
                        Ok(_) => {}, // Ignore other piece types
                        Err(_) => break, // Stop on error
                    }
                }
            }
            
            (record_count, total_size)
        })
    });
    
    group.finish();
}

/// Benchmark the combined BlockReader + ChunksParser processing.
/// This helps identify if the bottleneck is in the state transitions and combined overhead.
fn bench_block_reader_and_chunks_parser(c: &mut Criterion) {
    let mut group = c.benchmark_group("block_reader_and_chunks_parser");
    group.measurement_time(Duration::from_secs(30)); // Increase target time to 30 seconds
    group.sample_size(10);
    
    // Create a temporary file with the audio dataset
    let audio_records = generate_test_records(2000, 2000_000);
    let audio_file = write_records_to_file(&audio_records);
    
    // Benchmark the combination of BlockReader and ChunksParser directly
    group.bench_function(BenchmarkId::new("combined", "2000×2MB"), |b| {
        b.iter(|| {
            // Fresh reader for each iteration
            let mut reader = BlockReader::new(audio_file.reopen().unwrap()).unwrap();
            
            let mut record_count = 0;
            let mut total_size = 0;
            
            // Process all chunks from the file
            loop {
                match reader.read_chunks().unwrap() {
                    BlocksPiece::Chunks(chunk_data) => {
                        // Parse this chunk with ChunksParser
                        let mut parser = ChunksParser::new(chunk_data);
                        
                        // Process all pieces in this chunk
                        loop {
                            match parser.next() {
                                Ok(ChunkPiece::Record(data)) => {
                                    record_count += 1;
                                    total_size += data.len();
                                },
                                Ok(ChunkPiece::ChunksEnd) => break,
                                Ok(_) => {}, // Ignore other piece types
                                Err(_) => break, // Stop on error
                            }
                        }
                    },
                    BlocksPiece::EOF => break,
                }
            }
            
            (record_count, total_size)
        })
    });
    
    group.finish();
}

criterion_group!(
    benches,
    bench_write,
    bench_write_audio_dataset,
    bench_read,
    bench_read_audio_dataset,
    bench_read_iterator,
    bench_stream_processing,
    bench_block_write_chunks,
    bench_block_read_chunks,
    bench_chunks_parser,
    bench_block_reader_and_chunks_parser
);
criterion_main!(benches);

