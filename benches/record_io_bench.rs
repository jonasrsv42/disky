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

//! Benchmark for Riegeli record writing and reading.
//! 
//! This benchmark measures the performance of:
//! - Writing records of different sizes
//! - Reading records of different sizes
//! - Stream processing of records

#![feature(test)]
extern crate test;

use std::io::Cursor;

use disky::reader::{RecordReader, DiskyPiece};
use disky::writer::{RecordWriter, RecordWriterConfig};
use disky::compression::CompressionType;
use test::Bencher;

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

/// Write records to a buffer and return the buffer
fn write_records(records: &[Vec<u8>]) -> Vec<u8> {
    let buffer = Cursor::new(Vec::new());
    
    // Default config uses no compression
    let mut writer = RecordWriter::new(buffer).unwrap();
    
    for record in records {
        writer.write_record(record).unwrap();
    }
    
    writer.close().unwrap();
    
    writer.get_data().unwrap()
}

/// Read all records from a buffer
fn read_records(data: &[u8]) -> Vec<Vec<u8>> {
    let mut result = Vec::new();
    let mut reader = RecordReader::new(Cursor::new(data)).unwrap();
    
    loop {
        match reader.next_record().unwrap() {
            DiskyPiece::Record(bytes) => {
                result.push(bytes.to_vec());
            }
            DiskyPiece::EOF => break,
        }
    }
    
    result
}

// === Writing Benchmarks ===

#[bench]
fn bench_write_small_records(b: &mut Bencher) {
    let records = generate_test_records(1000, 100); // 1000 records of 100 bytes each
    
    b.iter(|| {
        write_records(&records)
    });
}

#[bench]
fn bench_write_medium_records(b: &mut Bencher) {
    let records = generate_test_records(100, 10_000); // 100 records of 10 KB each
    
    b.iter(|| {
        write_records(&records)
    });
}

#[bench]
fn bench_write_large_records(b: &mut Bencher) {
    let records = generate_test_records(10, 100_000); // 10 records of 100 KB each
    
    b.iter(|| {
        write_records(&records)
    });
}

// === Reading Benchmarks ===

#[bench]
fn bench_read_small_records(b: &mut Bencher) {
    let records = generate_test_records(1000, 100); // 1000 records of 100 bytes each
    let data = write_records(&records);
    
    b.iter(|| {
        read_records(&data)
    });
}

#[bench]
fn bench_read_medium_records(b: &mut Bencher) {
    let records = generate_test_records(100, 10_000); // 100 records of 10 KB each
    let data = write_records(&records);
    
    b.iter(|| {
        read_records(&data)
    });
}

#[bench]
fn bench_read_large_records(b: &mut Bencher) {
    let records = generate_test_records(10, 100_000); // 10 records of 100 KB each
    let data = write_records(&records);
    
    b.iter(|| {
        read_records(&data)
    });
}

// === Stream Processing Benchmarks ===

#[bench]
fn bench_stream_small_records(b: &mut Bencher) {
    let records = generate_test_records(1000, 100); // 1000 records of 100 bytes each
    let data = write_records(&records);
    
    b.iter(|| {
        // Simulate stream processing by walking through each record
        // and performing a simple operation on it
        let mut checksum: u64 = 0;
        let mut reader = RecordReader::new(Cursor::new(&data)).unwrap();
        
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
    });
}

#[bench]
fn bench_iterator_small_records(b: &mut Bencher) {
    let records = generate_test_records(1000, 100); // 1000 records of 100 bytes each
    let data = write_records(&records);
    
    b.iter(|| {
        // Using the iterator interface
        let reader = RecordReader::new(Cursor::new(&data)).unwrap();
        
        // Calculate a checksum using the iterator
        let checksum: u64 = reader
            .filter_map(|result| result.ok())
            .filter_map(|bytes| bytes.first().copied())
            .map(|byte| byte as u64)
            .fold(0, |acc, val| acc.wrapping_add(val));
        
        checksum
    });
}