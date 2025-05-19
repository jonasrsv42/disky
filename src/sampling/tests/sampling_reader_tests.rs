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

use std::collections::HashMap;

use bytes::Bytes;

use crate::error::Result;
use crate::reader::DiskyPiece;
use crate::sampling::SamplingReader;
use crate::sampling::SamplingReaderConfig;

// A simple iterator that produces a numbered sequence of bytes
struct SequenceIterator {
    prefix: &'static str,
    count: usize,
    current: usize,
}

impl SequenceIterator {
    fn new(prefix: &'static str, count: usize) -> Self {
        Self {
            prefix,
            count,
            current: 0,
        }
    }
}

impl Iterator for SequenceIterator {
    type Item = Result<Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.count {
            None
        } else {
            let data = format!("{}{}", self.prefix, self.current);
            self.current += 1;
            Some(Ok(Bytes::from(data)))
        }
    }
}

#[test]
fn test_sampling_reader_basic() {
    // Create three iterators with different lengths
    let iter_a = SequenceIterator::new("A", 10);
    let iter_b = SequenceIterator::new("B", 5);
    let iter_c = SequenceIterator::new("C", 15);
    
    // Create a sampling reader with equal weights
    let sources = vec![(1.0, iter_a), (1.0, iter_b), (1.0, iter_c)];
    let mut reader = SamplingReader::new(sources).unwrap();
    
    // Read all records
    let mut records = Vec::new();
    loop {
        match reader.read().unwrap() {
            DiskyPiece::Record(bytes) => {
                records.push(String::from_utf8_lossy(&bytes).to_string());
            },
            DiskyPiece::EOF => break,
        }
    }
    
    // We should have read all 30 records
    assert_eq!(records.len(), 30);
    
    // We should have 10 A records
    assert_eq!(records.iter().filter(|s| s.starts_with("A")).count(), 10);
    
    // We should have 5 B records
    assert_eq!(records.iter().filter(|s| s.starts_with("B")).count(), 5);
    
    // We should have 15 C records
    assert_eq!(records.iter().filter(|s| s.starts_with("C")).count(), 15);
}

#[test]
fn test_sampling_reader_weighted() {
    // Create a deterministic test with known seed
    let config = SamplingReaderConfig::with_seed(12345);
    
    // Create three iterators, all with the same length
    let iter_a = SequenceIterator::new("A", 100);
    let iter_b = SequenceIterator::new("B", 100);
    let iter_c = SequenceIterator::new("C", 100);
    
    // Create a sampling reader with different weights
    // A has weight 1, B has weight 2, C has weight 7
    let sources = vec![(1.0, iter_a), (2.0, iter_b), (7.0, iter_c)];
    let mut reader = SamplingReader::with_config(sources, config).unwrap();
    
    // Read 100 records
    let mut records = Vec::new();
    for _ in 0..100 {
        match reader.read().unwrap() {
            DiskyPiece::Record(bytes) => {
                records.push(String::from_utf8_lossy(&bytes).to_string());
            },
            DiskyPiece::EOF => break,
        }
    }
    
    // Count occurrences of each prefix
    let mut counts = HashMap::new();
    for record in &records {
        let prefix = &record[0..1];
        *counts.entry(prefix.to_string()).or_insert(0) += 1;
    }
    
    // With the weights (1,2,7), we expect roughly:
    // A: 10% (1/10)
    // B: 20% (2/10)
    // C: 70% (7/10)
    // But allow for some randomness with the fixed seed
    let a_count = counts.get("A").unwrap_or(&0);
    let b_count = counts.get("B").unwrap_or(&0);
    let c_count = counts.get("C").unwrap_or(&0);
    
    println!("Distribution with weights [1,2,7]: A={}%, B={}%, C={}%", 
             a_count, b_count, c_count);
             
    // Verify that C occurs more frequently than B, and B more than A
    assert!(c_count > b_count);
    assert!(b_count > a_count);
}

#[test]
fn test_sampling_reader_exhaustion() {
    // Create three iterators with small lengths
    let iter_a = SequenceIterator::new("A", 2);
    let iter_b = SequenceIterator::new("B", 3);
    let iter_c = SequenceIterator::new("C", 1);
    
    // Create a sampling reader
    let sources = vec![(1.0, iter_a), (1.0, iter_b), (1.0, iter_c)];
    let mut reader = SamplingReader::new(sources).unwrap();
    
    // Read all records
    let mut records = Vec::new();
    loop {
        match reader.read().unwrap() {
            DiskyPiece::Record(bytes) => {
                records.push(String::from_utf8_lossy(&bytes).to_string());
            },
            DiskyPiece::EOF => break,
        }
    }
    
    // We should have read all 6 records
    assert_eq!(records.len(), 6);
    
    // We should have 2 A records
    assert_eq!(records.iter().filter(|s| s.starts_with("A")).count(), 2);
    
    // We should have 3 B records
    assert_eq!(records.iter().filter(|s| s.starts_with("B")).count(), 3);
    
    // We should have 1 C record
    assert_eq!(records.iter().filter(|s| s.starts_with("C")).count(), 1);
    
    // Verify the reader is actually exhausted by calling read again
    match reader.read().unwrap() {
        DiskyPiece::EOF => {},
        _ => panic!("Expected EOF but got a record"),
    }
    
    // Verify active_sources is 0
    assert_eq!(reader.active_sources(), 0);
    assert_eq!(reader.total_sources(), 3);
}

#[test]
fn test_sampling_reader_as_iterator() {
    // Create a sampling reader using the Iterator trait
    let iter_a = SequenceIterator::new("A", 5);
    let iter_b = SequenceIterator::new("B", 7);
    
    let sources = vec![(1.0, iter_a), (1.0, iter_b)];
    let reader = SamplingReader::new(sources).unwrap();
    
    // Collect all bytes using the Iterator trait
    let bytes: Vec<Bytes> = reader.map(Result::unwrap).collect();
    
    // We should have 12 bytes in total
    assert_eq!(bytes.len(), 12);
    
    // Convert to strings for easier inspection
    let strings: Vec<String> = bytes.iter()
        .map(|b| String::from_utf8_lossy(b).to_string())
        .collect();
    
    // We should have 5 A records
    assert_eq!(strings.iter().filter(|s| s.starts_with("A")).count(), 5);
    
    // We should have 7 B records
    assert_eq!(strings.iter().filter(|s| s.starts_with("B")).count(), 7);
}

#[test]
#[should_panic(expected = "Non-positive weights are not allowed")]
fn test_sampling_reader_negative_weight() {
    // Create iterators with a negative weight
    let iter_a = SequenceIterator::new("A", 5);
    let iter_b = SequenceIterator::new("B", 7);
    
    let sources = vec![(1.0, iter_a), (-1.0, iter_b)];
    let _reader = SamplingReader::new(sources).unwrap(); // Should panic
}

#[test]
#[should_panic(expected = "Non-positive weights are not allowed")]
fn test_sampling_reader_zero_weight() {
    // Create iterators with a zero weight
    let iter_a = SequenceIterator::new("A", 5);
    let iter_b = SequenceIterator::new("B", 7);
    
    let sources = vec![(1.0, iter_a), (0.0, iter_b)];
    let _reader = SamplingReader::new(sources).unwrap(); // Should panic
}

#[test]
fn test_sampling_reader_with_record_readers() {
    use std::io::Cursor;
    use crate::reader::RecordReader;
    use crate::writer::RecordWriter;
    
    // Create first data buffer with 5 records containing "A" values
    let mut buffer_a = Vec::new();
    {
        let cursor = Cursor::new(&mut buffer_a);
        let mut writer = RecordWriter::new(cursor).unwrap();
        
        for i in 0..5 {
            let record_data = Bytes::from(format!("A{}", i));
            writer.write_record(&record_data).unwrap();
        }
        writer.close().unwrap();
    }
    
    // Create second data buffer with 3 records containing "B" values
    let mut buffer_b = Vec::new();
    {
        let cursor = Cursor::new(&mut buffer_b);
        let mut writer = RecordWriter::new(cursor).unwrap();
        
        for i in 0..3 {
            let record_data = Bytes::from(format!("B{}", i));
            writer.write_record(&record_data).unwrap();
        }
        writer.close().unwrap();
    }
    
    // Create two RecordReaders
    let reader_a = RecordReader::new(Cursor::new(&buffer_a)).unwrap();
    let reader_b = RecordReader::new(Cursor::new(&buffer_b)).unwrap();
    
    // Create a SamplingReader with both RecordReaders
    // Use a fixed seed for deterministic testing
    let config = SamplingReaderConfig::with_seed(42);
    let sources = vec![(1.0, reader_a), (1.0, reader_b)];
    let sampling_reader = SamplingReader::with_config(sources, config).unwrap();
    
    // Read all records using the Iterator trait
    let results: Vec<String> = sampling_reader
        .map(|result| {
            let bytes = result.unwrap();
            String::from_utf8_lossy(&bytes).to_string()
        })
        .collect();
    
    // Verify we have all 8 records
    assert_eq!(results.len(), 8);
    
    // Verify we have 5 "A" records
    assert_eq!(results.iter().filter(|s| s.starts_with("A")).count(), 5);
    
    // Verify we have 3 "B" records
    assert_eq!(results.iter().filter(|s| s.starts_with("B")).count(), 3);
    
    // Verify that all "A" records are included
    for i in 0..5 {
        let expected = format!("A{}", i);
        assert!(results.contains(&expected));
    }
    
    // Verify that all "B" records are included
    for i in 0..3 {
        let expected = format!("B{}", i);
        assert!(results.contains(&expected));
    }
}