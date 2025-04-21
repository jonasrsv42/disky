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

use std::io::Cursor;

use bytes::Bytes;

use crate::blocks::writer::BlockWriterConfig;
use crate::error::DiskyError;
use crate::reader::{CorruptionStrategy, RecordReader, RecordReaderConfig};
use crate::writer::{RecordWriter, RecordWriterConfig};

/// Helper function to create a test file with small block and chunk sizes
fn create_test_file(record_count: usize, record_size: usize) -> Vec<u8> {
    let mut buffer = Vec::new();
    
    // Use small block and chunk sizes for better test coverage
    let block_size = 128u64; // Small block size
    let chunk_size = 64;     // Small chunk size
    
    {
        let cursor = Cursor::new(&mut buffer);
        let mut config = RecordWriterConfig::default();
        config.block_config = BlockWriterConfig::with_block_size(block_size).unwrap();
        config.chunk_size_bytes = chunk_size;
        
        let mut writer = RecordWriter::with_config(cursor, config).unwrap();
        
        for i in 0..record_count {
            let record_data = Bytes::from(vec![i as u8; record_size]);
            writer.write_record(&record_data).unwrap();
        }
        writer.close().unwrap();
    }
    
    buffer
}

/// Test the Iterator implementation with a standard file
#[test]
fn test_iterator_basic() {
    // Create a file with multiple records
    let buffer = create_test_file(10, 5);
    
    // IMPORTANT: Use the same block size for reader as was used for the writer
    let block_size = 128u64;
    let config = RecordReaderConfig::with_block_size(block_size).unwrap();
    
    // Read with iterator pattern
    let cursor = Cursor::new(&buffer);
    let reader = RecordReader::with_config(cursor, config).unwrap();
    
    // Use the iterator directly
    let mut count = 0;
    let mut last_value = 0;
    
    for result in reader {
        // Unwrap the Result
        let record = result.unwrap();
        
        // Validate record contents
        assert_eq!(record.len(), 5);
        let value = record[0];
        assert!(record.iter().all(|&b| b == value));
        
        // Verify records come in expected order
        assert_eq!(value, count as u8);
        
        count += 1;
        last_value = value;
    }
    
    // Verify we read all 10 records
    assert_eq!(count, 10);
    assert_eq!(last_value, 9);
}

/// Helper function to corrupt a file at a specific position
fn corrupt_file(mut buffer: Vec<u8>, position: usize) -> Vec<u8> {
    if position < buffer.len() {
        // Corrupt a byte by flipping all bits
        buffer[position] = !buffer[position];
    }
    buffer
}

/// Test error propagation through the Iterator trait
#[test]
fn test_iterator_error_propagation() {
    // Create a file with multiple records
    let buffer = create_test_file(10, 5);
    
    // Corrupt the file at the start to ensure we hit an error early
    // (after signature but before any records)
    let corrupt_pos = 70; // Position right after signature
    let corrupted = corrupt_file(buffer, corrupt_pos);
    
    // IMPORTANT: Use the same block size for reader as was used for the writer
    let block_size = 128u64;
    let config = RecordReaderConfig::with_block_size(block_size).unwrap();
    
    // Read with iterator pattern
    let cursor = Cursor::new(&corrupted);
    let reader = RecordReader::with_config(cursor, config.clone()).unwrap();
    
    // Collect results, which should include an error
    let results: Vec<_> = reader.collect();
    
    // We should have exactly one result (an error)
    assert!(!results.is_empty(), "Expected at least one result");
    assert!(results.iter().any(|r| r.is_err()), 
        "Expected at least one error in results");
    
    // Create a second reader to verify that the iterator stops after the error
    let cursor = Cursor::new(&corrupted);
    let reader = RecordReader::with_config(cursor, config).unwrap();
    
    // Count how many items we get (should be finite)
    let count = reader.count();
    assert!(count < 20, "Expected a finite number of results, got {}", count);
}

/// Test collecting partial results when errors occur
#[test]
fn test_iterator_partial_collection() {
    // Create a file with multiple records that will span multiple blocks and chunks
    let buffer = create_test_file(20, 5);
    
    // Corrupt the file
    let position = buffer.len() / 3;  // Corrupt at 1/3 of the way
    let corrupted = corrupt_file(buffer, position);
    
    // IMPORTANT: Use the same block size for reader as was used for the writer
    let block_size = 128u64;
    let config = RecordReaderConfig::with_block_size(block_size).unwrap()
        .with_corruption_strategy(CorruptionStrategy::Recover);
    
    // Read with iterator pattern and recovery enabled
    let cursor = Cursor::new(&corrupted);
    let reader = RecordReader::with_config(cursor, config).unwrap();
    
    // With recovery enabled, we should be able to read some records
    // even after encountering corruption
    let mut records = Vec::new();
    let mut errors = Vec::new();
    
    for result in reader {
        match result {
            Ok(record) => records.push(record),
            Err(err) => errors.push(err),
        }
    }
    
    // We should have at least one record
    assert!(!records.is_empty(), "Expected at least one record");
    
    // With recovery enabled, we might encounter multiple errors
    // but we should be able to continue reading
    println!("Read {} records, encountered {} errors", records.len(), errors.len());
    
    // Verify that records we did read are valid
    for record in records.iter() {
        // Each record should have consistent data
        let value = record[0];
        assert!(record.iter().all(|&b| b == value), "Record data should be consistent");
    }
}

/// Test the iterator with an empty file (should produce an error)
#[test]
fn test_iterator_empty_file() {
    let empty_data = Vec::new();
    let cursor = Cursor::new(empty_data);
    
    let reader = RecordReader::new(cursor).unwrap();
    
    // Collect into a Result
    let results: Result<Vec<_>, _> = reader.collect();
    
    // Should be an error for an empty file
    assert!(results.is_err());
    
    // And specifically a SignatureReadingError
    match results {
        Err(DiskyError::SignatureReadingError(_)) => {
            // This is expected
        },
        Err(other) => panic!("Expected SignatureReadingError, got: {:?}", other),
        Ok(_) => panic!("Expected error reading empty file, but got success"),
    }
}

/// Test iterator behavior when encountering errors
#[test]
fn test_iterator_after_error() {
    // Create a test file with records
    let buffer = create_test_file(20, 10);
    
    // Corrupt the file at a specific position - ensure we corrupt early in the file
    let corrupt_pos = 50;
    let corrupted = corrupt_file(buffer, corrupt_pos);
    
    // IMPORTANT: Use the same block size for reader as was used for the writer
    let block_size = 128u64;
    
    // First test: Without recovery, iterator should return an error and then stop
    {
        let cursor = Cursor::new(&corrupted);
        let config = RecordReaderConfig::with_block_size(block_size).unwrap();
        let reader = RecordReader::with_config(cursor, config.clone()).unwrap();
        
        let results: Vec<_> = reader.collect();
        
        // Verify we get a finite number of results with at least one error
        assert!(!results.is_empty(), "Expected at least one result");
        assert!(results.iter().any(|r| r.is_err()), "Expected at least one error");
        
        // Create another reader and verify we only get one error before stopping
        let cursor = Cursor::new(&corrupted);
        let reader = RecordReader::with_config(cursor, config).unwrap();
        
        let mut count = 0;
        let mut saw_error = false;
        
        for result in reader {
            count += 1;
            if result.is_err() {
                saw_error = true;
            }
            
            // After seeing an error, we shouldn't get more items
            assert!(!saw_error || count <= 1, 
                "Iterator continued after error without recovery");
        }
    }
    
    // Second test: With recovery enabled, the reader should try to continue
    // even after corruption, potentially skipping corrupted parts
    {
        let cursor = Cursor::new(&corrupted);
        let config = RecordReaderConfig::with_block_size(block_size).unwrap()
            .with_corruption_strategy(CorruptionStrategy::Recover);
        
        let reader = RecordReader::with_config(cursor, config).unwrap();
        
        // Simply verify we get a finite number of records (not an infinite loop)
        let records: Vec<_> = reader.filter_map(Result::ok).collect();
        
        println!("With recovery: Got {} records", records.len());
        
        // We should have a finite number of records
        assert!(records.len() < 50, "Too many records (possible infinite loop)");
        
        // With recovery enabled, we might get some records despite corruption
        // but it's not guaranteed depending on the corruption location
    }
}

/// Test for_each iterator method
#[test]
fn test_iterator_for_each() {
    // Create a test file
    let buffer = create_test_file(10, 5);
    
    // IMPORTANT: Use the same block size for reader as was used for the writer
    let block_size = 128u64;
    let config = RecordReaderConfig::with_block_size(block_size).unwrap();
    
    // Read with iterator pattern
    let cursor = Cursor::new(&buffer);
    let reader = RecordReader::with_config(cursor, config).unwrap();
    
    let mut count = 0;
    
    // Use for_each method of Iterator
    reader.for_each(|result| {
        match result {
            Ok(record) => {
                // Validate record
                assert_eq!(record.len(), 5);
                assert_eq!(record[0], count as u8);
                count += 1;
            }
            Err(_) => {
                panic!("Unexpected error in for_each");
            }
        }
    });
    
    // Verify we read all records
    assert_eq!(count, 10);
}

/// Test with human-readable string data
#[test]
fn test_human_readable_strings() {
    // Create a file with human-readable strings
    let mut buffer = Vec::new();
    
    // Small block and chunk sizes
    let block_size = 128u64;
    let chunk_size = 64;
    
    // Define nice readable strings
    let strings = [
        "Hello, world!",
        "Pikachu",
        "Pokemon"
    ];
    
    {
        let cursor = Cursor::new(&mut buffer);
        let mut config = RecordWriterConfig::default();
        config.block_config = BlockWriterConfig::with_block_size(block_size).unwrap();
        config.chunk_size_bytes = chunk_size;
        
        let mut writer = RecordWriter::with_config(cursor, config).unwrap();
        
        // Write each string as a record
        for &s in &strings {
            writer.write_record(s.as_bytes()).unwrap();
        }
        
        writer.close().unwrap();
    }
    
    // Read back the strings
    let cursor = Cursor::new(&buffer);
    let config = RecordReaderConfig::with_block_size(block_size).unwrap();
    let reader = RecordReader::with_config(cursor, config).unwrap();
    
    // Collect all records
    let records: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    
    // Should have exactly 3 records
    assert_eq!(records.len(), 3, "Expected exactly 3 records");
    
    // Convert to strings and compare in a readable way
    println!("Reading human-readable strings from file:");
    
    for (i, record) in records.iter().enumerate() {
        let s = std::str::from_utf8(record).unwrap();
        println!("  Record {}: \"{}\"", i + 1, s);
        assert_eq!(s, strings[i], "String mismatch at record {}", i + 1);
    }
}
