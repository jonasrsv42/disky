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

//! Integration tests for the record writer/reader implementation.
//! 
//! These tests verify that data written with the writer can be correctly
//! read back with the reader.

use std::fs::File;
use std::path::Path;

use tempfile::NamedTempFile;

use disky::reader::RecordReader;
use disky::writer::{RecordWriter, RecordWriterConfig};
use disky::error::Result;

/// Helper function to write records to a temp file and return the file.
fn write_records_to_file<P>(records: &[P], config: Option<RecordWriterConfig>) -> Result<NamedTempFile> 
where 
    P: AsRef<[u8]>
{
    // Create a temporary file
    let file = NamedTempFile::new().expect("Failed to create temp file");
    
    // Create a writer with the provided config or default
    let mut writer = match config {
        Some(cfg) => RecordWriter::with_config(file.reopen()?, cfg)?,
        None => RecordWriter::new(file.reopen()?)?,
    };
    
    // Write all records
    for record in records {
        writer.write_record(record.as_ref())?;
    }
    
    // Close the writer to flush all data
    writer.close()?;
    
    Ok(file)
}

/// Helper function to read all records from a file
fn read_all_records<P: AsRef<Path>>(path: P) -> Result<Vec<Vec<u8>>> {
    // Open the file for reading
    let file = File::open(path)?;
    
    // Create a reader
    let reader = RecordReader::new(file)?;
    
    // Collect all records
    let records: Result<Vec<_>> = reader
        .map(|r| r.map(|bytes| bytes.to_vec()))
        .collect();
    
    records
}

/// Test that writing and reading a sequence of records works correctly.
#[test]
fn test_write_read_round_trip() -> Result<()> {
    // Create the test records
    let test_records = vec![
        b"record 1".to_vec(),
        b"record 2 with more data".to_vec(),
        b"record 3 with even more data for testing".to_vec(),
        b"record 4".to_vec(),
        b"".to_vec(), // Empty record - should be supported
    ];
    
    // Write records to a temporary file
    let temp_file = write_records_to_file(&test_records, None)?;
    
    // Read back records
    let read_records = read_all_records(temp_file.path())?;
    
    // Validate the number of records
    assert_eq!(read_records.len(), test_records.len(), 
        "Number of records read doesn't match number written");
    
    // Validate each record's content
    for (i, (written, read)) in test_records.iter().zip(read_records.iter()).enumerate() {
        assert_eq!(written, read, 
            "Content mismatch for record {}: expected {:?}, got {:?}", i, written, read);
    }
    
    Ok(())
}

/// Test that writing and reading using the RecordReader's iterator interface works correctly.
#[test]
fn test_write_read_iterator() -> Result<()> {
    // Create test records
    let test_records = vec![
        b"iterator test 1".to_vec(),
        b"iterator test 2".to_vec(),
        b"iterator test 3".to_vec(),
    ];
    
    // Write records to a temporary file
    let temp_file = write_records_to_file(&test_records, None)?;
    
    // Read back records using the iterator approach
    let read_records = read_all_records(temp_file.path())?;
    
    // Validate the number of records
    assert_eq!(read_records.len(), test_records.len(), 
        "Number of records read doesn't match number written");
    
    // Validate each record's content
    for (i, (written, read)) in test_records.iter().zip(read_records.iter()).enumerate() {
        assert_eq!(written, read, 
            "Content mismatch for record {}: expected {:?}, got {:?}", i, written, read);
    }
    
    Ok(())
}

/// Test writing many small records to ensure proper chunking.
#[test]
fn test_many_small_records() -> Result<()> {
    // Create many small records
    let test_records: Vec<Vec<u8>> = (0..1000)
        .map(|i| format!("small record {}", i).into_bytes())
        .collect();
    
    // Create a writer with a small chunk size to force multiple chunks
    let config = RecordWriterConfig {
        chunk_size_bytes: 5000, // Small chunk size to force multiple chunks
        ..Default::default()
    };
    
    // Write records to a temporary file
    let temp_file = write_records_to_file(&test_records, Some(config))?;
    
    // Read back records
    let read_records = read_all_records(temp_file.path())?;
    
    // Validate the number of records
    assert_eq!(read_records.len(), test_records.len(), 
        "Number of records read doesn't match number written");
    
    // Validate each record's content
    for (i, (written, read)) in test_records.iter().zip(read_records.iter()).enumerate() {
        assert_eq!(written, read, 
            "Content mismatch for record {}: expected {:?}, got {:?}", i, written, read);
    }
    
    Ok(())
}

/// Test writing and handling empty records.
#[test]
fn test_empty_records() -> Result<()> {
    // Test with only empty records
    {
        // Create a set of empty records
        let empty_records: Vec<Vec<u8>> = (0..5).map(|_| Vec::new()).collect();
        
        // Write records to a temporary file
        let temp_file = write_records_to_file(&empty_records, None)?;
        
        // Read back records
        let read_records = read_all_records(temp_file.path())?;
        
        // We should have 5 empty records
        assert_eq!(read_records.len(), 5, "Expected 5 empty records");
        
        // All records should be empty
        for (i, record) in read_records.iter().enumerate() {
            assert_eq!(record.len(), 0, "Record {} should be empty", i);
        }
    }
    
    // Test with mixed empty and non-empty records
    {
        // Create test records with mix of empty and non-empty
        let test_records: Vec<Vec<u8>> = vec![
            b"first".to_vec(),
            Vec::new(),
            b"middle".to_vec(),
            Vec::new(),
            Vec::new(),
            b"last".to_vec(),
        ];
        
        // Write records to a temporary file
        let temp_file = write_records_to_file(&test_records, None)?;
        
        // Read back records
        let read_records = read_all_records(temp_file.path())?;
        
        // Verify we got all records
        assert_eq!(read_records.len(), test_records.len(), 
            "Number of records read doesn't match number written");
        
        // Verify each record's content
        for (i, (written, read)) in test_records.iter().zip(read_records.iter()).enumerate() {
            assert_eq!(written, read, 
                "Content mismatch for record {}: expected {:?}, got {:?}", i, written, read);
        }
    }
    
    Ok(())
}

/// Test writing a few very large records to ensure proper block handling.
#[test]
fn test_large_records() -> Result<()> {
    // Create a few large records
    let test_records = vec![
        vec![b'X'; 100_000],  // 100 KB
        vec![b'Y'; 200_000],  // 200 KB
        vec![b'Z'; 150_000],  // 150 KB
    ];
    
    // Create a writer with a small block size
    let config = RecordWriterConfig {
        block_config: disky::blocks::writer::BlockWriterConfig::with_block_size(64 * 1024)?, // 64 KB blocks
        ..Default::default()
    };
    
    // Write records to a temporary file
    let temp_file = write_records_to_file(&test_records, Some(config))?;
    
    // Read back records
    let read_records = read_all_records(temp_file.path())?;
    
    // Validate the number of records
    assert_eq!(read_records.len(), test_records.len(), 
        "Number of records read doesn't match number written");
    
    // Validate each record's content
    for (i, (written, read)) in test_records.iter().zip(read_records.iter()).enumerate() {
        assert_eq!(written, read, 
            "Content mismatch for record {}: expected len {}, got len {}", 
            i, written.len(), read.len());
    }
    
    Ok(())
}
