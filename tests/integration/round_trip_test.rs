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

use std::io::Cursor;

use disky::reader::{RecordReader, DiskyPiece};
use disky::writer::{RecordWriter, RecordWriterConfig};
use disky::error::Result;

/// Test that writing and reading a sequence of records works correctly.
#[test]
fn test_write_read_round_trip() -> Result<()> {
    // Create the test records
    let test_records = vec![
        b"record 1".to_vec(),
        b"record 2 with more data".to_vec(),
        b"record 3 with even more data for testing".to_vec(),
        b"record 4".to_vec(),
        // Note: Empty records are not supported
    ];
    
    // Create an in-memory buffer
    let buffer = Cursor::new(Vec::new());
    
    // Create a writer and write the records
    let mut writer = RecordWriter::new(buffer)?;
    
    // Write all records
    for record in &test_records {
        writer.write_record(record)?;
    }
    
    // Close the writer to flush all data
    writer.close()?;
    
    // Get the buffer with the written data
    let written_data = writer.get_data().unwrap();
    
    // Create a reader for the written data
    let mut reader = RecordReader::new(Cursor::new(written_data))?;
    
    // Read back the records and validate
    let mut read_records = Vec::new();
    
    // Read all records
    loop {
        match reader.next_record()? {
            DiskyPiece::Record(bytes) => {
                read_records.push(bytes.to_vec());
            }
            DiskyPiece::EOF => break,
        }
    }
    
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
    
    // Create an in-memory buffer
    let buffer = Cursor::new(Vec::new());
    
    // Create a writer and write the records
    let mut writer = RecordWriter::new(buffer)?;
    
    // Write all records
    for record in &test_records {
        writer.write_record(record)?;
    }
    
    // Close the writer to flush all data
    writer.close()?;
    
    // Get the buffer with the written data
    let written_data = writer.get_data().unwrap();
    
    // Create a reader for the written data
    let reader = RecordReader::new(Cursor::new(written_data))?;
    
    // Use the iterator to read the records
    let read_records: Result<Vec<_>> = reader.map(|r| r.map(|bytes| bytes.to_vec())).collect();
    let read_records = read_records?;
    
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

/// Test writing and reading large records
#[test]
fn test_write_read_large_records() -> Result<()> {
    // Create test records - large enough to test performance
    let test_records = vec![
        vec![b'A'; 1000],
        vec![b'B'; 2000],
        vec![b'C'; 3000],
    ];
    
    // Create an in-memory buffer
    let buffer = Cursor::new(Vec::new());
    
    // Create a writer with default configuration (no compression)
    let mut writer = RecordWriter::new(buffer)?;
    
    // Write all records
    for record in &test_records {
        writer.write_record(record)?;
    }
    
    // Close the writer to flush all data
    writer.close()?;
    
    // Get the buffer with the written data
    let written_data = writer.get_data().unwrap();
    
    // Create a reader for the written data
    let reader = RecordReader::new(Cursor::new(written_data))?;
    
    // Read back the records
    let read_records: Result<Vec<_>> = reader.map(|r| r.map(|bytes| bytes.to_vec())).collect();
    let read_records = read_records?;
    
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

/// Test writing many small records to ensure proper chunking.
#[test]
fn test_many_small_records() -> Result<()> {
    // Create many small records
    let test_records: Vec<Vec<u8>> = (0..1000)
        .map(|i| format!("small record {}", i).into_bytes())
        .collect();
    
    // Create an in-memory buffer
    let buffer = Cursor::new(Vec::new());
    
    // Create a writer with a small chunk size to force multiple chunks
    let config = RecordWriterConfig {
        chunk_size_bytes: 5000, // Small chunk size to force multiple chunks
        ..Default::default()
    };
    
    let mut writer = RecordWriter::with_config(buffer, config)?;
    
    // Write all records
    for record in &test_records {
        writer.write_record(record)?;
    }
    
    // Close the writer to flush all data
    writer.close()?;
    
    // Get the buffer with the written data
    let written_data = writer.get_data().unwrap();
    
    // Create a reader for the written data
    let reader = RecordReader::new(Cursor::new(written_data))?;
    
    // Read back the records
    let read_records: Result<Vec<_>> = reader.map(|r| r.map(|bytes| bytes.to_vec())).collect();
    let read_records = read_records?;
    
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

/// Test writing a few very large records to ensure proper block handling.
#[test]
fn test_large_records() -> Result<()> {
    // Create a few large records
    let test_records = vec![
        vec![b'X'; 100_000],  // 100 KB
        vec![b'Y'; 200_000],  // 200 KB
        vec![b'Z'; 150_000],  // 150 KB
    ];
    
    // Create an in-memory buffer
    let buffer = Cursor::new(Vec::new());
    
    // Create a writer with a small block size
    let config = RecordWriterConfig {
        block_config: disky::blocks::writer::BlockWriterConfig::with_block_size(64 * 1024)?, // 64 KB blocks
        ..Default::default()
    };
    
    let mut writer = RecordWriter::with_config(buffer, config)?;
    
    // Write all records
    for record in &test_records {
        writer.write_record(record)?;
    }
    
    // Close the writer to flush all data
    writer.close()?;
    
    // Get the buffer with the written data
    let written_data = writer.get_data().unwrap();
    
    // Create a reader for the written data
    let reader = RecordReader::new(Cursor::new(written_data))?;
    
    // Read back the records
    let read_records: Result<Vec<_>> = reader.map(|r| r.map(|bytes| bytes.to_vec())).collect();
    let read_records = read_records?;
    
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