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

use bytes::{Bytes, BytesMut};

use crate::blocks::writer::BlockWriterConfig;
use crate::reader::{CorruptionStrategy, DiskyPiece, RecordReader, RecordReaderConfig};
use crate::writer::{RecordWriter, RecordWriterConfig};
use crate::chunks::signature_writer::SignatureWriter;

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

/// Test reading a file with extreme record sizes
#[test]
fn test_extreme_record_sizes() {
    // Test with various extreme sizes: empty (0), tiny (1), and larger sizes
    // We'll create a separate file for each to ensure clean testing
    let sizes = [0, 1, 2, 64, 1000];
    
    for &size in &sizes {
        println!("Testing record size: {}", size);
        
        // Create a test file with a single record of the given size
        let mut buffer = Vec::new();
        
        // Small block and chunk sizes
        let block_size = 128u64;
        let chunk_size = 64;
        
        {
            let cursor = Cursor::new(&mut buffer);
            let mut config = RecordWriterConfig::default();
            config.block_config = BlockWriterConfig::with_block_size(block_size).unwrap();
            config.chunk_size_bytes = chunk_size;
            
            let mut writer = RecordWriter::with_config(cursor, config).unwrap();
            
            // Create a record with the specified size
            let data = vec![0xAA; size];
            writer.write_record(&data).unwrap();
            
            writer.close().unwrap();
        }
        
        // Read the file with the same block size
        let cursor = Cursor::new(&buffer);
        let config = RecordReaderConfig::with_block_size(block_size).unwrap();
        let mut reader = RecordReader::with_config(cursor, config).unwrap();
        
        // Read the record
        match reader.next_record().unwrap() {
            DiskyPiece::Record(record) => {
                // Verify size
                assert_eq!(record.len(), size);
                
                // Verify content (if non-empty)
                if size > 0 {
                    assert!(record.iter().all(|&b| b == 0xAA));
                }
            }
            DiskyPiece::EOF => {
                panic!("Expected record, got EOF");
            }
        }
        
        // Should reach EOF after the single record
        match reader.next_record().unwrap() {
            DiskyPiece::Record(_) => {
                panic!("Expected EOF, got another record");
            }
            DiskyPiece::EOF => {
                // This is expected
            }
        }
    }
}

/// Test handling concatenated files (when two files are appended together)
#[test]
fn test_concatenated_files() {
    // Create two separate files
    // Use larger record size for first file to ensure it spans at least one block
    let file1 = create_test_file(5, 50); // 5 records of 50 bytes each = 250 bytes + headers
    let file2 = create_test_file(3, 20); // 3 records of 20 bytes each
    
    // Combine the files
    let mut concatenated = BytesMut::with_capacity(file1.len() + file2.len());
    concatenated.extend_from_slice(&file1);
    concatenated.extend_from_slice(&file2);
    let combined = concatenated.freeze();
    
    // IMPORTANT: Use the same block size for reader as was used for the writer
    let block_size = 128u64;
    let config = RecordReaderConfig::with_block_size(block_size).unwrap()
        .with_corruption_strategy(CorruptionStrategy::Recover);
    
    // Read with recovery enabled
    let cursor = Cursor::new(&combined);
    let reader = RecordReader::with_config(cursor, config).unwrap();
    
    // Collect all successful records, filtering out errors
    let records: Vec<_> = reader.filter_map(Result::ok).collect();
    
    println!("Found {} records in concatenated file", records.len());
    
    // With larger record sizes, we should be able to read at least some records
    // from the first file before encountering the second file's signature
    assert!(!records.is_empty(), "Expected at least some records, got 0");
    
    // Validate records for internal consistency
    for record in &records {
        let value = record[0];
        assert!(record.iter().all(|&b| b == value), "Record data is inconsistent");
        
        // Validate record size
        if value < 5 {
            // From first file (50 bytes per record)
            assert_eq!(record.len(), 50, "First file record has wrong size");
        } else {
            // From second file (20 bytes per record), if we get any
            assert_eq!(record.len(), 20, "Second file record has wrong size");
        }
    }
}

/// Test with repeated signature blocks (add signature in the middle of file)
#[test]
fn test_repeated_signatures() {
    // Create the first part of the file
    let mut buffer = Vec::new();
    
    // Small block and chunk sizes
    let block_size = 128u64;
    let chunk_size = 64;
    
    {
        let cursor = Cursor::new(&mut buffer);
        let mut config = RecordWriterConfig::default();
        config.block_config = BlockWriterConfig::with_block_size(block_size).unwrap();
        config.chunk_size_bytes = chunk_size;
        
        let mut writer = RecordWriter::with_config(cursor, config).unwrap();
        
        // Write first set of records (values 0-4)
        for i in 0..5 {
            let record_data = Bytes::from(vec![i as u8; 5]);
            writer.write_record(&record_data).unwrap();
        }
        
        writer.close().unwrap();
    }
    
    // Create a signature chunk
    let mut writer = SignatureWriter::new();
    let signature_bytes = writer.try_serialize_chunk().unwrap();
    
    // Create the second part of the file
    let mut second_part = Vec::new();
    {
        let cursor = Cursor::new(&mut second_part);
        let mut config = RecordWriterConfig::default();
        config.block_config = BlockWriterConfig::with_block_size(block_size).unwrap();
        config.chunk_size_bytes = chunk_size;
        
        let mut writer = RecordWriter::with_config(cursor, config).unwrap();
        
        // Write second set of records (values 5-9)
        for i in 5..10 {
            let record_data = Bytes::from(vec![i as u8; 5]);
            writer.write_record(&record_data).unwrap();
        }
        
        writer.close().unwrap();
    }
    
    // Combine everything: first part + extra signature + second part
    let mut combined = BytesMut::with_capacity(buffer.len() + signature_bytes.len() + second_part.len());
    combined.extend_from_slice(&buffer);
    combined.extend_from_slice(&signature_bytes);
    combined.extend_from_slice(&second_part);
    let final_buffer = combined.freeze();
    
    // Read with recovery enabled
    let cursor = Cursor::new(&final_buffer);
    let config = RecordReaderConfig::with_block_size(block_size).unwrap()
        .with_corruption_strategy(CorruptionStrategy::Recover);
    
    let reader = RecordReader::with_config(cursor, config).unwrap();
    
    // Collect all records
    let mut records = Vec::new();
    let mut errors = Vec::new();
    
    for result in reader {
        match result {
            Ok(record) => records.push(record),
            Err(err) => errors.push(err),
        }
    }
    
    // We should get at least the first set of records
    assert!(records.len() >= 5, "Expected at least 5 records, got {}", records.len());
    
    // Validate the records we did get
    for (idx, record) in records.iter().enumerate().take(10) {  // At most 10 records
        // Record data should be consistent
        assert!(record.iter().all(|&b| b == record[0]), "Record data inconsistent at index {}", idx);
        
        if idx < 5 {
            // First set of records should have expected values
            assert_eq!(record[0], idx as u8, "First set record {} has wrong value", idx);
        } else if idx < 10 {
            // Second set of records might be read if recovery works well
            let expected = (idx as u8) % 10;  // Handle looping around
            if record[0] == expected {
                println!("Successfully read record {} from second part", idx);
            }
        }
    }
    
    println!("Read {} records with {} errors from file with repeated signatures", 
             records.len(), errors.len());
}

/// Test recovery at file boundaries
#[test]
fn test_recovery_at_file_boundaries() {
    // Create a test file with larger records to ensure it spans multiple blocks
    let mut buffer = create_test_file(20, 30); // 30 bytes * 20 records = 600 bytes + headers
    
    // With our small block size of 128 bytes, this should span multiple blocks
    
    // IMPORTANT: First verify our test file is long enough to span multiple blocks
    assert!(buffer.len() > 256, "Test file must be at least 2 blocks for this test");
    
    // Corrupt the file only at a single location in the middle
    // (avoiding corrupting the headers)
    let middle = buffer.len() / 2;
    buffer[middle] = !buffer[middle];
    
    // IMPORTANT: Use the same block size for reader as was used for the writer
    let block_size = 128u64;
    let config = RecordReaderConfig::with_block_size(block_size).unwrap()
        .with_corruption_strategy(CorruptionStrategy::Recover);
    
    // Read with recovery enabled
    let cursor = Cursor::new(&buffer);
    let reader = RecordReader::with_config(cursor, config).unwrap();
    
    // With recovery enabled, we should still get at least some records
    let records: Vec<_> = reader.filter_map(Result::ok).collect();
    
    println!("Read {} records from boundary-corrupted file", records.len());
    
    // We should have read at least some records despite the corruption
    assert!(!records.is_empty(), "Expected some records despite corruption");
    
    // Check that the records we did read are valid
    for record in records.iter() {
        let value = record[0];
        assert!(record.iter().all(|&b| b == value), "Record data inconsistent");
        assert_eq!(record.len(), 30, "Record has wrong size");
    }
}

/// Test reading an almost-empty file (just the signature, no records)
#[test]
fn test_empty_records_file() {
    // Create a file with signature but no records
    let mut buffer = Vec::new();
    
    // Small block and chunk sizes
    let block_size = 128u64;
    
    {
        let cursor = Cursor::new(&mut buffer);
        let mut config = RecordWriterConfig::default();
        config.block_config = BlockWriterConfig::with_block_size(block_size).unwrap();
        let mut writer = RecordWriter::with_config(cursor, config).unwrap();
        
        // Write no records, just close immediately
        writer.close().unwrap();
    }
    
    // Read the file with the same block size
    let cursor = Cursor::new(&buffer);
    let config = RecordReaderConfig::with_block_size(block_size).unwrap();
    let mut reader = RecordReader::with_config(cursor, config).unwrap();
    
    // Should go straight to EOF
    match reader.next_record().unwrap() {
        DiskyPiece::Record(_) => {
            panic!("Expected EOF in empty records file, got a record");
        }
        DiskyPiece::EOF => {
            // This is expected
        }
    }
}

/// Test with a single byte record
#[test]
fn test_single_byte_record() {
    // Create a file with a single 1-byte record
    let mut buffer = Vec::new();
    
    // Small block and chunk sizes
    let block_size = 128u64;
    let chunk_size = 64;
    
    {
        let cursor = Cursor::new(&mut buffer);
        let mut config = RecordWriterConfig::default();
        config.block_config = BlockWriterConfig::with_block_size(block_size).unwrap();
        config.chunk_size_bytes = chunk_size;
        
        let mut writer = RecordWriter::with_config(cursor, config).unwrap();
        
        // Single byte record
        writer.write_record(&[42]).unwrap();
        
        writer.close().unwrap();
    }
    
    // Read the file
    let cursor = Cursor::new(&buffer);
    let config = RecordReaderConfig::with_block_size(block_size).unwrap();
    let mut reader = RecordReader::with_config(cursor, config).unwrap();
    
    // Should get the single byte record
    match reader.next_record().unwrap() {
        DiskyPiece::Record(record) => {
            assert_eq!(record.len(), 1);
            assert_eq!(record[0], 42);
        }
        DiskyPiece::EOF => {
            panic!("Expected record, got EOF");
        }
    }
    
    // Then EOF
    match reader.next_record().unwrap() {
        DiskyPiece::Record(_) => {
            panic!("Expected EOF, got another record");
        }
        DiskyPiece::EOF => {
            // This is expected
        }
    }
}

/// Test alternating record sizes (to exercise different chunk and record boundaries)
#[test]
fn test_alternating_record_sizes() {
    // Create a file with alternating small and larger records
    let mut buffer = Vec::new();
    
    // Small block and chunk sizes to create more boundaries
    let block_size = 128u64;
    let chunk_size = 64;
    
    {
        let cursor = Cursor::new(&mut buffer);
        let mut config = RecordWriterConfig::default();
        config.block_config = BlockWriterConfig::with_block_size(block_size).unwrap();
        config.chunk_size_bytes = chunk_size;
        
        let mut writer = RecordWriter::with_config(cursor, config).unwrap();
        
        // Write alternating record sizes
        for i in 0..20 {
            let size = if i % 2 == 0 { 5 } else { 50 };  // Alternate between 5 and 50 bytes
            let record_data = Bytes::from(vec![i as u8; size]);
            writer.write_record(&record_data).unwrap();
        }
        
        writer.close().unwrap();
    }
    
    // Read the file
    let cursor = Cursor::new(&buffer);
    let config = RecordReaderConfig::with_block_size(block_size).unwrap();
    let reader = RecordReader::with_config(cursor, config).unwrap();
    
    // Collect the records
    let records = reader.collect::<Result<Vec<_>, _>>().unwrap();
    
    // Should have 20 records
    assert_eq!(records.len(), 20);
    
    // Verify the record contents and sizes
    for (i, record) in records.iter().enumerate() {
        let expected_size = if i % 2 == 0 { 5 } else { 50 };
        assert_eq!(record.len(), expected_size);
        
        // All bytes should be the same value
        assert!(record.iter().all(|&b| b == i as u8));
    }
}
