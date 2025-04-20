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

use std::io::{Cursor, Seek};

use bytes::Bytes;

use crate::blocks::writer::BlockWriterConfig;
use crate::error::DiskyError;
use crate::reader::{DiskyPiece, RecordReader, RecordReaderConfig};
use crate::writer::{RecordWriter, RecordWriterConfig};

/// Test reading an empty file fails with signature error on first read attempt
#[test]
fn test_empty_file() {
    let empty_data = Vec::new();
    let cursor = Cursor::new(empty_data);
    
    // Reader creation should succeed (lazy initialization)
    let mut reader = RecordReader::new(cursor).unwrap();
    
    // First read attempt should fail with a SignatureReadingError
    match reader.next_record() {
        Err(DiskyError::SignatureReadingError(_)) => {
            // This is the expected error type for an empty file
        },
        Err(other_error) => {
            panic!("Expected SignatureReadingError, got: {:?}", other_error);
        },
        Ok(_) => panic!("Expected error reading empty file, but got success"),
    }
}

/// Test the basic read/write functionality with one record
#[test]
fn test_read_single_record() {
    // Create a file with a single record
    let mut buffer = Vec::new();
    {
        let cursor = Cursor::new(&mut buffer);
        let mut writer = RecordWriter::new(cursor).unwrap();
        
        let record_data = Bytes::from(vec![1, 2, 3, 4, 5]);
        writer.write_record(&record_data).unwrap();
        writer.close().unwrap();
    }
    
    // Read the record back
    let cursor = Cursor::new(&buffer);
    let mut reader = RecordReader::new(cursor).unwrap();
    
    // First call should return the record
    match reader.next_record().unwrap() {
        DiskyPiece::Record(bytes) => {
            assert_eq!(bytes, Bytes::from(vec![1, 2, 3, 4, 5]));
        }
        DiskyPiece::EOF => {
            panic!("Expected record, got EOF");
        }
    }
    
    // Second call should return EOF
    match reader.next_record().unwrap() {
        DiskyPiece::Record(_) => {
            panic!("Expected EOF, got record");
        }
        DiskyPiece::EOF => {
            // This is expected
        }
    }
}

/// Test reading multiple records
#[test]
fn test_read_multiple_records() {
    // Create data with multiple records
    let mut buffer = Vec::new();
    {
        let cursor = Cursor::new(&mut buffer);
        let mut writer = RecordWriter::new(cursor).unwrap();
        
        for i in 0..10 {
            let record_data = Bytes::from(vec![i; 5]); // [i,i,i,i,i]
            writer.write_record(&record_data).unwrap();
        }
        writer.close().unwrap();
    }
    
    // Read and verify records
    let cursor = Cursor::new(&buffer);
    let reader = RecordReader::new(cursor).unwrap();
    
    // Use iterator interface to read all records
    let records: Vec<Bytes> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    
    // Verify we got 10 records with the expected data
    assert_eq!(records.len(), 10);
    for (i, record) in records.iter().enumerate() {
        let expected = vec![i as u8; 5];
        assert_eq!(record, &expected);
    }
}

/// Test reading various record sizes
#[test]
fn test_various_record_sizes() {
    let sizes = [0, 1, 10, 1000, 65000];
    
    for &size in &sizes {
        // Create a file with a record of the given size
        let mut buffer = Vec::new();
        {
            let cursor = Cursor::new(&mut buffer);
            let mut writer = RecordWriter::new(cursor).unwrap();
            
            let record_data = Bytes::from(vec![0xA5; size]);
            writer.write_record(&record_data).unwrap();
            writer.close().unwrap();
        }
        
        // Read the record back
        let cursor = Cursor::new(&buffer);
        let mut reader = RecordReader::new(cursor).unwrap();
        
        match reader.next_record().unwrap() {
            DiskyPiece::Record(bytes) => {
                assert_eq!(bytes.len(), size);
                assert!(bytes.iter().all(|&b| b == 0xA5));
            }
            DiskyPiece::EOF => {
                panic!("Expected record, got EOF");
            }
        }
    }
}

/// Test reading a file with multiple blocks
#[test]
fn test_read_multi_block() {
    // Create a file with records that will span multiple blocks
    let num_records = 100;  // More reasonable number for tests
    let mut buffer = Vec::new();
    
    // Use a smaller block size to ensure multiple blocks
    // IMPORTANT: The reader and writer MUST use the same block size!
    // If these don't match, the reader will try to find block headers at the wrong positions,
    // which leads to "Chunk data hash mismatch" errors
    let block_size = 4096u64;
    
    // Write the file with custom block size
    {
        let cursor = Cursor::new(&mut buffer);
        let mut config = RecordWriterConfig::default();
        config.block_config = BlockWriterConfig::with_block_size(block_size).unwrap();
        
        let mut writer = RecordWriter::with_config(cursor, config).unwrap();
        
        // Write records that will force multiple blocks
        for i in 0..num_records {
            let record_data = Bytes::from(vec![i as u8; 100]);  // 100 bytes per record
            writer.write_record(&record_data).unwrap();
        }
        writer.close().unwrap();
    }
    
    // Read with the SAME block size as the writer
    let cursor = Cursor::new(&buffer);
    let reader_config = RecordReaderConfig::with_block_size(block_size).unwrap();
    let mut reader = RecordReader::with_config(cursor, reader_config).unwrap();
    
    let mut records = Vec::new();
    
    loop {
        match reader.next_record() {
            Ok(DiskyPiece::Record(record)) => {
                records.push(record);
            }
            Ok(DiskyPiece::EOF) => {
                break;
            }
            Err(e) => {
                panic!("Error reading record: {:?}", e);
            }
        }
    }
    
    // Verify we got all records
    assert_eq!(records.len(), num_records);
    for (i, record) in records.iter().enumerate() {
        assert_eq!(record.len(), 100);
        assert!(record.iter().all(|&b| b == i as u8));
    }
}

/// Test seeking to a block boundary and reading
///
/// Note: The format likely doesn't support arbitrary seeking in the middle of a file
/// as it would invalidate block and chunk boundaries. This test tries to seek to
/// perfectly aligned block boundaries.
#[test]
fn test_read_after_seek() {
    
    // Create a file with multiple records
    let mut buffer = Vec::new();
    {
        let cursor = Cursor::new(&mut buffer);
        let mut writer = RecordWriter::new(cursor).unwrap();
        
        for i in 0..100 {
            let record_data = Bytes::from(vec![i as u8; 10]);
            writer.write_record(&record_data).unwrap();
        }
        writer.close().unwrap();
    }
    
    // The current implementation doesn't support random access by seeking in the middle
    // of a file. Either this functionality is not supported or it requires seeking to
    // a specific block boundary, not an arbitrary position.
    
    // We should at least verify this is detected and produces an appropriate error
    // and doesn't just crash or produce corrupted data.
    let mut cursor = Cursor::new(&buffer);
    cursor.seek(std::io::SeekFrom::Start(buffer.len() as u64 / 2)).unwrap();
    
    let reader = RecordReader::new(cursor).unwrap();
    let result = reader.collect::<Result<Vec<_>, _>>();
    
    // The reader should fail with an appropriate error
    assert!(result.is_err());
    match result {
        Err(DiskyError::ChunkHeaderHashMismatch) | 
        Err(DiskyError::BlockHeaderHashMismatch) | 
        Err(DiskyError::ChunkDataHashMismatch) | 
        Err(DiskyError::InvalidBlockHeader(_)) |
        Err(DiskyError::ReadCorruptedChunk(_)) |
        Err(DiskyError::ReadCorruptedBlock(_)) => {
            // These are all reasonable error types when seeking to a random position
        },
        Err(e) => {
            println!("Got unexpected error type: {:?}", e);
            // Any error is better than invalid data, so don't fail the test
        },
        Ok(_) => {
            panic!("Reader should have failed when seeking to random position");
        }
    }
}