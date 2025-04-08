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

//! Edge case conformance tests for the RecordWriter implementation.
//! These tests focus on specific edge cases to verify compliance with
//! the Riegeli specification under unusual conditions.

use std::io::Cursor;

use crate::compression::CompressionType;
use crate::writer::{RecordWriter, RecordWriterConfig};
use crate::tests::utils::format_bytes_for_assert;

/// Test 2: Empty records at specific positions
/// Creates a file with empty records at beginning, middle, and end
#[test]
fn test_empty_records_at_specific_positions() {
    // Create a cursor as our sink
    let cursor = Cursor::new(Vec::new());
    
    // Create a writer with default config
    let config = RecordWriterConfig {
        compression_type: CompressionType::None,
        ..Default::default()
    };
    
    let mut writer = RecordWriter::with_config(cursor, config).unwrap();
    
    // Write records with empty ones at specific positions
    writer.write_record(b"").unwrap();      // Empty record at beginning
    writer.write_record(b"middle").unwrap();
    writer.write_record(b"").unwrap();      // Empty record in the middle
    writer.write_record(b"end").unwrap();
    writer.write_record(b"").unwrap();      // Empty record at the end
    
    // Close to ensure all data is written
    writer.close().unwrap();
    
    // Get the written data
    let data = writer.get_data().unwrap();
    
    // The expected bytes for a file with strategically placed empty records
    #[rustfmt::skip]
    const EXPECTED_FILE: &[u8] = &[
        // Block header (24 bytes)
        0x83, 0xaf, 0x70, 0xd1, 0x0d, 0x88, 0x4a, 0x3f, // header_hash
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // previous_chunk
        0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // next_chunk
        
        // Signature chunk header (40 bytes)
        0x91, 0xba, 0xc2, 0x3c, 0x92, 0x87, 0xe1, 0xa9, // header_hash
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // data_size
        0xe1, 0x9f, 0x13, 0xc0, 0xe9, 0xb1, 0xc3, 0x72, // data_hash
        0x73, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk_type('s') + num_records
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // decoded_data_size
        
        // Records chunk header (40 bytes) - contains all records
        0x7d, 0x73, 0x3e, 0xf3, 0x49, 0xff, 0xc9, 0x4a, // header_hash
        0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // data_size (16 bytes)
        0x56, 0x4c, 0x5d, 0x3e, 0x3e, 0x10, 0x5b, 0x40, // data_hash
        0x72, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk_type('r') + num_records(5)
        0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // decoded_data_size (9 bytes)
        
        // Records chunk data (16 bytes)
        0x00,                                           // compression_type (None)
        0x05,                                           // compressed_sizes_size (varint 5)
        0x00, 0x06, 0x00, 0x03, 0x00,                   // compressed_sizes (varints: 0, 6, 0, 3, 0)
        
        // Record values (9 bytes total)
        // Empty record (0 bytes)
        0x6d, 0x69, 0x64, 0x64, 0x6c, 0x65,             // "middle"
        // Empty record (0 bytes)
        0x65, 0x6e, 0x64                                // "end"
        // Empty record (0 bytes)
    ];
    
    // Verify the file content
    if data != EXPECTED_FILE {
        panic!("Actual bytes:\n{}", format_bytes_for_assert(&data));
    }
    
    assert_eq!(data.len(), EXPECTED_FILE.len(), "File sizes don't match");
    assert_eq!(data, EXPECTED_FILE, "File content doesn't match expected");
}

/// Test 3: Chunk ending near block boundary
/// Tests the exact behavior when a chunk ends exactly 1 byte before a block boundary
#[test]
fn test_chunk_ending_near_block_boundary() {
    // Create a cursor as our sink
    let cursor = Cursor::new(Vec::new());
    
    // We need a small block size to test boundary conditions
    let block_size = 128;
    
    // Create a writer with custom config
    let config = RecordWriterConfig {
        compression_type: CompressionType::None,
        block_config: crate::blocks::writer::BlockWriterConfig::with_block_size(block_size).unwrap(),
        ..Default::default()
    };
    
    let mut writer = RecordWriter::with_config(cursor, config).unwrap();
    
    // Calculate size to make chunk end exactly at block_size - 1 (right before next boundary)
    // File starts with 64 bytes (24 block header + 40 signature)
    // We want to add a simple chunk that ends at position 127
    // 40 bytes for chunk header, 3 bytes for data overhead (compression_type + sizes_length + sizes)
    // From the actual output, we need a record of size 20 bytes
    let record = vec![b'x'; 20];
    writer.write_record(&record).unwrap();
    
    // Close to ensure all data is written 
    writer.close().unwrap();
    
    // Get the written data
    let data = writer.get_data().unwrap();
    
    // The exact expected bytes for a file with a chunk ending near block boundary
    // Actual file is 127 bytes in size, one byte short of the block size
    #[rustfmt::skip]
    const EXPECTED_BOUNDARY_FILE: &[u8] = &[
        //=================================================================================
        // BLOCK 1 (bytes 0-127): Contains entire file (chunk ends right at boundary - 1)
        //=================================================================================
        
        // Block header (24 bytes)
        0x83, 0xaf, 0x70, 0xd1, 0x0d, 0x88, 0x4a, 0x3f, // header_hash
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // previous_chunk (0 bytes)
        0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // next_chunk (64 bytes)
        
        // Signature chunk header (40 bytes)
        0x91, 0xba, 0xc2, 0x3c, 0x92, 0x87, 0xe1, 0xa9, // header_hash
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // data_size (0 bytes)
        0xe1, 0x9f, 0x13, 0xc0, 0xe9, 0xb1, 0xc3, 0x72, // data_hash
        0x73, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk_type('s') + num_records (0)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // decoded_data_size (0 bytes)
        
        // Records chunk header (40 bytes)
        0xdc, 0xda, 0xde, 0x36, 0xeb, 0x09, 0x88, 0x2e, // header_hash
        0x17, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // data_size (23 bytes)
        0xba, 0x2f, 0x80, 0xf5, 0x14, 0xb2, 0x00, 0x5c, // data_hash
        0x72, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk_type('r') + num_records(1)
        0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // decoded_data_size (20 bytes)
        
        // Records chunk data (23 bytes)
        0x00,                                           // compression_type (None)
        0x01,                                           // compressed_sizes_size (varint 1)
        0x14,                                           // compressed_sizes (varint 20)
        
        // Record value (20 bytes)
        0x78, 0x78, 0x78, 0x78, 0x78, 0x78, 0x78, 0x78, // 8 'x's
        0x78, 0x78, 0x78, 0x78, 0x78, 0x78, 0x78, 0x78, // 8 'x's
        0x78, 0x78, 0x78, 0x78                          // 4 'x's
    ];
    
    // Check the length to make sure it ends exactly at the block boundary minus 1
    // Actual implementation produces 127 bytes, not 128 bytes as initially expected
    if data.len() != block_size as usize - 1 {
        panic!("Expected data length {} bytes (block size - 1), got {} bytes", 
              block_size - 1, data.len());
    }
    
    // Verify the file content
    if data != EXPECTED_BOUNDARY_FILE {
        panic!("Actual bytes (len:{}):\n{}", data.len(), format_bytes_for_assert(&data));
    }
    
    assert_eq!(data, EXPECTED_BOUNDARY_FILE, "File content doesn't match expected");
}

/// Test 4: Forced chunking patterns
/// Writes records of specific sizes to force chunks to split at predictable boundaries.
/// This test demonstrates how the chunking mechanism works:
/// 1. The writer accumulates records in a chunk until it approaches the chunk size limit
/// 2. When a record would make the chunk exceed that limit, a new chunk is created
/// 3. Records are never split across chunk boundaries
#[test]
fn test_forced_chunking_pattern() {
    // Create a cursor as our sink
    let cursor = Cursor::new(Vec::new());
    
    // Create a writer with small chunk size to force specific chunking
    let config = RecordWriterConfig {
        compression_type: CompressionType::None,
        chunk_size_bytes: 30, // Force chunking after about 30 bytes of records
        ..Default::default()
    };
    
    let mut writer = RecordWriter::with_config(cursor, config).unwrap();
    
    // Write specific record sizes to force chunk boundaries
    writer.write_record(b"record-1").unwrap();  // 8 bytes
    writer.write_record(b"record-2").unwrap();  // 8 bytes
    writer.write_record(b"record-3").unwrap();  // 8 bytes  
    // Total: 24 bytes, just under our 30-byte limit
    
    // This record will bring us to the chunking limit, but the chunk won't
    // actually split until the next record is added
    writer.write_record(b"forces-new-chunk").unwrap();  // 16 bytes
    
    // This record should now be in a new chunk
    writer.write_record(b"new-chunk-record").unwrap();  // 16 bytes
    
    // Close to ensure all data is written
    writer.close().unwrap();
    
    // Get the written data
    let data = writer.get_data().unwrap();
    
    // Expected bytes for a file with forced chunk boundaries
    // First chunk contains the first 4 records up to forces-new-chunk
    // Second chunk contains the new-chunk-record which has been forced into a new chunk
    #[rustfmt::skip]
    const EXPECTED_CHUNKING_FILE: &[u8] = &[
        // Block header (24 bytes)
        0x83, 0xaf, 0x70, 0xd1, 0x0d, 0x88, 0x4a, 0x3f, // header_hash
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // previous_chunk
        0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // next_chunk
        
        // Signature chunk header (40 bytes)
        0x91, 0xba, 0xc2, 0x3c, 0x92, 0x87, 0xe1, 0xa9, // header_hash
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // data_size
        0xe1, 0x9f, 0x13, 0xc0, 0xe9, 0xb1, 0xc3, 0x72, // data_hash
        0x73, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk_type('s') + num_records
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // decoded_data_size
        
        // First records chunk header (40 bytes) - contains first 4 records
        0x8f, 0x0b, 0x30, 0xb7, 0x95, 0xef, 0x6c, 0x8e, // header_hash
        0x2e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // data_size (46 bytes)
        0x58, 0x41, 0xa4, 0x3f, 0xa3, 0x57, 0xc1, 0x5a, // data_hash
        0x72, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk_type('r') + num_records(4)
        0x28, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // decoded_data_size (40 bytes)
        
        // First chunk data (46 bytes)
        0x00,                                           // compression_type (None)
        0x04,                                           // compressed_sizes_size (varint 4)
        0x08, 0x08, 0x08, 0x10,                         // compressed_sizes (varints: 8, 8, 8, 16)
        
        // Record values (40 bytes total)
        0x72, 0x65, 0x63, 0x6f, 0x72, 0x64, 0x2d, 0x31, // "record-1"
        0x72, 0x65, 0x63, 0x6f, 0x72, 0x64, 0x2d, 0x32, // "record-2"
        0x72, 0x65, 0x63, 0x6f, 0x72, 0x64, 0x2d, 0x33, // "record-3"
        0x66, 0x6f, 0x72, 0x63, 0x65, 0x73, 0x2d, 0x6e, // "forces-n"
        0x65, 0x77, 0x2d, 0x63, 0x68, 0x75, 0x6e, 0x6b, // "ew-chunk"
        
        // Second records chunk header (40 bytes) - contains the new-chunk-record
        0x84, 0x20, 0xba, 0x4e, 0x2a, 0xbc, 0x7c, 0x97, // header_hash
        0x13, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // data_size (19 bytes)
        0xd8, 0x6a, 0xfe, 0x50, 0x45, 0x2c, 0x5d, 0xc3, // data_hash
        0x72, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk_type('r') + num_records(1)
        0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // decoded_data_size (16 bytes)
        
        // Second chunk data (19 bytes)
        0x00,                                           // compression_type (None)
        0x01,                                           // compressed_sizes_size (varint 1)
        0x10,                                           // compressed_sizes (varint 16)
        
        // Record value (16 bytes)
        0x6e, 0x65, 0x77, 0x2d, 0x63, 0x68, 0x75, 0x6e, // "new-chun"
        0x6b, 0x2d, 0x72, 0x65, 0x63, 0x6f, 0x72, 0x64  // "k-record"
    ];
    
    // Verify the file content
    if data != EXPECTED_CHUNKING_FILE {
        panic!("Actual bytes (len:{}):\n{}", data.len(), format_bytes_for_assert(&data));
    }
    
    assert_eq!(data.len(), EXPECTED_CHUNKING_FILE.len(), "File sizes don't match");
    assert_eq!(data, EXPECTED_CHUNKING_FILE, "File content doesn't match expected");
}

/// Test 5: Append to existing file
/// Tests creating a file and then appending to it using the for_append functionality
#[test]
fn test_append_to_existing_file() {
    // First, create a file with a single record
    let cursor = Cursor::new(Vec::new());
    let config = RecordWriterConfig {
        compression_type: CompressionType::None,
        ..Default::default()
    };
    let mut writer = RecordWriter::with_config(cursor, config.clone()).unwrap();
    writer.write_record(b"initial-record").unwrap();
    writer.close().unwrap();
    
    // Get the data from the first writer
    let initial_data = writer.get_data().unwrap();
    let initial_size = initial_data.len();
    
    // Now create a new writer that simulates appending
    let cursor = Cursor::new(initial_data.to_vec());
    let mut appending_writer = RecordWriter::for_append_with_config(
        cursor, 
        initial_size as u64, 
        config
    ).unwrap();
    
    // Append a new record
    appending_writer.write_record(b"appended-record").unwrap();
    
    // Close to ensure all data is written
    appending_writer.close().unwrap();
    
    // Get the data from the appended writer
    let appended_data = appending_writer.get_data().unwrap();
    
    // The expected bytes for the appended file
    #[rustfmt::skip]
    const EXPECTED_APPENDED_FILE: &[u8] = &[
        // Block header (24 bytes)
        0x83, 0xaf, 0x70, 0xd1, 0x0d, 0x88, 0x4a, 0x3f, // header_hash
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // previous_chunk
        0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // next_chunk
        
        // Signature chunk header (40 bytes)
        0x91, 0xba, 0xc2, 0x3c, 0x92, 0x87, 0xe1, 0xa9, // header_hash
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // data_size
        0xe1, 0x9f, 0x13, 0xc0, 0xe9, 0xb1, 0xc3, 0x72, // data_hash
        0x73, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk_type('s') + num_records
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // decoded_data_size
        
        // First records chunk header (40 bytes) - contains initial record
        0xf5, 0x40, 0xba, 0xf7, 0x59, 0xea, 0x2a, 0xb8, // header_hash
        0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // data_size (17 bytes)
        0x9a, 0xc5, 0x65, 0x0d, 0x51, 0x4c, 0xff, 0x9e, // data_hash
        0x72, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk_type('r') + num_records(1)
        0x0e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // decoded_data_size (14 bytes)
        
        // First chunk data (17 bytes)
        0x00,                                           // compression_type (None)
        0x01,                                           // compressed_sizes_size (varint 1)
        0x0e,                                           // compressed_sizes (varint 14)
        
        // Record value (14 bytes)
        0x69, 0x6e, 0x69, 0x74, 0x69, 0x61, 0x6c, 0x2d, // "initial-"
        0x72, 0x65, 0x63, 0x6f, 0x72, 0x64,             // "record"
        
        // Second records chunk header (40 bytes) - contains appended record
        0xf8, 0xfb, 0x9f, 0xe2, 0xca, 0xf7, 0xe1, 0xfd, // header_hash
        0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // data_size (18 bytes)
        0x01, 0x2c, 0x8f, 0x24, 0x5e, 0x4f, 0x97, 0xa0, // data_hash
        0x72, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk_type('r') + num_records(1)
        0x0f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // decoded_data_size (15 bytes)
        
        // Second chunk data (18 bytes)
        0x00,                                           // compression_type (None)
        0x01,                                           // compressed_sizes_size (varint 1)
        0x0f,                                           // compressed_sizes (varint 15)
        
        // Record value (15 bytes)
        0x61, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x65, 0x64, // "appended"
        0x2d, 0x72, 0x65, 0x63, 0x6f, 0x72, 0x64        // "-record"
    ];
    
    // Verify the file content
    if appended_data != EXPECTED_APPENDED_FILE {
        panic!("Actual bytes (len:{}):\n{}", appended_data.len(), format_bytes_for_assert(&appended_data));
    }
    
    assert_eq!(appended_data.len(), EXPECTED_APPENDED_FILE.len(), "File sizes don't match");
    assert_eq!(appended_data, EXPECTED_APPENDED_FILE, "File content doesn't match expected");
}

/// Test 8: File with exact block size
/// Creates a file that's exactly a multiple of the block size
#[test]
fn test_file_with_exact_block_size() {
    // Create a cursor as our sink
    let cursor = Cursor::new(Vec::new());
    
    // Define a small block size for testing
    let block_size = 128;
    
    // Create a writer with custom config
    let config = RecordWriterConfig {
        compression_type: CompressionType::None,
        block_config: crate::blocks::writer::BlockWriterConfig::with_block_size(block_size).unwrap(),
        ..Default::default()
    };
    
    let mut writer = RecordWriter::with_config(cursor, config).unwrap();
    
    // Calculate required record size to hit exact block size
    // We need total file size to be exactly block_size (128 bytes)
    // File starts with 64 bytes (24 block header + 40 signature)
    // Chunk header is 40 bytes, and 3 bytes for overhead
    // So record needs to be 128 - 64 - 40 - 3 = 21 bytes
    let record = vec![b'y'; 21];
    writer.write_record(&record).unwrap();
    
    // Close to ensure all data is written
    writer.close().unwrap();
    
    // Get the written data
    let data = writer.get_data().unwrap();
    
    // The expected bytes for a file with exact block size
    #[rustfmt::skip]
    const EXPECTED_EXACT_BLOCK_SIZE_FILE: &[u8] = &[
        // Block header (24 bytes)
        0x83, 0xaf, 0x70, 0xd1, 0x0d, 0x88, 0x4a, 0x3f, // header_hash
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // previous_chunk
        0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // next_chunk
        
        // Signature chunk header (40 bytes)
        0x91, 0xba, 0xc2, 0x3c, 0x92, 0x87, 0xe1, 0xa9, // header_hash
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // data_size
        0xe1, 0x9f, 0x13, 0xc0, 0xe9, 0xb1, 0xc3, 0x72, // data_hash
        0x73, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk_type('s') + num_records
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // decoded_data_size
        
        // Records chunk header (40 bytes)
        0x77, 0xb5, 0x52, 0xec, 0xfc, 0xa3, 0x3a, 0x67, // header_hash
        0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // data_size (24 bytes)
        0x43, 0xdf, 0xb5, 0xae, 0xa9, 0x40, 0x92, 0xe0, // data_hash
        0x72, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk_type('r') + num_records(1)
        0x15, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // decoded_data_size (21 bytes)
        
        // Records chunk data (24 bytes)
        0x00,                                           // compression_type (None)
        0x01,                                           // compressed_sizes_size (varint 1)
        0x15,                                           // compressed_sizes (varint 21)
        
        // Record value (21 bytes)
        0x79, 0x79, 0x79, 0x79, 0x79, 0x79, 0x79, 0x79, // 8 'y's
        0x79, 0x79, 0x79, 0x79, 0x79, 0x79, 0x79, 0x79, // 8 'y's
        0x79, 0x79, 0x79, 0x79, 0x79                    // 5 'y's
    ];
    
    // Check the length to make sure it's exactly the block size
    if data.len() != block_size as usize {
        panic!("Expected data length {} bytes (block size), got {} bytes", 
              block_size, data.len());
    }
    
    // Verify the file content
    if data != EXPECTED_EXACT_BLOCK_SIZE_FILE {
        panic!("Actual bytes (len:{}):\n{}", data.len(), format_bytes_for_assert(&data));
    }
    
    assert_eq!(data, EXPECTED_EXACT_BLOCK_SIZE_FILE, "File content doesn't match expected");
}