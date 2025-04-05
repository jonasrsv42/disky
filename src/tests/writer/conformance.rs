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

//! Conformance tests for the RecordWriter implementation.
//! These tests verify the exact byte-by-byte output of the writer.

use std::io::Cursor;

use crate::compression::CompressionType;
use crate::writer::{RecordWriter, RecordWriterConfig, WriterState};

/// Helper function to print bytes in a format that's easy to copy for assertions
fn format_bytes_for_assert(bytes: &[u8]) -> String {
    let mut result = String::from("&[\n    ");
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 && i % 8 == 0 {
            result.push_str(",\n    ");
        } else if i > 0 {
            result.push_str(", ");
        }
        result.push_str(&format!("0x{:02x}", b));
    }
    result.push_str("\n]");
    result
}

/// Test a small, predictable file containing just the signature chunk
#[test]
fn test_empty_file_bytes() {
    // Create a cursor as our sink
    let cursor = Cursor::new(Vec::new());
    
    // Create a writer
    let mut writer = RecordWriter::new(cursor).unwrap();
    
    // Close immediately without writing any records (just the signature)
    writer.close().unwrap();
    
    // Get the written data
    let data = writer.get_data().unwrap();
    
    // The expected bytes for an empty file with just the signature chunk
    // Note: No empty chunk is written after our fix
    #[rustfmt::skip]
    const EXPECTED_EMPTY_FILE: &[u8] = &[
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
        
        // No extra empty chunk after our fix
    ];
    
    // Verify the content
    if data != EXPECTED_EMPTY_FILE {
        panic!("Actual bytes:\n{}", format_bytes_for_assert(&data));
    }
    
    assert_eq!(data.len(), EXPECTED_EMPTY_FILE.len(), "File sizes don't match");
    assert_eq!(data, EXPECTED_EMPTY_FILE, "File content doesn't match expected");
}

/// Test a file with exactly one known record
#[test]
fn test_single_record_file_bytes() {
    // Create a cursor as our sink
    let cursor = Cursor::new(Vec::new());
    
    // Create a writer with fixed compression type for consistent output
    let config = RecordWriterConfig {
        compression_type: CompressionType::None,
        ..Default::default()
    };
    
    let mut writer = RecordWriter::with_config(cursor, config).unwrap();
    
    // Write a single record with predictable content
    writer.write_record(b"test-record").unwrap();
    
    // Close to ensure all data is written
    writer.close().unwrap();
    
    // Get the written data
    let data = writer.get_data().unwrap();
    
    // The expected bytes for a file with a single record "test-record"
    // This matches what our implementation should produce after the fix
    #[rustfmt::skip]
    const EXPECTED_SINGLE_RECORD_FILE: &[u8] = &[
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
        0xbd, 0xb9, 0x1d, 0x4e, 0x15, 0xe0, 0x15, 0x9b, // header_hash
        0x0e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // data_size (14 bytes)
        0x7e, 0x26, 0xcb, 0x1d, 0xa1, 0xb3, 0xe9, 0x8a, // data_hash
        0x72, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk_type('r') + num_records(1)
        0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // decoded_data_size (11 bytes)
        
        // Records chunk data (14 bytes)
        0x00,                                           // compression_type (None)
        0x01,                                           // compressed_sizes_size (varint 1)
        0x0b,                                           // compressed_sizes (varint 11)
        
        // Record value (11 bytes)
        0x74, 0x65, 0x73, 0x74, 0x2d, 0x72, 0x65, 0x63, 0x6f, 0x72, 0x64,  // "test-record"
        
        // No extra empty chunk after our fix
    ];
    
    // Verify the file content
    if data != EXPECTED_SINGLE_RECORD_FILE {
        panic!("Actual bytes:\n{}", format_bytes_for_assert(&data));
    }
    
    assert_eq!(data.len(), EXPECTED_SINGLE_RECORD_FILE.len(), "File sizes don't match");
    assert_eq!(data, EXPECTED_SINGLE_RECORD_FILE, "File content doesn't match expected");
}

/// Test writing two records that should be in a single chunk
#[test]
fn test_two_records_same_chunk_bytes() {
    // Create a cursor as our sink
    let cursor = Cursor::new(Vec::new());
    
    // Create a writer with fixed compression type for consistent output
    // and large chunk size to ensure both records go in the same chunk
    let config = RecordWriterConfig {
        compression_type: CompressionType::None,
        chunk_size_bytes: 1024, // Plenty of space for both records
        ..Default::default()
    };
    
    let mut writer = RecordWriter::with_config(cursor, config).unwrap();
    
    // Write two records with predictable content
    writer.write_record(b"first").unwrap();
    writer.write_record(b"second").unwrap();
    
    // Close to ensure all data is written
    writer.close().unwrap();
    
    // Get the written data
    let data = writer.get_data().unwrap();
    
    // The expected bytes for a file with two records in a single chunk
    // Updated to reflect the fix for the empty chunk bug
    #[rustfmt::skip]
    const EXPECTED_TWO_RECORDS_FILE: &[u8] = &[
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
        0xe5, 0xdf, 0x3d, 0x19, 0x01, 0x02, 0x5c, 0x97, // header_hash
        0x0f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // data_size (15 bytes)
        0x96, 0xc5, 0xcf, 0x8f, 0xee, 0xde, 0x25, 0x67, // data_hash
        0x72, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk_type('r') + num_records(2)
        0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // decoded_data_size (11 bytes)
        
        // Records chunk data (15 bytes)
        0x00,                                           // compression_type (None)
        0x02,                                           // compressed_sizes_size (varint 2)
        0x05, 0x06,                                     // compressed_sizes (varints: 5, 6)
        
        // Record values (11 bytes total)
        0x66, 0x69, 0x72, 0x73, 0x74,                   // "first"
        0x73, 0x65, 0x63, 0x6f, 0x6e, 0x64,             // "second"
        
        // No extra empty chunk after our fix
    ];
    
    // Print actual bytes if different from expected (for debugging)
    if data != EXPECTED_TWO_RECORDS_FILE {
        panic!("Actual bytes:\n{}", format_bytes_for_assert(&data));
    }
    
    assert_eq!(data.len(), EXPECTED_TWO_RECORDS_FILE.len(), "File sizes don't match");
    assert_eq!(data, EXPECTED_TWO_RECORDS_FILE, "File content doesn't match expected");
}

/// Test writing a record that crosses multiple block boundaries with predictable sizes
#[test]
fn test_record_crossing_multiple_block_boundaries() {
    // Create a cursor as our sink
    let cursor = Cursor::new(Vec::new());
    
    // Create a writer with small block size for easier testing
    // Use a 100-byte block size (much smaller than the default 64KB)
    let config = RecordWriterConfig {
        compression_type: CompressionType::None,
        block_config: crate::blocks::writer::BlockWriterConfig::with_block_size(100).unwrap(),
        ..Default::default()
    };
    
    let mut writer = RecordWriter::with_config(cursor, config).unwrap();
    
    // Create a record large enough to cross multiple block boundaries
    // After the 64-byte signature (24-byte block header + 40-byte chunk header),
    // we've used 64 bytes of the first 100-byte block. So we have 36 bytes left.
    // Let's create a record that's 200 bytes to cross at least 2 more block boundaries.
    let record = vec![b'X'; 200];
    
    // Write the record
    writer.write_record(&record).unwrap();
    
    // Close to ensure all data is written
    writer.close().unwrap();
    
    // Get the written data
    let data = writer.get_data().unwrap();
    
    // Expected bytes produced by our implementation for a file with a 200-byte record crossing multiple blocks
    // The total file is exactly 380 bytes long
    #[rustfmt::skip]
    const EXPECTED_BYTES: &[u8] = &[
        //=================================================================================
        // BLOCK 1 (bytes 0-99): Initial block with file signature and chunk header (100 bytes)
        //=================================================================================
        
        // Block header (24 bytes) at position 0
        0x83, 0xaf, 0x70, 0xd1, 0x0d, 0x88, 0x4a, 0x3f, // header_hash
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // previous_chunk (0 bytes)
        0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // next_chunk (64 bytes)
        
        // Signature chunk header (40 bytes) at position 24
        0x91, 0xba, 0xc2, 0x3c, 0x92, 0x87, 0xe1, 0xa9, // header_hash
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // data_size (0 bytes)
        0xe1, 0x9f, 0x13, 0xc0, 0xe9, 0xb1, 0xc3, 0x72, // data_hash
        0x73, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk_type('s') + num_records (0)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // decoded_data_size (0 bytes)
        
        // Records chunk header (36 bytes) at position 64 - partial, continues in next block
        0xd4, 0x1e, 0x2f, 0x19, 0x31, 0x68, 0x3b, 0x49, // header_hash
        0xcc, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // data_size (204 bytes including varint)
        0xb4, 0x43, 0x03, 0x51, 0xde, 0x79, 0xc9, 0x5d, // data_hash
        0x72, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk_type('r') + num_records (1)
        0xc8, 0x00, 0x00, 0x00,                         // partial decoded_data_size (200 bytes)
        
        //=================================================================================
        // BLOCK 2 (bytes 100-199): Continuation of records chunk header + start of data (100 bytes)
        //=================================================================================
        
        // Block header (24 bytes) at position 100
        0x77, 0x7a, 0x72, 0x39, 0x59, 0xa0, 0xe2, 0x2b, // header_hash
        0x24, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // previous_chunk (36 bytes from chunk start to block boundary)
        0xe8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // next_chunk (232 bytes to end of chunk including header)
        
        // Remaining 4 bytes of records chunk header + record data
        0x00, 0x00, 0x00, 0x00,                         // remaining decoded_data_size bytes
        
        // Record chunk data starts here (72 bytes in this block)
        0x00,                                           // compression_type (None)
        0x02,                                           // compressed_sizes_size (varint 2)
        0xc8, 0x01,                                     // compressed_sizes (varint 200)
        
        // First part of 200-byte record content (68 bytes in this block)
        0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, // "XXXXXXXX" (bytes 1-8)
        0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, // "XXXXXXXX" (bytes 9-16)
        0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, // "XXXXXXXX" (bytes 17-24)
        0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, // "XXXXXXXX" (bytes 25-32)
        0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, // "XXXXXXXX" (bytes 33-40)
        0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, // "XXXXXXXX" (bytes 41-48)
        0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, // "XXXXXXXX" (bytes 49-56)
        0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, // "XXXXXXXX" (bytes 57-64)
        0x58, 0x58, 0x58, 0x58,                         // "XXXX" (bytes 65-68)
        
        //=================================================================================
        // BLOCK 3 (bytes 200-299): Middle part of record data (100 bytes)
        //=================================================================================
        
        // Block header (24 bytes) at position 200
        0x62, 0xa7, 0x1b, 0x9c, 0x90, 0x61, 0xd2, 0xb3, // header_hash
        0x88, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // previous_chunk (136 bytes from chunk start to block boundary)
        0x9c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // next_chunk (156 bytes to end of chunk including header)
        
        // Middle part of 200-byte record content (76 bytes in this block)
        0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, // "XXXXXXXX" (bytes 69-76)
        0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, // "XXXXXXXX" (bytes 77-84)
        0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, // "XXXXXXXX" (bytes 85-92)
        0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, // "XXXXXXXX" (bytes 93-100)
        0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, // "XXXXXXXX" (bytes 101-108)
        0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, // "XXXXXXXX" (bytes 109-116)
        0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, // "XXXXXXXX" (bytes 117-124)
        0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, // "XXXXXXXX" (bytes 125-132)
        0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, // "XXXXXXXX" (bytes 133-140)
        0x58, 0x58, 0x58, 0x58,                         // "XXXX" (bytes 141-144)
        
        //=================================================================================
        // BLOCK 4 (bytes 300-379): Final part of record data (80 bytes)
        //=================================================================================
        
        // Block header (24 bytes) at position 300
        0x53, 0xa7, 0x07, 0x22, 0x61, 0x71, 0x2e, 0x59, // header_hash
        0xec, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // previous_chunk (236 bytes from chunk start to block boundary)
        0x50, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // next_chunk (80 bytes to end of chunk including header)
        
        // Final part of 200-byte record content (56 bytes in this block)
        0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, // "XXXXXXXX" (bytes 145-152)
        0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, // "XXXXXXXX" (bytes 153-160)
        0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, // "XXXXXXXX" (bytes 161-168)
        0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, // "XXXXXXXX" (bytes 169-176)
        0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, // "XXXXXXXX" (bytes 177-184)
        0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, // "XXXXXXXX" (bytes 185-192)
        0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58, 0x58  // "XXXXXXXX" (bytes 193-200)
    ];
    
    // Verify the actual bytes match our expected bytes
    // Check that the actual output size is 380 bytes
    if data.len() != 380 {
        panic!("Output data size mismatch: expected 380 bytes, but got {} bytes", data.len());
    }
    
    // Only compare the first 380 bytes
    if EXPECTED_BYTES.len() < 380 {
        panic!("Expected bytes array is too short, should be 380 bytes but is {} bytes", 
                EXPECTED_BYTES.len());
    }
    
    // Compare each byte with our expected values
    for i in 0..380 {
        if data[i] != EXPECTED_BYTES[i] {
            panic!("Byte mismatch at position {}: expected 0x{:02x}, found 0x{:02x}\n\nActual bytes:\n{}",
                    i, EXPECTED_BYTES[i], data[i], format_bytes_for_assert(&data[0..i+1]));
        }
    }
    
    // Additional checks for block structure
    assert_eq!(data.len(), 380, "File size should be exactly 380 bytes");
    
    // First block header (at position 0)
    assert_eq!(&data[0..8], &[0x83, 0xaf, 0x70, 0xd1, 0x0d, 0x88, 0x4a, 0x3f], 
                "First block header hash incorrect");
    assert_eq!(data[8..16], [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], 
                "First block header previous_chunk should be 0");
    assert_eq!(data[16..24], [0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], 
                "First block header next_chunk should be 64 bytes for the file signature");
                
    // Second block header (at position 100)
    assert_eq!(data[108..116], [0x24, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], 
                "Second block header previous_chunk should be 36 bytes (100-64)");
    assert_eq!(data[116..124], [0xe8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], 
                "Second block header next_chunk should include header size (232 bytes)");
    
    // Third block header (at position 200)
    assert_eq!(data[208..216], [0x88, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], 
                "Third block header previous_chunk should be 136 bytes (200-64)");
    assert_eq!(data[216..224], [0x9c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], 
                "Third block header next_chunk should include header size (156 bytes)");
}

/// Test specifically to verify that our fix for the empty chunk bug works
#[test]
fn test_no_extra_empty_chunk() {
    // Create a cursor as our sink
    let cursor = Cursor::new(Vec::new());
    
    // Create a writer with fixed compression type for consistent output
    let config = RecordWriterConfig {
        compression_type: CompressionType::None,
        ..Default::default()
    };
    
    let mut writer = RecordWriter::with_config(cursor, config).unwrap();
    
    // Write a single record with known content
    writer.write_record(b"test-record").unwrap();
    
    // Now we'll verify the state transitions
    assert_eq!(writer.get_state(), &WriterState::RecordsWritten);
    
    // Flush the chunk
    writer.flush_chunk().unwrap();
    
    // After flushing, state should be Flushed
    assert_eq!(writer.get_state(), &WriterState::Flushed);
    
    // Flushing again should not create another chunk
    writer.flush().unwrap();
    writer.flush_chunk().unwrap(); // This should be a no-op due to the state check
    
    // State should still be Flushed
    assert_eq!(writer.get_state(), &WriterState::Flushed);
    
    // Get the written data
    let data = writer.get_data().unwrap();
    
    // Expected bytes for a file with our single flushed record chunk
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
        
        // Records chunk header (40 bytes)
        0xbd, 0xb9, 0x1d, 0x4e, 0x15, 0xe0, 0x15, 0x9b, // header_hash
        0x0e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // data_size (14 bytes)
        0x7e, 0x26, 0xcb, 0x1d, 0xa1, 0xb3, 0xe9, 0x8a, // data_hash
        0x72, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk_type('r') + num_records(1)
        0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // decoded_data_size (11 bytes)
        
        // Records chunk data (14 bytes)
        0x00,                                           // compression_type (None)
        0x01,                                           // compressed_sizes_size (varint 1)
        0x0b,                                           // compressed_sizes (varint 11)
        
        // Record value (11 bytes)
        0x74, 0x65, 0x73, 0x74, 0x2d, 0x72, 0x65, 0x63, 0x6f, 0x72, 0x64,  // "test-record"
        
        // No extra empty chunk with our fix
    ];
    
    if data != EXPECTED_FILE {
        panic!("Actual bytes:\n{}", format_bytes_for_assert(&data));
    }
    
    assert_eq!(data.len(), EXPECTED_FILE.len(), "File sizes don't match");
    assert_eq!(data, EXPECTED_FILE, "File content doesn't match expected");
}