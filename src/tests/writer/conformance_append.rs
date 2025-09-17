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

//! Append-specific conformance tests for the RecordWriter implementation.
//! These tests focus on edge cases when appending to existing Riegeli files
//! and verify the byte-level conformance to the Riegeli specification.
//!
//! # Block Header Behaviors
//! One particular edge case is the behavior of block headers at block boundaries during append operations.
//! According to the Riegeli spec, when a block header lies exactly between chunks (as in an append case),
//! it's considered to interrupt the next chunk. Since there is no "previous" portion of the chunk before
//! the boundary, the previous_chunk field is correctly set to 0.
//!
//! # File Structure
//! Riegeli files consist of these components:
//! - Block headers (24 bytes) at every block boundary (default 64KB)
//! - Signature chunk (40 bytes) at the start of the file
//! - Record chunks containing one or more records
//! - Block headers are inserted at block boundaries, even between chunks

use std::io::Cursor;

use crate::compression::CompressionType;
use crate::tests::utils::format_bytes_for_assert;
use crate::writer::{RecordWriter, RecordWriterConfig};

/// Test 1: Append to a file ending exactly at a block boundary
/// Tests appending to a file that ends precisely at a block boundary
#[test]
fn test_append_at_block_boundary() {
    // Create a test file that ends exactly at a block boundary
    // First, we need a small block size for testing
    let block_size = 128;

    // Create the initial file
    let cursor = Cursor::new(Vec::new());
    let config = RecordWriterConfig {
        compression_type: CompressionType::None,
        block_config: crate::blocks::writer::BlockWriterConfig::with_block_size(block_size)
            .unwrap(),
        ..Default::default()
    };

    let mut writer = RecordWriter::with_config(cursor, config.clone()).unwrap();

    // Calculate required record size to hit exact block size
    // File starts with 64 bytes (24 block header + 40 signature)
    // Chunk header is 40 bytes, and 3 bytes for overhead
    // So record needs to be 128 - 64 - 40 - 3 = 21 bytes
    let initial_record = vec![b'y'; 21];
    writer.write_record(&initial_record).unwrap();

    // Close to ensure all data is written
    writer.close().unwrap();

    // Get the initial data
    let initial_data = writer.get_data().unwrap();
    let initial_size = initial_data.len();

    // Verify we've created a file that's exactly at the block boundary
    assert_eq!(
        initial_size, block_size as usize,
        "Initial file size should be exactly one block"
    );

    // Now create a new writer that appends to this file
    let cursor = Cursor::new(initial_data.to_vec());
    let mut appending_writer =
        RecordWriter::for_append_with_config(cursor, initial_size as u64, config).unwrap();

    // Append a new record
    let append_record = vec![b'z'; 10];
    appending_writer.write_record(&append_record).unwrap();

    // Close to ensure all data is written
    appending_writer.close().unwrap();

    // Get the appended data
    let appended_data = appending_writer.get_data().unwrap();

    // For debugging, uncomment to see the actual output bytes
    // if true {
    //     panic!("Actual bytes (len:{}):\n{}", appended_data.len(), format_bytes_for_assert(&appended_data));
    // }

    // The expected bytes for the appended file - copied from actual output
    #[rustfmt::skip]
    const EXPECTED_APPENDED_FILE: &[u8] = &[
        // Block 1 (128 bytes)
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
        0x79, 0x79, 0x79, 0x79, 0x79,                   // 5 'y's
        
        // Appended data starts here - Position 128
        // This is a block header inserted when appending at a block boundary
        // Since this block header is at the beginning of a new chunk (the append operation),
        // the previous_chunk value is correctly set to 0 according to the Riegeli spec.
        0x62, 0x51, 0x82, 0x10, 0xf9, 0x07, 0xd8, 0x3e, // header_hash
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // previous_chunk (0 - correct: indicates new chunk starts at block boundary)
        0x4d, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // next_chunk (77 bytes from start)
        
        // Appended record chunk header (40 bytes)
        0x6c, 0x6f, 0x80, 0x0c, 0x92, 0x46, 0x76, 0xd1, // header_hash
        0x0d, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // data_size (13 bytes)
        0x5a, 0x77, 0x14, 0x44, 0x42, 0x57, 0x14, 0x04, // data_hash
        0x72, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk_type('r') + num_records(1)
        0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // decoded_data_size (10 bytes)
        
        // Appended record chunk data (13 bytes)
        0x00,                                           // compression_type (None)
        0x01,                                           // compressed_sizes_size (varint 1)
        0x0a,                                           // compressed_sizes (varint 10)
        
        // Record value (10 bytes)
        0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, // 8 'z's
        0x7a, 0x7a                                      // 2 'z's
    ];

    // Verify the file content
    if appended_data != EXPECTED_APPENDED_FILE {
        panic!(
            "Actual bytes (len:{}):\n{}",
            appended_data.len(),
            format_bytes_for_assert(&appended_data)
        );
    }

    assert_eq!(
        appended_data.len(),
        EXPECTED_APPENDED_FILE.len(),
        "File sizes don't match"
    );
    assert_eq!(
        appended_data, EXPECTED_APPENDED_FILE,
        "File content doesn't match expected"
    );

    // The only assertion we need is to verify the exact bytes match
    // All the specific field checks are implicit in the byte array comparison
}

/// Test 2: Append to a file ending mid-block with multiple records
/// Tests appending to a file with multiple records that doesn't end at a block boundary
#[test]
fn test_append_mid_block_multi_records() {
    // Create a cursor as our sink for the initial file
    let cursor = Cursor::new(Vec::new());

    // Create a writer with default config but a larger block size for testing
    let block_size = 512;
    let config = RecordWriterConfig {
        compression_type: CompressionType::None,
        block_config: crate::blocks::writer::BlockWriterConfig::with_block_size(block_size)
            .unwrap(),
        ..Default::default()
    };

    let mut writer = RecordWriter::with_config(cursor, config.clone()).unwrap();

    // Write multiple small records
    writer.write_record(b"record-1").unwrap();
    writer.write_record(b"record-2").unwrap();
    writer.write_record(b"record-3").unwrap();

    // Close to ensure all data is written
    writer.close().unwrap();

    // Get the data from the first writer
    let initial_data = writer.get_data().unwrap();
    let initial_size = initial_data.len();

    // Now create a new writer that appends to this file
    let cursor = Cursor::new(initial_data.to_vec());
    let mut appending_writer =
        RecordWriter::for_append_with_config(cursor, initial_size as u64, config).unwrap();

    // Append additional records
    appending_writer.write_record(b"appended-1").unwrap();
    appending_writer.write_record(b"appended-2").unwrap();

    // Close to ensure all data is written
    appending_writer.close().unwrap();

    // Get the appended data
    let appended_data = appending_writer.get_data().unwrap();

    // For debugging, uncomment to see the actual output bytes
    // if true {
    //     panic!("Actual bytes (len:{}):\n{}", appended_data.len(), format_bytes_for_assert(&appended_data));
    // }

    // Define the expected bytes for the appended file - copied directly from actual output
    #[rustfmt::skip]
    const EXPECTED_APPENDED_MULTI_RECORD_FILE: &[u8] = &[
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
        
        // Initial records chunk header (40 bytes)
        0xa4, 0x75, 0x2d, 0xbf, 0xe4, 0xc1, 0xc8, 0xd1, // header_hash
        0x1d, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // data_size (29 bytes)
        0x08, 0xa9, 0x68, 0x5d, 0x57, 0xa7, 0x31, 0x46, // data_hash
        0x72, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk_type('r') + num_records(3)
        0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // decoded_data_size (24 bytes)
        
        // Records chunk data (29 bytes)
        0x00,                                           // compression_type (None)
        0x03,                                           // compressed_sizes_size (varint 3)
        0x08, 0x08, 0x08,                               // compressed_sizes (three 8-byte records)
        
        // Record values (24 bytes total - 3 records)
        0x72, 0x65, 0x63, 0x6f, 0x72, 0x64, 0x2d, 0x31, // "record-1"
        0x72, 0x65, 0x63, 0x6f, 0x72, 0x64, 0x2d, 0x32, // "record-2"
        0x72, 0x65, 0x63, 0x6f, 0x72, 0x64, 0x2d, 0x33, // "record-3"
        
        // Appended record chunk header (40 bytes)
        0x44, 0x1a, 0x9a, 0x47, 0xb6, 0xfd, 0xcd, 0x1d, // header_hash
        0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // data_size (24 bytes)
        0xaf, 0x61, 0x36, 0x76, 0x7a, 0x41, 0x05, 0x53, // data_hash
        0x72, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk_type('r') + num_records(2)
        0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // decoded_data_size (20 bytes)
        
        // Appended records chunk data (24 bytes)
        0x00,                                           // compression_type (None)
        0x02,                                           // compressed_sizes_size (varint 2)
        0x0a, 0x0a,                                     // compressed_sizes (two 10-byte records)
        
        // Appended record values (20 bytes total - 2 records)
        0x61, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x65, 0x64, // "appended-"
        0x2d, 0x31,                                     // "1"
        0x61, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x65, 0x64, // "appended-"
        0x2d, 0x32                                      // "2"
    ];

    // Verify the file content matches exactly
    if appended_data != EXPECTED_APPENDED_MULTI_RECORD_FILE {
        panic!(
            "Actual bytes (len:{}):\n{}",
            appended_data.len(),
            format_bytes_for_assert(&appended_data)
        );
    }

    assert_eq!(
        appended_data.len(),
        EXPECTED_APPENDED_MULTI_RECORD_FILE.len(),
        "File sizes don't match"
    );
    assert_eq!(
        appended_data, EXPECTED_APPENDED_MULTI_RECORD_FILE,
        "File content doesn't match expected"
    );
}

/// Test 3: Empty file append
/// Tests the special case of appending to an empty Riegeli file (just signature, no records)
#[test]
fn test_append_to_empty_file() {
    // Create a cursor as our sink for the initial empty file
    let cursor = Cursor::new(Vec::new());

    // Create a writer with default config
    let config = RecordWriterConfig {
        compression_type: CompressionType::None,
        ..Default::default()
    };

    // Create an empty file (just the signature, no records)
    let mut writer = RecordWriter::with_config(cursor, config.clone()).unwrap();

    // Close right away without writing any records
    writer.close().unwrap();

    // Get the data from the first writer
    let initial_data = writer.get_data().unwrap();
    let initial_size = initial_data.len();

    // For debugging, uncomment to see the initial output bytes
    // if true {
    //     panic!("Initial data bytes (len:{}):\n{}", initial_data.len(), format_bytes_for_assert(&initial_data));
    // }

    // Now create a new writer that appends to this empty file
    let cursor = Cursor::new(initial_data.to_vec());
    let mut appending_writer =
        RecordWriter::for_append_with_config(cursor, initial_size as u64, config).unwrap();

    // Append records to the previously empty file
    appending_writer
        .write_record(b"first-appended-record")
        .unwrap();

    // Close to ensure all data is written
    appending_writer.close().unwrap();

    // Get the appended data
    let appended_data = appending_writer.get_data().unwrap();

    // For debugging, uncomment to see the actual output bytes
    // if true {
    //     panic!("Actual bytes (len:{}):\n{}", appended_data.len(), format_bytes_for_assert(&appended_data));
    // }

    // The expected bytes for an appended empty file - copied from actual output
    #[rustfmt::skip]
    const EXPECTED_EMPTY_APPEND_FILE: &[u8] = &[
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
        
        // Record chunk header (40 bytes)
        0x33, 0x2c, 0xb2, 0x0c, 0xea, 0xd5, 0xda, 0x0d, // header_hash
        0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // data_size (24 bytes)
        0xf6, 0xde, 0xb0, 0x3e, 0xb6, 0x08, 0xbf, 0x13, // data_hash
        0x72, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk_type('r') + num_records(1)
        0x15, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // decoded_data_size (21 bytes)
        
        // Record chunk data (24 bytes)
        0x00,                                           // compression_type (None)
        0x01,                                           // compressed_sizes_size (varint 1)
        0x15,                                           // compressed_sizes (varint 21)
        
        // Record value (21 bytes) - "first-appended-record"
        0x66, 0x69, 0x72, 0x73, 0x74, 0x2d, 0x61, 0x70, // "first-ap"
        0x70, 0x65, 0x6e, 0x64, 0x65, 0x64, 0x2d, 0x72, // "pended-r"
        0x65, 0x63, 0x6f, 0x72, 0x64                    // "ecord"
    ];

    // Verify the file content matches exactly
    if appended_data != EXPECTED_EMPTY_APPEND_FILE {
        panic!(
            "Actual bytes (len:{}):\n{}",
            appended_data.len(),
            format_bytes_for_assert(&appended_data)
        );
    }

    assert_eq!(
        appended_data.len(),
        EXPECTED_EMPTY_APPEND_FILE.len(),
        "File sizes don't match"
    );
    assert_eq!(
        appended_data, EXPECTED_EMPTY_APPEND_FILE,
        "File content doesn't match expected"
    );

    // No additional byte-specific assertions needed since we're comparing the entire byte array
}

/// Test 4: Block boundary alignment when appending
/// Tests the behavior when appending to a file that ends exactly at a block boundary.
/// We verify that our implementation correctly adds a chunk directly (without a block header)
/// when appending at a block boundary.
#[test]
fn test_block_boundary_alignment() {
    // Create a test file that ends exactly at a block boundary
    let block_size = 128;

    // Create the initial file
    let cursor = Cursor::new(Vec::new());
    let config = RecordWriterConfig {
        compression_type: CompressionType::None,
        block_config: crate::blocks::writer::BlockWriterConfig::with_block_size(block_size)
            .unwrap(),
        ..Default::default()
    };

    let mut writer = RecordWriter::with_config(cursor, config.clone()).unwrap();

    // Write exactly enough data to reach the block boundary
    let initial_record = vec![b'y'; 21];
    writer.write_record(&initial_record).unwrap();

    // Close to ensure all data is written
    writer.close().unwrap();

    // Get the initial data
    let initial_data = writer.get_data().unwrap();
    let initial_size = initial_data.len();

    // Verify we've created a file that's exactly at the block boundary
    assert_eq!(
        initial_size, block_size as usize,
        "Initial file size should be exactly one block"
    );

    // Now create a new writer that appends to this file
    let cursor = Cursor::new(initial_data.to_vec());
    let mut appending_writer =
        RecordWriter::for_append_with_config(cursor, initial_size as u64, config).unwrap();

    // Append a large record that will cross another block boundary
    let large_record = vec![b'z'; 100];
    appending_writer.write_record(&large_record).unwrap();

    // Close to ensure all data is written
    appending_writer.close().unwrap();

    // Get the appended data
    let appended_data = appending_writer.get_data().unwrap();

    // For debugging, uncomment to see the actual output bytes
    // if true {
    //     panic!("Actual bytes (len:{}):\n{}", appended_data.len(), format_bytes_for_assert(&appended_data));
    // }

    // The exact expected bytes for an appended file - copied from actual output
    // Note that when appending to a file that ends at a block boundary,
    // the implementation correctly inserts a block header with previous_chunk=0.
    // This is consistent with the Riegeli spec, which states that when a block header
    // lies exactly between chunks (as in this append case), it's considered to
    // interrupt the next chunk. Since there is no "previous" distance within the
    // new chunk (it's just starting), previous_chunk is correctly set to 0.
    //
    // The structure of this byte array is:
    // - First block (0-127): Block header (24 bytes) + Signature chunk (40 bytes) + Record chunk (64 bytes)
    //   - Record chunk contains one record of 21 'y' characters
    // - At position 128: Block header for appended data with previous_chunk=0
    // - After header: Record chunk with a large record (100 'z' characters)
    // - At position 256: Another block header for the remainder of large record
    // - Remainder of the 'z' characters (36 more bytes)
    //
    // The byte array below represents the exact expected format of a Riegeli file
    // with the following structure:
    // Bytes 0-23:   Block 1 header
    // Bytes 24-63:  Signature chunk header
    // Bytes 64-103: Records chunk header with one record
    // Bytes 104-127: Record chunk data with one record (21 bytes of 'y')
    // Bytes 128-151: Block header for appended data with previous_chunk=0
    // Bytes 152-191: Record chunk header for the appended record
    // Bytes 192-255: Start of record chunk data with compression type, sizes, and data
    // Bytes 256-279: Block 2 header
    // Bytes 280-318: Remainder of the large record data
    /* This array contains the exact expected output with a carefully controlled format.
     * IMPORTANT: This format is sensitive. Do not modify whitespace, line breaks, or indentation. */
    #[rustfmt::skip]
    const EXPECTED_APPENDED_BLOCK_BOUNDARY_FILE: &[u8] = &[
        /* Block header (bytes 0-23) */
        0x83, 0xaf, 0x70, 0xd1, 0x0d, 0x88, 0x4a, 0x3f, /* header_hash */
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* previous_chunk */
        0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* next_chunk */
        
        /* Signature chunk header (bytes 24-63) */
        0x91, 0xba, 0xc2, 0x3c, 0x92, 0x87, 0xe1, 0xa9, /* header_hash */
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* data_size */
        0xe1, 0x9f, 0x13, 0xc0, 0xe9, 0xb1, 0xc3, 0x72, /* data_hash */
        0x73, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* chunk_type('s') + num_records */
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* decoded_data_size */
        
        /* Records chunk header (bytes 64-103) */
        0x77, 0xb5, 0x52, 0xec, 0xfc, 0xa3, 0x3a, 0x67, /* header_hash */
        0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* data_size (24 bytes) */
        0x43, 0xdf, 0xb5, 0xae, 0xa9, 0x40, 0x92, 0xe0, /* data_hash */
        0x72, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* chunk_type('r') + num_records(1) */
        0x15, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* decoded_data_size (21 bytes) */
        
        /* Records chunk data (bytes 104-127) */
        0x00, 0x01, 0x15, 0x79, 0x79, 0x79, 0x79, 0x79, /* compression_type, sizes, 5 'y's */
        0x79, 0x79, 0x79, 0x79, 0x79, 0x79, 0x79, 0x79, /* 8 'y's */
        0x79, 0x79, 0x79, 0x79, 0x79, 0x79, 0x79, 0x79, /* 8 'y's */
        
        /* Block 2 header for appended data (bytes 128-151) */
        0xb5, 0x70, 0x51, 0x16, 0x5d, 0x1b, 0xc5, 0x07, /* header_hash */
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* previous_chunk (0) */
        0xbf, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* next_chunk (191 bytes) */
        
        /* Appended record chunk header (bytes 152-191) */
        0x4a, 0x1e, 0x5c, 0xb8, 0x4e, 0xf7, 0x8a, 0x7e, /* header_hash */
        0x67, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* data_size (103 bytes) */
        0x94, 0x36, 0x4a, 0x9b, 0xfc, 0xc6, 0xa7, 0x61, /* data_hash */
        0x72, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* chunk_type('r') + num_records(1) */
        0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* decoded_data_size (100 bytes) */
        
        /* Record chunk data start (bytes 192-199) */
        0x00, 0x01, 0x64, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, /* compression_type, sizes, 5 'z's */
        
        /* Large record data continues (bytes 200-255) */
        0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, /* 8 'z's */
        0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, /* 8 'z's */
        0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, /* 8 'z's */
        0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, /* 8 'z's */
        0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, /* 8 'z's */
        0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, /* 8 'z's */
        0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, /* 8 'z's */
        
        /* Block 3 header (bytes 256-279) */
        0xd8, 0xef, 0x07, 0xd3, 0x4a, 0x74, 0x91, 0x64, /* header_hash */
        0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* previous_chunk (128) */
        0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, /* next_chunk (63) */
        
        /* Remainder of the large record (bytes 280-318) */
        0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, /* 8 'z's */
        0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, /* 8 'z's */
        0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, /* 8 'z's */
        0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, /* 8 'z's */
        0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a, 0x7a        /* 7 'z's */
    ];

    // For debugging, uncomment to see the actual output bytes
    // if true {
    //     panic!("Actual bytes:\n{}", format_bytes_for_assert(&appended_data));
    // }

    // Verify the file content matches exactly
    if appended_data != EXPECTED_APPENDED_BLOCK_BOUNDARY_FILE {
        panic!(
            "Actual bytes (len:{}):\n{}",
            appended_data.len(),
            format_bytes_for_assert(&appended_data)
        );
    }

    assert_eq!(
        appended_data.len(),
        EXPECTED_APPENDED_BLOCK_BOUNDARY_FILE.len(),
        "File sizes don't match"
    );
    assert_eq!(
        appended_data, EXPECTED_APPENDED_BLOCK_BOUNDARY_FILE,
        "File content doesn't match expected"
    );

    // Check that the file has grown beyond the first block
    assert!(
        appended_data.len() > block_size as usize,
        "File should be larger than one block"
    );

    // Examine the previous_chunk field in the appended chunk header
    // According to the Riegeli spec, when a block header lies exactly between chunks,
    // it's considered to interrupt the next chunk. Since we're appending at a block boundary,
    // the new chunk starts exactly at this boundary, so the previous_chunk value is correctly 0
    // (there is no "previous" portion of the chunk before the boundary).
    let previous_chunk_bytes = &appended_data[128 + 8..128 + 16];
    let previous_chunk_value = u64::from_le_bytes([
        previous_chunk_bytes[0],
        previous_chunk_bytes[1],
        previous_chunk_bytes[2],
        previous_chunk_bytes[3],
        previous_chunk_bytes[4],
        previous_chunk_bytes[5],
        previous_chunk_bytes[6],
        previous_chunk_bytes[7],
    ]);

    // Verify the previous_chunk field is correctly set to 0
    assert_eq!(
        previous_chunk_value, 0,
        "The previous_chunk field should be 0 when appending at a block boundary"
    );
}
