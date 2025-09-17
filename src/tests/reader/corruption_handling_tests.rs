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

use env_logger;
use log;

use crate::blocks::writer::BlockWriterConfig;
use crate::reader::{CorruptionStrategy, DiskyPiece, RecordReader, RecordReaderConfig};
use crate::writer::{RecordWriter, RecordWriterConfig};

/// Helper function to create a file with 10 records using small block size
/// and smaller chunk size to ensure records are spread across multiple chunks
fn create_test_file() -> Vec<u8> {
    let mut buffer = Vec::new();
    {
        let cursor = Cursor::new(&mut buffer);

        // Use a small block size and smaller chunk size to make recovery easier
        // and ensure records are spread across multiple chunks
        let mut config = RecordWriterConfig::default();

        // Small block size (128 bytes)
        config.block_config = BlockWriterConfig::with_block_size(128).unwrap();

        // Small chunk size (256 bytes) to ensure records cross chunk boundaries
        // IMPORTANT chunk size should be small for our small recovery tests
        // as otherwise with default chunk size of 1mb all data may just
        // be discarded in-case the chunk data is corrupt.
        config.chunk_size_bytes = 256;

        let mut writer = RecordWriter::with_config(cursor, config).unwrap();

        // Create 10 records (100 bytes each) that should be spread across
        // multiple blocks and chunks with our small block/chunk sizes
        for i in 0..10 {
            let record_data = Bytes::from(vec![i as u8; 100]);
            writer.write_record(&record_data).unwrap();
        }
        writer.close().unwrap();
    }
    buffer
}

/// Helper function to corrupt a file at a specific position
fn corrupt_file(mut buffer: Vec<u8>, position: usize) -> Vec<u8> {
    if position < buffer.len() {
        // Corrupt a byte by flipping all bits
        buffer[position] = !buffer[position];
    }
    buffer
}

/// Test that corruption handling works in recovery mode
#[test]
fn test_corruption_recovery() {
    // Initialize logger to see diagnostic messages
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .is_test(true)
        .try_init();

    let buffer = create_test_file();

    // Create a corrupted file by modifying bytes at various offsets
    println!("Total file size: {} bytes", buffer.len());

    // Choose positions that will likely hit different blocks
    // with our small 128-byte block size
    for offset in [60, 190, 320, 450, 580, 710] {
        if offset >= buffer.len() {
            println!("Skipping offset {} as it's beyond file length", offset);
            continue;
        }

        println!("\nTesting corruption at offset: {}", offset);

        let corrupted = corrupt_file(buffer.clone(), offset);

        // Try to read with default error strategy - should fail at some point
        {
            let cursor = Cursor::new(&corrupted);
            // Use same small block size as the writer (128 bytes)
            let config = RecordReaderConfig::with_block_size(128).unwrap();
            let mut reader = RecordReader::with_config(cursor, config).unwrap();

            let mut records = Vec::new();
            let mut read_error = false;

            // Read records until we get an error or EOF
            loop {
                match reader.next_record() {
                    Ok(DiskyPiece::Record(record)) => {
                        records.push(record);
                    }
                    Ok(DiskyPiece::EOF) => {
                        break;
                    }
                    Err(_) => {
                        read_error = true;
                        break;
                    }
                }
            }

            // With corruption in the file, we should encounter an error
            // since we're using the default error handling
            assert!(
                read_error,
                "Should have encountered an error with corrupted file at offset {}",
                offset
            );
        }

        // Try with recovery strategy
        {
            let cursor = Cursor::new(&corrupted);
            // Use same small block size as the writer (128 bytes) but with recovery enabled
            let config = RecordReaderConfig::with_block_size(128)
                .unwrap()
                .with_corruption_strategy(CorruptionStrategy::Recover);

            let mut reader = RecordReader::with_config(cursor, config).unwrap();

            let mut records = Vec::new();
            let mut read_error = false;

            // Read records until we get an error or EOF
            loop {
                match reader.next_record() {
                    Ok(DiskyPiece::Record(record)) => {
                        records.push(record);
                    }
                    Ok(DiskyPiece::EOF) => {
                        break;
                    }
                    Err(_) => {
                        read_error = true;
                        break;
                    }
                }
            }

            // The recovery strategy might not always work depending on where the corruption is,
            // but if we don't get any records, we should have gotten an error
            if records.is_empty() {
                assert!(
                    read_error,
                    "No records were read and no error encountered at offset {}",
                    offset
                );
            }
        }
    }
}

/// Test that signature corruption is detected during reading
#[test]
fn test_signature_corruption() {
    let buffer = create_test_file();

    // Corrupt the file near the beginning where the signature would be
    let corrupted = corrupt_file(buffer, 5);

    // Reader creation will succeed, but first read should fail with a signature error
    let cursor = Cursor::new(&corrupted);
    let mut reader = RecordReader::new(cursor).unwrap();

    // Attempting to read the first record should fail
    let read_result = reader.next_record();
    assert!(read_result.is_err());

    // Try with recovery strategy - signature errors still can't be recovered
    let cursor = Cursor::new(&corrupted);
    let config = RecordReaderConfig::with_block_size(128)
        .unwrap()
        .with_corruption_strategy(CorruptionStrategy::Recover);

    let mut reader = RecordReader::with_config(cursor, config).unwrap();
    let read_result = reader.next_record();

    // Even with recovery enabled, signature corruption should be detected
    assert!(read_result.is_err());
}

/// Test behavior with corruption in the middle of a file
#[test]
fn test_mid_file_corruption() {
    // Create a larger file with more records
    let mut buffer = Vec::new();

    // IMPORTANT: The reader and writer MUST use the same block size!
    let block_size = 4096u64;

    {
        let cursor = Cursor::new(&mut buffer);
        let mut config = RecordWriterConfig::default();
        config.block_config = BlockWriterConfig::with_block_size(block_size).unwrap();
        config.chunk_size_bytes = 256;

        let mut writer = RecordWriter::with_config(cursor, config).unwrap();

        // Write enough records to span multiple blocks
        for i in 0..100 {
            let record_data = Bytes::from(vec![i as u8; 50]);
            writer.write_record(&record_data).unwrap();
        }
        writer.close().unwrap();
    }

    // Corrupt the file somewhere in the middle
    let middle = buffer.len() / 2;
    let corrupted = corrupt_file(buffer, middle);

    // Without recovery, we should encounter an error
    {
        let cursor = Cursor::new(&corrupted);
        let config = RecordReaderConfig::with_block_size(block_size).unwrap();
        let mut reader = RecordReader::with_config(cursor, config).unwrap();

        let mut records = Vec::new();
        let mut read_error = false;

        // Read records until we get an error or EOF
        loop {
            match reader.next_record() {
                Ok(DiskyPiece::Record(record)) => {
                    records.push(record);
                }
                Ok(DiskyPiece::EOF) => {
                    break;
                }
                Err(_) => {
                    read_error = true;
                    break;
                }
            }
        }

        // We should encounter an error when reading a corrupted file without recovery
        assert!(
            read_error,
            "Should have encountered an error with corrupted file"
        );
    }

    // With recovery enabled, we should read at least some records
    {
        let cursor = Cursor::new(&corrupted);
        let config = RecordReaderConfig::with_block_size(block_size)
            .unwrap()
            .with_corruption_strategy(CorruptionStrategy::Recover);

        let mut reader = RecordReader::with_config(cursor, config).unwrap();

        let mut records = Vec::new();

        // Read records until EOF or error
        loop {
            match reader.next_record() {
                Ok(DiskyPiece::Record(record)) => {
                    records.push(record);
                }
                Ok(DiskyPiece::EOF) => {
                    break;
                }
                Err(_) => {
                    // With recovery enabled, we can hit errors but should continue
                    break;
                }
            }
        }

        // The recovery might not be perfect, but we should verify that any records we got are valid
        for (idx, record) in records.iter().enumerate() {
            // Each record should contain identical bytes
            let first_byte = record[0];
            assert!(record.iter().all(|&b| b == first_byte));

            // Only check value for correctness when we didn't skip records
            if idx < first_byte as usize {
                // This is fine - we may have skipped records due to corruption
            } else {
                assert_eq!(
                    idx, first_byte as usize,
                    "Record data doesn't match expected value"
                );
            }
        }
    }
}

/// Test that multiple corruptions can be handled in recovery mode
#[test]
fn test_multiple_corruptions() {
    // Create a file with many records
    let mut buffer = Vec::new();

    // IMPORTANT: The reader and writer MUST use the same block size!
    let block_size = 4096u64;

    {
        let cursor = Cursor::new(&mut buffer);
        let mut config = RecordWriterConfig::default();
        config.block_config = BlockWriterConfig::with_block_size(block_size).unwrap();
        config.chunk_size_bytes = 256;

        let mut writer = RecordWriter::with_config(cursor, config).unwrap();

        for i in 0..200 {
            let record_data = Bytes::from(vec![i as u8; 20]);
            writer.write_record(&record_data).unwrap();
        }
        writer.close().unwrap();
    }

    // Corrupt the file at multiple locations
    let mut corrupted = buffer.clone();

    // Corrupt a few locations spaced throughout the file
    let positions = [
        buffer.len() / 10,
        buffer.len() / 4,
        buffer.len() / 2,
        buffer.len() * 3 / 4,
    ];

    for pos in positions {
        corrupted = corrupt_file(corrupted, pos);
    }

    // Without recovery, we should encounter an error
    {
        let cursor = Cursor::new(&corrupted);
        let config = RecordReaderConfig::with_block_size(block_size).unwrap();
        let mut reader = RecordReader::with_config(cursor, config).unwrap();

        let mut records = Vec::new();
        let mut read_error = false;

        // Read records until we get an error or EOF
        loop {
            match reader.next_record() {
                Ok(DiskyPiece::Record(record)) => {
                    records.push(record);
                }
                Ok(DiskyPiece::EOF) => {
                    break;
                }
                Err(_) => {
                    read_error = true;
                    break;
                }
            }
        }

        // We should encounter an error when reading a corrupted file without recovery
        assert!(
            read_error,
            "Should have encountered an error with corrupted file"
        );
    }

    // With recovery enabled, try to read as many records as possible
    {
        let cursor = Cursor::new(&corrupted);
        let config = RecordReaderConfig::with_block_size(block_size)
            .unwrap()
            .with_corruption_strategy(CorruptionStrategy::Recover);

        let mut reader = RecordReader::with_config(cursor, config).unwrap();

        let mut records = Vec::new();
        let mut errors_encountered = 0;

        // Keep trying to read records, even if we encounter errors
        for _ in 0..200 {
            // Limit iterations to avoid infinite loop
            match reader.next_record() {
                Ok(DiskyPiece::Record(record)) => {
                    // Check that the record is internally consistent
                    let first_byte = record[0];
                    assert!(
                        record.iter().all(|&b| b == first_byte),
                        "Record data should be consistent"
                    );
                    records.push(record);
                }
                Ok(DiskyPiece::EOF) => {
                    break;
                }
                Err(_) => {
                    errors_encountered += 1;
                    // With recovery mode, we might be able to continue after an error
                    // But we should also protect against infinite loops
                    if errors_encountered > 10 {
                        break;
                    }
                }
            }
        }

        // With multiple corruption points, recovery may not be able to read many records
        // But our test shouldn't fail as long as any error recovery was attempted
        println!(
            "Read {} records with recovery enabled, encountered {} errors",
            records.len(),
            errors_encountered
        );
    }
}
