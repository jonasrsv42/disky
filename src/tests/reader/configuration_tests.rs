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
use crate::reader::{CorruptionStrategy, RecordReader, RecordReaderConfig};
use crate::writer::{RecordWriter, RecordWriterConfig};

/// Test creating a reader with custom block size
#[test]
fn test_custom_block_size() {
    // Create a file with some records
    let mut buffer = Vec::new();

    // IMPORTANT: The reader and writer MUST use the same block size!
    let block_size = 8192u64;

    {
        let cursor = Cursor::new(&mut buffer);
        // Use a specific block size for the writer
        let mut config = RecordWriterConfig::default();
        config.block_config = BlockWriterConfig::with_block_size(block_size).unwrap();

        let mut writer = RecordWriter::with_config(cursor, config).unwrap();

        // Write some records
        for i in 0..10 {
            let record_data = Bytes::from(vec![i as u8; 100]);
            writer.write_record(&record_data).unwrap();
        }
        writer.close().unwrap();
    }

    // Create a reader with the same block size as the writer
    let cursor = Cursor::new(&buffer);
    let config = RecordReaderConfig::with_block_size(block_size).unwrap();
    let reader = RecordReader::with_config(cursor, config).unwrap();

    // Read all records
    let records: Vec<Bytes> = reader.collect::<Result<Vec<_>, _>>().unwrap();

    // Verify we got all records correctly
    assert_eq!(records.len(), 10);
    for (i, record) in records.iter().enumerate() {
        assert_eq!(record.len(), 100);
        assert!(record.iter().all(|&b| b == i as u8));
    }
}

/// Test creating a reader with the minimum possible block size
#[test]
fn test_minimum_block_size() {
    // Create a file with a few small records
    let mut buffer = Vec::new();

    // IMPORTANT: The reader and writer MUST use the same block size!
    // Use the minimum allowable block size
    let block_size = 128u64;

    {
        let cursor = Cursor::new(&mut buffer);
        // Use a small block size
        let mut config = RecordWriterConfig::default();
        config.block_config = BlockWriterConfig::with_block_size(block_size).unwrap();

        let mut writer = RecordWriter::with_config(cursor, config).unwrap();

        // Write small records
        for i in 0..5 {
            let record_data = Bytes::from(vec![i as u8; 10]);
            writer.write_record(&record_data).unwrap();
        }
        writer.close().unwrap();
    }

    // Create a reader with the same block size as the writer
    let cursor = Cursor::new(&buffer);
    let config = RecordReaderConfig::with_block_size(block_size).unwrap();
    let reader = RecordReader::with_config(cursor, config).unwrap();

    // Read all records
    let records: Vec<Bytes> = reader.collect::<Result<Vec<_>, _>>().unwrap();

    // Verify we got all records correctly
    assert_eq!(records.len(), 5);
}

/// Test setting corruption recovery strategy
#[test]
fn test_corruption_strategy_config() {
    // Create a file with some records
    let mut buffer = Vec::new();
    {
        let cursor = Cursor::new(&mut buffer);
        let mut writer = RecordWriter::new(cursor).unwrap();

        // Write some records
        for i in 0..5 {
            let record_data = Bytes::from(vec![i as u8; 20]);
            writer.write_record(&record_data).unwrap();
        }
        writer.close().unwrap();
    }

    // Create a reader with corruption recovery enabled
    let cursor = Cursor::new(&buffer);
    let config =
        RecordReaderConfig::default().with_corruption_strategy(CorruptionStrategy::Recover);

    let reader = RecordReader::with_config(cursor, config).unwrap();

    // Just verify we can read all records (no corruption to recover from)
    let records: Vec<Bytes> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(records.len(), 5);
}

/// Test error on too small block size
#[test]
fn test_too_small_block_size() {
    // Try to create a config with a block size that's too small
    let config = RecordReaderConfig::with_block_size(10);

    // This should fail
    assert!(config.is_err());
}

/// Test chaining of configuration methods
#[test]
fn test_config_chaining() {
    // Test that method chaining works for configuration
    let config =
        RecordReaderConfig::default().with_corruption_strategy(CorruptionStrategy::Recover);

    // Verify the corruption strategy was set
    assert_eq!(config.corruption_strategy, CorruptionStrategy::Recover);

    // Create a file with some records
    let mut buffer = Vec::new();
    {
        let cursor = Cursor::new(&mut buffer);
        let mut writer = RecordWriter::new(cursor).unwrap();
        writer.write_record(&Bytes::from(vec![1, 2, 3])).unwrap();
        writer.close().unwrap();
    }

    // Create a reader with the chained config
    let cursor = Cursor::new(&buffer);
    let reader = RecordReader::with_config(cursor, config).unwrap();

    // Verify we can read the record
    let records: Vec<Bytes> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(records.len(), 1);
}
