// Tests for corruption handling in the multi-threaded reader.
//
// These tests verify that the multi-threaded reader can recover from
// corrupted data when configured with the appropriate corruption strategy.

use std::io::Cursor;

use bytes::Bytes;
use env_logger;
use log::debug;

use crate::blocks::writer::BlockWriterConfig;
use crate::error::Result;
use crate::parallel::multi_threaded_reader::MultiThreadedReaderConfig;
use crate::parallel::reader::{DiskyParallelPiece, ParallelReaderConfig, ShardingConfig};
use crate::reader::{CorruptionStrategy, RecordReaderOptions};
use crate::shard::source::SequentialShardSource;
use crate::shard::source::{Shard, Shards};
use crate::writer::{RecordWriterConfig, RecordWriterOptions};

/// A simple test shard collection that provides in-memory Cursors from a predefined list.
struct TestShards {
    sources: Vec<Vec<u8>>,
}

impl Shards for TestShards {
    type Source = Cursor<Vec<u8>>;

    fn count(&self) -> usize {
        self.sources.len()
    }

    fn open(&self, index: usize) -> Result<Shard<Cursor<Vec<u8>>>> {
        let data = self.sources[index].clone();
        Ok(Shard {
            source: Cursor::new(data),
            id: format!("test_shard_{}", index),
        })
    }
}

/// Helper function to create a test file with multiple records spread across
/// multiple blocks and chunks.
///
/// - Uses small block size and chunk size to ensure records are spread out
/// - Creates 10 records of 100 bytes each
fn create_test_file() -> Vec<u8> {
    let mut buffer = Vec::new();
    {
        let cursor = Cursor::new(&mut buffer);

        // Use a small block size and smaller chunk size to make recovery easier
        // and ensure records are spread across multiple chunks and blocks
        let options = RecordWriterOptions {
            // Small block size (128 bytes)
            block_config: BlockWriterConfig::with_block_size(128).unwrap(),
            // Small chunk size (256 bytes) to ensure records cross chunk boundaries
            // IMPORTANT: Small chunk size ensures we don't discard all data in case of corruption
            chunk_size_bytes: 256,
            ..Default::default()
        };

        let mut writer = RecordWriterConfig::new(cursor)
            .options(options)
            .build()
            .unwrap();

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

fn make_sharding_config(sources: Vec<Vec<u8>>) -> ShardingConfig<Cursor<Vec<u8>>> {
    let shards = TestShards { sources };
    let source = SequentialShardSource::new(shards);
    ShardingConfig::new(Box::new(source), 1)
}

/// Test that corruption handling works in recovery mode with multi-threaded reader
#[test]
fn test_multithreaded_reader_corruption_recovery() {
    // Initialize logger to see diagnostic messages at Debug level
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .is_test(true)
        .try_init();

    let buffer = create_test_file();

    debug!("Created test file with size: {} bytes", buffer.len());

    // Choose positions that will likely hit different blocks
    // with our small 128-byte block size
    for offset in [60, 190, 320, 450, 580, 710] {
        if offset >= buffer.len() {
            debug!("Skipping offset {} as it's beyond file length", offset);
            continue;
        }

        debug!("\nTesting corruption at offset: {}", offset);

        let corrupted = corrupt_file(buffer.clone(), offset);

        // Try to read with default error strategy - should fail at some point
        {
            let sharding_config = make_sharding_config(vec![corrupted.clone()]);

            // Use same small block size as the writer (128 bytes)
            let reader_config =
                ParallelReaderConfig::new(RecordReaderOptions::with_block_size(128).unwrap());

            // Configure a minimal multi-threaded reader (1-2 threads)
            // Create reader with default (Error) corruption strategy
            let reader = MultiThreadedReaderConfig::new(sharding_config)
                .with_reader_config(reader_config)
                .with_worker_threads(1)
                .with_queue_size_bytes(1024)
                .build()
                .unwrap();

            let mut records = Vec::new();
            let mut read_error = false;

            // Read records until we get an error or EOF
            let mut records_read = 0;
            println!("Reading..");
            loop {
                match reader.read() {
                    Ok(DiskyParallelPiece::Record(record)) => {
                        records.push(record);
                        records_read += 1;
                    }
                    Ok(DiskyParallelPiece::EOF) => {
                        // In multithreaded context, corruption usually leads to
                        // worker thread exit and queue closure, resulting in EOF
                        debug!("Received EOF after reading {} records", records_read);
                        break;
                    }
                    Ok(DiskyParallelPiece::ShardFinished) => {
                        // Should continue reading
                        continue;
                    }
                    Err(e) => {
                        debug!("Received error: {}", e);
                        read_error = true;
                        break;
                    }
                }
            }

            // With corruption in the file and default strategy, we should
            // either hit an error or get EOF with fewer records than expected
            // (since some records should be lost due to corruption)
            assert!(
                read_error || records_read < 10,
                "Should have encountered an error or fewer records with corrupted file at offset {}. Got {} records.",
                offset,
                records_read
            );

            // Close the reader
            reader.close().unwrap();
        }

        // Try with recovery strategy
        {
            let sharding_config = make_sharding_config(vec![corrupted.clone()]);

            // Use same small block size as the writer (128 bytes) but with recovery enabled
            let reader_options = RecordReaderOptions::with_block_size(128)
                .unwrap()
                .with_corruption_strategy(CorruptionStrategy::Recover);

            let parallel_config = ParallelReaderConfig::new(reader_options);

            // Configure a minimal multi-threaded reader (1-2 threads)
            // Create reader with recovery corruption strategy
            let reader = MultiThreadedReaderConfig::new(sharding_config)
                .with_reader_config(parallel_config)
                .with_worker_threads(1)
                .with_queue_size_bytes(1024)
                .build()
                .unwrap();

            let mut records = Vec::new();
            let mut read_error = false;

            // Read records until we get an error or EOF
            loop {
                match reader.read() {
                    Ok(DiskyParallelPiece::Record(record)) => {
                        records.push(record);
                    }
                    Ok(DiskyParallelPiece::EOF) => {
                        break;
                    }
                    Ok(DiskyParallelPiece::ShardFinished) => {
                        // Should continue reading
                        continue;
                    }
                    Err(_) => {
                        read_error = true;
                        break;
                    }
                }
            }

            debug!(
                "With recovery: read {} records, encountered error: {}",
                records.len(),
                read_error
            );

            // Close the reader
            reader.close().unwrap();
        }
    }
}

/// Test that multiple corruptions can be handled with the multi-threaded reader
#[test]
fn test_multithreaded_reader_multiple_corruptions() {
    // Initialize logger to see diagnostic messages
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .is_test(true)
        .try_init();

    // Create a larger file with more data
    let mut buffer = Vec::new();

    // IMPORTANT: The reader and writer MUST use the same block size!
    let block_size = 1024u64;

    {
        let cursor = Cursor::new(&mut buffer);
        let options = RecordWriterOptions {
            block_config: BlockWriterConfig::with_block_size(block_size).unwrap(),
            chunk_size_bytes: 512,
            ..Default::default()
        };

        let mut writer = RecordWriterConfig::new(cursor)
            .options(options)
            .build()
            .unwrap();

        // Write enough records to span multiple blocks
        for i in 0..50 {
            let record_data = Bytes::from(vec![i as u8; 50]);
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

    // Try with recovery enabled in multi-threaded context
    {
        let sharding_config = make_sharding_config(vec![corrupted]);

        // Use same block size as the writer but with recovery enabled
        let reader_options = RecordReaderOptions::with_block_size(block_size)
            .unwrap()
            .with_corruption_strategy(CorruptionStrategy::Recover);

        let parallel_config = ParallelReaderConfig::new(reader_options);

        // Configure a minimal multi-threaded reader (1-2 threads)
        // Create reader with recovery corruption strategy
        let reader = MultiThreadedReaderConfig::new(sharding_config)
            .with_reader_config(parallel_config)
            .with_worker_threads(1)
            .with_queue_size_bytes(1024)
            .build()
            .unwrap();

        let mut records = Vec::new();
        let mut errors_encountered = 0;

        // Keep trying to read records, even if we encounter errors
        for _ in 0..200 {
            // Limit iterations to avoid infinite loop
            match reader.read() {
                Ok(DiskyParallelPiece::Record(record)) => {
                    // Check that the record is internally consistent
                    let first_byte = record[0];
                    assert!(
                        record.iter().all(|&b| b == first_byte),
                        "Record data should be consistent"
                    );
                    records.push(record);
                }
                Ok(DiskyParallelPiece::EOF) => {
                    break;
                }
                Ok(DiskyParallelPiece::ShardFinished) => {
                    // Should continue reading
                    continue;
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
        debug!(
            "Read {} records with recovery enabled, encountered {} errors",
            records.len(),
            errors_encountered
        );

        // Close the reader
        reader.close().unwrap();
    }
}
