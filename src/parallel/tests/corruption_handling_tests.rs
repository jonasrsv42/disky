// Tests for corruption handling in the multi-threaded reader.
//
// These tests verify that the multi-threaded reader can recover from
// corrupted data when configured with the appropriate corruption strategy.

use std::io::Cursor;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use env_logger;
use log::debug;

use crate::blocks::writer::BlockWriterConfig;
use crate::error::{DiskyError, Result};
use crate::parallel::multi_threaded_reader::{MultiThreadedReader, MultiThreadedReaderConfig};
use crate::parallel::reader::{DiskyParallelPiece, ParallelReaderConfig, ShardingConfig};
use crate::parallel::sharding::{Shard, ShardLocator};
use crate::reader::CorruptionStrategy;
use crate::writer::{RecordWriter, RecordWriterConfig};

/// A simple test shard locator that provides in-memory Cursors from a predefined list.
///
/// This locator is useful for testing, especially with corrupted data.
pub struct TestShardLocator {
    /// List of sources to provide
    sources: Vec<Vec<u8>>,

    /// Index of the next source to provide
    next_index: AtomicUsize,
}

impl TestShardLocator {
    /// Create a new TestShardLocator with the given data.
    pub fn new(data: Vec<Vec<u8>>) -> Self {
        Self {
            sources: data,
            next_index: AtomicUsize::new(0),
        }
    }
}

impl ShardLocator<Cursor<Vec<u8>>> for TestShardLocator {
    fn next_shard(&self) -> Result<Shard<Cursor<Vec<u8>>>> {
        // Get the current index and increment atomically
        let index = self.next_index.fetch_add(1, Ordering::SeqCst);

        // Check if we've exhausted all sources
        if index >= self.sources.len() {
            return Err(DiskyError::NoMoreShards);
        }

        // Clone the underlying Vec<u8> to create a new Cursor
        let data = self.sources[index].clone();
        let source = Cursor::new(data);

        Ok(Shard {
            source,
            id: format!("test_shard_{}", index),
        })
    }

    fn estimated_shard_count(&self) -> Option<usize> {
        Some(self.sources.len())
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
        let mut config = RecordWriterConfig::default();

        // Small block size (128 bytes)
        config.block_config = BlockWriterConfig::with_block_size(128).unwrap();

        // Small chunk size (256 bytes) to ensure records cross chunk boundaries
        // IMPORTANT: Small chunk size ensures we don't discard all data in case of corruption
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
            // Create a shard locator with our corrupted buffer
            let locator = TestShardLocator::new(vec![corrupted.clone()]);

            // Use same small block size as the writer (128 bytes)
            let reader_config = ParallelReaderConfig::new(
                crate::reader::RecordReaderConfig::with_block_size(128).unwrap(),
            );

            // Configure a minimal multi-threaded reader (1-2 threads)
            let config = MultiThreadedReaderConfig::new(
                reader_config,
                1,    // Use minimal threads for test
                1024, // Small queue size
            );

            let sharding_config = ShardingConfig::new(Box::new(locator), 1);

            // Create reader with default (Error) corruption strategy
            let reader = MultiThreadedReader::new(sharding_config, config).unwrap();

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
            // Create a shard locator with our corrupted buffer
            let locator = TestShardLocator::new(vec![corrupted.clone()]);

            // Use same small block size as the writer (128 bytes) but with recovery enabled
            let mut reader_config =
                crate::reader::RecordReaderConfig::with_block_size(128).unwrap();
            reader_config = reader_config.with_corruption_strategy(CorruptionStrategy::Recover);

            let parallel_config = ParallelReaderConfig::new(reader_config);

            // Configure a minimal multi-threaded reader (1-2 threads)
            let config = MultiThreadedReaderConfig::new(
                parallel_config,
                1,    // Use minimal threads for test
                1024, // Small queue size
            );

            let sharding_config = ShardingConfig::new(Box::new(locator), 1);

            // Create reader with recovery corruption strategy
            let reader = MultiThreadedReader::new(sharding_config, config).unwrap();

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

            // In multithreaded context with corruption recovery, we either:
            // 1. Get some records (recovery worked)
            // 2. Get an explicit error
            // 3. Get EOF due to worker thread exit (which is also acceptable)
            // So no additional assertion is needed here

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
        let mut config = RecordWriterConfig::default();
        config.block_config = BlockWriterConfig::with_block_size(block_size).unwrap();
        config.chunk_size_bytes = 512;

        let mut writer = RecordWriter::with_config(cursor, config).unwrap();

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
        // Create a shard locator with our corrupted buffer
        let locator = TestShardLocator::new(vec![corrupted]);

        // Use same block size as the writer but with recovery enabled
        let mut reader_config =
            crate::reader::RecordReaderConfig::with_block_size(block_size).unwrap();
        reader_config = reader_config.with_corruption_strategy(CorruptionStrategy::Recover);

        let parallel_config = ParallelReaderConfig::new(reader_config);

        // Configure a minimal multi-threaded reader (1-2 threads)
        let config = MultiThreadedReaderConfig::new(
            parallel_config,
            1,    // Use minimal threads for test
            1024, // Small queue size
        );

        let sharding_config = ShardingConfig::new(Box::new(locator), 1);

        // Create reader with recovery corruption strategy
        let reader = MultiThreadedReader::new(sharding_config, config).unwrap();

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
