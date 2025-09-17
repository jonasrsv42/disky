use crate::error::{DiskyError, Result};
use crate::parallel::byte_queue::ByteQueue;
use crate::parallel::reader::{ParallelReader, ParallelReaderConfig, ShardingConfig};
use crate::parallel::sharding::Autosharder;
use crate::parallel::sharding::MemoryShardLocator;
use crate::parallel::writer::{
    ParallelWriter, ParallelWriterConfig, ShardingConfig as WriterShardingConfig,
};
use std::io::Cursor;
use std::sync::Arc;

/// Creates a valid Disky file with the specified number of records
fn create_valid_data(records: usize) -> Vec<u8> {
    let mut buffer = Vec::new();

    {
        let cursor = Cursor::new(&mut buffer);
        let mut writer = crate::writer::RecordWriter::new(cursor).unwrap();

        for i in 0..records {
            writer
                .write_record(format!("Record {}", i).as_bytes())
                .unwrap();
        }

        writer.close().unwrap();
    }

    buffer
}

/// Test that pending read tasks have their promises fulfilled with QueueClosed error when closing reader
#[test]
fn test_reader_close_fulfills_pending_promises() -> Result<()> {
    // Create a factory with multiple valid shards
    let factory = move || {
        static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let shard_num = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        match shard_num {
            0 => Ok(Cursor::new(create_valid_data(5))), // Shard with 5 records
            _ => Err(DiskyError::NoMoreShards),         // No more shards
        }
    };

    // Create a shard locator with 1 shard
    let shard_count = 1;
    let locator = Box::new(MemoryShardLocator::new(factory, shard_count));

    // Create a sharding config
    let sharding_config = ShardingConfig::new(locator, shard_count);

    // Create a parallel reader with default config
    let reader = ParallelReader::new(sharding_config, ParallelReaderConfig::default())?;

    // Queue up more read tasks than there are records
    // to ensure some promises remain unfulfilled
    let mut promises = Vec::new();
    for _ in 0..10 {
        // Only 5 records exist
        promises.push(reader.read_async()?);
    }

    // Process just the first few tasks to leave some pending
    for _ in 0..5 {
        reader.process_next_task()?;
    }

    // Close the reader - this should fulfill all pending promises with a QueueClosed error
    reader.close()?;

    // Check that all remaining promises received a QueueClosed error
    for promise in promises.iter().skip(5) {
        match promise.wait() {
            Ok(Err(DiskyError::QueueClosed(_))) => {
                // Expected QueueClosed error
            }
            other => {
                panic!("Expected QueueClosed error, got {:?}", other);
            }
        }
    }

    Ok(())
}

/// Test that pending drain tasks have their promises fulfilled with QueueClosed error when closing reader
#[test]
fn test_reader_close_fulfills_pending_drain_promises() -> Result<()> {
    // Create a factory with multiple valid shards
    let factory = move || {
        static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let shard_num = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        match shard_num {
            0 => Ok(Cursor::new(create_valid_data(5))), // Shard with 5 records
            _ => Err(DiskyError::NoMoreShards),         // No more shards
        }
    };

    // Create a shard locator with 1 shard
    let shard_count = 1;
    let locator = Box::new(MemoryShardLocator::new(factory, shard_count));

    // Create a sharding config
    let sharding_config = ShardingConfig::new(locator, shard_count);

    // Create a parallel reader with default config
    let reader = ParallelReader::new(sharding_config, ParallelReaderConfig::default())?;

    // Create a byte queue
    let byte_queue = Arc::new(ByteQueue::new(16384));

    // Queue up drain tasks
    let mut promises = Vec::new();
    for _ in 0..3 {
        // More drains than shards
        promises.push(reader.drain_resource_async(byte_queue.clone())?);
    }

    // Process just the first task to leave some pending
    reader.process_next_task()?;

    // Close the reader - this should fulfill all pending promises with a QueueClosed error
    reader.close()?;

    // Check that all remaining promises received a QueueClosed error
    for promise in promises.iter().skip(1) {
        match promise.wait() {
            Ok(Err(DiskyError::QueueClosed(_))) => {
                // Expected QueueClosed error
            }
            other => {
                panic!("Expected QueueClosed error, got {:?}", other);
            }
        }
    }

    Ok(())
}

/// Test that pending close tasks have their promises fulfilled with QueueClosed error when closing reader
#[test]
fn test_reader_close_fulfills_pending_close_promises() -> Result<()> {
    // Create a factory with multiple valid shards
    let factory = move || {
        static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let shard_num = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        match shard_num {
            0 => Ok(Cursor::new(create_valid_data(5))), // Shard with 5 records
            _ => Err(DiskyError::NoMoreShards),         // No more shards
        }
    };

    // Create a shard locator with 1 shard
    let shard_count = 1;
    let locator = Box::new(MemoryShardLocator::new(factory, shard_count));

    // Create a sharding config
    let sharding_config = ShardingConfig::new(locator, shard_count);

    // Create a parallel reader with default config
    let reader = ParallelReader::new(sharding_config, ParallelReaderConfig::default())?;

    // Queue up multiple async close tasks
    let promises = vec![reader.close_async()?, reader.close_async()?];

    // Don't process any tasks, close directly
    reader.close()?;

    // Check that all pending close promises received a QueueClosed error
    for promise in promises.iter() {
        match promise.wait() {
            Ok(Err(DiskyError::QueueClosed(_))) => {
                // Expected QueueClosed error
            }
            other => {
                panic!("Expected QueueClosed error, got {:?}", other);
            }
        }
    }

    Ok(())
}

/// Test that pending write tasks have their promises fulfilled with QueueClosed error when closing writer
#[test]
fn test_writer_close_fulfills_pending_write_promises() -> Result<()> {
    // Create an in-memory sharder for testing
    let sharder = Autosharder::new(|| Ok(Cursor::new(Vec::new())));

    // Create sharding config with 1 shard
    let sharding_config = WriterShardingConfig::new(Box::new(sharder), 1);

    // Create writer with default config
    let writer = ParallelWriter::new(sharding_config, ParallelWriterConfig::default())?;

    // Queue up several async write operations
    let mut promises = Vec::new();
    for i in 0..10 {
        let data = bytes::Bytes::from(format!("Record {}", i).into_bytes());
        promises.push(writer.write_record_async(data)?);
    }

    // Process just the first few tasks to leave some pending
    for _ in 0..5 {
        writer.process_next_task()?;
    }

    // Close the writer - this should fulfill all pending promises with a QueueClosed error
    writer.close()?;

    // Check that all remaining promises received a QueueClosed error
    for promise in promises.iter().skip(5) {
        match promise.wait() {
            Ok(Err(DiskyError::QueueClosed(_))) => {
                // Expected QueueClosed error
            }
            other => {
                panic!("Expected QueueClosed error, got {:?}", other);
            }
        }
    }

    Ok(())
}

/// Test that pending flush tasks have their promises fulfilled with QueueClosed error when closing writer
#[test]
fn test_writer_close_fulfills_pending_flush_promises() -> Result<()> {
    // Create an in-memory sharder for testing
    let sharder = Autosharder::new(|| Ok(Cursor::new(Vec::new())));

    // Create sharding config with 1 shard
    let sharding_config = WriterShardingConfig::new(Box::new(sharder), 1);

    // Create writer with default config
    let writer = ParallelWriter::new(sharding_config, ParallelWriterConfig::default())?;

    // Queue up several async write operations and process them
    for i in 0..5 {
        let data = bytes::Bytes::from(format!("Record {}", i).into_bytes());
        let promise = writer.write_record_async(data)?;
        writer.process_next_task()?;
        let _ = promise.wait()?;
    }

    // Queue up multiple flush operations
    let flush_promises = vec![
        writer.flush_async()?,
        writer.flush_async()?,
        writer.flush_async()?,
    ];

    // Process only one flush task
    writer.process_next_task()?;

    // Close the writer - this should fulfill all pending promises with a QueueClosed error
    writer.close()?;

    // Check that all remaining promises received a QueueClosed error
    for promise in flush_promises.iter().skip(1) {
        match promise.wait() {
            Ok(Err(DiskyError::QueueClosed(_))) => {
                // Expected QueueClosed error
            }
            other => {
                panic!("Expected QueueClosed error, got {:?}", other);
            }
        }
    }

    Ok(())
}

/// Test that pending close tasks have their promises fulfilled with QueueClosed error when closing writer
#[test]
fn test_writer_close_fulfills_pending_close_promises() -> Result<()> {
    // Create an in-memory sharder for testing
    let sharder = Autosharder::new(|| Ok(Cursor::new(Vec::new())));

    // Create sharding config with 1 shard
    let sharding_config = WriterShardingConfig::new(Box::new(sharder), 1);

    // Create writer with default config
    let writer = ParallelWriter::new(sharding_config, ParallelWriterConfig::default())?;

    // Queue up multiple async close tasks
    let close_promises = vec![writer.close_async()?, writer.close_async()?];

    // Don't process any tasks, close directly
    writer.close()?;

    // Check that all pending close promises received a QueueClosed error
    for promise in close_promises.iter() {
        match promise.wait() {
            Ok(Err(DiskyError::QueueClosed(_))) => {
                // Expected QueueClosed error
            }
            other => {
                panic!("Expected QueueClosed error, got {:?}", other);
            }
        }
    }

    Ok(())
}

/// Test that concurrent operations work correctly with the new close behavior
#[test]
fn test_concurrent_operations_with_close() -> Result<()> {
    // Create an in-memory sharder for testing
    let sharder = Autosharder::new(|| Ok(Cursor::new(Vec::new())));

    // Create sharding config with 1 shard
    let sharding_config = WriterShardingConfig::new(Box::new(sharder), 1);

    // Create writer with default config
    let writer = Arc::new(ParallelWriter::new(
        sharding_config,
        ParallelWriterConfig::default(),
    )?);

    // Create a bunch of write tasks in one thread
    let writer_clone = Arc::clone(&writer);
    let task_thread = std::thread::spawn(move || -> Result<()> {
        let mut promises = Vec::new();

        // Submit several write tasks
        for i in 0..100 {
            let data = bytes::Bytes::from(format!("Record {}", i).into_bytes());
            promises.push(writer_clone.write_record_async(data)?);
        }

        // Submit a close task at the end
        promises.push(writer_clone.close_async()?);

        // Wait for a short while to allow the main thread to close
        std::thread::sleep(std::time::Duration::from_millis(50));

        // All promises should either complete successfully or with QueueClosed
        for promise in promises {
            match promise.wait() {
                Ok(Ok(())) => {
                    // Task completed successfully
                }
                Ok(Err(DiskyError::QueueClosed(_))) => {
                    // Task was canceled due to queue closure - also acceptable
                }
                other => {
                    return Err(DiskyError::Other(format!(
                        "Unexpected promise result: {:?}",
                        other
                    )));
                }
            }
        }

        Ok(())
    });

    // Concurrently in the main thread, process tasks for a bit then close
    for _ in 0..20 {
        if let Ok(()) = writer.process_next_task() {
            std::thread::sleep(std::time::Duration::from_millis(1));
        } else {
            break;
        }
    }

    // Close the writer from the main thread
    writer.close()?;

    // Wait for the task thread to complete
    match task_thread.join() {
        Ok(result) => result?,
        Err(e) => return Err(DiskyError::Other(format!("Task thread panicked: {:?}", e))),
    }

    Ok(())
}
