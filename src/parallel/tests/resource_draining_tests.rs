use crate::error::Result;
use crate::parallel::byte_queue::ByteQueue;
use crate::parallel::reader::{ParallelReader, ParallelReaderConfig, DiskyParallelPiece, ShardingConfig};
use crate::parallel::sharding::MemoryShardLocator;
use crate::writer::RecordWriter;
use std::io::Cursor;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[test]
fn test_drain_resource_basic() -> Result<()> {
    // Create a counter to track shard creation
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();

    // Create a factory function that produces cursors with different data
    let factory = move || {
        let shard_num = counter_clone.fetch_add(1, Ordering::SeqCst);

        // Create a Vec to hold the serialized data
        let mut buffer = Vec::new();

        // Create a cursor for the RecordWriter to use
        let cursor = Cursor::new(&mut buffer);

        // Put some records in the buffer
        let num_records = 3;
        {
            let mut writer = RecordWriter::new(cursor)?;

            for i in 0..num_records {
                writer.write_record(format!("Shard {} Record {}", shard_num, i).as_bytes())?;
            }

            writer.close()?;
        }

        // Return a new cursor with the data for reading
        Ok(Cursor::new(buffer))
    };

    // Create a memory shard locator with 3 shards
    let shard_count = 3;
    let factory1 = factory.clone();
    let locator = Box::new(MemoryShardLocator::new(factory1, shard_count));

    // Create a sharding config
    let sharding_config = ShardingConfig::new(locator, shard_count);

    // Create a parallel reader
    let reader = ParallelReader::new(sharding_config, ParallelReaderConfig::default())?;

    // Create a byte queue to receive the records
    let byte_queue = Arc::new(ByteQueue::new(16384)); // 16KB limit

    // Test synchronous draining - we should be able to drain a full resource
    reader.drain_resource(byte_queue.clone())?;

    // Read records from the queue
    let mut record_count = 0;
    loop {
        match byte_queue.try_read_front()? {
            Some(DiskyParallelPiece::Record(bytes)) => {
                record_count += 1;
                let record = String::from_utf8_lossy(&bytes);
                assert!(record.starts_with("Shard "));
                assert!(record.contains("Record "));
            }
            Some(DiskyParallelPiece::ShardFinished) => {
                // Shard finished, break out and test draining again
                break;
            }
            Some(DiskyParallelPiece::EOF) => {
                // No more records
                break;
            }
            None => {
                // Queue is empty, break
                break;
            }
        }
    }

    // We should have read 3 records from the first shard
    assert_eq!(record_count, 3);

    // Queue another drain and process it
    reader.drain_resource(byte_queue.clone())?;

    // Read records from the second shard
    let mut record_count = 0;
    loop {
        match byte_queue.try_read_front()? {
            Some(DiskyParallelPiece::Record(bytes)) => {
                record_count += 1;
                let record = String::from_utf8_lossy(&bytes);
                assert!(record.starts_with("Shard "));
                assert!(record.contains("Record "));
            }
            Some(DiskyParallelPiece::ShardFinished) => {
                // Shard finished, break
                break;
            }
            Some(DiskyParallelPiece::EOF) => {
                // No more records
                break;
            }
            None => {
                // Queue is empty, break
                break;
            }
        }
    }

    // We should have read 3 records from the second shard
    assert_eq!(record_count, 3);

    // Drain the last shard
    reader.drain_resource(byte_queue.clone())?;

    // Read records from the third shard
    let mut record_count = 0;
    loop {
        match byte_queue.try_read_front()? {
            Some(DiskyParallelPiece::Record(bytes)) => {
                record_count += 1;
                let record = String::from_utf8_lossy(&bytes);
                assert!(record.starts_with("Shard "));
                assert!(record.contains("Record "));
            }
            Some(DiskyParallelPiece::ShardFinished) => {
                // Shard finished, continue
                continue;
            }
            Some(DiskyParallelPiece::EOF) => {
                // No more records
                break;
            }
            None => {
                // Queue is empty, break
                break;
            }
        }
    }

    // We should have read 3 records from the third shard
    assert_eq!(record_count, 3);

    // Test asynchronous draining
    // Reset counter for factory
    counter.store(0, Ordering::SeqCst);

    // Create new reader
    let factory2 = factory.clone();
    let sharding_config = ShardingConfig::new(
        Box::new(MemoryShardLocator::new(factory2, shard_count)),
        shard_count,
    );

    let reader = ParallelReader::new(sharding_config, ParallelReaderConfig::default())?;

    // Create a byte queue for async draining
    let async_queue = Arc::new(ByteQueue::new(16384));

    // Queue an async drain
    let promise = reader.drain_resource_async(async_queue.clone())?;

    // Process the tasks
    reader.process_all_tasks()?;

    // Verify the drain completed successfully
    promise.wait()??; // Double ? to unwrap both Promise and inner Result

    // Read records from the queue
    let mut record_count = 0;
    loop {
        match async_queue.try_read_front()? {
            Some(DiskyParallelPiece::Record(bytes)) => {
                record_count += 1;
                let record = String::from_utf8_lossy(&bytes);
                assert!(record.starts_with("Shard "));
                assert!(record.contains("Record "));
            }
            Some(DiskyParallelPiece::ShardFinished) => {
                // Got ShardFinished control message
                break;
            }
            Some(DiskyParallelPiece::EOF) => {
                break;
            }
            None => {
                break;
            }
        }
    }

    // We should have read 3 records from the first shard
    assert_eq!(record_count, 3);

    // Close the reader
    reader.close()?;

    Ok(())
}

#[test]
fn test_drain_resource_empty_shard() -> Result<()> {
    // Create a factory function that produces empty but valid shards with just a signature
    let factory = || {
        let mut buffer = Vec::new();
        let cursor = Cursor::new(&mut buffer);

        // Create a writer and just close it to write a signature
        {
            let mut writer = RecordWriter::new(cursor)?;
            writer.close()?;
        }

        Ok(Cursor::new(buffer))
    };

    // Create a memory shard locator with 3 empty shards (with signatures)
    let shard_count = 3;
    let locator = Box::new(MemoryShardLocator::new(factory, shard_count));

    // Create a sharding config
    let sharding_config = ShardingConfig::new(locator, shard_count);

    // Create a parallel reader
    let reader = ParallelReader::new(sharding_config, ParallelReaderConfig::default())?;

    // Create a byte queue
    let byte_queue = Arc::new(ByteQueue::new(16384));

    // Drain the resource (should be empty)
    reader.drain_resource(byte_queue.clone())?;

    // Check the queue - should get either ShardFinished or EOF
    match byte_queue.try_read_front()? {
        Some(DiskyParallelPiece::Record(_)) => {
            panic!("Expected ShardFinished or EOF, got record");
        }
        Some(DiskyParallelPiece::ShardFinished) => {
            // This is expected
        }
        Some(DiskyParallelPiece::EOF) => {
            // This is also expected
        }
        None => {
            panic!("Expected ShardFinished or EOF, got empty queue");
        }
    }

    // Close the reader
    reader.close()?;

    Ok(())
}

#[test]
fn test_drain_resource_mixed_records() -> Result<()> {
    // Create a counter to track shard creation
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();

    // Create a factory function that produces shards with varying numbers of records
    let factory = move || {
        let shard_num = counter_clone.fetch_add(1, Ordering::SeqCst);

        // Create a Vec to hold the serialized data
        let mut buffer = Vec::new();

        // Create a cursor for the RecordWriter to use
        let cursor = Cursor::new(&mut buffer);

        // Put records in the buffer - different count for each shard
        let num_records = match shard_num {
            0 => 5, // 5 records in first shard
            1 => 0, // 0 records in second shard (empty but valid)
            2 => 3, // 3 records in third shard
            _ => 1, // 1 record in any additional shards
        };

        {
            let mut writer = RecordWriter::new(cursor)?;

            for i in 0..num_records {
                writer.write_record(format!("Shard {} Record {}", shard_num, i).as_bytes())?;
            }

            writer.close()?;
        }

        // Return a new cursor with the data for reading
        Ok(Cursor::new(buffer))
    };

    // Create a memory shard locator with 3 shards of different sizes
    let shard_count = 3;
    let locator = Box::new(MemoryShardLocator::new(factory, shard_count));

    // Create a sharding config
    let sharding_config = ShardingConfig::new(locator, shard_count);

    // Create a parallel reader
    let reader = ParallelReader::new(sharding_config, ParallelReaderConfig::default())?;

    // Create a byte queue
    let byte_queue = Arc::new(ByteQueue::new(16384));

    // Drain all resources and collect all records
    let mut total_records = 0;

    // Keep draining until we get EOF
    'second: loop {
        // Drain a resource
        reader.drain_resource(byte_queue.clone())?;

        // Read all records from this drain
        let mut shard_records = 0;

        loop {
            match byte_queue.try_read_front()? {
                Some(DiskyParallelPiece::Record(bytes)) => {
                    shard_records += 1;
                    let record = String::from_utf8_lossy(&bytes);
                    assert!(record.starts_with("Shard "));
                    assert!(record.contains("Record "));
                }
                Some(DiskyParallelPiece::ShardFinished) => {
                    break;
                }
                Some(DiskyParallelPiece::EOF) => {
                    break 'second;
                }
                None => {
                    panic!("Got None");
                }
            }
        }

        total_records += shard_records;
    }

    // We should have read 8 records total (5 + 0 + 3)
    assert_eq!(total_records, 8);

    // Close the reader
    reader.close()?;

    Ok(())
}

