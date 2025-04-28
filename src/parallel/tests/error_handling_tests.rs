use crate::error::{DiskyError, Result};
use crate::parallel::byte_queue::ByteQueue;
use crate::parallel::reader::{ParallelReader, ParallelReaderConfig, DiskyParallelPiece, ShardingConfig};
use crate::parallel::sharding::MemoryShardLocator;
use crate::writer::RecordWriter;
use std::io::Cursor;
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// Creates a valid Disky file with the specified number of records
fn create_valid_data(records: usize) -> Vec<u8> {
    let mut buffer = Vec::new();
    
    {
        let cursor = Cursor::new(&mut buffer);
        let mut writer = RecordWriter::new(cursor).unwrap();
        
        for i in 0..records {
            writer.write_record(format!("Record {}", i).as_bytes()).unwrap();
        }
        
        writer.close().unwrap();
    }
    
    buffer
}

/// Creates corrupted data by modifying some bytes in a valid Disky file
fn create_corrupted_data(records: usize) -> Vec<u8> {
    let mut data = create_valid_data(records);
    
    // Corrupt some bytes in the middle (where it will affect block headers)
    if data.len() > 100 {
        for i in 50..60 {
            data[i] ^= 0xFF; // Flip bits
        }
    }
    
    data
}

/// Test how the reader handles corruption with recovery disabled
#[test]
fn test_corruption_without_recovery() -> Result<()> {
    // Create a factory that produces one valid file, then one corrupted file
    let factory = move || {
        static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let shard_num = COUNTER.fetch_add(1, Ordering::SeqCst);
        
        if shard_num == 0 {
            // First shard is good
            Ok(Cursor::new(create_valid_data(3)))
        } else if shard_num == 1 {
            // Second shard is corrupted
            Ok(Cursor::new(create_corrupted_data(3)))
        } else {
            // No more shards
            Err(DiskyError::NoMoreShards)
        }
    };
    
    // Create a shard locator with 2 shards
    let shard_count = 2;
    let locator = Box::new(MemoryShardLocator::new(factory, shard_count));
    
    // Create a sharding config
    let sharding_config = ShardingConfig::new(locator, shard_count);
    
    // Create a parallel reader with default config (no recovery)
    let reader = ParallelReader::new(
        sharding_config,
        ParallelReaderConfig::default(),
    )?;
    
    // Create a byte queue
    let byte_queue = Arc::new(ByteQueue::new(16384));
    
    // First resource should drain successfully
    reader.drain_resource(byte_queue.clone())?;
    
    // Read the records and ensure we got 3
    let mut record_count = 0;
    loop {
        match byte_queue.try_read_front()? {
            Some(DiskyParallelPiece::Record(_)) => {
                record_count += 1;
            }
            Some(DiskyParallelPiece::ShardFinished) | Some(DiskyParallelPiece::EOF) => {
                break;
            }
            None => {
                break;
            }
        }
    }
    
    assert_eq!(record_count, 3, "Expected 3 records from first shard");
    
    // Second drain should fail due to corruption
    let result = reader.drain_resource(byte_queue.clone());
    assert!(result.is_err(), "Expected error from corrupted shard");
    
    // Close the reader
    reader.close()?;
    
    Ok(())
}

/// Test how the reader handles multiple valid shards
#[test]
fn test_multiple_valid_shards() -> Result<()> {
    // Create a factory that produces: valid, valid
    let factory = move || {
        static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let shard_num = COUNTER.fetch_add(1, Ordering::SeqCst);
        
        match shard_num {
            0 => Ok(Cursor::new(create_valid_data(3))),  // First shard with 3 records
            1 => Ok(Cursor::new(create_valid_data(2))),  // Second shard with 2 records
            _ => Err(DiskyError::NoMoreShards),          // No more shards
        }
    };
    
    // Create a shard locator with 2 shards
    let shard_count = 2;
    let locator = Box::new(MemoryShardLocator::new(factory, shard_count));
    
    // Create a sharding config
    let sharding_config = ShardingConfig::new(locator, shard_count);
    
    // Create a parallel reader with default config
    let reader = ParallelReader::new(
        sharding_config,
        ParallelReaderConfig::default(),
    )?;
    
    // Read until EOF and count records
    let mut record_count = 0;
    let mut shard_finishes = 0;
    
    loop {
        match reader.read()? {
            DiskyParallelPiece::Record(_) => {
                record_count += 1;
            }
            DiskyParallelPiece::ShardFinished => {
                shard_finishes += 1;
                continue;
            }
            DiskyParallelPiece::EOF => {
                break;
            }
        }
    }
    
    // We should be able to read 5 records total (3 from first shard, 2 from second shard)
    assert_eq!(record_count, 5, "Expected 5 records total, got {}", record_count);
    
    // Should have encountered at least 1 shard finish before EOF
    // The exact count could be 1 or 2 depending on internal implementation
    assert_eq!(shard_finishes, 2, "Expected 2 shard finish, got {}", shard_finishes);
    
    // Close the reader
    reader.close()?;
    
    Ok(())
}

/// Test async reading across multiple shards
#[test]
fn test_async_reading_across_shards() -> Result<()> {
    // Create a factory with multiple valid shards
    let factory = move || {
        static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let shard_num = COUNTER.fetch_add(1, Ordering::SeqCst);
        
        match shard_num {
            0 => Ok(Cursor::new(create_valid_data(4))),  // First shard with 4 records
            1 => Ok(Cursor::new(create_valid_data(3))),  // Second shard with 3 records
            _ => Err(DiskyError::NoMoreShards),          // No more shards
        }
    };
    
    // Create a shard locator with 2 shards
    let shard_count = 2;
    let locator = Box::new(MemoryShardLocator::new(factory, shard_count));
    
    // Create a sharding config
    let sharding_config = ShardingConfig::new(locator, shard_count);
    
    // Create a parallel reader with default config
    let reader = ParallelReader::new(
        sharding_config,
        ParallelReaderConfig::default(),
    )?;
    
    // Queue up some async reads for all records plus 1 extra to test EOF
    let mut promises = Vec::new();
    for _ in 0..8 { // 4+3+1 EOF check
        promises.push(reader.read_async()?);
    }
    
    // Process all the tasks
    reader.process_all_tasks()?;
    
    // Count the different types of results
    let mut record_count = 0;
    let mut shard_finished_count = 0;
    let mut eof_count = 0;
    
    for promise in promises.iter() {
        let result = promise.wait()?;
        
        match result {
            Ok(DiskyParallelPiece::Record(_)) => {
                record_count += 1;
            }
            Ok(DiskyParallelPiece::ShardFinished) => {
                shard_finished_count += 1;
            }
            Ok(DiskyParallelPiece::EOF) => {
                eof_count += 1;
            }
            Err(e) => {
                panic!("Unexpected error: {:?}", e);
            }
        }
    }
    
    // We should get 7 records total (4 from first shard, 3 from second shard)
    assert_eq!(record_count, 7, "Expected 7 successful record reads");
    
    // We should get either a ShardFinished or EOF for the last read
    assert_eq!(shard_finished_count + eof_count, 1, 
               "Expected exactly 1 terminal state (ShardFinished or EOF)");
    
    // Close the reader
    reader.close()?;
    
    Ok(())
}

/// Test draining multiple valid resources to a byte queue
#[test]
fn test_byte_queue_drain_multiple_resources() -> Result<()> {
    // Create a factory with multiple valid shards
    let factory = move || {
        static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let shard_num = COUNTER.fetch_add(1, Ordering::SeqCst);
        
        match shard_num {
            0 => Ok(Cursor::new(create_valid_data(3))),  // First shard with 3 records
            1 => Ok(Cursor::new(create_valid_data(2))),  // Second shard with 2 records
            _ => Err(DiskyError::NoMoreShards),          // No more shards
        }
    };
    
    // Create a shard locator with 2 shards
    let shard_count = 2;
    let locator = Box::new(MemoryShardLocator::new(factory, shard_count));
    
    // Create a sharding config
    let sharding_config = ShardingConfig::new(locator, shard_count);
    
    // Create a parallel reader with default config
    let reader = ParallelReader::new(
        sharding_config,
        ParallelReaderConfig::default(),
    )?;
    
    // Create a byte queue
    let byte_queue = Arc::new(ByteQueue::new(16384));
    
    // First resource should drain successfully
    reader.drain_resource(byte_queue.clone())?;
    
    // Read the records and ensure we got 3
    let mut first_record_count = 0;
    loop {
        match byte_queue.try_read_front()? {
            Some(DiskyParallelPiece::Record(_)) => {
                first_record_count += 1;
            }
            Some(DiskyParallelPiece::ShardFinished) | Some(DiskyParallelPiece::EOF) => {
                break;
            }
            None => {
                break;
            }
        }
    }
    
    assert_eq!(first_record_count, 3, "Expected 3 records from first shard");
    
    // Drain the second resource
    reader.drain_resource(byte_queue.clone())?;
    
    // Read the records and ensure we got 2
    let mut second_record_count = 0;
    loop {
        match byte_queue.try_read_front()? {
            Some(DiskyParallelPiece::Record(_)) => {
                second_record_count += 1;
            }
            Some(DiskyParallelPiece::ShardFinished) | Some(DiskyParallelPiece::EOF) => {
                break;
            }
            None => {
                break;
            }
        }
    }
    
    assert_eq!(second_record_count, 2, "Expected 2 records from second shard");
    
    // Drain again - should get EOF
    reader.drain_resource(byte_queue.clone())?;
    
    // Should get EOF in the queue
    match byte_queue.try_read_front()? {
        Some(DiskyParallelPiece::EOF) => {
            // Expected
        }
        other => {
            panic!("Expected EOF, got {:?}", other);
        }
    }
    
    // Close the reader
    reader.close()?;
    
    Ok(())
}

/// Test async drain with multiple resources
#[test]
fn test_async_drain_multiple_resources() -> Result<()> {
    // Create a factory with multiple valid shards
    let factory = move || {
        static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let shard_num = COUNTER.fetch_add(1, Ordering::SeqCst);
        
        match shard_num {
            0 => Ok(Cursor::new(create_valid_data(3))),  // First shard with 3 records
            1 => Ok(Cursor::new(create_valid_data(2))),  // Second shard with 2 records
            _ => Err(DiskyError::NoMoreShards),          // No more shards
        }
    };
    
    // Create a shard locator with 2 shards
    let shard_count = 2;
    let locator = Box::new(MemoryShardLocator::new(factory, shard_count));
    
    // Create a sharding config
    let sharding_config = ShardingConfig::new(locator, shard_count);
    
    // Create a parallel reader with default config
    let reader = ParallelReader::new(
        sharding_config,
        ParallelReaderConfig::default(),
    )?;
    
    // Create a byte queue
    let byte_queue = Arc::new(ByteQueue::new(16384));
    
    // Queue an async drain for the first shard
    let promise = reader.drain_resource_async(byte_queue.clone())?;
    
    // Process the tasks
    reader.process_all_tasks()?;
    
    // First drain should succeed
    assert!(promise.wait().is_ok(), "Expected successful first drain");
    
    // Read the records and ensure we got 3
    let mut record_count = 0;
    loop {
        match byte_queue.try_read_front()? {
            Some(DiskyParallelPiece::Record(_)) => {
                record_count += 1;
            }
            Some(DiskyParallelPiece::ShardFinished) | Some(DiskyParallelPiece::EOF) => {
                break;
            }
            None => {
                break;
            }
        }
    }
    
    assert_eq!(record_count, 3, "Expected 3 records from first shard");
    
    // Queue an async drain for the second shard
    let promise = reader.drain_resource_async(byte_queue.clone())?;
    
    // Process the tasks
    reader.process_all_tasks()?;
    
    // Second drain should succeed
    assert!(promise.wait().is_ok(), "Expected successful second drain");
    
    // Read the records and ensure we got 2
    let mut record_count = 0;
    loop {
        match byte_queue.try_read_front()? {
            Some(DiskyParallelPiece::Record(_)) => {
                record_count += 1;
            }
            Some(DiskyParallelPiece::ShardFinished) | Some(DiskyParallelPiece::EOF) => {
                break;
            }
            None => {
                break;
            }
        }
    }
    
    assert_eq!(record_count, 2, "Expected 2 records from second shard");
    
    // Queue a third async drain which should return EOF
    let promise = reader.drain_resource_async(byte_queue.clone())?;
    
    // Process the tasks
    reader.process_all_tasks()?;
    
    // Third drain should succeed but produce EOF
    assert!(promise.wait().is_ok(), "Expected successful third drain with EOF");
    
    // Should get EOF in the queue
    match byte_queue.try_read_front()? {
        Some(DiskyParallelPiece::EOF) => {
            // Expected
        }
        other => {
            panic!("Expected EOF, got {:?}", other);
        }
    }
    
    // Close the reader
    reader.close()?;
    
    Ok(())
}

/// Test reading after encountering a corrupt shard
#[test]
fn test_reading_after_corruption() -> Result<()> {
    // Create a factory with: good shard, corrupt shard, good shard
    let factory = move || {
        static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let shard_num = COUNTER.fetch_add(1, Ordering::SeqCst);
        
        match shard_num {
            0 => Ok(Cursor::new(create_valid_data(3))),     // First shard is good
            1 => Ok(Cursor::new(create_corrupted_data(3))), // Second shard is corrupt
            2 => Ok(Cursor::new(create_valid_data(2))),     // Third shard is good
            _ => Err(DiskyError::NoMoreShards),
        }
    };
    
    // Create a shard locator with 3 shards
    let shard_count = 3;
    let locator = Box::new(MemoryShardLocator::new(factory, shard_count));
    
    // Create a sharding config
    let sharding_config = ShardingConfig::new(locator, shard_count);
    
    // Create a parallel reader with default config
    let reader = ParallelReader::new(
        sharding_config,
        ParallelReaderConfig::default(),
    )?;
    
    // Create a byte queue
    let byte_queue = Arc::new(ByteQueue::new(16384));
    
    // First drain should succeed (good shard)
    reader.drain_resource(byte_queue.clone())?;
    
    // Read all records from the queue
    let mut records = Vec::new();
    loop {
        match byte_queue.try_read_front()? {
            Some(DiskyParallelPiece::Record(bytes)) => {
                records.push(bytes);
            }
            Some(DiskyParallelPiece::ShardFinished) | Some(DiskyParallelPiece::EOF) => {
                break;
            }
            None => {
                break;
            }
        }
    }
    
    assert_eq!(records.len(), 3, "Expected 3 records from first shard");
    
    // Second drain should fail (corrupt shard)
    let result = reader.drain_resource(byte_queue.clone());
    assert!(result.is_err(), "Expected error from corrupted second shard");
    
    // Third drain should succeed (good shard)
    let result = reader.drain_resource(byte_queue.clone());
    assert!(result.is_ok(), "Expected success on third shard after corruption");
    
    // Read all records from the queue
    records.clear();
    loop {
        match byte_queue.try_read_front()? {
            Some(DiskyParallelPiece::Record(bytes)) => {
                records.push(bytes);
            }
            Some(DiskyParallelPiece::ShardFinished) | Some(DiskyParallelPiece::EOF) => {
                break;
            }
            None => {
                break;
            }
        }
    }
    
    assert_eq!(records.len(), 2, "Expected 2 records from third shard after corruption");
    
    // Close the reader
    reader.close()?;
    
    Ok(())
}

/// Test async reading with corrupt shard in between good shards
#[test]
fn test_async_reading_with_corruption() -> Result<()> {
    // Create a factory with: good shard, corrupt shard, good shard
    let factory = move || {
        static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let shard_num = COUNTER.fetch_add(1, Ordering::SeqCst);
        
        match shard_num {
            0 => Ok(Cursor::new(create_valid_data(3))),     // First shard is good
            1 => Ok(Cursor::new(create_corrupted_data(3))), // Second shard is corrupt
            2 => Ok(Cursor::new(create_valid_data(2))),     // Third shard is good
            _ => Err(DiskyError::NoMoreShards),
        }
    };
    
    // Create a shard locator with 3 shards
    let shard_count = 3;
    let locator = Box::new(MemoryShardLocator::new(factory, shard_count));
    
    // Create a sharding config
    let sharding_config = ShardingConfig::new(locator, shard_count);
    
    // Create a parallel reader with default config
    let reader = ParallelReader::new(
        sharding_config,
        ParallelReaderConfig::default(),
    )?;
    
    // Create a byte queue for first shard (good)
    let first_queue = Arc::new(ByteQueue::new(16384));
    let first_promise = reader.drain_resource_async(first_queue.clone())?;
    
    // Create a byte queue for second shard (corrupt)
    let second_queue = Arc::new(ByteQueue::new(16384));
    let second_promise = reader.drain_resource_async(second_queue.clone())?;
    
    // Create a byte queue for third shard (good)
    let third_queue = Arc::new(ByteQueue::new(16384));
    let third_promise = reader.drain_resource_async(third_queue.clone())?;
    
    // Process all tasks
    reader.process_all_tasks()?;
    
    // First drain should succeed
    assert!(first_promise.wait().is_ok(), "Expected successful first drain");
    
    // Second drain should fail
    let second_result = second_promise.wait();
    assert!(second_result.is_ok() && second_result.unwrap().is_err(), 
            "Expected error in second drain");
    
    // Third drain should succeed even after second one failed
    assert!(third_promise.wait().is_ok(), "Expected successful third drain after corruption");
    
    // Read records from first queue
    let mut record_count = 0;
    loop {
        match first_queue.try_read_front()? {
            Some(DiskyParallelPiece::Record(_)) => {
                record_count += 1;
            }
            Some(DiskyParallelPiece::ShardFinished) | Some(DiskyParallelPiece::EOF) => {
                break;
            }
            None => {
                break;
            }
        }
    }
    
    assert_eq!(record_count, 3, "Expected 3 records from first shard");
    
    // Read records from third queue (after corrupt second shard)
    record_count = 0;
    loop {
        match third_queue.try_read_front()? {
            Some(DiskyParallelPiece::Record(_)) => {
                record_count += 1;
            }
            Some(DiskyParallelPiece::ShardFinished) | Some(DiskyParallelPiece::EOF) => {
                break;
            }
            None => {
                break;
            }
        }
    }
    
    assert_eq!(record_count, 2, "Expected 2 records from third shard after corruption");
    
    // Close the reader
    reader.close()?;
    
    Ok(())
}
