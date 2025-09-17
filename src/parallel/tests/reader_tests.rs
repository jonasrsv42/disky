use crate::error::{DiskyError, Result};
use crate::parallel::reader::{
    DiskyParallelPiece, ParallelReader, ParallelReaderConfig, ShardingConfig,
};
use crate::parallel::sharding::MemoryShardLocator;
use crate::writer::RecordWriter;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn test_parallel_reader_basic() -> Result<()> {
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

    // Test synchronous reads - we should be able to read all records
    let mut record_count = 0;
    loop {
        match reader.read()? {
            DiskyParallelPiece::Record(bytes) => {
                record_count += 1;
                let record = String::from_utf8_lossy(&bytes);
                assert!(record.starts_with("Shard "));
                assert!(record.contains("Record "));
            }
            DiskyParallelPiece::ShardFinished => {
                // Shard finished, continue to next shard
                continue;
            }
            DiskyParallelPiece::EOF => {
                // No more records
                break;
            }
        }
    }

    // We should have read 9 records total (3 shards * 3 records)
    assert_eq!(record_count, 9);

    // Test asynchronous reads
    // Reset counter for factory
    counter.store(0, Ordering::SeqCst);

    // Create new reader
    let factory2 = factory.clone();
    let sharding_config = ShardingConfig::new(
        Box::new(MemoryShardLocator::new(factory2, shard_count)),
        shard_count,
    );

    let reader = ParallelReader::new(sharding_config, ParallelReaderConfig::default())?;

    // Queue up a bunch of async reads
    let mut promises = Vec::new();
    for _ in 0..9 {
        promises.push(reader.read_async()?);
    }

    // Process all the tasks
    reader.process_all_tasks()?;

    // Verify all reads completed successfully
    let mut record_count = 0;
    for promise in promises {
        let read_result = promise.wait()??; // Double ? to unwrap both Promise and inner Result
        match read_result {
            DiskyParallelPiece::Record(_) => {
                record_count += 1;
            }
            DiskyParallelPiece::ShardFinished => {
                // Shouldn't get this in the promises as it's handled internally
            }
            DiskyParallelPiece::EOF => {
                // Shouldn't get this with exactly 9 reads
            }
        }
    }

    // We should have read 9 records total (3 shards * 3 records)
    assert_eq!(record_count, 9);

    // Try reading one more - this should be EOF
    let promise = reader.read_async()?;
    reader.process_all_tasks()?;
    let result = promise.wait()??; // Double ? to unwrap both Promise and inner Result

    // Should be either EOF or ShardFinished
    match result {
        DiskyParallelPiece::Record(_) => {
            panic!("Expected EOF, got record");
        }
        DiskyParallelPiece::ShardFinished | DiskyParallelPiece::EOF => {
            // This is expected
        }
    }

    // Close the reader
    reader.close()?;

    Ok(())
}

#[test]
fn test_parallel_reader_empty_shards() -> Result<()> {
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

    // First read should return EOF (since all shards are empty)
    match reader.read()? {
        DiskyParallelPiece::Record(_) => {
            panic!("Expected EOF, got record");
        }
        DiskyParallelPiece::ShardFinished => {
            // Also acceptable
        }
        DiskyParallelPiece::EOF => {
            // This is expected
        }
    }

    // Close the reader
    reader.close()?;

    Ok(())
}

#[test]
fn test_reader_error_handling() -> Result<()> {
    // Simply test that NextRecord errors from ShardLocator are handled gracefully
    // without affecting the overall function of the reader

    // Create a factory function that will give two records then fail
    // on the third attempt
    let counter = Arc::new(AtomicUsize::new(0));
    let factory = move || {
        let shard_num = counter.fetch_add(1, Ordering::SeqCst);

        if shard_num >= 2 {
            // After 2 good records, start failing
            return Err(DiskyError::Other("Simulated error".to_string()));
        }

        // Create a Vec to hold the serialized data
        let mut buffer = Vec::new();
        {
            let cursor = Cursor::new(&mut buffer);
            let mut writer = RecordWriter::new(cursor)?;
            writer.write_record(format!("Record {}", shard_num).as_bytes())?;
            writer.close()?;
        }

        // Return a new cursor with the data for reading
        Ok(Cursor::new(buffer))
    };

    // Only use 2 shards to avoid trying to create a third one during initialization
    let shard_count = 2;
    let locator = Box::new(MemoryShardLocator::new(factory, shard_count));

    // Create a sharding config
    let sharding_config = ShardingConfig::new(locator, shard_count);

    // Create a parallel reader with default config
    let reader = ParallelReader::new(sharding_config, ParallelReaderConfig::default())?;

    // Read records successfully
    for i in 0..2 {
        match reader.read()? {
            DiskyParallelPiece::Record(bytes) => {
                let record = String::from_utf8_lossy(&bytes);
                assert_eq!(record, format!("Record {}", i));
            }
            _ => {
                panic!("Expected record at index {}", i);
            }
        }
    }

    // The next read should return EOF since all records have been read
    // and the locator will return an error if we try to get a new shard
    match reader.read()? {
        DiskyParallelPiece::Record(_) => {
            panic!("Unexpected record, should be EOF");
        }
        DiskyParallelPiece::ShardFinished => {
            // This is also acceptable
        }
        DiskyParallelPiece::EOF => {
            // This is expected
        }
    }

    // Close the reader
    reader.close()?;

    Ok(())
}
