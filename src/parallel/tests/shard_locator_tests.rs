use std::io::{Cursor, Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::tempdir;

use crate::error::{DiskyError, Result};
use crate::parallel::sharding::{
    FileShardLocator, FileSharder, MemoryShardLocator, RandomRepeatingFileShardLocator,
    ShardLocator, Sharder,
};

#[test]
fn test_file_shard_locator_basic() -> Result<()> {
    // Create a temporary directory
    let temp_dir = tempdir()?;
    let dir_path = temp_dir.path().to_path_buf();

    // Create a file sharder to generate some test files
    let sharder = FileSharder::with_prefix(dir_path.clone(), "test");

    // Create a few shard files
    let shard_count = 3;
    let mut file_handles = Vec::new();

    for i in 0..shard_count {
        let mut sink = sharder.create_sink()?;
        // Write some test data to identify each shard
        write!(sink, "This is shard {}", i)?;
        file_handles.push(sink);
    }

    // Close file handles
    drop(file_handles);

    // Create a locator for the shards
    let locator = FileShardLocator::new(dir_path, "test")?;

    // Test estimated_shard_count
    assert_eq!(locator.estimated_shard_count(), Some(shard_count));

    // Test next_shard
    for i in 0..shard_count {
        // Get the next shard
        let mut shard = locator.next_shard()?;

        // Read the test data to verify it's the right shard
        let mut buffer = String::new();
        shard.source.read_to_string(&mut buffer)?;
        assert_eq!(buffer, format!("This is shard {}", i));
    }

    // Test that we get NoMoreShards error after all shards are read
    match locator.next_shard() {
        Err(DiskyError::NoMoreShards) => {
            // This is expected
        }
        Ok(_) => panic!("Expected NoMoreShards error"),
        Err(e) => panic!("Unexpected error: {}", e),
    }

    Ok(())
}

#[test]
fn test_file_shard_locator_empty() -> Result<()> {
    // Create a temporary directory
    let temp_dir = tempdir()?;
    let dir_path = temp_dir.path().to_path_buf();

    // Attempt to create a locator when no files exist
    let result = FileShardLocator::new(dir_path, "nonexistent");

    // Constructor should now fail with a "No shards found" error
    match result {
        Err(DiskyError::Other(msg)) => {
            // Verify the error message contains "No shards found"
            assert!(
                msg.contains("No shards found"),
                "Expected 'No shards found' error, got: {}",
                msg
            );
        }
        Ok(_) => panic!("Expected an error, but got success"),
        Err(e) => panic!("Unexpected error type: {}", e),
    }

    Ok(())
}

#[test]
fn test_memory_shard_locator() -> Result<()> {
    // Create a counter to track shard creation
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();

    // Create a factory function that produces cursors with different data
    let factory = move || {
        let shard_num = counter_clone.fetch_add(1, Ordering::SeqCst);
        let data = format!("Memory shard {}", shard_num).into_bytes();
        Ok(Cursor::new(data))
    };

    // Create a memory shard locator with 3 shards
    let shard_count = 3;
    let locator = MemoryShardLocator::new(factory, shard_count);

    // Check estimated shard count
    assert_eq!(locator.estimated_shard_count(), Some(shard_count));

    // Read all shards
    for i in 0..shard_count {
        let mut shard = locator.next_shard()?;

        // Read the content to verify it's what we expect
        let mut buffer = Vec::new();
        shard.source.read_to_end(&mut buffer)?;

        let content = String::from_utf8(buffer).unwrap();
        assert_eq!(content, format!("Memory shard {}", i));
    }

    // Test that we get NoMoreShards error after all shards are read
    match locator.next_shard() {
        Err(DiskyError::NoMoreShards) => {
            // This is expected
        }
        Ok(_) => panic!("Expected NoMoreShards error"),
        Err(e) => panic!("Unexpected error: {}", e),
    }

    Ok(())
}

#[test]
fn test_shard_locator_error_handling() -> Result<()> {
    // Create a counter to track how many times the factory is called
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();

    // Create a factory function that fails after the first shard
    let factory = move || -> Result<Cursor<Vec<u8>>> {
        let count = counter_clone.fetch_add(1, Ordering::SeqCst);
        if count >= 1 {
            return Err(DiskyError::Other("Simulated error".to_string()));
        }
        let data = format!("Memory shard {}", count).into_bytes();
        Ok(Cursor::new(data))
    };

    // Create a memory shard locator with 3 shards
    let shard_count = 3;
    let locator = MemoryShardLocator::new(factory, shard_count);

    // First shard should be readable
    let mut shard = locator.next_shard()?;
    let mut buffer = Vec::new();
    shard.source.read_to_end(&mut buffer)?;
    let content = String::from_utf8(buffer).unwrap();
    assert_eq!(content, "Memory shard 0");

    // Second shard should return the simulated error
    match locator.next_shard() {
        Err(DiskyError::Other(msg)) if msg == "Simulated error" => {
            // This is expected
        }
        Ok(_) => panic!("Expected error"),
        Err(e) => panic!("Unexpected error: {}", e),
    }

    Ok(())
}

#[test]
fn test_random_repeating_file_shard_locator_empty() -> Result<()> {
    // Create a temporary directory
    let temp_dir = tempdir()?;
    let dir_path = temp_dir.path().to_path_buf();

    // Attempt to create a locator when no files exist
    let result = RandomRepeatingFileShardLocator::new(dir_path, "nonexistent");

    // Constructor should fail with a "No shards found" error
    match result {
        Err(DiskyError::Other(msg)) => {
            // Verify the error message contains "No shards found"
            assert!(
                msg.contains("No shards found"),
                "Expected 'No shards found' error, got: {}",
                msg
            );
        }
        Ok(_) => panic!("Expected an error, but got success"),
        Err(e) => panic!("Unexpected error type: {}", e),
    }

    Ok(())
}
