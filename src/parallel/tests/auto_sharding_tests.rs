use std::io::Cursor;
use std::sync::{Arc, Mutex};

use crate::parallel::sharding::Autosharder;
use crate::parallel::writer::{ParallelWriter, ParallelWriterConfig, ShardingConfig};
use crate::writer::RecordWriterConfig;

#[test]
fn test_auto_sharding_on_byte_limit() {
    // Track how many sinks have been created
    let sinks_created = Arc::new(Mutex::new(0usize));
    let sinks_created_clone = sinks_created.clone();
    
    // Create a sharder that counts the number of sinks created
    let sharder = Autosharder::new(move || {
        let mut count = sinks_created_clone.lock().unwrap();
        *count += 1;
        Ok(Cursor::new(Vec::new()))
    });
    
    // Create sharding config with auto-sharding enabled
    // Start with just 1 shard
    let sharding_config = ShardingConfig::with_auto_sharding(
        Box::new(sharder),
        1
    );
    
    // Create a config with a low max_bytes_per_writer limit
    let config = ParallelWriterConfig {
        writer_config: RecordWriterConfig::default(),
        max_bytes_per_writer: Some(10), // Only allow 10 bytes per writer
    };
    
    let parallel_writer = ParallelWriter::new(sharding_config, config).unwrap();
    
    // Initially we should have 1 writer
    assert_eq!(parallel_writer.available_resource_count().unwrap(), 1);
    
    // Initially we should have created 1 sink
    assert_eq!(*sinks_created.lock().unwrap(), 1);
    
    // Write a 6-byte record - should not trigger auto-sharding
    parallel_writer.write_record(b"first").unwrap();
    
    // Should still have 1 writer and no new sinks created
    assert_eq!(parallel_writer.available_resource_count().unwrap(), 1);
    assert_eq!(*sinks_created.lock().unwrap(), 1);
    
    // Write a 6-byte record - should exceed the 10-byte limit and trigger auto-sharding
    parallel_writer.write_record(b"second").unwrap();
    
    // The original writer should be forgotten, but a new one created via auto-sharding
    assert_eq!(parallel_writer.available_resource_count().unwrap(), 1);
    
    // We should have created a second sink
    assert_eq!(*sinks_created.lock().unwrap(), 2);
    
    // Write multiple records to trigger auto-sharding again
    parallel_writer.write_record(b"record3").unwrap(); // 7 bytes
    parallel_writer.write_record(b"record4").unwrap(); // 7 bytes, should trigger auto-sharding
    
    // Still have 1 writer
    assert_eq!(parallel_writer.available_resource_count().unwrap(), 1);
    
    // We should have created a third sink
    assert_eq!(*sinks_created.lock().unwrap(), 3);
}

#[test]
fn test_auto_sharding_disabled() {
    // Track how many sinks have been created
    let sinks_created = Arc::new(Mutex::new(0usize));
    let sinks_created_clone = sinks_created.clone();
    
    // Create a sharder that counts the number of sinks created
    let sharder = Autosharder::new(move || {
        let mut count = sinks_created_clone.lock().unwrap();
        *count += 1;
        Ok(Cursor::new(Vec::new()))
    });
    
    // Create sharding config with auto-sharding disabled (default)
    // Start with just 1 shard
    let sharding_config = ShardingConfig::new(
        Box::new(sharder),
        1
    );
    
    // Create a config with a low max_bytes_per_writer limit
    let config = ParallelWriterConfig {
        writer_config: RecordWriterConfig::default(),
        max_bytes_per_writer: Some(10), // Only allow 10 bytes per writer
    };
    
    let parallel_writer = ParallelWriter::new(sharding_config, config).unwrap();
    
    // Initially we should have 1 writer
    assert_eq!(parallel_writer.available_resource_count().unwrap(), 1);
    
    // Initially we should have created 1 sink
    assert_eq!(*sinks_created.lock().unwrap(), 1);
    
    // Write a 6-byte record - should not trigger any change
    parallel_writer.write_record(b"first").unwrap();
    
    // Should still have 1 writer and no new sinks created
    assert_eq!(parallel_writer.available_resource_count().unwrap(), 1);
    assert_eq!(*sinks_created.lock().unwrap(), 1);
    
    // Write a 6-byte record - should exceed the 10-byte limit
    // Since auto-sharding is disabled, the resource will be dropped without replacement
    parallel_writer.write_record(b"second").unwrap();
    
    // The writer should be forgotten, and since auto-sharding is disabled, no replacement
    assert_eq!(parallel_writer.available_resource_count().unwrap(), 0);
    
    // We should NOT have created any new sinks
    assert_eq!(*sinks_created.lock().unwrap(), 1);
    
    // Attempting to write another record should now fail since there are no available writers
    assert!(parallel_writer.write_record(b"third").is_err());
}