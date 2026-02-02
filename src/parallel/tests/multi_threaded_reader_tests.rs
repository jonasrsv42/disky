use std::io::Cursor;

use bytes::Bytes;

use crate::error::Result;
use crate::parallel::multi_threaded_reader::{MultiThreadedReaderConfig, ReadingOrder};
use crate::parallel::reader::{DiskyParallelPiece, ShardingConfig};
use crate::shard::source::{MemoryShards, SequentialShardSource};

/// Create test data with some records
fn create_test_data() -> Vec<u8> {
    let mut data = Vec::new();

    {
        let cursor = Cursor::new(&mut data);
        let mut writer = crate::writer::RecordWriterConfig::new(cursor)
            .build()
            .unwrap();

        writer.write_record(b"record1").unwrap();
        writer.write_record(b"record2").unwrap();
        writer.write_record(b"record3").unwrap();
        writer.write_record(b"record4").unwrap();
        writer.write_record(b"record5").unwrap();
    }

    data
}

/// Create test data with records that identify their source shard
fn create_identifiable_data(shard_id: usize) -> Vec<u8> {
    let mut data = Vec::new();

    {
        let cursor = Cursor::new(&mut data);
        let mut writer = crate::writer::RecordWriterConfig::new(cursor)
            .build()
            .unwrap();

        for i in 1..=5 {
            let record = format!("shard{}_record{}", shard_id, i);
            writer.write_record(record.as_bytes()).unwrap();
        }
    }

    data
}

/// Create a ShardingConfig for multiple shards with identical test data
fn create_multi_shard_config(shard_count: usize) -> ShardingConfig {
    let test_data = create_test_data();
    let shards = MemoryShards::new(move |_index: usize| Ok(test_data.clone()), shard_count);
    let source = SequentialShardSource::new(shards);
    ShardingConfig::new(Box::new(source), shard_count)
}

/// Create a ShardingConfig for multiple shards with identifiable records
fn create_identifiable_shard_config(shard_count: usize) -> ShardingConfig {
    let shards = MemoryShards::new(
        move |index: usize| Ok(create_identifiable_data(index + 1)),
        shard_count,
    );
    let source = SequentialShardSource::new(shards);
    ShardingConfig::new(Box::new(source), shard_count)
}

/// Test that the multi-threaded reader can read records from a single shard
#[test]
fn test_multi_threaded_reader_single_shard() -> Result<()> {
    // Create the sharding config with a single shard
    let sharding_config = create_multi_shard_config(1);

    // Create the reader
    let reader = MultiThreadedReaderConfig::new(sharding_config)
        .with_worker_threads(2)
        .with_queue_size_bytes(1024 * 1024)
        .build()?;

    // Read records
    let mut records = Vec::new();
    loop {
        match reader.read()? {
            DiskyParallelPiece::Record(bytes) => {
                records.push(bytes);
            }
            DiskyParallelPiece::EOF => break,
            _ => {}
        }
    }

    // Verify that we read all records
    assert_eq!(records.len(), 5);
    assert_eq!(&records[0][..], b"record1");
    assert_eq!(&records[1][..], b"record2");
    assert_eq!(&records[2][..], b"record3");
    assert_eq!(&records[3][..], b"record4");
    assert_eq!(&records[4][..], b"record5");

    // Close the reader
    reader.close()?;

    Ok(())
}

/// Test that the multi-threaded reader can read records from multiple shards
#[test]
fn test_multi_threaded_reader_multiple_shards() -> Result<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Trace)
        .is_test(true)
        .try_init();

    // Create the sharding config with 3 shards
    let sharding_config = create_multi_shard_config(3);

    // Create the reader with multiple threads
    let reader = MultiThreadedReaderConfig::new(sharding_config)
        .with_worker_threads(4)
        .with_queue_size_bytes(1024 * 1024)
        .build()?;

    // Read records using the iterator
    let records: Vec<_> = reader.collect::<Result<Vec<Bytes>>>()?;

    // We expect 15 records (5 per shard * 3 shards)
    assert_eq!(records.len(), 15);

    // Check a sampling of records
    assert!(records.iter().any(|r| &r[..] == b"record1"));
    assert!(records.iter().any(|r| &r[..] == b"record5"));

    Ok(())
}

/// Test try_read functionality
#[test]
fn test_multi_threaded_reader_try_read() -> Result<()> {
    // Create the sharding config with a single shard
    let sharding_config = create_multi_shard_config(1);

    // Create the reader
    let reader = MultiThreadedReaderConfig::new(sharding_config)
        .with_worker_threads(2)
        .with_queue_size_bytes(1024 * 1024)
        .build()?;

    // Read records using try_read
    let mut records = Vec::new();
    while let Some(piece) = reader.try_read()? {
        match piece {
            DiskyParallelPiece::Record(bytes) => {
                records.push(bytes);
            }
            DiskyParallelPiece::EOF => break,
            _ => {}
        }
    }

    // If we got records, verify them
    if !records.is_empty() {
        assert!(records.len() <= 5);
        assert_eq!(&records[0][..], b"record1");
    }

    // Now read the rest using blocking read
    while records.len() < 5 {
        match reader.read()? {
            DiskyParallelPiece::Record(bytes) => {
                records.push(bytes);
            }
            DiskyParallelPiece::EOF => break,
            _ => {}
        }
    }

    // Verify all records
    assert_eq!(records.len(), 5);
    assert_eq!(&records[0][..], b"record1");
    assert_eq!(&records[1][..], b"record2");
    assert_eq!(&records[2][..], b"record3");
    assert_eq!(&records[3][..], b"record4");
    assert_eq!(&records[4][..], b"record5");

    // Close the reader
    reader.close()?;

    Ok(())
}

/// Test single-threaded drain reading mode with multiple shards
#[test]
fn test_single_threaded_drain_mode() -> Result<()> {
    // Create the sharding config with identifiable shards
    let sharding_config = create_identifiable_shard_config(3);

    // Create the reader with only one worker thread and drain mode
    let reader = MultiThreadedReaderConfig::new(sharding_config)
        .with_worker_threads(1)
        .with_queue_size_bytes(1024 * 1024)
        .with_reading_order(ReadingOrder::Drain)
        .build()?;

    // Read all records
    let mut records = Vec::new();
    loop {
        match reader.read()? {
            DiskyParallelPiece::Record(bytes) => {
                records.push(bytes);
            }
            DiskyParallelPiece::EOF => break,
            _ => {}
        }
    }

    // We expect 15 records (5 per shard * 3 shards)
    assert_eq!(records.len(), 15);

    // With drain mode, we should read all records from shard 1 first, then shard 2, then shard 3
    // Convert bytes to strings for easier assertions
    let record_strings: Vec<String> = records
        .iter()
        .map(|b| String::from_utf8_lossy(&b[..]).to_string())
        .collect();

    // First 5 records should be from shard 1
    for i in 0..5 {
        assert!(record_strings[i].starts_with("shard1_"));
    }

    // Next 5 records should be from shard 2
    for i in 5..10 {
        assert!(record_strings[i].starts_with("shard2_"));
    }

    // Last 5 records should be from shard 3
    for i in 10..15 {
        assert!(record_strings[i].starts_with("shard3_"));
    }

    // Verify that records from each shard are in correct order
    for shard_id in 1..=3 {
        for record_num in 1..=5 {
            let expected = format!("shard{}_record{}", shard_id, record_num);
            let index = (shard_id - 1) * 5 + (record_num - 1);
            assert_eq!(record_strings[index], expected);
        }
    }

    // Close the reader
    reader.close()?;

    Ok(())
}

/// Test single-threaded round-robin reading mode with multiple shards
#[test]
fn test_single_threaded_round_robin_mode() -> Result<()> {
    // Create the sharding config with identifiable shards
    let sharding_config = create_identifiable_shard_config(3);

    // Create the reader with only one worker thread and round-robin mode
    let reader = MultiThreadedReaderConfig::new(sharding_config)
        .with_worker_threads(1)
        .with_queue_size_bytes(1024 * 1024)
        .with_reading_order(ReadingOrder::RoundRobin)
        .build()?;

    // Read all records
    let mut records = Vec::new();
    loop {
        match reader.read()? {
            DiskyParallelPiece::Record(bytes) => {
                records.push(bytes);
            }
            DiskyParallelPiece::EOF => break,
            _ => {}
        }
    }

    // We expect 15 records (5 per shard * 3 shards)
    assert_eq!(records.len(), 15);

    // Convert bytes to strings for easier assertions
    let record_strings: Vec<String> = records
        .iter()
        .map(|b| String::from_utf8_lossy(&b[..]).to_string())
        .collect();

    // With round-robin mode, we should read records 1 from all shards, then records 2, etc.
    // The first 3 records should be "record1" from each shard
    assert!(record_strings[0].starts_with("shard1_record1"));
    assert!(record_strings[1].starts_with("shard2_record1"));
    assert!(record_strings[2].starts_with("shard3_record1"));

    // The next 3 records should be "record2" from each shard
    assert!(record_strings[3].starts_with("shard1_record2"));
    assert!(record_strings[4].starts_with("shard2_record2"));
    assert!(record_strings[5].starts_with("shard3_record2"));

    // Check pattern for all records (should be interleaved)
    for record_num in 1..=5 {
        for shard_id in 1..=3 {
            let expected = format!("shard{}_record{}", shard_id, record_num);
            let index = (record_num - 1) * 3 + (shard_id - 1);
            assert_eq!(record_strings[index], expected);
        }
    }

    // Close the reader
    reader.close()?;

    Ok(())
}

/// Test multi-threaded reading with different reading orders
#[test]
fn test_multi_threaded_reading_order() -> Result<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .is_test(true)
        .try_init();
    // Test both reading modes with 2 threads (non-deterministic order, but should read all records)
    for reading_order in [ReadingOrder::Drain, ReadingOrder::RoundRobin] {
        // Create a fresh sharding config for each test
        let sharding_config = create_identifiable_shard_config(3);

        // Create the reader with multiple threads
        let reader = MultiThreadedReaderConfig::new(sharding_config)
            .with_worker_threads(2)
            .with_queue_size_bytes(1024 * 1024)
            .with_reading_order(reading_order)
            .build()?;

        // Read all records
        let mut records = Vec::new();
        loop {
            match reader.read()? {
                DiskyParallelPiece::Record(bytes) => {
                    records.push(bytes);
                }
                DiskyParallelPiece::EOF => break,
                _ => {}
            }
        }

        // We expect 15 records (5 per shard * 3 shards)
        assert_eq!(records.len(), 15);

        // Convert bytes to strings for easier assertions
        let record_strings: Vec<String> = records
            .iter()
            .map(|b| String::from_utf8_lossy(&b[..]).to_string())
            .collect();

        // Verify we have all expected records (regardless of order)
        for shard_id in 1..=3 {
            for record_num in 1..=5 {
                let expected = format!("shard{}_record{}", shard_id, record_num);
                assert!(
                    record_strings.iter().any(|s| s == &expected),
                    "Missing record: {}",
                    expected
                );
            }
        }

        // Close the reader
        reader.close()?;
    }

    Ok(())
}
