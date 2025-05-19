use std::io::{Cursor, Seek, SeekFrom};

use bytes::Bytes;

use crate::error::Result;
use crate::parallel::multi_threaded_reader::{
    MultiThreadedReader, MultiThreadedReaderConfig, ReadingOrder,
};
use crate::parallel::reader::{DiskyParallelPiece, ParallelReaderConfig, ShardingConfig};
use crate::parallel::sharding::MemoryShardLocator;

/// Create a memory cursor with some test records
fn create_test_cursor() -> Result<Cursor<Vec<u8>>> {
    // Create a buffer
    let mut data = Vec::new();

    {
        // Create a cursor that we can seek on
        let cursor = Cursor::new(&mut data);

        // Create a writer with the cursor
        let mut writer = crate::writer::RecordWriter::with_config(
            cursor,
            crate::writer::RecordWriterConfig::default(),
        )?;

        // Write some test records
        writer.write_record(b"record1")?;
        writer.write_record(b"record2")?;
        writer.write_record(b"record3")?;
        writer.write_record(b"record4")?;
        writer.write_record(b"record5")?;

        // Writer goes out of scope here, releasing the cursor
    }

    // Create a new cursor with the data
    let mut cursor = Cursor::new(data);

    // Rewind the cursor to the beginning
    cursor.seek(SeekFrom::Start(0))?;

    // Return the cursor
    Ok(cursor)
}

/// Create a memory cursor with records that identify their source
fn create_identifiable_cursor(shard_id: usize) -> Result<Cursor<Vec<u8>>> {
    // Create a buffer
    let mut data = Vec::new();

    {
        // Create a cursor that we can seek on
        let cursor = Cursor::new(&mut data);

        // Create a writer with the cursor
        let mut writer = crate::writer::RecordWriter::with_config(
            cursor,
            crate::writer::RecordWriterConfig::default(),
        )?;

        // Write records with shard id prefix for identification
        for i in 1..=5 {
            let record = format!("shard{}_record{}", shard_id, i);
            writer.write_record(record.as_bytes())?;
        }
    }

    // Create a new cursor with the data
    let mut cursor = Cursor::new(data);

    // Rewind the cursor to the beginning
    cursor.seek(SeekFrom::Start(0))?;

    // Return the cursor
    Ok(cursor)
}

/// Create a memory shard locator with multiple shards
fn create_multi_shard_locator(
    shard_count: usize,
) -> Box<dyn crate::parallel::sharding::ShardLocator<Cursor<Vec<u8>>> + Send + Sync> {
    // Create the locator
    let locator = MemoryShardLocator::new(move || create_test_cursor(), shard_count);

    Box::new(locator)
}

/// Create a memory shard locator with multiple shards that have identifiable records
fn create_identifiable_shard_locator(
    shard_count: usize,
) -> Box<dyn crate::parallel::sharding::ShardLocator<Cursor<Vec<u8>>> + Send + Sync> {
    // Track which shard we're currently creating
    let counter = std::sync::atomic::AtomicUsize::new(0);

    // Create the locator where each shard has records identifying which shard they're from
    let locator = MemoryShardLocator::new(
        move || {
            let shard_id = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1; // +1 so we have shards numbered from 1
            create_identifiable_cursor(shard_id)
        },
        shard_count,
    );

    Box::new(locator)
}

/// Test that the multi-threaded reader can read records from a single shard
#[test]
fn test_multi_threaded_reader_single_shard() -> Result<()> {
    // Create a locator with a single shard
    let locator = create_multi_shard_locator(1);

    // Create the sharding config
    let sharding_config = ShardingConfig::new(locator, 1);

    // Create the reader config
    let reader_config = MultiThreadedReaderConfig::new(
        ParallelReaderConfig::default(),
        2,           // Two worker threads
        1024 * 1024, // 1MB queue
    );

    // Create the reader
    let reader = MultiThreadedReader::new(sharding_config, reader_config)?;

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

    // Create a locator with multiple shards
    let locator = create_multi_shard_locator(3);

    // Create the sharding config
    let sharding_config = ShardingConfig::new(locator, 2);

    // Create the reader config with multiple threads
    let reader_config = MultiThreadedReaderConfig::new(
        ParallelReaderConfig::default(),
        4,           // Four worker threads
        1024 * 1024, // 1MB queue
    );

    // Create the reader
    let reader = MultiThreadedReader::new(sharding_config, reader_config)?;

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
    // Create a locator with a single shard
    let locator = create_multi_shard_locator(1);

    // Create the sharding config
    let sharding_config = ShardingConfig::new(locator, 1);

    // Create the reader config
    let reader_config = MultiThreadedReaderConfig::new(
        ParallelReaderConfig::default(),
        2,           // Two worker threads
        1024 * 1024, // 1MB queue
    );

    // Create the reader
    let reader = MultiThreadedReader::new(sharding_config, reader_config)?;

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
    // Create a locator with multiple shards, each with identifiable records
    let locator = create_identifiable_shard_locator(3);

    // Create the sharding config
    let sharding_config = ShardingConfig::new(locator, 3);

    // Create the reader config with only one worker thread and drain mode
    let reader_config = MultiThreadedReaderConfig::new(
        ParallelReaderConfig::default(),
        1,           // One worker thread (single-threaded)
        1024 * 1024, // 1MB queue
    )
    .with_reading_order(ReadingOrder::Drain);

    // Create the reader
    let reader = MultiThreadedReader::new(sharding_config, reader_config)?;

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
    // Create a locator with multiple shards, each with identifiable records
    let locator = create_identifiable_shard_locator(3);

    // Create the sharding config
    let sharding_config = ShardingConfig::new(locator, 3);

    // Create the reader config with only one worker thread and round-robin mode
    let reader_config = MultiThreadedReaderConfig::new(
        ParallelReaderConfig::default(),
        1,           // One worker thread (single-threaded)
        1024 * 1024, // 1MB queue
    )
    .with_reading_order(ReadingOrder::RoundRobin);

    // Create the reader
    let reader = MultiThreadedReader::new(sharding_config, reader_config)?;

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
    // Test both reading modes with 2 threads (non-deterministic order, but should read all records)
    for reading_order in [ReadingOrder::Drain, ReadingOrder::RoundRobin] {
        // Create a fresh locator for each test (instead of trying to clone the box)
        let locator = create_identifiable_shard_locator(3);

        // Create the sharding config
        let sharding_config = ShardingConfig::new(locator, 3);

        // Create the reader config with multiple threads
        let reader_config = MultiThreadedReaderConfig::new(
            ParallelReaderConfig::default(),
            2,           // Two worker threads
            1024 * 1024, // 1MB queue
        )
        .with_reading_order(reading_order);

        // Create the reader
        let reader = MultiThreadedReader::new(sharding_config, reader_config)?;

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

