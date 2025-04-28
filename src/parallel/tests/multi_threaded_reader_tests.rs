use std::io::{Cursor, Seek, SeekFrom};

use bytes::Bytes;

use crate::error::Result;
use crate::parallel::multi_threaded_reader::{MultiThreadedReader, MultiThreadedReaderConfig};
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

/// Create a memory shard locator with multiple shards
fn create_multi_shard_locator(
    shard_count: usize,
) -> Box<dyn crate::parallel::sharding::ShardLocator<Cursor<Vec<u8>>> + Send + Sync> {
    // Create the locator
    let locator = MemoryShardLocator::new(move || create_test_cursor(), shard_count);

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
        2, // Two worker threads
        1024 * 1024, // 1MB queue
    );

    // Create the reader
    let reader = MultiThreadedReader::new(sharding_config, reader_config)?;

    // Give workers time to read into the queue
    std::thread::sleep(std::time::Duration::from_millis(50));

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

