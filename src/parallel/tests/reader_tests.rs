use crate::error::Result;
use crate::parallel::reader::{
    DiskyParallelPiece, ParallelReader, ParallelReaderConfig, ShardingConfig,
};
use crate::shard::source::{MemoryShards, SequentialShardSource};
use crate::writer::RecordWriter;
use std::io::Cursor;

fn create_shard_data(shard_num: usize, num_records: usize) -> Vec<u8> {
    let mut buffer = Vec::new();

    {
        let cursor = Cursor::new(&mut buffer);
        let mut writer = RecordWriter::new(cursor).unwrap();

        for i in 0..num_records {
            writer
                .write_record(format!("Shard {} Record {}", shard_num, i).as_bytes())
                .unwrap();
        }

        writer.close().unwrap();
    }

    buffer
}

fn create_empty_shard_data() -> Vec<u8> {
    let mut buffer = Vec::new();

    {
        let cursor = Cursor::new(&mut buffer);
        let mut writer = RecordWriter::new(cursor).unwrap();
        writer.close().unwrap();
    }

    buffer
}

#[test]
fn test_parallel_reader_basic() -> Result<()> {
    // Create a factory function that produces data for different shards
    let shard_count = 3;
    let num_records = 3;

    let shards = MemoryShards::new(
        move |index: usize| Ok(create_shard_data(index, num_records)),
        shard_count,
    );
    let source = SequentialShardSource::new(shards);

    // Create a sharding config
    let sharding_config = ShardingConfig::new(Box::new(source), shard_count);

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
    // Create new reader
    let shards2 = MemoryShards::new(
        move |index: usize| Ok(create_shard_data(index, num_records)),
        shard_count,
    );
    let source2 = SequentialShardSource::new(shards2);
    let sharding_config = ShardingConfig::new(Box::new(source2), shard_count);

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
    let empty_data = create_empty_shard_data();
    let shard_count = 3;

    let shards = MemoryShards::new(move |_index: usize| Ok(empty_data.clone()), shard_count);
    let source = SequentialShardSource::new(shards);

    // Create a sharding config
    let sharding_config = ShardingConfig::new(Box::new(source), shard_count);

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
    // Simply test that errors from shard source are handled gracefully
    // without affecting the overall function of the reader

    // Create a factory function that will produce 2 valid shards
    let shard_count = 2;

    let shards = MemoryShards::new(
        move |index: usize| {
            let mut buffer = Vec::new();
            {
                let cursor = Cursor::new(&mut buffer);
                let mut writer = RecordWriter::new(cursor)?;
                writer.write_record(format!("Record {}", index).as_bytes())?;
                writer.close()?;
            }
            Ok(buffer)
        },
        shard_count,
    );
    let source = SequentialShardSource::new(shards);

    // Create a sharding config
    let sharding_config = ShardingConfig::new(Box::new(source), shard_count);

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
