use bytes::Bytes;
use disky::error::Result;
use disky::parallel::multi_threaded_reader::MultiThreadedReaderConfig;
use disky::parallel::multi_threaded_writer::MultiThreadedWriterConfig;
use disky::parallel::reader::{DiskyParallelPiece, ShardingConfig as ReaderShardingConfig};
use disky::parallel::writer::ShardingConfig as WriterShardingConfig;
use disky::shard::sink::FileShardsBuilder;
use disky::shard::source::{FileShards, SequentialShardSource};
use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tempfile::tempdir;

#[test]
fn test_multi_threaded_reader_with_multi_threaded_writer() -> Result<()> {
    // Create a temporary directory for our test
    let temp_dir = tempdir()?;
    let dir_path = temp_dir.path().to_path_buf();

    // Create a file sharder with the temporary directory
    let file_sharder = FileShardsBuilder::new(dir_path.clone(), "mt_test")
        .build()
        .unwrap();

    // Configure with 3 shards
    let sharding_config = WriterShardingConfig::new(Box::new(file_sharder), 3);

    // Initialize the multi-threaded writer with 2 worker threads
    let writer = MultiThreadedWriterConfig::new(sharding_config)
        .with_worker_threads(2)
        .build()?;

    // Write some test records - 50 records total
    let num_records = 50;
    let mut expected_records = HashSet::new();

    for i in 0..num_records {
        let record = format!("Test record {}", i);
        let bytes = Bytes::from(record.clone().into_bytes());
        writer.write_record_blocking(bytes)?;
        expected_records.insert(record);
    }

    // Flush and close the writer
    writer.flush()?;
    writer.close()?;

    // Now create a reader to read the records back
    let file_shards = FileShards::from_pattern(dir_path, "mt_test")?;
    let source = SequentialShardSource::new(file_shards);
    let reader_sharding_config = ReaderShardingConfig::new(Box::new(source), 3);

    // Create the multi-threaded reader with 2 worker threads
    let reader = MultiThreadedReaderConfig::new(reader_sharding_config)
        .with_worker_threads(2)
        .with_queue_size_bytes(1024 * 1024) // 1MB queue size
        .build()?;

    // Read all records
    let mut actual_records = HashSet::new();
    loop {
        match reader.read()? {
            DiskyParallelPiece::Record(bytes) => {
                let record = String::from_utf8(bytes.to_vec()).expect("Invalid UTF-8");
                actual_records.insert(record);
            }
            DiskyParallelPiece::EOF => {
                // No more records
                break;
            }
            _ => {} // Skip other control messages
        }
    }

    // Verify we got all records
    assert_eq!(actual_records.len(), num_records);
    assert_eq!(actual_records, expected_records);

    // Close the reader
    reader.close()?;

    Ok(())
}

#[test]
fn test_multi_threaded_reader_writer_async() -> Result<()> {
    // Create a temporary directory for our test
    let temp_dir = tempdir()?;
    let dir_path = temp_dir.path().to_path_buf();

    // Create a file sharder with the temporary directory
    let file_sharder = FileShardsBuilder::new(dir_path.clone(), "mt_async_test")
        .build()
        .unwrap();

    // Configure with 4 shards for more parallelism
    let sharding_config = WriterShardingConfig::new(Box::new(file_sharder), 4);

    // Initialize the multi-threaded writer with 3 worker threads
    let writer = MultiThreadedWriterConfig::new(sharding_config)
        .with_worker_threads(3)
        .build()?;

    // Write some test records asynchronously - 100 records total
    let num_records = 100;
    let mut write_promises = Vec::new();
    let mut expected_records = HashSet::new();

    for i in 0..num_records {
        let record = format!("Async test record {}", i);
        let bytes = Bytes::from(record.clone().into_bytes());
        write_promises.push(writer.write_record(bytes)?);
        expected_records.insert(record);
    }

    // Wait for all write promises to complete
    for promise in write_promises {
        let _ = promise.wait()?;
    }

    // Flush asynchronously and wait
    let flush_promise = writer.flush_async()?;
    let _ = flush_promise.wait()?;

    // Close asynchronously and wait
    writer.close()?;

    // Now create a reader to read the records back
    let file_shards = FileShards::from_pattern(dir_path, "mt_async_test")?;
    let source = SequentialShardSource::new(file_shards);
    let reader_sharding_config = ReaderShardingConfig::new(Box::new(source), 4);

    // Create the multi-threaded reader with 3 worker threads
    let reader = MultiThreadedReaderConfig::new(reader_sharding_config)
        .with_worker_threads(3)
        .with_queue_size_bytes(1024 * 1024) // 1MB queue size
        .build()?;

    // Read all records
    let mut actual_records = HashSet::new();

    // Use the iterator approach
    for record_result in reader {
        match record_result {
            Ok(bytes) => {
                let record = String::from_utf8(bytes.to_vec()).expect("Invalid UTF-8");
                actual_records.insert(record);
            }
            Err(e) => {
                panic!("Error reading record: {}", e);
            }
        }
    }

    // Verify we got all records
    assert_eq!(actual_records.len(), num_records);
    assert_eq!(actual_records, expected_records);

    Ok(())
}

#[test]
fn test_large_records_multi_threaded() -> Result<()> {
    // Create a temporary directory for our test
    let temp_dir = tempdir()?;
    let dir_path = temp_dir.path().to_path_buf();

    // Create a file sharder with the temporary directory
    let file_sharder = FileShardsBuilder::new(dir_path.clone(), "mt_large_test")
        .build()
        .unwrap();

    // Configure with 2 shards
    let sharding_config = WriterShardingConfig::new(Box::new(file_sharder), 2);

    // Initialize the multi-threaded writer with 2 worker threads
    let writer = MultiThreadedWriterConfig::new(sharding_config)
        .with_worker_threads(2)
        .build()?;

    // Write a few large records
    let num_records = 5;
    let record_size = 1_000_000; // 1MB per record
    let mut expected_hashes = Vec::new();

    for i in 0..num_records {
        // Create a large record with repeating pattern including the index
        let mut record = Vec::with_capacity(record_size);
        let pattern = format!("LargeRecord-{}-", i);
        let pattern_bytes = pattern.as_bytes();

        while record.len() < record_size {
            record.extend_from_slice(pattern_bytes);
        }

        // Truncate to exact size
        record.truncate(record_size);

        // Calculate a hash of the record for verification
        let hash = {
            let mut hasher = DefaultHasher::new();
            record.hash(&mut hasher);
            hasher.finish()
        };

        expected_hashes.push(hash);

        // Write the record
        writer.write_slice_blocking(&record)?;
    }

    // Flush and close the writer
    writer.flush()?;
    drop(writer); // joins and closes to ensure all resources are properly cleaned up

    // Now create a reader to read the records back
    let file_shards = FileShards::from_pattern(dir_path, "mt_large_test")?;
    let source = SequentialShardSource::new(file_shards);
    let reader_sharding_config = ReaderShardingConfig::new(Box::new(source), 2);

    // Create the multi-threaded reader with 2 worker threads and large queue size
    let reader = MultiThreadedReaderConfig::new(reader_sharding_config)
        .with_worker_threads(2)
        .with_queue_size_bytes(10 * 1024 * 1024) // 10MB queue size for large records
        .build()?;

    // Read all records and verify their hashes
    let mut actual_hashes = Vec::new();
    loop {
        match reader.read()? {
            DiskyParallelPiece::Record(bytes) => {
                // Calculate the hash of the record
                let hash = {
                    let mut hasher = DefaultHasher::new();
                    bytes.to_vec().hash(&mut hasher);
                    hasher.finish()
                };

                actual_hashes.push(hash);

                // Verify the record size
                assert_eq!(bytes.len(), record_size);
            }
            DiskyParallelPiece::EOF => {
                // No more records
                break;
            }
            _ => {} // Skip other control messages
        }
    }

    // Verify we got all records with matching hashes
    assert_eq!(actual_hashes.len(), num_records);

    // Sort both arrays for comparison since order might differ
    let mut expected_hashes_sorted = expected_hashes.clone();
    expected_hashes_sorted.sort();
    let mut actual_hashes_sorted = actual_hashes.clone();
    actual_hashes_sorted.sort();

    assert_eq!(actual_hashes_sorted, expected_hashes_sorted);

    // Close the reader
    reader.close()?;

    Ok(())
}
