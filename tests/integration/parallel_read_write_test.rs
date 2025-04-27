//use bytes::Bytes;
//use disky::error::Result;
//use disky::parallel::reader::{
//    ParallelReader, ParallelReaderConfig, ReadResult, ShardingConfig as ReaderShardingConfig,
//};
//use disky::parallel::sharding::{FileShardLocator, FileSharder};
//use disky::parallel::writer::{
//    ParallelWriter, ParallelWriterConfig, ShardingConfig as WriterShardingConfig,
//};
//use std::collections::hash_map::DefaultHasher;
//use std::collections::HashSet;
//use std::hash::{Hash, Hasher};
//use std::path::PathBuf;
//use tempfile::tempdir;
//
//#[test]
//fn test_parallel_reader_with_parallel_writer() -> Result<()> {
//    // Create a temporary directory for our test
//    let temp_dir = tempdir()?;
//    let dir_path = temp_dir.path().to_path_buf();
//
//    // Create a file sharder with the temporary directory
//    let file_sharder = FileSharder::new(dir_path.clone(), "test_shard");
//
//    // Configure with 3 shards
//    let sharding_config = WriterShardingConfig::new(Box::new(file_sharder), 3);
//
//    // Initialize the parallel writer
//    let writer = ParallelWriter::new(sharding_config, ParallelWriterConfig::default())?;
//
//    // Write some test records - 30 records total
//    let num_records = 30;
//    let mut expected_records = HashSet::new();
//
//    for i in 0..num_records {
//        let record = format!("Test record {}", i);
//        writer.write_record(record.as_bytes())?;
//        expected_records.insert(record);
//    }
//
//    // Flush and close the writer
//    writer.flush()?;
//    writer.close()?;
//
//    // Now create a reader to read the records back
//    let locator = FileShardLocator::new(dir_path, "test_shard")?;
//    let reader_sharding_config = ReaderShardingConfig::new(
//        Box::new(locator),
//        3, // Same number of shards as the writer
//    );
//
//    let reader = ParallelReader::new(reader_sharding_config, ParallelReaderConfig::default())?;
//
//    // Read all records synchronously
//    let mut actual_records = HashSet::new();
//    loop {
//        match reader.read()? {
//            ReadResult::Record(bytes) => {
//                let record = String::from_utf8(bytes.to_vec()).expect("Invalid UTF-8");
//                actual_records.insert(record);
//            }
//            ReadResult::ShardFinished => {
//                // Shard finished, continue to next shard
//                continue;
//            }
//            ReadResult::EOF => {
//                // No more records
//                break;
//            }
//        }
//    }
//
//    // Verify we got all records
//    assert_eq!(actual_records.len(), num_records);
//    assert_eq!(actual_records, expected_records);
//
//    // Close the reader
//    reader.close()?;
//
//    Ok(())
//}
//
//#[test]
//fn test_parallel_reader_async_with_parallel_writer() -> Result<()> {
//    // Create a temporary directory for our test
//    let temp_dir = tempdir()?;
//    let dir_path = temp_dir.path().to_path_buf();
//
//    // Create a file sharder with the temporary directory
//    let file_sharder = FileSharder::new(dir_path.clone(), "async_test");
//
//    // Configure with 5 shards for more parallelism
//    let sharding_config = WriterShardingConfig::new(Box::new(file_sharder), 5);
//
//    // Initialize the parallel writer
//    let writer = ParallelWriter::new(sharding_config, ParallelWriterConfig::default())?;
//
//    // Write some test records asynchronously - 50 records total
//    let num_records = 50;
//    let mut write_promises = Vec::new();
//    let mut expected_records = HashSet::new();
//
//    for i in 0..num_records {
//        let record = format!("Async test record {}", i);
//        let bytes = Bytes::from(record.clone().into_bytes());
//        write_promises.push(writer.write_record_async(bytes)?);
//        expected_records.insert(record);
//    }
//
//    // Process all write tasks
//    writer.process_all_tasks()?;
//
//    // Wait for all write promises to complete
//    for promise in write_promises {
//        promise.wait()??;
//    }
//
//    // Flush and close the writer
//    writer.flush()?;
//    writer.close()?;
//
//    // Now create a reader to read the records back
//    let locator = FileShardLocator::new(dir_path, "async_test")?;
//    let reader_sharding_config = ReaderShardingConfig::new(
//        Box::new(locator),
//        5, // Same number of shards as the writer
//    );
//
//    let reader = ParallelReader::new(reader_sharding_config, ParallelReaderConfig::default())?;
//
//    // Read all records asynchronously
//    let mut read_promises = Vec::new();
//    for _ in 0..num_records {
//        read_promises.push(reader.read_async()?);
//    }
//
//    // Process all read tasks
//    reader.process_all_tasks()?;
//
//    // Check results
//    let mut actual_records = HashSet::new();
//    for promise in read_promises {
//        match promise.wait()? {
//            Ok(ReadResult::Record(bytes)) => {
//                let record = String::from_utf8(bytes.to_vec()).expect("Invalid UTF-8");
//                actual_records.insert(record);
//            }
//            Ok(ReadResult::ShardFinished) => {
//                // This shouldn't happen in the promises
//                panic!("Unexpected ShardFinished in read promise");
//            }
//            Ok(ReadResult::EOF) => {
//                // This shouldn't happen with exactly num_records reads
//                panic!("Unexpected EOF in read promise");
//            }
//            Err(e) => {
//                panic!("Error reading record: {}", e);
//            }
//        }
//    }
//
//    // Verify we got all records
//    assert_eq!(actual_records.len(), num_records);
//    assert_eq!(actual_records, expected_records);
//
//    // Close the reader
//    reader.close()?;
//
//    Ok(())
//}
//
//#[test]
//fn test_large_records_parallel_read_write() -> Result<()> {
//    // Create a temporary directory for our test
//    let temp_dir = tempdir()?;
//    let dir_path = temp_dir.path().to_path_buf();
//
//    // Create a file sharder with the temporary directory
//    let file_sharder = FileSharder::new(dir_path.clone(), "large_test");
//
//    // Configure with 2 shards
//    let sharding_config = WriterShardingConfig::new(Box::new(file_sharder), 2);
//
//    // Initialize the parallel writer
//    let writer = ParallelWriter::new(sharding_config, ParallelWriterConfig::default())?;
//
//    // Write a few large records
//    let num_records = 5;
//    let record_size = 1_000_000; // 1MB per record
//    let mut expected_hashes = Vec::new();
//
//    for i in 0..num_records {
//        // Create a large record with repeating pattern including the index
//        let mut record = Vec::with_capacity(record_size);
//        let pattern = format!("LargeRecord-{}-", i);
//        let pattern_bytes = pattern.as_bytes();
//
//        while record.len() < record_size {
//            record.extend_from_slice(pattern_bytes);
//        }
//
//        // Truncate to exact size
//        record.truncate(record_size);
//
//        // Calculate a hash of the record for verification
//        let hash = {
//            let mut hasher = DefaultHasher::new();
//            record.hash(&mut hasher);
//            hasher.finish()
//        };
//
//        expected_hashes.push(hash);
//
//        // Write the record
//        writer.write_record(&record)?;
//    }
//
//    // Flush and close the writer
//    writer.flush()?;
//    writer.close()?;
//
//    // Now create a reader to read the records back
//    let locator = FileShardLocator::new(dir_path, "large_test")?;
//    let reader_sharding_config = ReaderShardingConfig::new(
//        Box::new(locator),
//        2, // Same number of shards as the writer
//    );
//
//    let reader = ParallelReader::new(reader_sharding_config, ParallelReaderConfig::default())?;
//
//    // Read all records and verify their hashes
//    let mut actual_hashes = Vec::new();
//    loop {
//        match reader.read()? {
//            ReadResult::Record(bytes) => {
//                // Calculate the hash of the record
//                let hash = {
//                    let mut hasher = DefaultHasher::new();
//                    bytes.to_vec().hash(&mut hasher);
//                    hasher.finish()
//                };
//
//                actual_hashes.push(hash);
//
//                // Verify the record size
//                assert_eq!(bytes.len(), record_size);
//            }
//            ReadResult::ShardFinished => {
//                // Shard finished, continue to next shard
//                continue;
//            }
//            ReadResult::EOF => {
//                // No more records
//                break;
//            }
//        }
//    }
//
//    // Verify we got all records with matching hashes
//    assert_eq!(actual_hashes.len(), num_records);
//    assert_eq!(actual_hashes, expected_hashes);
//
//    // Close the reader
//    reader.close()?;
//
//    Ok(())
//}
//
