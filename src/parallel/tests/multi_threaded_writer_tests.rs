use bytes::Bytes;
use std::io::Cursor;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::error::Result;
use crate::parallel::multi_threaded_writer::{MultiThreadedWriter, MultiThreadedWriterConfig};
use crate::parallel::sharding::{Autosharder, Sharder};
use crate::parallel::writer::{ParallelWriterConfig, ShardingConfig};

// Helper function to create an in-memory sharder
fn create_memory_sharder() -> Box<dyn Sharder<Cursor<Vec<u8>>> + Send + Sync> {
    Box::new(Autosharder::new(|| Ok(Cursor::new(Vec::new()))))
}

// Helper to create a multi-threaded writer with minimal configuration
fn create_test_writer(
    num_shards: usize,
    num_threads: usize,
) -> Result<MultiThreadedWriter<Cursor<Vec<u8>>>> {
    let sharder = create_memory_sharder();
    let sharding_config = ShardingConfig::new(sharder, num_shards);
    let writer_config = ParallelWriterConfig::default();
    let mt_config = MultiThreadedWriterConfig::new(writer_config, num_threads);

    MultiThreadedWriter::new(sharding_config, mt_config)
}

// Helper to generate test data of a specific size
fn create_test_data(size: usize) -> Bytes {
    let data = (0..size).map(|i| (i % 256) as u8).collect::<Vec<u8>>();
    Bytes::from(data)
}

#[test]
fn test_basic_write_operations() -> Result<()> {
    // Create a writer with 2 shards and 2 worker threads
    let writer = create_test_writer(2, 2)?;

    // Write some records asynchronously
    let promise1 = writer.write_record(create_test_data(100))?;
    let promise2 = writer.write_record(create_test_data(200))?;

    // Give the worker threads some time to process
    thread::sleep(Duration::from_millis(50));

    // Check that the tasks have been processed
    assert_eq!(writer.pending_tasks()?, 0);

    // Wait for the promises to complete
    let _ = promise1.wait()?;
    let _ = promise2.wait()?;

    // Flush and close
    writer.flush()?;
    writer.close()?;

    Ok(())
}

#[test]
fn test_blocking_operations() -> Result<()> {
    // Create a writer with 2 shards and 2 worker threads
    let writer = create_test_writer(2, 2)?;

    // Write some records using blocking operations
    writer.write_record_blocking(create_test_data(100))?;
    writer.write_record_blocking(create_test_data(200))?;

    // Write using the slice convenience method
    let slice_data = [1, 2, 3, 4, 5];
    writer.write_slice_blocking(&slice_data)?;

    // Flush and close
    writer.flush()?;
    writer.close()?;

    Ok(())
}

#[test]
fn test_async_operations() -> Result<()> {
    // Create a writer with 2 shards and 4 worker threads
    let writer = create_test_writer(2, 4)?;

    // Write a bunch of records asynchronously
    let mut promises = Vec::new();
    for i in 0..20 {
        let size = 100 + (i * 10);
        let promise = writer.write_record(create_test_data(size))?;
        promises.push(promise);
    }

    // Give the worker threads some time to process
    thread::sleep(Duration::from_millis(100));

    // Flush asynchronously
    let flush_promise = writer.flush_async()?;

    // Wait for all operations to complete
    for promise in promises {
        let _ = promise.wait()?;
    }

    let _ = flush_promise.wait()?;

    // Close. 
    let _ = writer.close()?;

    Ok(())
}

#[test]
fn test_multiple_threads_write_same_writer() -> Result<()> {
    // Create a shared writer with 2 shards and 4 worker threads
    let writer = Arc::new(create_test_writer(2, 4)?);

    // Number of writer threads and records per thread
    let num_threads = 5;
    let records_per_thread = 10;

    // Spawn multiple threads to write records
    let mut handles = Vec::new();
    for i in 0..num_threads {
        let writer_clone = Arc::clone(&writer);

        let handle = thread::spawn(move || -> Result<()> {
            for j in 0..records_per_thread {
                // Mix of async and blocking writes
                if j % 2 == 0 {
                    let size = 100 + (i * 10) + j;
                    writer_clone.write_record_blocking(create_test_data(size))?;
                } else {
                    let size = 50 + (i * 10) + j;
                    let promise = writer_clone.write_record(create_test_data(size))?;
                    let _ = promise.wait()?;
                }
            }
            Ok(())
        });

        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap()?;
    }

    // Flush and close
    writer.flush()?;

    // Verify no pending tasks
    assert_eq!(writer.pending_tasks()?, 0);

    writer.close()?;

    Ok(())
}

#[test]
fn test_error_handling() -> Result<()> {
    // Create a writer with 2 shards and 2 worker threads
    let writer = create_test_writer(2, 2)?;

    // Write some records asynchronously
    let promise1 = writer.write_record(create_test_data(100))?;
    let promise2 = writer.write_record(create_test_data(200))?;

    // Give the worker threads some time to process
    thread::sleep(Duration::from_millis(50));

    // Close the writer
    writer.close()?;

    // Wait for the promises to complete
    let _ = promise1.wait()?;
    let _ = promise2.wait()?;

    // Trying to write after closing should fail
    let result = writer.write_record(create_test_data(100));
    assert!(result.is_err());

    let result = writer.write_record_blocking(create_test_data(100));
    assert!(result.is_err());

    let result = writer.write_slice_blocking(&[1, 2, 3]);
    assert!(result.is_err());

    Ok(())
}

#[test]
fn test_double_close_returns_error() -> Result<()> {
    // Create a writer with 2 shards and 2 worker threads
    let writer = create_test_writer(2, 2)?;

    // First close should succeed
    writer.close()?;

    // Second close should fail with WritingClosedFile error
    let result = writer.close();
    assert!(result.is_err());

    // Verify it's the specific error we expect
    if let Err(e) = result {
        assert!(matches!(e, crate::error::DiskyError::WritingClosedFile));
    }

    Ok(())
}

#[test]
fn test_writer_shutdown() -> Result<()> {
    // Create a writer with 2 shards and 4 worker threads
    let writer = create_test_writer(2, 4)?;

    // Write some records
    let promise1 = writer.write_record(create_test_data(100))?;
    let promise2 = writer.write_record(create_test_data(200))?;

    // Ensure worker threads have started
    thread::sleep(Duration::from_millis(20));

    // Properly shut down the writer by joining
    drop(writer);

    // Promises should be fulfilled since join waits for completion
    let _ = promise1.wait()?;
    let _ = promise2.wait()?;

    Ok(())
}

#[test]
fn test_large_number_of_records() -> Result<()> {
    // Create a writer with 2 shards and 4 worker threads
    let writer = create_test_writer(2, 4)?;

    // Write a large number of small records
    let num_records = 1000;
    let mut promises = Vec::with_capacity(num_records);

    for i in 0..num_records {
        let size = 10 + (i % 10); // Small varying sizes
        let promise = writer.write_record(create_test_data(size))?;
        promises.push(promise);
    }

    // Give the worker threads time to process
    while writer.pending_tasks()? > 0 {
        thread::sleep(Duration::from_millis(10));
    }

    // Ensure all promises are fulfilled
    for promise in promises {
        let _ = promise.wait()?;
    }

    writer.flush()?;
    writer.close()?;

    Ok(())
}
