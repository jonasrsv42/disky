use std::io::Cursor;
use std::sync::mpsc;
use std::thread;
use std::sync::Arc;
use bytes::Bytes;

use crate::parallel::writer::{ParallelWriter, ParallelWriterConfig};
use crate::writer::{RecordWriter, RecordWriterConfig};

#[test]
fn test_parallel_writer_basic() {
    // Create a few in-memory writers
    let mut writers = Vec::new();
    for _ in 0..3 {
        let cursor = Cursor::new(Vec::new());
        let writer = RecordWriter::with_config(cursor, RecordWriterConfig::default()).unwrap();
        writers.push(Box::new(writer));
    }

    // Create parallel writer
    let parallel_writer =
        Arc::new(ParallelWriter::new(writers, ParallelWriterConfig::default()).unwrap());

    // Queue up some writes
    let data1 = Bytes::from(b"hello world".to_vec());
    let data2 = Bytes::from(b"testing 123".to_vec());
    let data3 = Bytes::from(b"parallel writes".to_vec());

    let promise1 = parallel_writer.write_record_async(data1.clone()).unwrap();
    let promise2 = parallel_writer.write_record_async(data2.clone()).unwrap();
    let promise3 = parallel_writer.write_record_async(data3.clone()).unwrap();

    // Channel to signal when tasks are ready to be processed
    let (ready_tx, ready_rx) = mpsc::channel();

    // Process tasks in a separate thread
    let writer_clone = parallel_writer.clone();
    let handle = thread::spawn(move || {
        // Signal that we're ready to process tasks
        ready_tx.send(()).unwrap();

        // Process all tasks
        writer_clone.process_all_tasks().unwrap()
    });

    // Wait until the worker thread is ready
    ready_rx.recv().unwrap();

    // Wait for promises to be fulfilled
    let result1 = promise1.wait().unwrap();
    let result2 = promise2.wait().unwrap();
    let result3 = promise3.wait().unwrap();

    // All writes should succeed
    assert!(result1.is_ok(), "First write should succeed");
    assert!(result2.is_ok(), "Second write should succeed");
    assert!(result3.is_ok(), "Third write should succeed");

    // Wait for processing thread to finish
    handle.join().unwrap();
}

#[test]
fn test_sync_and_async_writes() {
    // Create a few in-memory writers
    let mut writers = Vec::new();
    for _ in 0..3 {
        let cursor = Cursor::new(Vec::new());
        let writer = RecordWriter::with_config(cursor, RecordWriterConfig::default()).unwrap();
        writers.push(Box::new(writer));
    }

    // Create parallel writer
    let parallel_writer =
        Arc::new(ParallelWriter::new(writers, ParallelWriterConfig::default()).unwrap());

    // Perform some synchronous writes
    parallel_writer.write_record(b"sync record 1").unwrap();
    parallel_writer.write_record(b"sync record 2").unwrap();

    // Queue some async writes
    let data1 = Bytes::from(b"async record 1".to_vec());
    let data2 = Bytes::from(b"async record 2".to_vec());

    let promise1 = parallel_writer.write_record_async(data1).unwrap();
    let promise2 = parallel_writer.write_record_async(data2).unwrap();

    // Process all pending writes first
    parallel_writer.process_all_tasks().unwrap();

    // Check promises
    assert!(
        promise1.wait().unwrap().is_ok(),
        "First async write should succeed"
    );
    assert!(
        promise2.wait().unwrap().is_ok(),
        "Second async write should succeed"
    );

    // Test async flush operation
    let flush_promise = parallel_writer.flush_async().unwrap();
    parallel_writer.process_all_tasks().unwrap();
    assert!(
        flush_promise.wait().unwrap().is_ok(),
        "Async flush should succeed"
    );

    // Test synchronous flush - should work without requiring process_all_tasks
    parallel_writer.flush().unwrap();

    // Test async close operation
    let close_promise = parallel_writer.close_async().unwrap();
    parallel_writer.process_all_tasks().unwrap();
    assert!(
        close_promise.wait().unwrap().is_ok(),
        "Async close should succeed"
    );
}

#[test]
fn test_sync_operations() {
    // Create a few in-memory writers
    let mut writers = Vec::new();
    for _ in 0..3 {
        let cursor = Cursor::new(Vec::new());
        let writer = RecordWriter::with_config(cursor, RecordWriterConfig::default()).unwrap();
        writers.push(Box::new(writer));
    }

    // Create parallel writer
    let parallel_writer =
        ParallelWriter::new(writers, ParallelWriterConfig::default()).unwrap();

    // Perform synchronous writes
    parallel_writer.write_record(b"record 1").unwrap();
    parallel_writer.write_record(b"record 2").unwrap();
    parallel_writer.write_record(b"record 3").unwrap();

    // Perform synchronous flush directly - this should work without deadlock
    parallel_writer.flush().unwrap();

    // Perform synchronous close directly - this should work without deadlock
    parallel_writer.close().unwrap();

    // The queue should now be closed
    assert!(parallel_writer.write_record(b"should fail").is_err());
}

#[test]
fn test_task_queue_operations() {
    // Create a parallel writer with a few in-memory writers
    let mut writers = Vec::new();
    for _ in 0..3 {
        let cursor = Cursor::new(Vec::new());
        let writer = RecordWriter::with_config(cursor, RecordWriterConfig::default()).unwrap();
        writers.push(Box::new(writer));
    }
    
    let parallel_writer = Arc::new(
        ParallelWriter::new(writers, ParallelWriterConfig::default()).unwrap()
    );
    
    // Test task queue operations
    assert_eq!(parallel_writer.pending_task_count().unwrap(), 0);
    assert!(!parallel_writer.has_pending_tasks().unwrap());
    
    // Add some async tasks
    let data1 = Bytes::from(b"task 1".to_vec());
    let data2 = Bytes::from(b"task 2".to_vec());
    
    let promise1 = parallel_writer.write_record_async(data1).unwrap();
    let promise2 = parallel_writer.write_record_async(data2).unwrap();
    
    // Check task count
    assert_eq!(parallel_writer.pending_task_count().unwrap(), 2);
    assert!(parallel_writer.has_pending_tasks().unwrap());
    
    // Process tasks in a separate thread to test concurrency
    let (ready_tx, ready_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let writer_clone = parallel_writer.clone();
    
    let handle = thread::spawn(move || {
        // Signal ready to process
        ready_tx.send(()).unwrap();
        
        // Process all tasks
        writer_clone.process_all_tasks().unwrap();
        
        // Signal completion
        done_tx.send(()).unwrap();
    });
    
    // Wait for thread to be ready
    ready_rx.recv().unwrap();
    
    // Queue an async flush operation
    let flush_promise = parallel_writer.flush_async().unwrap();
    
    // Wait for processing to complete
    done_rx.recv().unwrap();
    
    // Wait for promises to be fulfilled
    assert!(promise1.wait().unwrap().is_ok());
    assert!(promise2.wait().unwrap().is_ok());
    
    // Process the flush task
    parallel_writer.process_next_task().unwrap();
    assert!(flush_promise.wait().unwrap().is_ok());
    
    // Queue should be empty now
    assert_eq!(parallel_writer.pending_task_count().unwrap(), 0);
    assert!(!parallel_writer.has_pending_tasks().unwrap());
    
    // Test resource count
    assert!(parallel_writer.available_resource_count().unwrap() >= 3);
    
    // Wait for the thread to complete
    handle.join().unwrap();
}

#[test]
fn test_async_write_failure() {
    // Create parallel writer with just a single writer for simplicity
    let mut writers = Vec::new();
    let cursor = Cursor::new(Vec::new());
    let writer = RecordWriter::with_config(cursor, RecordWriterConfig::default()).unwrap();
    writers.push(Box::new(writer));
    
    let parallel_writer = 
        ParallelWriter::new(writers, ParallelWriterConfig::default()).unwrap();
        
    // First close the task queue to make write_record_async fail
    parallel_writer.close().unwrap();
    
    // Now try to queue an async write - should fail
    let data = Bytes::from(b"test data".to_vec());
    assert!(parallel_writer.write_record_async(data).is_err());
    
    // Sync writes should also fail
    assert!(parallel_writer.write_record(b"test data").is_err());
}

#[test]
fn test_bytes_written_tracking() {
    // Create a parallel writer with a single writer
    let cursor = Cursor::new(Vec::new());
    let writer = RecordWriter::with_config(cursor, RecordWriterConfig::default()).unwrap();
    let writers = vec![Box::new(writer)];
    
    let parallel_writer = 
        ParallelWriter::new(writers, ParallelWriterConfig::default()).unwrap();
    
    // Write some records with known sizes
    let record1 = b"first record";
    let record2 = b"second record with more data";
    let record3 = b"third";
    
    parallel_writer.write_record(record1).unwrap();
    parallel_writer.write_record(record2).unwrap();
    parallel_writer.write_record(record3).unwrap();
    
    // Access the resource to check bytes_written
    let resource_pool = parallel_writer.get_resource_pool();
    resource_pool.process_all_resources(|resource| {
        // Calculate the expected bytes
        let expected_bytes = record1.len() + record2.len() + record3.len();
        
        // Verify the bytes_written counter
        assert_eq!(resource.bytes_written, expected_bytes, 
                   "Bytes written counter should match sum of record lengths");
        
        Ok(())
    }).unwrap();
}

#[test]
fn test_bytes_written_with_async_writes() {
    // Create a parallel writer with a single writer
    let cursor = Cursor::new(Vec::new());
    let writer = RecordWriter::with_config(cursor, RecordWriterConfig::default()).unwrap();
    let writers = vec![Box::new(writer)];
    
    let parallel_writer = Arc::new(
        ParallelWriter::new(writers, ParallelWriterConfig::default()).unwrap()
    );
    
    // Prepare test data with known sizes
    let record1 = Bytes::from(b"first async record".to_vec());
    let record2 = Bytes::from(b"second async record with more bytes".to_vec());
    
    // Keep track of expected total bytes
    let expected_bytes = record1.len() + record2.len();
    
    // Queue async writes
    let promise1 = parallel_writer.write_record_async(record1).unwrap();
    let promise2 = parallel_writer.write_record_async(record2).unwrap();
    
    // Process the writes
    parallel_writer.process_all_tasks().unwrap();
    
    // Check promises
    assert!(promise1.wait().unwrap().is_ok());
    assert!(promise2.wait().unwrap().is_ok());
    
    // Verify bytes_written counter
    parallel_writer.get_resource_pool().process_all_resources(|resource| {
        assert_eq!(resource.bytes_written, expected_bytes, 
                   "Bytes written counter should track async writes correctly");
        
        Ok(())
    }).unwrap();
}

#[test]
fn test_writer_rotation_on_byte_limit() {
    // Create multiple writers
    let mut writers = Vec::new();
    for _ in 0..3 {
        let cursor = Cursor::new(Vec::new());
        let writer = RecordWriter::with_config(cursor, RecordWriterConfig::default()).unwrap();
        writers.push(Box::new(writer));
    }
    
    // Create a config with a low max_bytes_per_writer limit
    let config = ParallelWriterConfig {
        writer_config: RecordWriterConfig::default(),
        max_bytes_per_writer: Some(15), // Only allow 15 bytes per writer
    };
    
    let parallel_writer = ParallelWriter::new(writers, config).unwrap();
    
    // Create larger records to ensure we hit the byte limit faster
    let record_data = [1u8; 10]; // 10-byte record
    
    // We'll write enough records to exceed all writers' limits
    // Each writer gets ~1/3 of the writes due to round-robin distribution
    // With a 15-byte limit and 10-byte records, each writer should handle 1 record
    // and get dropped on attempt to write the second record (which would make it 20 bytes)
    
    // Write 9 records, which should eventually cause all writers to hit their
    // byte limits and be dropped
    for i in 0..9 {
        println!("Writing record {}", i);
        
        match parallel_writer.write_record(&record_data) {
            Ok(_) => {},
            Err(e) => {
                println!("Error on record {}: {}", i, e);
                // After all writers hit their limit, we expect an error
                // This means we've successfully dropped all writers
                break;
            }
        }
        
        // Print available writers after each write
        println!("Available writers after record {}: {}", 
                i, parallel_writer.available_resource_count().unwrap());
        
        // Print bytes written by each writer
        parallel_writer.get_resource_pool().process_all_resources(|resource| {
            println!("Writer ID: {}, bytes written: {}", resource.id, resource.bytes_written);
            Ok(())
        }).unwrap();
    }
    
    // By this point, if our max_bytes_per_writer logic is working,
    // we should have dropped all writers because they all exceeded the limit
    assert!(parallel_writer.available_resource_count().unwrap() < 3, 
           "Writers should have been dropped after exceeding byte limit");
}

#[test]
fn test_partial_writer_rotation_on_byte_limit() {
    // Create just 2 writers this time
    let mut writers = Vec::new();
    for _ in 0..2 {
        let cursor = Cursor::new(Vec::new());
        let writer = RecordWriter::with_config(cursor, RecordWriterConfig::default()).unwrap();
        writers.push(Box::new(writer));
    }
    
    // Create a config with a byte limit that will cause only one writer to be dropped
    let config = ParallelWriterConfig {
        writer_config: RecordWriterConfig::default(),
        // Only allow 10 bytes per writer, which means first writer will be dropped
        // after the first record, but second writer should remain available
        max_bytes_per_writer: Some(8),
    };
    
    let parallel_writer = ParallelWriter::new(writers, config).unwrap();
    
    // Total of 2 writers initially
    assert_eq!(parallel_writer.available_resource_count().unwrap(), 2);
    
    // First record (5 bytes) - should be handled by first writer
    let record1 = b"first";
    parallel_writer.write_record(record1).unwrap();
    
    // Second record (6 bytes) - should be handled by second writer
    let record2 = b"second";
    parallel_writer.write_record(record2).unwrap();
    
    // Third record (5 bytes) - should go back to first writer and exceed the limit
    let record3 = b"third";
    parallel_writer.write_record(record3).unwrap();
    
    // Check writers status
    parallel_writer.get_resource_pool().process_all_resources(|resource| {
        println!("Writer ID: {}, bytes written: {}", resource.id, resource.bytes_written);
        Ok(())
    }).unwrap();
    
    // We should have exactly 1 writer now (the other was dropped after exceeding limit)
    assert_eq!(parallel_writer.available_resource_count().unwrap(), 1, 
              "Should have 1 writer left after 1 reached byte limit");
    
    // The remaining writer should still be usable
    let record4 = b"fourth";
    assert!(parallel_writer.write_record(record4).is_ok(),
            "Should be able to write to the remaining writer");
}