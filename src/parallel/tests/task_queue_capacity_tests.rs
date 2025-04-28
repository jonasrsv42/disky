use std::sync::Arc;
use std::thread;
use std::time::Duration;

use bytes::Bytes;

use crate::parallel::multi_threaded_writer::MultiThreadedWriterConfig;
use crate::parallel::sharding::Autosharder;
use crate::parallel::writer::{ParallelWriter, ParallelWriterConfig, ShardingConfig};
use crate::error::Result;

#[test]
fn test_bounded_task_queue() -> Result<()> {
    // Create a configuration with a very small task queue capacity (1)
    let writer_config = ParallelWriterConfig::default()
        .with_task_queue_capacity(1);
    
    // Create an in-memory test sharder
    let sharder = Autosharder::new(|| {
        Ok(std::io::Cursor::new(Vec::new()))
    });
    
    let sharding_config = ShardingConfig::new(
        Box::new(sharder),
        1 // Just a single shard to simplify testing
    );
    
    // Create a writer with bounded queue
    let writer = Arc::new(ParallelWriter::new(sharding_config, writer_config)?);
    
    // Setup channels for synchronization
    let (task1_queued_tx, task1_queued_rx) = std::sync::mpsc::channel::<()>();
    let (ready_for_task2_tx, ready_for_task2_rx) = std::sync::mpsc::channel::<()>();
    let (task2_blocked_tx, task2_blocked_rx) = std::sync::mpsc::channel::<()>();
    // No longer needed, removing unblock channel
    let (task2_queued_tx, task2_queued_rx) = std::sync::mpsc::channel::<()>();
    
    // Start a thread that will write tasks but not process them
    let writer_thread = {
        let writer_clone = Arc::clone(&writer);
        thread::spawn(move || -> Result<()> {
            // Queue up first task
            writer_clone.write_record_async(Bytes::from("task1"))?;
            task1_queued_tx.send(()).unwrap();
            
            // Wait for the test to tell us to continue
            ready_for_task2_rx.recv().unwrap();
            
            // Signal that we're about to queue the second task
            task2_blocked_tx.send(()).unwrap();
            
            // Queue up second task - this will block until the first is processed
            // since our capacity is 1
            writer_clone.write_record_async(Bytes::from("task2"))?;
            
            // Signal that the second task is now queued (this will only happen after unblocking)
            task2_queued_tx.send(()).unwrap();
            
            Ok(())
        })
    };
    
    // Wait for the first task to be queued
    task1_queued_rx.recv().unwrap();
    
    // Tell the writer thread to try to queue task2
    ready_for_task2_tx.send(()).unwrap();
    
    // Wait for confirmation that it's about to queue task2
    task2_blocked_rx.recv().unwrap();
    
    // Wait a tiny bit to make sure the other thread has reached the blocking call
    // We use a minimal sleep here since we can't know exactly when the OS
    // context switches to the other thread. An alternative would be to read
    // the queue length, but that could be racy too.
    thread::sleep(Duration::from_millis(10));
    
    // Verify that the second task is not yet queued by checking that
    // the task2_queued channel is empty
    assert!(task2_queued_rx.try_recv().is_err());
    
    // Process the first task to unblock the writer thread
    writer.process_next_task()?;
    
    // Now the second task should be queued
    task2_queued_rx.recv().unwrap();
    
    // Process the second task
    writer.process_next_task()?;
    
    // Wait for the writer thread to complete
    writer_thread.join().unwrap()?;
    
    Ok(())
}

#[test]
fn test_multithreaded_writer_with_bounded_queue() -> Result<()> {
    // Create a configuration with a small queue capacity and just one worker thread
    // to make the test more predictable
    let config = MultiThreadedWriterConfig::new(
        ParallelWriterConfig::default().with_task_queue_capacity(5),
        1 // Just one worker thread
    );
    
    // Create an in-memory test sharder
    let sharder = Autosharder::new(|| {
        Ok(std::io::Cursor::new(Vec::new()))
    });
    
    let sharding_config = ShardingConfig::new(
        Box::new(sharder),
        1 // Just a single shard to simplify testing
    );
    
    // Create a multi-threaded writer
    let writer = crate::parallel::multi_threaded_writer::MultiThreadedWriter::new(
        sharding_config,
        config
    )?;
    
    // Setup a channel to track task completions
    let (completion_tx, completion_rx) = std::sync::mpsc::channel();
    
    // Set up a separate thread to submit tasks
    let handle = thread::spawn(move || -> Result<()> {
        // Submit more tasks than the queue can hold
        // The queue should block after the first 5 tasks until the worker thread
        // processes some of them
        for i in 0..15 {
            let data = format!("task-{}", i);
            writer.write_record(Bytes::from(data))?;
            
            // Each successfully written task should also notify the completion channel
            completion_tx.send(i).unwrap();
        }
        
        // Wait for tasks to be processed
        writer.flush()?;
        
        // Check that all tasks were processed
        assert_eq!(writer.pending_tasks()?, 0);
        
        Ok(())
    });
    
    // Wait for all 15 task completions
    let mut completed_tasks = Vec::new();
    for _ in 0..15 {
        let task_id = completion_rx.recv().unwrap();
        completed_tasks.push(task_id);
    }
    
    // Verify we got all 15 tasks
    assert_eq!(completed_tasks.len(), 15);
    
    // Wait for the thread to complete
    handle.join().unwrap()?;
    
    Ok(())
}