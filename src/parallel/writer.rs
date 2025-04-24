//! Parallel implementation of the Disky writer.
//!
//! This module contains a parallel implementation of the writer
//! that leverages multiple threads for improved performance.
//! 
//! Unlike traditional thread pool implementations, this writer does not
//! manage its own worker threads. Instead, it exposes methods to push
//! write tasks to a queue and process tasks from that queue, allowing
//! external systems (like Python threads) to drive the processing.

use std::io::{Seek, Write};
use std::sync::Arc;

use crate::error::Result;
use crate::parallel::promise::Promise;
use crate::parallel::queue::Queue;
use crate::writer::{RecordWriter, RecordWriterConfig};

/// A write task that will be processed by a worker
#[derive(Clone, Debug)]
pub struct WriteTask {
    /// The record data to write
    pub data: Vec<u8>,
    /// Promise that will be fulfilled when the task is done
    pub completion: Promise<Result<()>>,
}

/// Resource containing an initialized writer that can be used to write records
pub struct WriterResource<W: Write + Seek + Send + 'static> {
    /// The actual record writer, boxed to signify exclusive ownership
    pub writer: Box<RecordWriter<W>>,
    /// Identifier for the writer
    pub id: usize,
}

/// Configuration for the parallel writer
#[derive(Debug, Clone)]
pub struct ParallelWriterConfig {
    /// Configuration for the underlying record writer
    pub writer_config: RecordWriterConfig,
}

impl Default for ParallelWriterConfig {
    fn default() -> Self {
        Self {
            writer_config: RecordWriterConfig::default(),
        }
    }
}

/// A parallel record writer that distributes work across multiple threads.
///
/// This writer uses a task queue to accept write requests and a resource queue
/// of available writers. Unlike traditional thread pool implementations,
/// it does not manage its own worker threads. Instead, it exposes methods to push
/// write tasks to a queue and process tasks from that queue, allowing external
/// systems (like Python threads) to drive the processing.
pub struct ParallelWriter<W: Write + Seek + Send + 'static> {
    /// Queue of write tasks to be processed
    task_queue: Arc<Queue<WriteTask>>,
    
    /// Queue of available writer resources
    resource_queue: Arc<Queue<WriterResource<W>>>,
}

impl<W: Write + Seek + Send + 'static> ParallelWriter<W> {
    /// Create a new parallel writer with the given writers
    ///
    /// # Arguments
    /// * `writers` - A vector of RecordWriter instances that will be used to write records
    /// * `config` - Configuration for the parallel writer
    pub fn new(
        writers: Vec<RecordWriter<W>>,
        _config: ParallelWriterConfig,
    ) -> Result<Self> {
        let task_queue = Arc::new(Queue::new());
        let resource_queue = Arc::new(Queue::new());
        
        // Initialize the resource queue with the provided writers
        for (id, writer) in writers.into_iter().enumerate() {
            resource_queue.push_back(WriterResource { 
                writer: Box::new(writer), 
                id 
            })?;
        }
        
        Ok(Self {
            task_queue,
            resource_queue,
        })
    }
    
    /// Write a record asynchronously
    ///
    /// This method queues the record for writing and returns a Promise that will
    /// be fulfilled when the write is done.
    ///
    /// # Arguments
    /// * `data` - The record data to write
    pub fn write_record_async(&self, data: Vec<u8>) -> Result<Promise<Result<()>>> {
        let completion = Promise::new();
        let completion_clone = completion.clone();
        
        let task = WriteTask {
            data,
            completion: completion_clone,
        };
        
        self.task_queue.push_back(task)?;
        
        Ok(completion)
    }
    
    /// Write a record synchronously
    ///
    /// This method queues the record for writing and waits for it to complete
    ///
    /// # Arguments
    /// * `data` - The record data to write
    pub fn write_record(&self, data: &[u8]) -> Result<()> {
        let completion = self.write_record_async(data.to_vec())?;
        
        // Wait for the task to complete and unwrap the result
        let result = completion.wait()?;
        result
    }
    
    /// Process the next available task
    ///
    /// This function will try to take the first write task in the task queue
    /// and use the first writer resource in the resource queue to write to disk.
    /// Returns true if a task was processed, false otherwise.
    ///
    /// This method is intended to be called from worker threads managed by
    /// the caller, not by the ParallelWriter itself.
    pub fn process_next_task(&self) -> Result<bool> {
        // Try to get a task from the queue
        let task_result = self.task_queue.poll()?;
        
        if let Some(task) = task_result {
            // Try to get a writer resource
            let resource_result = self.resource_queue.poll()?;
            
            if let Some(mut resource) = resource_result {
                // Process the task
                let result = resource.writer.write_record(&task.data);
                
                // Complete the promise with the result
                if let Err(e) = task.completion.fulfill(result) {
                    // Log the error but continue processing
                    eprintln!("Failed to fulfill promise: {}", e);
                }
                
                // Put the resource back in the queue
                self.resource_queue.push_back(resource)?;
                
                return Ok(true);
            } else {
                // No resource available, put the task back
                self.task_queue.push_back(task)?;
                return Ok(false);
            }
        }
        
        // No task available
        Ok(false)
    }
    
    /// Process all available tasks
    ///
    /// This function will process all tasks in the queue, or until
    /// there are no more available writer resources.
    /// Returns the number of tasks processed.
    pub fn process_all_tasks(&self) -> Result<usize> {
        let mut count = 0;
        while self.process_next_task()? {
            count += 1;
        }
        
        Ok(count)
    }
    
    /// Wait for a writer resource and process a task
    ///
    /// This function will wait until a writer resource is available,
    /// then process a task with it.
    /// Returns true if a task was processed, false if there are no tasks.
    pub fn wait_and_process_task(&self) -> Result<bool> {
        // Wait for a resource
        self.resource_queue.wait_front()?;
        
        // Now try to process a task
        self.process_next_task()
    }
    
    /// Flush all writers in the resource queue
    ///
    /// This will flush all writers to ensure data is written to disk.
    pub fn flush(&self) -> Result<()> {
        // Get all resources - need to call read_all through the Arc's dereferenced value
        let resources = (*self.resource_queue).read_all()?;
        
        // Flush each writer
        for mut resource in resources {
            resource.writer.flush()?;
            self.resource_queue.push_back(resource)?;
        }
        
        Ok(())
    }
    
    /// Check if there are any pending tasks
    pub fn has_pending_tasks(&self) -> Result<bool> {
        Ok(!self.task_queue.is_empty()?)
    }
    
    /// Get the number of pending tasks
    pub fn pending_task_count(&self) -> Result<usize> {
        self.task_queue.len()
    }
    
    /// Get the number of available writer resources
    pub fn available_resource_count(&self) -> Result<usize> {
        self.resource_queue.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_parallel_writer_basic() {
        // Create a few in-memory writers
        let mut writers = Vec::new();
        for _ in 0..3 {
            let cursor = Cursor::new(Vec::new());
            let writer = RecordWriter::with_config(cursor, RecordWriterConfig::default()).unwrap();
            writers.push(writer);
        }

        // Create parallel writer
        let parallel_writer = ParallelWriter::new(
            writers,
            ParallelWriterConfig::default(),
        ).unwrap();

        // Queue up some writes
        let data1 = b"hello world".to_vec();
        let data2 = b"testing 123".to_vec();
        let data3 = b"parallel writes".to_vec();

        let promise1 = parallel_writer.write_record_async(data1.clone()).unwrap();
        let promise2 = parallel_writer.write_record_async(data2.clone()).unwrap();
        let promise3 = parallel_writer.write_record_async(data3.clone()).unwrap();

        // Process tasks in a separate thread
        let writer_clone = parallel_writer.clone();
        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            writer_clone.process_all_tasks().unwrap()
        });

        // Wait for promises to be fulfilled
        let result1 = promise1.wait().unwrap();
        let result2 = promise2.wait().unwrap();
        let result3 = promise3.wait().unwrap();

        // All writes should succeed
        assert!(result1.is_ok());
        assert!(result2.is_ok());
        assert!(result3.is_ok());

        // Wait for processing thread to finish
        let processed = handle.join().unwrap();
        assert_eq!(processed, 3);
    }
}

// Make the Clone implementation for ParallelWriter
impl<W: Write + Seek + Send + 'static> Clone for ParallelWriter<W> {
    fn clone(&self) -> Self {
        Self {
            task_queue: Arc::clone(&self.task_queue),
            resource_queue: Arc::clone(&self.resource_queue),
        }
    }
}