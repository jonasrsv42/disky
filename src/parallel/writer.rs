//! Parallel implementation of the Disky writer.
//!
//! This module provides a parallel writer for Disky records, designed to improve
//! performance by distributing write operations across multiple writer instances.
//!
//! # Overview
//!
//! The `ParallelWriter` manages a pool of `RecordWriter` instances that can process
//! write operations concurrently. Unlike traditional thread pool implementations,
//! this writer does not manage its own worker threads. Instead, it exposes methods
//! to push write tasks to a queue and process tasks from that queue, allowing
//! external systems (like Python threads) to drive the processing.
//!
//! # Architecture
//!
//! The implementation is built around two key components:
//!
//! 1. A **task queue** that holds pending write operations
//! 2. A **resource queue** that maintains a pool of writer resources
//!
//! When a write request is received, it can be processed either:
//! - Synchronously, by directly using an available writer
//! - Asynchronously, by queuing the request and returning an `Arc<Promise>` that will be fulfilled when the write completes
//!
//! # Usage Examples
//!
//! ```rust,no_run
//! use disky::parallel::writer::{ParallelWriter, ParallelWriterConfig};
//! use disky::writer::{RecordWriter, RecordWriterConfig};
//! use std::fs::File;
//! use bytes::Bytes;
//!
//! // Create writer resources
//! let mut writers = Vec::new();
//! for i in 0..4 {
//!     let file = File::create(format!("output-{}.bin", i)).unwrap();
//!     let writer = RecordWriter::new(file).unwrap();
//!     writers.push(Box::new(writer));
//! }
//!
//! // Initialize the parallel writer
//! let parallel_writer = ParallelWriter::new(
//!     writers,
//!     ParallelWriterConfig::default()
//! ).unwrap();
//!
//! // Synchronous write
//! parallel_writer.write_record(b"synchronous record").unwrap();
//!
//! // Asynchronous write
//! let data = Bytes::from(b"asynchronous record".to_vec());
//! let promise = parallel_writer.write_record_async(data).unwrap();
//!
//! // Process the async task (can be done in a separate thread)
//! parallel_writer.process_all_tasks().unwrap();
//!
//! // Wait for completion (consumes the promise)
//! let result = promise.wait().unwrap();
//! assert!(result.is_ok());
//!
//! // Flush and close
//! parallel_writer.flush().unwrap();
//! parallel_writer.close().unwrap();
//! ```

use std::io::{Seek, Write};
use std::sync::Arc;

use crate::error::Result;
use crate::parallel::promise::Promise;
use crate::parallel::resource_pool::ResourcePool;
use crate::parallel::task_queue::TaskQueue;
use crate::writer::{RecordWriter, RecordWriterConfig};
use bytes::Bytes;
use log::error;

/// A task that will be processed by a worker
///
/// This enum represents different types of operations that can be performed
/// by worker threads. Each variant contains a promise that will be fulfilled
/// when the operation completes.
#[derive(Debug)]
pub enum Task {
    /// Write a record to disk
    Write {
        /// The record data to write
        data: Bytes,
        /// Promise that will be fulfilled when the write completes
        completion: Arc<Promise<Result<()>>>,
    },

    /// Flush all writers to disk
    Flush {
        /// Promise that will be fulfilled when the flush completes
        completion: Arc<Promise<Result<()>>>,
    },

    /// Close all writers
    Close {
        /// Promise that will be fulfilled when the close completes
        completion: Arc<Promise<Result<()>>>,
    },
}

impl Clone for Task {
    fn clone(&self) -> Self {
        match self {
            Task::Write { data, completion } => Task::Write {
                data: data.clone(),
                completion: Arc::clone(completion),
            },
            Task::Flush { completion } => Task::Flush {
                completion: Arc::clone(completion),
            },
            Task::Close { completion } => Task::Close {
                completion: Arc::clone(completion),
            },
        }
    }
}

/// Resource containing an initialized writer that can be used to write records
///
/// Each resource represents a single writer instance that can process write requests.
/// The writers are managed by the parallel writer and checked out from a pool when needed.
pub struct WriterResource<Sink: Write + Seek + Send + 'static> {
    /// The actual record writer, boxed to signify exclusive ownership
    pub writer: Box<RecordWriter<Sink>>,
    /// Identifier for the writer, useful for tracking which writer processed which record
    pub id: usize,
}

/// Configuration for the parallel writer
///
/// Controls the behavior of the parallel writer, including the configuration
/// of the underlying record writers.
#[derive(Debug, Clone)]
pub struct ParallelWriterConfig {
    /// Configuration for the underlying record writer instances
    pub writer_config: RecordWriterConfig,
}

impl Default for ParallelWriterConfig {
    fn default() -> Self {
        Self {
            writer_config: RecordWriterConfig::default(),
        }
    }
}

/// A parallel record writer that distributes work across multiple writers.
///
/// The `ParallelWriter` maintains a pool of `RecordWriter` instances and distributes
/// write operations among them. It supports both synchronous and asynchronous write
/// operations, and allows external consumers to drive the processing of tasks.
///
/// Key features:
/// - Supports both synchronous and asynchronous writes
/// - Allows multiple threads to process tasks concurrently
/// - Maintains a queue of pending write tasks
/// - Manages a pool of writer resources
/// - Tracks active resources to ensure proper flush/close coordination
///
/// This design is particularly useful for integrating with existing threading
/// or async systems, as it does not create its own worker threads.
pub struct ParallelWriter<Sink: Write + Seek + Send + 'static> {
    /// Queue of tasks to be processed
    task_queue: Arc<TaskQueue<Task>>,

    /// Queue of writer resources with active tracking
    resource_queue: Arc<ResourcePool<WriterResource<Sink>>>,
}

impl<Sink: Write + Seek + Send + 'static> ParallelWriter<Sink> {
    /// Create a new parallel writer with the given writers
    ///
    /// Initializes a parallel writer with a set of pre-configured writer instances.
    /// Each writer becomes a resource in the writer pool and is assigned a unique ID.
    ///
    /// # Arguments
    /// * `writers` - A vector of RecordWriter instances that will be used to write records
    /// * `config` - Configuration for the parallel writer
    ///
    /// # Returns
    /// A Result containing the new ParallelWriter instance or an error
    ///
    /// # Example
    /// ```rust,no_run
    /// # use disky::parallel::writer::{ParallelWriter, ParallelWriterConfig};
    /// # use disky::writer::RecordWriter;
    /// # use std::io::Cursor;
    /// let mut writers = Vec::new();
    /// for _ in 0..3 {
    ///     let cursor = Cursor::new(Vec::new());
    ///     let writer = RecordWriter::new(cursor).unwrap();
    ///     writers.push(Box::new(writer));
    /// }
    ///
    /// let parallel_writer = ParallelWriter::new(
    ///     writers,
    ///     ParallelWriterConfig::default()
    /// ).unwrap();
    /// ```
    
    /// For testing purposes: Get access to the resource pool containing the writers
    #[cfg(test)]
    pub(crate) fn get_resource_pool(&self) -> &Arc<ResourcePool<WriterResource<Sink>>> {
        &self.resource_queue
    }
    pub fn new(
        writers: Vec<Box<RecordWriter<Sink>>>,
        _config: ParallelWriterConfig,
    ) -> Result<Self> {
        let task_queue = Arc::new(TaskQueue::new());
        let resource_queue = Arc::new(ResourcePool::new());

        // Initialize the resource queue with the provided writers
        for (id, writer) in writers.into_iter().enumerate() {
            resource_queue.add_resource(WriterResource { writer, id })?;
        }

        Ok(Self {
            task_queue,
            resource_queue,
        })
    }

    /// Write a record asynchronously
    ///
    /// This method queues the record for writing and returns an Arc<Promise> that will
    /// be fulfilled when the write is completed. The actual write operation will
    /// be performed when `process_task` or `process_all_tasks` is called.
    ///
    /// # Arguments
    /// * `data` - The record data to write
    ///
    /// # Returns
    /// An Arc<Promise> that will be fulfilled with the result of the write operation
    ///
    /// # Example
    /// ```rust,no_run
    /// # use disky::parallel::writer::{ParallelWriter, ParallelWriterConfig};
    /// # use disky::writer::RecordWriter;
    /// # use std::io::Cursor;
    /// # use bytes::Bytes;
    /// # let mut writers = Vec::new();
    /// # for _ in 0..3 {
    /// #     let cursor = Cursor::new(Vec::new());
    /// #     let writer = RecordWriter::new(cursor).unwrap();
    /// #     writers.push(Box::new(writer));
    /// # }
    /// # let parallel_writer = ParallelWriter::new(writers, ParallelWriterConfig::default()).unwrap();
    /// let data = Bytes::from(b"async record".to_vec());
    /// let promise = parallel_writer.write_record_async(data).unwrap();
    ///
    /// // Process the task
    /// parallel_writer.process_all_tasks().unwrap();
    ///
    /// // Wait for completion
    /// let result = promise.wait().unwrap();
    /// assert!(result.is_ok());
    /// ```
    pub fn write_record_async(&self, data: Bytes) -> Result<Arc<Promise<Result<()>>>> {
        let completion = Arc::new(Promise::new());

        let task = Task::Write {
            data,
            completion: Arc::clone(&completion),
        };

        self.task_queue.push_back(task)?;

        Ok(completion)
    }

    /// Write a record synchronously
    ///
    /// This method directly grabs a writer resource from the pool and
    /// uses it to write the record. The resource is returned to the pool
    /// after the write operation completes.
    ///
    /// # Arguments
    /// * `data` - The record data to write
    ///
    /// # Returns
    /// A Result indicating success or failure
    ///
    /// # Example
    /// ```rust,no_run
    /// # use disky::parallel::writer::{ParallelWriter, ParallelWriterConfig};
    /// # use disky::writer::RecordWriter;
    /// # use std::io::Cursor;
    /// # let mut writers = Vec::new();
    /// # for _ in 0..3 {
    /// #     let cursor = Cursor::new(Vec::new());
    /// #     let writer = RecordWriter::new(cursor).unwrap();
    /// #     writers.push(Box::new(writer));
    /// # }
    /// # let parallel_writer = ParallelWriter::new(writers, ParallelWriterConfig::default()).unwrap();
    /// parallel_writer.write_record(b"synchronous record").unwrap();
    /// ```
    pub fn write_record(&self, data: &[u8]) -> Result<()> {
        // Process a resource by writing the record to it
        let mut resource = self.resource_queue.get_resource()?;
        resource.writer.write_record(data)
    }

    /// Process a single task
    ///
    /// This method processes a single task of any type (Write, Flush, or Close).
    /// The task is processed immediately using the appropriate method, and the
    /// completion promise is fulfilled with the result.
    ///
    /// # Arguments
    /// * `task` - The task to process
    ///
    /// # Returns
    /// * `Ok(())` - If the task was processed successfully
    /// * `Err` - If an error occurred during processing
    ///
    /// Note that even if the task itself fails (e.g., a write fails), this method
    /// will still return Ok() since the task was processed. The error is stored in
    /// the completion promise.
    pub fn process_task(&self, task: Task) -> Result<()> {
        match task {
            Task::Write { data, completion } => {
                // Process the write task
                let result = self.write_record(&data);

                // Complete the promise with the result
                if let Err(e) = completion.fulfill(result) {
                    // Log the error but continue processing
                    error!("Failed to fulfill write promise: {}", e);
                }
            }
            Task::Flush { completion } => {
                // Process the flush task
                let result = self.flush();

                // Complete the promise with the result
                if let Err(e) = completion.fulfill(result) {
                    error!("Failed to fulfill flush promise: {}", e);
                }
            }
            Task::Close { completion } => {
                // Process the close task
                let result = self.close();

                // Complete the promise with the result
                if let Err(e) = completion.fulfill(result) {
                    error!("Failed to fulfill close promise: {}", e);
                }
            }
        }

        Ok(())
    }

    /// Process the next available task
    ///
    /// This function will try to take the first write task in the task queue
    /// and use an available writer resource to process it.
    ///
    /// This method is intended to be called from worker threads managed by
    /// the caller, not by the ParallelWriter itself.
    ///
    /// # Returns
    /// A Result indicating success or failure
    ///
    /// # Example
    /// ```rust,no_run
    /// # use disky::parallel::writer::{ParallelWriter, ParallelWriterConfig};
    /// # use disky::writer::RecordWriter;
    /// # use std::io::Cursor;
    /// # use bytes::Bytes;
    /// # use std::thread;
    /// # use std::sync::Arc;
    /// # let mut writers = Vec::new();
    /// # for _ in 0..3 {
    /// #     let cursor = Cursor::new(Vec::new());
    /// #     let writer = RecordWriter::new(cursor).unwrap();
    /// #     writers.push(Box::new(writer));
    /// # }
    /// # let parallel_writer = Arc::new(ParallelWriter::new(writers, ParallelWriterConfig::default()).unwrap());
    /// # let data = Bytes::from(b"async record".to_vec());
    /// # parallel_writer.write_record_async(data).unwrap();
    ///
    /// // Process tasks in a worker thread, checking for queue closure
    /// let writer_clone = parallel_writer.clone();
    /// thread::spawn(move || {
    ///     loop {
    ///         // Process task or break if queue is closed
    ///         match writer_clone.process_next_task() {
    ///             Ok(_) => {},
    ///             Err(e) => {
    ///                 // Handle queue closure or other errors
    ///                 eprintln!("Error or queue closed: {}", e);
    ///                 break;
    ///             },
    ///         }
    ///     }
    /// });
    /// ```
    pub fn process_next_task(&self) -> Result<()> {
        // Try to get a task from the queue
        let task = self.task_queue.read_front()?;

        self.process_task(task)
    }

    /// Process all available tasks
    ///
    /// This function will process all tasks in the task queue.
    /// It's more efficient than calling `process_next_task` multiple times
    /// because it grabs all available tasks at once.
    ///
    /// # Returns
    /// A Result indicating success or failure
    ///
    /// # Example
    /// ```rust,no_run
    /// # use disky::parallel::writer::{ParallelWriter, ParallelWriterConfig};
    /// # use disky::writer::RecordWriter;
    /// # use std::io::Cursor;
    /// # use bytes::Bytes;
    /// # use std::sync::Arc;
    /// # use std::thread;
    /// # let mut writers = Vec::new();
    /// # for _ in 0..3 {
    /// #     let cursor = Cursor::new(Vec::new());
    /// #     let writer = RecordWriter::new(cursor).unwrap();
    /// #     writers.push(Box::new(writer));
    /// # }
    /// # let parallel_writer = Arc::new(ParallelWriter::new(writers, ParallelWriterConfig::default()).unwrap());
    /// # for i in 0..5 {
    /// #     let data = Bytes::from(format!("record {}", i).into_bytes());
    /// #     parallel_writer.write_record_async(data).unwrap();
    /// # }
    ///
    /// // Process all tasks in a worker thread, handling errors
    /// let writer_clone = parallel_writer.clone();
    /// thread::spawn(move || {
    ///     // Keep processing until an error occurs
    ///     loop {
    ///         match writer_clone.process_all_tasks() {
    ///             Ok(_) => {
    ///                 // Small sleep to avoid tight loop
    ///                 std::thread::sleep(std::time::Duration::from_millis(1));
    ///             },
    ///             Err(e) => {
    ///                 // Handle queue closure or other errors
    ///                 eprintln!("Error or queue closed: {}", e);
    ///                 break;
    ///             },
    ///         }
    ///     }
    /// });
    /// ```
    pub fn process_all_tasks(&self) -> Result<()> {
        for task in self.task_queue.read_all()? {
            self.process_task(task)?;
        }

        Ok(())
    }

    /// Flush all writers in the resource queue asynchronously
    ///
    /// This will queue a flush task and return a Promise that will be fulfilled
    /// when the flush is complete. The task must be processed by calling
    /// `process_next_task` or `process_all_tasks`.
    ///
    /// # Returns
    /// A Promise that will be fulfilled when the flush is complete
    ///
    /// # Example
    /// ```rust,no_run
    /// # use disky::parallel::writer::{ParallelWriter, ParallelWriterConfig};
    /// # use disky::writer::RecordWriter;
    /// # use std::io::Cursor;
    /// # let mut writers = Vec::new();
    /// # for _ in 0..3 {
    /// #     let cursor = Cursor::new(Vec::new());
    /// #     let writer = RecordWriter::new(cursor).unwrap();
    /// #     writers.push(Box::new(writer));
    /// # }
    /// # let parallel_writer = ParallelWriter::new(writers, ParallelWriterConfig::default()).unwrap();
    /// # parallel_writer.write_record(b"some data").unwrap();
    ///
    /// // Queue a flush operation
    /// let promise = parallel_writer.flush_async().unwrap();
    ///
    /// // Process the flush task
    /// parallel_writer.process_all_tasks().unwrap();
    ///
    /// // Wait for the flush to complete
    /// promise.wait().unwrap().unwrap();
    /// ```
    pub fn flush_async(&self) -> Result<Arc<Promise<Result<()>>>> {
        let completion = Arc::new(Promise::new());

        // Create a flush task
        let task = Task::Flush {
            completion: Arc::clone(&completion),
        };

        // Queue the task
        self.task_queue.push_back(task)?;

        Ok(completion)
    }

    /// Synchronously flush all writers
    ///
    /// This method directly flushes all writers without using the task queue.
    /// It's safe to use in a single thread without causing deadlocks because
    /// it uses the pause mechanism to ensure no resources are being processed.
    ///
    /// # Example
    /// ```rust,no_run
    /// # use disky::parallel::writer::{ParallelWriter, ParallelWriterConfig};
    /// # use disky::writer::RecordWriter;
    /// # use std::io::Cursor;
    /// # let mut writers = Vec::new();
    /// # for _ in 0..3 {
    /// #     let cursor = Cursor::new(Vec::new());
    /// #     let writer = RecordWriter::new(cursor).unwrap();
    /// #     writers.push(Box::new(writer));
    /// # }
    /// # let parallel_writer = ParallelWriter::new(writers, ParallelWriterConfig::default()).unwrap();
    /// # parallel_writer.write_record(b"some data").unwrap();
    ///
    /// // Flush all writers synchronously
    /// parallel_writer.flush().unwrap();
    /// ```
    pub fn flush(&self) -> Result<()> {
        // Use the enhanced method that handles pause/process/resume in one operation
        self.resource_queue
            .process_all_resources(|resource| resource.writer.flush())
    }

    /// Close all writers asynchronously
    ///
    /// This will queue a close task and return an Arc<Promise> that will be fulfilled
    /// when the close is complete. The task must be processed by calling
    /// `process_next_task` or `process_all_tasks`.
    ///
    /// # Returns
    /// An Arc<Promise> that will be fulfilled when the close is complete
    ///
    /// # Example
    /// ```rust,no_run
    /// # use disky::parallel::writer::{ParallelWriter, ParallelWriterConfig};
    /// # use disky::writer::RecordWriter;
    /// # use std::io::Cursor;
    /// # let mut writers = Vec::new();
    /// # for _ in 0..3 {
    /// #     let cursor = Cursor::new(Vec::new());
    /// #     let writer = RecordWriter::new(cursor).unwrap();
    /// #     writers.push(Box::new(writer));
    /// # }
    /// # let parallel_writer = ParallelWriter::new(writers, ParallelWriterConfig::default()).unwrap();
    /// # parallel_writer.write_record(b"some data").unwrap();
    /// # parallel_writer.flush().unwrap();
    ///
    /// // Queue a close operation
    /// let promise = parallel_writer.close_async().unwrap();
    ///
    /// // Process the close task
    /// parallel_writer.process_all_tasks().unwrap();
    ///
    /// // Wait for the close to complete
    /// promise.wait().unwrap().unwrap();
    /// ```
    pub fn close_async(&self) -> Result<Arc<Promise<Result<()>>>> {
        let completion = Arc::new(Promise::new());

        // Create a close task
        let task = Task::Close {
            completion: Arc::clone(&completion),
        };

        // Queue the task
        self.task_queue.push_back(task)?;

        Ok(completion)
    }

    /// Synchronously close all writers
    ///
    /// This method directly closes all writers without using the task queue.
    /// It's safe to use in a single thread without causing deadlocks because
    /// it uses the pause mechanism to ensure no resources are being processed.
    ///
    /// This method first closes all writer resources, then closes the task queue.
    /// If either operation returns an error, the error is propagated, with
    /// resource errors taking precedence over task queue errors.
    ///
    /// # Returns
    /// * `Ok(())` - If both resource pool and task queue were closed successfully
    /// * `Err` - If an error occurred while closing either the resource pool or task queue
    ///
    /// # Example
    /// ```rust,no_run
    /// # use disky::parallel::writer::{ParallelWriter, ParallelWriterConfig};
    /// # use disky::writer::RecordWriter;
    /// # use std::io::Cursor;
    /// # let mut writers = Vec::new();
    /// # for _ in 0..3 {
    /// #     let cursor = Cursor::new(Vec::new());
    /// #     let writer = RecordWriter::new(cursor).unwrap();
    /// #     writers.push(Box::new(writer));
    /// # }
    /// # let parallel_writer = ParallelWriter::new(writers, ParallelWriterConfig::default()).unwrap();
    /// # parallel_writer.write_record(b"some data").unwrap();
    /// # parallel_writer.flush().unwrap();
    ///
    /// // Close all writers synchronously
    /// parallel_writer.close().unwrap();
    /// ```
    pub fn close(&self) -> Result<()> {
        // First, try to close all writer resources
        let resource_close_result = self
            .resource_queue
            .process_then_close(|resource| resource.writer.close());
            
        // Then, try to close the task queue, regardless of whether the resource close succeeded
        let task_close_result = self.task_queue.close();
        
        // Return the first error encountered, prioritizing resource errors over task queue errors
        match (resource_close_result, task_close_result) {
            (Err(e), _) => Err(e),                    // Resource error takes precedence
            (Ok(()), Err(e)) => Err(e),               // Task queue error if no resource error
            (Ok(()), Ok(())) => Ok(())                // Success if both operations succeeded
        }
    }

    /// Check if there are any pending tasks
    ///
    /// Returns true if there are tasks in the queue waiting to be processed.
    ///
    /// # Returns
    /// A Result containing a boolean indicating if there are pending tasks
    ///
    /// # Example
    /// ```rust,no_run
    /// # use disky::parallel::writer::{ParallelWriter, ParallelWriterConfig};
    /// # use disky::writer::RecordWriter;
    /// # use std::io::Cursor;
    /// # use bytes::Bytes;
    /// # let mut writers = Vec::new();
    /// # for _ in 0..3 {
    /// #     let cursor = Cursor::new(Vec::new());
    /// #     let writer = RecordWriter::new(cursor).unwrap();
    /// #     writers.push(Box::new(writer));
    /// # }
    /// # let parallel_writer = ParallelWriter::new(writers, ParallelWriterConfig::default()).unwrap();
    /// # let data = Bytes::from(b"async record".to_vec());
    /// # parallel_writer.write_record_async(data).unwrap();
    ///
    /// if parallel_writer.has_pending_tasks().unwrap() {
    ///     // Process pending tasks
    ///     parallel_writer.process_all_tasks().unwrap();
    /// }
    /// ```
    pub fn has_pending_tasks(&self) -> Result<bool> {
        Ok(!self.task_queue.is_empty()?)
    }

    /// Get the number of pending tasks
    ///
    /// Returns the count of tasks currently in the queue.
    ///
    /// # Returns
    /// A Result containing the number of pending tasks
    ///
    /// # Example
    /// ```rust,no_run
    /// # use disky::parallel::writer::{ParallelWriter, ParallelWriterConfig};
    /// # use disky::writer::RecordWriter;
    /// # use std::io::Cursor;
    /// # use bytes::Bytes;
    /// # let mut writers = Vec::new();
    /// # for _ in 0..3 {
    /// #     let cursor = Cursor::new(Vec::new());
    /// #     let writer = RecordWriter::new(cursor).unwrap();
    /// #     writers.push(Box::new(writer));
    /// # }
    /// # let parallel_writer = ParallelWriter::new(writers, ParallelWriterConfig::default()).unwrap();
    /// # for i in 0..5 {
    /// #     let data = Bytes::from(format!("record {}", i).into_bytes());
    /// #     parallel_writer.write_record_async(data).unwrap();
    /// # }
    ///
    /// let count = parallel_writer.pending_task_count().unwrap();
    /// println!("There are {} pending tasks", count);
    /// ```
    pub fn pending_task_count(&self) -> Result<usize> {
        self.task_queue.len()
    }

    /// Get the number of available writer resources
    ///
    /// Returns the count of writer resources currently available in the pool.
    ///
    /// # Returns
    /// A Result containing the number of available resources
    ///
    /// # Example
    /// ```rust,no_run
    /// # use disky::parallel::writer::{ParallelWriter, ParallelWriterConfig};
    /// # use disky::writer::RecordWriter;
    /// # use std::io::Cursor;
    /// # let mut writers = Vec::new();
    /// # for _ in 0..3 {
    /// #     let cursor = Cursor::new(Vec::new());
    /// #     let writer = RecordWriter::new(cursor).unwrap();
    /// #     writers.push(Box::new(writer));
    /// # }
    /// # let parallel_writer = ParallelWriter::new(writers, ParallelWriterConfig::default()).unwrap();
    ///
    /// let count = parallel_writer.available_resource_count().unwrap();
    /// println!("There are {} available writer resources", count);
    /// ```
    pub fn available_resource_count(&self) -> Result<usize> {
        self.resource_queue.available_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::sync::mpsc;
    use std::thread;

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
}
