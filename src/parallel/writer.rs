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
//! - Asynchronously, by queuing the request and returning a `Promise` that will be fulfilled when the write completes
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
//! // Wait for completion
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
use crate::parallel::queue::Queue;
use crate::writer::{RecordWriter, RecordWriterConfig};
use bytes::Bytes;
use log::error;

/// A write task that will be processed by a worker
///
/// This structure represents a pending asynchronous write operation.
/// It contains both the data to be written and a promise that will be
/// fulfilled when the write operation completes.
#[derive(Clone, Debug)]
pub struct WriteTask {
    /// The record data to write
    pub data: Bytes,
    /// Promise that will be fulfilled when the task is done
    pub completion: Promise<Result<()>>,
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
///
/// This design is particularly useful for integrating with existing threading
/// or async systems, as it does not create its own worker threads.
pub struct ParallelWriter<Sink: Write + Seek + Send + 'static> {
    /// Queue of write tasks to be processed
    task_queue: Arc<Queue<WriteTask>>,

    /// Queue of available writer resources
    resource_queue: Arc<Queue<WriterResource<Sink>>>,
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
    pub fn new(
        writers: Vec<Box<RecordWriter<Sink>>>,
        _config: ParallelWriterConfig,
    ) -> Result<Self> {
        let task_queue = Arc::new(Queue::new());
        let resource_queue = Arc::new(Queue::new());

        // Initialize the resource queue with the provided writers
        for (id, writer) in writers.into_iter().enumerate() {
            resource_queue.push_back(WriterResource { writer, id })?;
        }

        Ok(Self {
            task_queue,
            resource_queue,
        })
    }

    /// Write a record asynchronously
    ///
    /// This method queues the record for writing and returns a Promise that will
    /// be fulfilled when the write is completed. The actual write operation will
    /// be performed when `process_task` or `process_all_tasks` is called.
    ///
    /// # Arguments
    /// * `data` - The record data to write
    ///
    /// # Returns
    /// A Promise that will be fulfilled with the result of the write operation
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
    pub fn write_record_async(&self, data: Bytes) -> Result<Promise<Result<()>>> {
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
        let mut resource = self.resource_queue.read_front()?;

        let ret_val = resource.writer.write_record(data);
        self.resource_queue.push_back(resource)?;

        return ret_val;
    }

    /// Process a single write task
    ///
    /// This method processes a given write task by writing the data and
    /// fulfilling the associated promise with the result.
    ///
    /// # Arguments
    /// * `task` - The WriteTask to process
    ///
    /// # Returns
    /// A Result indicating success or failure of the task processing
    pub fn process_task(&self, WriteTask { data, completion }: WriteTask) -> Result<()> {
        let result = self.write_record(&data);

        // Complete the promise with the result
        if let Err(e) = completion.fulfill(result) {
            // Log the error but continue processing
            error!("Failed to fulfill promise: {}", e)
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
    /// // Process a single task in a worker thread
    /// let writer_clone = parallel_writer.clone();
    /// thread::spawn(move || {
    ///     writer_clone.process_next_task().unwrap();
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
    /// // Process all tasks in a worker thread
    /// let writer_clone = parallel_writer.clone();
    /// thread::spawn(move || {
    ///     writer_clone.process_all_tasks().unwrap();
    /// });
    /// ```
    pub fn process_all_tasks(&self) -> Result<()> {
        for task in self.task_queue.read_all()? {
            self.process_task(task)?;
        }

        Ok(())
    }

    /// Flush all writers in the resource queue
    ///
    /// This will flush all writers to ensure data is written to disk.
    /// Flushing is important to ensure that data is actually persisted
    /// to the underlying storage.
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
    /// # parallel_writer.write_record(b"some data").unwrap();
    ///
    /// // Flush all writers
    /// parallel_writer.flush().unwrap();
    /// ```
    pub fn flush(&self) -> Result<()> {
        // Get all resources - need to call read_all through the Arc's dereferenced value
        let resources = self.resource_queue.read_all()?;

        // Flush each writer
        for mut resource in resources {
            let result = resource.writer.flush();

            // Push resource back before checking result.
            self.resource_queue.push_back(resource)?;

            // Check result.
            result?;
        }

        Ok(())
    }

    /// Close all writers to finalize the file format
    ///
    /// This will close all writers to ensure valid files for reading.
    /// Closing is essential for writing the final file format elements
    /// that make the file valid and readable.
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
    /// # parallel_writer.write_record(b"some data").unwrap();
    /// # parallel_writer.flush().unwrap();
    ///
    /// // Close all writers
    /// parallel_writer.close().unwrap();
    /// ```
    pub fn close(&self) -> Result<()> {
        // Get all resources
        let resources = self.resource_queue.read_all()?;

        // Close each writer
        for mut resource in resources {
            // Push resource back before checking result.
            let result = resource.writer.close();

            // Push back
            self.resource_queue.push_back(resource)?;

            // Check result.
            result?;
        }

        Ok(())
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
        self.resource_queue.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::{DiskyPiece, RecordReader};
    use std::collections::HashSet;
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
        assert!(result1.is_ok(), "First write should succeed");
        assert!(result2.is_ok(), "Second write should succeed");
        assert!(result3.is_ok(), "Third write should succeed");

        // Wait for processing thread to finish
        handle.join().unwrap();
    }

    #[test]
    fn test_verify_written_content() {
        // Create separate writers with cursors for testing
        let mut cursors = Vec::new();
        let mut writers = Vec::new();

        for _ in 0..2 {
            let cursor = Cursor::new(Vec::new());
            cursors.push(cursor);
        }

        // Create writers from the cursors
        for cursor in cursors.iter() {
            let writer =
                RecordWriter::with_config(cursor.clone(), RecordWriterConfig::default()).unwrap();
            writers.push(Box::new(writer));
        }

        // Create parallel writer
        let parallel_writer =
            ParallelWriter::new(writers, ParallelWriterConfig::default()).unwrap();

        // Define test data with distinct records
        let test_records = vec![
            b"record one".to_vec(),
            b"record two".to_vec(),
            b"record three".to_vec(),
            b"record four".to_vec(),
        ];

        // Track the expected content
        let expected_records: HashSet<Vec<u8>> = test_records.iter().cloned().collect();

        // Write records asynchronously
        let promises = test_records
            .iter()
            .map(|data| {
                parallel_writer
                    .write_record_async(Bytes::from(data.clone()))
                    .unwrap()
            })
            .collect::<Vec<_>>();

        // Process all tasks
        parallel_writer.process_all_tasks().unwrap();

        // Flush to ensure all data is written
        parallel_writer.flush().unwrap();

        // Verify all promises completed successfully
        for promise in promises {
            let result = promise.wait().unwrap();
            assert!(result.is_ok(), "Write operation should succeed");
        }

        // Close all writers to finalize the file format
        parallel_writer.close().unwrap();

        // Get all writers from the resource queue
        let resources = parallel_writer.resource_queue.read_all().unwrap();

        // Collect all the written data
        let mut actual_records = HashSet::new();

        for resource in resources {
            // Get the data from each writer
            let data_result = resource.writer.get_data();
            if let Ok(data) = data_result {
                // Create a reader to read the records from this data
                let cursor = Cursor::new(data);
                let mut reader = RecordReader::new(cursor).unwrap();

                // Read all records from this writer
                loop {
                    match reader.next_record().unwrap() {
                        DiskyPiece::Record(bytes) => {
                            actual_records.insert(bytes.to_vec());
                        }
                        DiskyPiece::EOF => break,
                    }
                }
            }

            // No need to return the resource to the queue as we're in a test
            // and the writers have already been consumed by get_data
        }

        // Verify the content matches what we expected
        assert_eq!(
            actual_records.len(),
            expected_records.len(),
            "Should read back the same number of records"
        );

        // Every written record should be in our expected set
        for record in &actual_records {
            assert!(
                expected_records.contains(record),
                "Read record {:?} was not in expected records",
                record
            );
        }
    }

    #[test]
    fn test_synchronous_writes() {
        // Create separate writers with cursors for testing
        let mut cursors = Vec::new();
        let mut writers = Vec::new();

        for _ in 0..2 {
            let cursor = Cursor::new(Vec::new());
            cursors.push(cursor);
        }

        // Create writers from the cursors
        for cursor in cursors.iter() {
            let writer =
                RecordWriter::with_config(cursor.clone(), RecordWriterConfig::default()).unwrap();
            writers.push(Box::new(writer));
        }

        // Create parallel writer
        let parallel_writer =
            ParallelWriter::new(writers, ParallelWriterConfig::default()).unwrap();

        // Define test data
        let test_records = vec![
            b"sync record 1".to_vec(),
            b"sync record 2".to_vec(),
            b"sync record 3".to_vec(),
        ];

        // Write records synchronously
        for record in &test_records {
            let result = parallel_writer.write_record(record);
            assert!(result.is_ok(), "Synchronous write should succeed");
        }

        // Verify there are no pending tasks (since we wrote synchronously)
        assert_eq!(
            parallel_writer.pending_task_count().unwrap(),
            0,
            "No pending tasks should exist after synchronous writes"
        );

        // Flush to ensure all data is written
        parallel_writer.flush().unwrap();

        // Close all writers to finalize the file format
        parallel_writer.close().unwrap();

        // Get all writers from the resource queue
        let resources = parallel_writer.resource_queue.read_all().unwrap();

        // Collect all the written data
        let mut actual_records = Vec::new();

        for resource in resources {
            // Get the data from each writer
            let data_result = resource.writer.get_data();
            if let Ok(data) = data_result {
                // Create a reader to read the records from this data
                let cursor = Cursor::new(data);
                let mut reader = RecordReader::new(cursor).unwrap();

                // Read all records from this writer
                loop {
                    match reader.next_record().unwrap() {
                        DiskyPiece::Record(bytes) => {
                            actual_records.push(bytes.to_vec());
                        }
                        DiskyPiece::EOF => break,
                    }
                }
            }

            // No need to return the resource to the queue as we're in a test
            // and the writers have already been consumed by get_data
        }

        // Verify we read back the same number of records
        assert_eq!(
            actual_records.len(),
            test_records.len(),
            "Should read back the same number of records"
        );

        // Verify each record was written correctly
        for expected in &test_records {
            assert!(
                actual_records.iter().any(|actual| actual == expected),
                "Expected record {:?} not found in output",
                expected
            );
        }
    }

    #[test]
    fn test_mixed_sync_async_writes() {
        // Create separate writers with cursors for testing
        let mut cursors = Vec::new();
        let mut writers = Vec::new();

        for _ in 0..2 {
            let cursor = Cursor::new(Vec::new());
            cursors.push(cursor);
        }

        // Create writers from the cursors
        for cursor in cursors.iter() {
            let writer =
                RecordWriter::with_config(cursor.clone(), RecordWriterConfig::default()).unwrap();
            writers.push(Box::new(writer));
        }

        // Create parallel writer
        let parallel_writer =
            ParallelWriter::new(writers, ParallelWriterConfig::default()).unwrap();

        // Define test data
        let sync_records = vec![b"sync record 1".to_vec(), b"sync record 2".to_vec()];

        let async_records = vec![b"async record 1".to_vec(), b"async record 2".to_vec()];

        // Combine all expected records
        let mut all_expected_records = HashSet::new();
        for record in &sync_records {
            all_expected_records.insert(record.clone());
        }
        for record in &async_records {
            all_expected_records.insert(record.clone());
        }

        // Write synchronous records
        for record in &sync_records {
            let result = parallel_writer.write_record(record);
            assert!(result.is_ok(), "Synchronous write should succeed");
        }

        // Write asynchronous records
        let promises = async_records
            .iter()
            .map(|data| {
                parallel_writer
                    .write_record_async(Bytes::from(data.clone()))
                    .unwrap()
            })
            .collect::<Vec<_>>();

        // Process all async tasks
        parallel_writer.process_all_tasks().unwrap();

        // Wait for all promises
        for promise in promises {
            let result = promise.wait().unwrap();
            assert!(result.is_ok(), "Async write should succeed");
        }

        // Verify there are no more pending tasks
        assert_eq!(
            parallel_writer.pending_task_count().unwrap(),
            0,
            "No pending tasks should remain after processing"
        );

        // Flush to ensure all data is written
        parallel_writer.flush().unwrap();

        // Close all writers to finalize the file format
        parallel_writer.close().unwrap();

        // Get all writers from the resource queue
        let resources = parallel_writer.resource_queue.read_all().unwrap();

        // Collect all the written data
        let mut actual_records = HashSet::new();

        for resource in resources {
            // Get the data from each writer
            let data_result = resource.writer.get_data();
            if let Ok(data) = data_result {
                // Create a reader to read the records from this data
                let cursor = Cursor::new(data);
                let mut reader = RecordReader::new(cursor).unwrap();

                // Read all records from this writer
                loop {
                    match reader.next_record().unwrap() {
                        DiskyPiece::Record(bytes) => {
                            actual_records.insert(bytes.to_vec());
                        }
                        DiskyPiece::EOF => break,
                    }
                }
            }

            // No need to return the resource to the queue as we're in a test
            // and the writers have already been consumed by get_data
        }

        // Verify we read back the same number of records
        assert_eq!(
            actual_records.len(),
            all_expected_records.len(),
            "Should read back the same number of records"
        );

        // Verify all expected records are present
        for expected in &all_expected_records {
            assert!(
                actual_records.contains(expected),
                "Expected record {:?} not found in output",
                expected
            );
        }
    }
}
