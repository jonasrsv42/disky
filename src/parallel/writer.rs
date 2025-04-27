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
//! use disky::parallel::writer::{ParallelWriter, ParallelWriterConfig, ShardingConfig};
//! use disky::parallel::sharding::{Autosharder, FileSharder};
//! use std::path::PathBuf;
//! use bytes::Bytes;
//!
//! // Create a file sharder that creates sequentially numbered files
//! let file_sharder = FileSharder::new(
//!     PathBuf::from("/tmp/output"),
//!     "shard"
//! );
//!
//! // Configure with 4 shards
//! let sharding_config = ShardingConfig::new(
//!     Box::new(file_sharder),
//!     4
//! );
//!
//! // Initialize the parallel writer
//! let parallel_writer = ParallelWriter::new(
//!     sharding_config,
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
use crate::parallel::sharding::Sharder;
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
    /// Total number of bytes written by this writer resource
    pub bytes_written: usize,
}

/// Configuration for the parallel writer
///
/// Controls the behavior of the parallel writer, including the configuration
/// of the underlying record writers.
#[derive(Debug, Clone)]
pub struct ParallelWriterConfig {
    /// Configuration for the underlying record writer instances
    pub writer_config: RecordWriterConfig,

    /// Maximum number of bytes a writer can write before it's dropped from the pool
    /// Set to None to allow unlimited bytes per writer (default)
    pub max_bytes_per_writer: Option<usize>,
}

impl Default for ParallelWriterConfig {
    fn default() -> Self {
        Self {
            writer_config: RecordWriterConfig::default(),
            max_bytes_per_writer: None,
        }
    }
}

/// Auto-sharding configuration for the parallel writer
///
/// Controls how auto-sharding is performed in the parallel writer,
/// including the sharder implementation and number of shards.
pub struct ShardingConfig<Sink: Write + Seek + Send + 'static> {
    /// The sharder implementation for creating new sinks
    pub sharder: Box<dyn Sharder<Sink> + Send + Sync>,

    /// Number of shards to create and maintain
    pub shards: usize,

    /// Whether to enable automatic shard creation and management
    pub enable_auto_sharding: bool,
}

impl<Sink: Write + Seek + Send + 'static> ShardingConfig<Sink> {
    /// Create a new ShardingConfig with the given sharder and number of shards
    ///
    /// By default, auto-sharding is disabled.
    ///
    /// # Arguments
    /// * `sharder` - The sharder implementation to use for creating new sinks
    /// * `shards` - The number of shards to create
    ///
    /// # Returns
    /// A new ShardingConfig instance
    pub fn new(sharder: Box<dyn Sharder<Sink> + Send + Sync>, shards: usize) -> Self {
        // Ensure shards is at least 1
        let shards = std::cmp::max(shards, 1);

        Self {
            sharder,
            shards,
            // Auto-sharding is disabled by default
            enable_auto_sharding: false,
        }
    }

    /// Create a new ShardingConfig with auto-sharding enabled
    ///
    /// # Arguments
    /// * `sharder` - The sharder implementation to use for creating new sinks
    /// * `shards` - The number of shards to create and maintain
    ///
    /// # Returns
    /// A new ShardingConfig instance with auto-sharding enabled
    pub fn with_auto_sharding(
        sharder: Box<dyn Sharder<Sink> + Send + Sync>,
        shards: usize,
    ) -> Self {
        // Ensure shards is at least 1
        let shards = std::cmp::max(shards, 1);

        Self {
            sharder,
            shards,
            enable_auto_sharding: true,
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
/// - Can automatically drop writers that exceed a configured byte limit
/// - Supports auto-sharding to dynamically create new writers when needed
///
/// This design is particularly useful for integrating with existing threading
/// or async systems, as it does not create its own worker threads.
pub struct ParallelWriter<Sink: Write + Seek + Send + 'static> {
    /// Queue of tasks to be processed
    task_queue: Arc<TaskQueue<Task>>,

    /// Queue of writer resources with active tracking
    resource_pool: Arc<ResourcePool<WriterResource<Sink>>>,

    /// Configuration for the parallel writer
    config: ParallelWriterConfig,

    /// Sharding configuration for creating new writers
    sharding_config: ShardingConfig<Sink>,
}

impl<Sink: Write + Seek + Send + 'static> ParallelWriter<Sink> {
    /// For testing purposes: Get access to the resource pool containing the writers
    #[cfg(test)]
    pub(crate) fn get_resource_pool(&self) -> &Arc<ResourcePool<WriterResource<Sink>>> {
        &self.resource_pool
    }

    /// Create a new parallel writer with the given sharding configuration.
    ///
    /// This constructor initializes a parallel writer using a sharding configuration,
    /// which specifies how new shards will be created when needed.
    ///
    /// # Arguments
    /// * `sharding_config` - Configuration for creating and managing shards
    /// * `config` - Configuration for the parallel writer
    ///
    /// # Returns
    /// A Result containing the new ParallelWriter instance or an error
    ///
    /// # Example
    /// ```rust,no_run
    /// # use disky::parallel::writer::{ParallelWriter, ParallelWriterConfig, ShardingConfig};
    /// # use disky::parallel::sharding::Autosharder;
    /// # use std::io::Cursor;
    ///
    /// // Create a simple in-memory sharder
    /// let sharder = Autosharder::new(|| {
    ///     Ok(Cursor::new(Vec::new()))
    /// });
    ///
    /// // Create a sharding configuration with 3 shards
    /// let sharding_config = ShardingConfig::new(
    ///     Box::new(sharder),
    ///     3
    /// );
    ///
    /// // Create a parallel writer with the sharding configuration
    /// let parallel_writer = ParallelWriter::new(
    ///     sharding_config,
    ///     ParallelWriterConfig::default()
    /// ).unwrap();
    /// ```
    pub fn new(
        sharding_config: ShardingConfig<Sink>,
        config: ParallelWriterConfig,
    ) -> Result<Self> {
        let task_queue = Arc::new(TaskQueue::new());
        let resource_pool = Arc::new(ResourcePool::new());

        let initial_shards = sharding_config.shards;

        let writer = Self {
            task_queue,
            resource_pool,
            config,
            sharding_config,
        };

        for _ in 0..initial_shards {
            writer.create_new_shard()?;
        }

        Ok(writer)
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
    /// # use disky::parallel::writer::{ParallelWriter, ParallelWriterConfig, ShardingConfig};
    /// # use disky::parallel::sharding::Autosharder;
    /// # use std::io::Cursor;
    /// # use bytes::Bytes;
    /// # let sharder = Autosharder::new(|| Ok(Cursor::new(Vec::new())));
    /// # let sharding_config = ShardingConfig::new(Box::new(sharder), 3);
    /// # let parallel_writer = ParallelWriter::new(sharding_config, ParallelWriterConfig::default()).unwrap();
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
    /// # use disky::parallel::writer::{ParallelWriter, ParallelWriterConfig, ShardingConfig};
    /// # use disky::parallel::sharding::Autosharder;
    /// # use std::io::Cursor;
    /// # let sharder = Autosharder::new(|| Ok(Cursor::new(Vec::new())));
    /// # let sharding_config = ShardingConfig::new(Box::new(sharder), 3);
    /// # let parallel_writer = ParallelWriter::new(sharding_config, ParallelWriterConfig::default()).unwrap();
    /// parallel_writer.write_record(b"synchronous record").unwrap();
    /// ```
    /// Create a new shard and add it to the resource pool
    ///
    /// This method creates a new sink using the sharder, wraps it in a RecordWriter,
    /// and adds it to the resource pool. It's used internally when a shard is dropped
    /// due to exceeding byte limits and auto-sharding is enabled.
    ///
    /// # Returns
    /// A Result indicating success or failure
    fn create_new_shard(&self) -> Result<()> {
        // Create a new sink using the sharder
        let sink = self.sharding_config.sharder.create_sink()?;

        // Create a new RecordWriter with the sink and configuration
        let writer = RecordWriter::with_config(sink, self.config.writer_config.clone())?;

        // Add the writer to the resource pool
        self.resource_pool.add_resource(WriterResource {
            writer: Box::new(writer),
            bytes_written: 0,
        })
    }

    pub fn write_record(&self, data: &[u8]) -> Result<()> {
        // Process a resource by writing the record to it
        let mut resource = self.resource_pool.get_resource()?;

        // Write the record using the underlying writer - return early if error
        resource.writer.write_record(data)?;

        // Only increment bytes_written after successful write
        resource.bytes_written += data.len();

        // Check if we need to forget this resource due to exceeding byte limit
        if let Some(max_bytes) = self.config.max_bytes_per_writer {
            if resource.bytes_written >= max_bytes {
                // Try to close the writer
                let close_result = resource.writer.close();

                // Always forget the resource regardless of close result
                resource.forget();

                // If auto-sharding is enabled, create a new shard to replace the one we're forgetting
                if self.sharding_config.enable_auto_sharding {
                    self.create_new_shard()?;
                }

                // Now return close error if there was one
                close_result?;
            }
        }

        Ok(())
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
                // We don't need to duplicate our max_bytes logic here since write_record already handles it
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
    /// # use disky::parallel::writer::{ParallelWriter, ParallelWriterConfig, ShardingConfig};
    /// # use disky::parallel::sharding::Autosharder;
    /// # use std::io::Cursor;
    /// # use bytes::Bytes;
    /// # use std::thread;
    /// # use std::sync::Arc;
    /// # let sharder = Autosharder::new(|| Ok(Cursor::new(Vec::new())));
    /// # let sharding_config = ShardingConfig::new(Box::new(sharder), 3);
    /// # let parallel_writer = Arc::new(ParallelWriter::new(sharding_config, ParallelWriterConfig::default()).unwrap());
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
    /// # use disky::parallel::writer::{ParallelWriter, ParallelWriterConfig, ShardingConfig};
    /// # use disky::parallel::sharding::Autosharder;
    /// # use std::io::Cursor;
    /// # use bytes::Bytes;
    /// # use std::sync::Arc;
    /// # use std::thread;
    /// # let sharder = Autosharder::new(|| Ok(Cursor::new(Vec::new())));
    /// # let sharding_config = ShardingConfig::new(Box::new(sharder), 3);
    /// # let parallel_writer = Arc::new(ParallelWriter::new(sharding_config, ParallelWriterConfig::default()).unwrap());
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
    /// # use disky::parallel::writer::{ParallelWriter, ParallelWriterConfig, ShardingConfig};
    /// # use disky::parallel::sharding::Autosharder;
    /// # use std::io::Cursor;
    /// # let sharder = Autosharder::new(|| Ok(Cursor::new(Vec::new())));
    /// # let sharding_config = ShardingConfig::new(Box::new(sharder), 3);
    /// # let parallel_writer = ParallelWriter::new(sharding_config, ParallelWriterConfig::default()).unwrap();
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
    /// # use disky::parallel::writer::{ParallelWriter, ParallelWriterConfig, ShardingConfig};
    /// # use disky::parallel::sharding::Autosharder;
    /// # use std::io::Cursor;
    /// # let sharder = Autosharder::new(|| Ok(Cursor::new(Vec::new())));
    /// # let sharding_config = ShardingConfig::new(Box::new(sharder), 3);
    /// # let parallel_writer = ParallelWriter::new(sharding_config, ParallelWriterConfig::default()).unwrap();
    /// # parallel_writer.write_record(b"some data").unwrap();
    ///
    /// // Flush all writers synchronously
    /// parallel_writer.flush().unwrap();
    /// ```
    pub fn flush(&self) -> Result<()> {
        // Use the enhanced method that handles pause/process/resume in one operation
        self.resource_pool
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
    /// # use disky::parallel::writer::{ParallelWriter, ParallelWriterConfig, ShardingConfig};
    /// # use disky::parallel::sharding::Autosharder;
    /// # use std::io::Cursor;
    /// # let sharder = Autosharder::new(|| Ok(Cursor::new(Vec::new())));
    /// # let sharding_config = ShardingConfig::new(Box::new(sharder), 3);
    /// # let parallel_writer = ParallelWriter::new(sharding_config, ParallelWriterConfig::default()).unwrap();
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
    /// # use disky::parallel::writer::{ParallelWriter, ParallelWriterConfig, ShardingConfig};
    /// # use disky::parallel::sharding::Autosharder;
    /// # use std::io::Cursor;
    /// # let sharder = Autosharder::new(|| Ok(Cursor::new(Vec::new())));
    /// # let sharding_config = ShardingConfig::new(Box::new(sharder), 3);
    /// # let parallel_writer = ParallelWriter::new(sharding_config, ParallelWriterConfig::default()).unwrap();
    /// # parallel_writer.write_record(b"some data").unwrap();
    /// # parallel_writer.flush().unwrap();
    ///
    /// // Close all writers synchronously
    /// parallel_writer.close().unwrap();
    /// ```
    pub fn close(&self) -> Result<()> {
        // First, try to close all writer resources
        let resource_close_result = self
            .resource_pool
            .process_then_close(|resource| resource.writer.close());

        // Then, try to close the task queue, regardless of whether the resource close succeeded
        let task_close_result = self.task_queue.close();

        // Return the first error encountered, prioritizing resource errors over task queue errors
        match (resource_close_result, task_close_result) {
            (Err(e), _) => Err(e),      // Resource error takes precedence
            (Ok(()), Err(e)) => Err(e), // Task queue error if no resource error
            (Ok(()), Ok(())) => Ok(()), // Success if both operations succeeded
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
    /// # use disky::parallel::writer::{ParallelWriter, ParallelWriterConfig, ShardingConfig};
    /// # use disky::parallel::sharding::Autosharder;
    /// # use std::io::Cursor;
    /// # use bytes::Bytes;
    /// # let sharder = Autosharder::new(|| Ok(Cursor::new(Vec::new())));
    /// # let sharding_config = ShardingConfig::new(Box::new(sharder), 3);
    /// # let parallel_writer = ParallelWriter::new(sharding_config, ParallelWriterConfig::default()).unwrap();
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
    /// # use disky::parallel::writer::{ParallelWriter, ParallelWriterConfig, ShardingConfig};
    /// # use disky::parallel::sharding::Autosharder;
    /// # use std::io::Cursor;
    /// # use bytes::Bytes;
    /// # let sharder = Autosharder::new(|| Ok(Cursor::new(Vec::new())));
    /// # let sharding_config = ShardingConfig::new(Box::new(sharder), 3);
    /// # let parallel_writer = ParallelWriter::new(sharding_config, ParallelWriterConfig::default()).unwrap();
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
    /// # use disky::parallel::writer::{ParallelWriter, ParallelWriterConfig, ShardingConfig};
    /// # use disky::parallel::sharding::Autosharder;
    /// # use std::io::Cursor;
    /// # let sharder = Autosharder::new(|| Ok(Cursor::new(Vec::new())));
    /// # let sharding_config = ShardingConfig::new(Box::new(sharder), 3);
    /// # let parallel_writer = ParallelWriter::new(sharding_config, ParallelWriterConfig::default()).unwrap();
    ///
    /// let count = parallel_writer.available_resource_count().unwrap();
    /// println!("There are {} available writer resources", count);
    /// ```
    pub fn available_resource_count(&self) -> Result<usize> {
        self.resource_pool.available_count()
    }
}
