//! Parallel implementation of the Disky reader.
//!
//! This module provides a parallel reader for Disky records, designed to improve
//! performance by reading from multiple sharded files.

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use log::error;

use crate::error::{DiskyError, Result};
use crate::parallel::byte_queue::ByteQueue;
use crate::parallel::promise::Promise;
use crate::parallel::resource_pool::ResourcePool;
use crate::parallel::task_queue::TaskQueue;
use crate::shard::source::Shard;
use crate::tree::reader::Reader;

/// Result of reading a record from the parallel reader
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiskyParallelPiece {
    /// A record was successfully read
    Record(Bytes),

    /// Current shard is finished, but there might be more shards
    ShardFinished,

    /// All shards are finished, no more records available
    EOF,
}

/// Task for reading records
#[derive(Debug)]
pub enum Task {
    /// Read the next record from a reader
    NextRecord {
        /// Promise that will be fulfilled when the read completes
        completion: Arc<Promise<Result<DiskyParallelPiece>>>,
    },

    /// Drain all records from a resource into a byte queue
    DrainResource {
        /// The byte queue to drain records into
        byte_queue: Arc<ByteQueue>,
        /// Promise that will be fulfilled when the drain completes
        completion: Arc<Promise<Result<()>>>,
    },

    /// Close a reader
    Close {
        /// Promise that will be fulfilled when the close completes
        completion: Arc<Promise<Result<()>>>,
    },
}

impl Clone for Task {
    fn clone(&self) -> Self {
        match self {
            Task::NextRecord { completion } => Task::NextRecord {
                completion: Arc::clone(completion),
            },
            Task::DrainResource {
                byte_queue,
                completion,
            } => Task::DrainResource {
                byte_queue: Arc::clone(byte_queue),
                completion: Arc::clone(completion),
            },
            Task::Close { completion } => Task::Close {
                completion: Arc::clone(completion),
            },
        }
    }
}

/// Resource containing an initialized reader
pub struct ReaderResource {
    /// The record reader (boxed iterator)
    pub reader: Reader,

    /// Identifier for the shard (e.g., file path, ordinal index).
    /// Used in error messages to identify which shard failed.
    pub shard_id: String,
}

/// Sharding configuration for the parallel reader.
///
/// Controls how shards are located and loaded in the parallel reader.
pub struct ShardingConfig {
    /// The shard source iterator, wrapped in Mutex for thread-safe access.
    pub source: Mutex<Box<dyn Iterator<Item = Result<Shard>> + Send>>,

    /// Number of shards to keep active at once.
    pub shards: usize,
}

impl ShardingConfig {
    /// Create a new ShardingConfig.
    ///
    /// # Arguments
    /// * `source` - Iterator producing shards
    /// * `shards` - Maximum number of shards to keep active at once
    pub fn new(source: Box<dyn Iterator<Item = Result<Shard>> + Send>, shards: usize) -> Self {
        Self {
            source: Mutex::new(source),
            shards: std::cmp::max(shards, 1),
        }
    }
}

/// Configuration for the parallel reader
#[derive(Debug, Clone, Copy, Default)]
pub struct ParallelReaderConfig {}

impl ParallelReaderConfig {
    /// Creates a new configuration
    pub fn new() -> Self {
        Self {}
    }
}

/// A reader for processing multiple sharded Disky files
///
/// The ParallelReader allows efficient reading from multiple sharded files
/// by distributing read operations across multiple reader instances.
pub struct ParallelReader {
    /// Queue of tasks to be processed
    task_queue: Arc<TaskQueue<Task>>,

    /// Pool of reader resources
    reader_pool: Arc<ResourcePool<ReaderResource>>,

    /// Sharding configuration for the reader
    sharding_config: ShardingConfig,
}

impl ParallelReader {
    /// Gets a new shard and adds it to the resource pool
    ///
    /// This method gets a new shard from the iterator and adds its reader
    /// to the resource pool.
    ///
    /// # Returns
    /// Returns `Ok(true)` if a shard was added, `Ok(false)` if the source
    /// is exhausted, or `Err` on failure.
    fn get_new_shard(&self) -> Result<bool> {
        let next = self
            .sharding_config
            .source
            .lock()
            .map_err(|_| DiskyError::Other("Failed to lock shard source".to_string()))?
            .next();

        match next {
            Some(Ok(shard)) => {
                self.reader_pool.add_resource(ReaderResource {
                    reader: shard.reader,
                    shard_id: shard.id,
                })?;
                Ok(true)
            }
            Some(Err(e)) => Err(e),
            None => Ok(false),
        }
    }

    /// Creates a new ParallelReader with the given sharding and reader configurations
    ///
    /// This constructor initializes a reader with shards from the provided sharding config.
    ///
    /// # Arguments
    /// * `sharding_config` - Configuration for creating and managing shards
    /// * `_config` - Configuration for the parallel reader (currently unused)
    ///
    /// # Returns
    /// A new ParallelReader instance
    pub fn new(sharding_config: ShardingConfig, _config: ParallelReaderConfig) -> Result<Self> {
        let task_queue = Arc::new(TaskQueue::new());
        let reader_pool = Arc::new(ResourcePool::new());

        let reader = Self {
            task_queue,
            reader_pool,
            sharding_config,
        };

        // Create initial shards up to the configured limit
        for _ in 0..reader.sharding_config.shards {
            if !reader.get_new_shard()? {
                break;
            }
        }

        // Check if we found any shards
        if reader.reader_pool.available_count()? == 0 {
            return Err(DiskyError::Other("No shards found".to_string()));
        }

        Ok(reader)
    }

    /// Process a read task
    ///
    /// This method processes a single read task. It retrieves the appropriate reader
    /// resource and performs the requested operation.
    ///
    /// # Arguments
    /// * `task` - The read task to process
    ///
    /// # Returns
    /// Ok(()) if the task was processed successfully
    pub fn process_task(&self, task: Task) -> Result<()> {
        match task {
            Task::NextRecord { completion } => {
                // Process the read task
                let result = self.read();

                // Complete the promise with the result
                if let Err(e) = completion.fulfill(result) {
                    // Log the error but continue processing
                    error!("Failed to fulfill read promise: {}", e);
                }
            }
            Task::DrainResource {
                byte_queue,
                completion,
            } => {
                // Process the drain resource task
                let result = self.drain_resource(byte_queue);

                // Complete the promise with the result
                if let Err(e) = completion.fulfill(result) {
                    error!("Failed to fulfill drain resource promise: {}", e);
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

    /// Process the next available task in the queue
    ///
    /// # Returns
    /// Ok(()) if a task was processed, or an error
    pub fn process_next_task(&self) -> Result<()> {
        // Try to get a task from the queue
        let task = self.task_queue.read_front()?;

        self.process_task(task)
    }

    /// Process all available tasks in the queue
    ///
    /// # Returns
    /// Ok(()) if all tasks were processed, or an error
    pub fn process_all_tasks(&self) -> Result<()> {
        for task in self.task_queue.read_all()? {
            self.process_task(task)?;
        }

        Ok(())
    }

    /// Read a record asynchronously
    ///
    /// This method queues a read operation and returns a Promise that will be fulfilled
    /// when the read is completed. The actual read operation will be performed when
    /// `process_task` or `process_all_tasks` is called.
    ///
    /// # Returns
    /// A Promise that will be fulfilled with the result
    pub fn read_async(&self) -> Result<Arc<Promise<Result<DiskyParallelPiece>>>> {
        let completion = Arc::new(Promise::new());

        let task = Task::NextRecord {
            completion: Arc::clone(&completion),
        };

        self.task_queue.push_back(task)?;

        Ok(completion)
    }

    /// Read a record synchronously
    ///
    /// This method directly gets a reader resource from the pool and uses it
    /// to read a record. The resource is removed from the pool if it has reached EOF.
    ///
    /// # Returns
    /// - Ok(DiskyParallelPiece::Record(bytes)) if a record was read
    /// - Ok(DiskyParallelPiece::ShardFinished) if the current shard is exhausted but there might be more
    /// - Ok(DiskyParallelPiece::EOF) if all shards are exhausted
    /// - Err(...) if an error occurred
    pub fn read(&self) -> Result<DiskyParallelPiece> {
        // Try to get a reader resource
        match self.reader_pool.get_resource() {
            Ok(mut resource) => {
                // Try to read a record
                match resource.reader.next() {
                    Some(Ok(bytes)) => {
                        // Successfully read a record
                        Ok(DiskyParallelPiece::Record(bytes))
                    }
                    Some(Err(e)) => Err(DiskyError::ShardError {
                        shard_id: resource.shard_id.clone(),
                        source: Box::new(e),
                    }),
                    None => {
                        // This reader reached EOF, remove it from the pool
                        resource.forget();

                        // Try to get a new shard (ok regardless of whether one was found)
                        self.get_new_shard()?;
                        Ok(DiskyParallelPiece::ShardFinished)
                    }
                }
            }
            Err(DiskyError::PoolExhausted) => {
                // No more resources in the pool and we've already tried to create new shards,
                // so we are truly at EOF
                Ok(DiskyParallelPiece::EOF)
            }
            Err(e) => Err(e),
        }
    }

    /// Close the reader
    ///
    /// This method closes the resource pool and task queue, then processes any remaining
    /// tasks in the queue by fulfilling their promises with a QueueClosed error. This prevents
    /// deadlocks where threads are waiting for promises that never get fulfilled.
    ///
    /// # Returns
    /// Ok(()) if the reader was closed successfully, or an error
    pub fn close(&self) -> Result<()> {
        // First, try to close the resource pool
        let resource_close_result = self.reader_pool.close();

        // Then, try to close the task queue, regardless of whether the resource close succeeded
        // This prevents new tasks from being added
        let task_close_result = self.task_queue.close();

        // Now drain any remaining tasks from the queue and fulfill their promises with an error
        // This prevents deadlocks where threads are waiting for promises that never get fulfilled
        let remaining_tasks = self.task_queue.read_all().unwrap_or_default();

        for task in remaining_tasks {
            match task {
                Task::NextRecord { completion } => {
                    let _ = completion.fulfill(Err(DiskyError::QueueClosed(
                        "Reader queue was closed before read could be processed".to_string(),
                    )));
                }
                Task::DrainResource { completion, .. } => {
                    let _ = completion.fulfill(Err(DiskyError::QueueClosed(
                        "Reader queue was closed before drain could be processed".to_string(),
                    )));
                }
                Task::Close { completion } => {
                    let _ = completion.fulfill(Err(DiskyError::QueueClosed(
                        "Reader queue was already closed".to_string(),
                    )));
                }
            }
        }

        // Return the first error encountered, prioritizing resource errors over task queue errors
        match (resource_close_result, task_close_result) {
            (Err(e), _) => Err(e),      // Resource error takes precedence
            (Ok(()), Err(e)) => Err(e), // Task queue error if no resource error
            (Ok(()), Ok(())) => Ok(()), // Success if both operations succeeded
        }
    }

    /// Close the reader asynchronously
    ///
    /// This will queue a close task and return a Promise that will be fulfilled
    /// when the close is complete. The task must be processed by calling
    /// `process_next_task` or `process_all_tasks`.
    ///
    /// # Returns
    /// A Promise that will be fulfilled when the close is complete
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

    /// Get the number of available reader resources
    ///
    /// # Returns
    /// A Result containing the number of available resources
    pub fn available_reader_count(&self) -> Result<usize> {
        self.reader_pool.available_count()
    }

    /// Get the number of pending tasks
    ///
    /// # Returns
    /// The number of pending tasks or an error
    pub fn pending_task_count(&self) -> Result<usize> {
        self.task_queue.len()
    }

    /// Check if there are any pending tasks
    ///
    /// # Returns
    /// true if there are pending tasks, false otherwise, or an error
    pub fn has_pending_tasks(&self) -> Result<bool> {
        Ok(!self.task_queue.is_empty()?)
    }

    /// Drains records from a resource into a byte queue
    ///
    /// This method grabs a reader resource and drains all records from it
    /// until it completes (reaches EOF or needs a new shard), putting all
    /// records into the provided ByteQueue.
    ///
    /// # Arguments
    /// * `byte_queue` - The byte queue to drain records into
    ///
    /// # Returns
    /// Ok(()) if the drain was successful, or an error
    pub fn drain_resource(&self, byte_queue: Arc<ByteQueue>) -> Result<()> {
        // Try to get a reader resource directly
        match self.reader_pool.get_resource() {
            Ok(mut resource) => {
                // Drain all records from this resource
                loop {
                    match resource.reader.next() {
                        Some(Ok(bytes)) => {
                            // Successfully read a record, add to the queue
                            byte_queue.push_back(DiskyParallelPiece::Record(bytes))?;
                        }
                        Some(Err(e)) => {
                            return Err(DiskyError::ShardError {
                                shard_id: resource.shard_id.clone(),
                                source: Box::new(e),
                            });
                        }
                        None => {
                            // This reader reached EOF, remove it from the pool
                            resource.forget();

                            // Try to get a new shard (ok regardless of whether one was found)
                            self.get_new_shard()?;
                            byte_queue.push_back(DiskyParallelPiece::ShardFinished)?;
                            return Ok(());
                        }
                    }
                }
            }
            Err(DiskyError::PoolExhausted) => {
                // No more resources in the pool and we've already tried to create new shards.
                // Signal EOF but also propagate the PoolExhausted error so workers can exit
                byte_queue.push_back(DiskyParallelPiece::EOF)?;
                Err(DiskyError::PoolExhausted)
            }
            Err(e) => Err(e),
        }
    }

    /// Drains records asynchronously from a resource into a byte queue
    ///
    /// This method queues a drain operation and returns a Promise that will be fulfilled
    /// when the drain is completed. The actual drain operation will be performed when
    /// `process_task` or `process_all_tasks` is called.
    ///
    /// # Arguments
    /// * `byte_queue` - The byte queue to drain records into
    ///
    /// # Returns
    /// A Promise that will be fulfilled with the drain result
    pub fn drain_resource_async(
        &self,
        byte_queue: Arc<ByteQueue>,
    ) -> Result<Arc<Promise<Result<()>>>> {
        let completion = Arc::new(Promise::new());

        let task = Task::DrainResource {
            byte_queue: Arc::clone(&byte_queue),
            completion: Arc::clone(&completion),
        };

        self.task_queue.push_back(task)?;

        Ok(completion)
    }
}

impl Drop for ParallelReader {
    fn drop(&mut self) {
        // Close resources directly, ignoring any errors during drop
        let _ = self.reader_pool.close();
        let _ = self.task_queue.close();
    }
}
