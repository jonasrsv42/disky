//! Parallel implementation of the Disky reader.
//!
//! This module provides a parallel reader for Disky records, designed to improve
//! performance by reading from multiple sharded files.

use std::io::{Read, Seek};
use std::sync::Arc;

use bytes::Bytes;
use log::error;

use crate::error::{DiskyError, Result};
use crate::parallel::byte_queue::ByteQueue;
use crate::parallel::promise::Promise;
use crate::parallel::resource_pool::ResourcePool;
use crate::parallel::sharding::ShardLocator;
use crate::parallel::task_queue::TaskQueue;
use crate::reader::{DiskyPiece, RecordReader, RecordReaderConfig};

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
            Task::DrainResource { byte_queue, completion } => Task::DrainResource {
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
pub struct ReaderResource<Source: Read + Seek + Send + 'static> {
    /// The record reader, boxed to signify exclusive ownership
    pub reader: Box<RecordReader<Source>>,
}

/// Sharding configuration for the parallel reader
///
/// Controls how shards are located and loaded in the parallel reader.
pub struct ShardingConfig<Source: Read + Seek + Send + 'static> {
    /// The shard locator for finding and opening shards
    pub locator: Box<dyn ShardLocator<Source> + Send + Sync>,
    
    /// Number of shards to keep active at once
    pub shards: usize,
}

impl<Source: Read + Seek + Send + 'static> ShardingConfig<Source> {
    /// Create a new ShardingConfig with the given locator and number of shards
    ///
    /// This constructor will automatically adjust the number of shards to be
    /// the minimum of the requested count and the estimated count from the locator.
    ///
    /// # Arguments
    /// * `locator` - The shard locator to use for finding and opening shards
    /// * `shards` - The maximum number of shards to keep active at once
    ///
    /// # Returns
    /// A new ShardingConfig instance
    pub fn new(locator: Box<dyn ShardLocator<Source> + Send + Sync>, shards: usize) -> Self {
        // Ensure shards is at least 1
        let requested_shards = std::cmp::max(shards, 1);
        
        // Calculate the actual number of shards based on estimated count
        let actual_shards = match locator.estimated_shard_count() {
            Some(count) => std::cmp::min(requested_shards, count),
            None => requested_shards,
        };
        
        Self {
            locator,
            shards: actual_shards,
        }
    }
}

/// Configuration for the parallel reader
#[derive(Debug, Clone)]
pub struct ParallelReaderConfig {
    /// Configuration for the underlying record readers
    pub reader_config: RecordReaderConfig,
}

impl Default for ParallelReaderConfig {
    fn default() -> Self {
        Self {
            reader_config: RecordReaderConfig::default(),
        }
    }
}

impl ParallelReaderConfig {
    /// Creates a new configuration with the specified reader config
    pub fn new(reader_config: RecordReaderConfig) -> Self {
        Self {
            reader_config,
        }
    }
}

/// A reader for processing multiple sharded Disky files
///
/// The ParallelReader allows efficient reading from multiple sharded files
/// by distributing read operations across multiple reader instances.
pub struct ParallelReader<Source: Read + Seek + Send + 'static> {
    /// Queue of tasks to be processed
    task_queue: Arc<TaskQueue<Task>>,
    
    /// Pool of reader resources
    reader_pool: Arc<ResourcePool<ReaderResource<Source>>>,
    
    /// Configuration for the parallel reader
    config: ParallelReaderConfig,
    
    /// Sharding configuration for the reader
    sharding_config: ShardingConfig<Source>,
}

impl<Source: Read + Seek + Send + 'static> ParallelReader<Source> {
    /// Gets a new shard and adds it to the resource pool
    ///
    /// This method gets a new source using the shard locator, wraps it in a RecordReader,
    /// and adds it to the resource pool.
    ///
    /// # Returns
    /// A Result indicating success or failure
    fn get_new_shard(&self) -> Result<()> {
        // Get a new source using the locator
        let source = self.sharding_config.locator.next_shard()?;
            
        // Create a new RecordReader with the source and configuration
        let reader = RecordReader::with_config(source, self.config.reader_config.clone())?;
            
        // Add the reader to the resource pool
        self.reader_pool.add_resource(ReaderResource {
            reader: Box::new(reader),
        })
    }
    
    /// Creates a new ParallelReader with the given sharding and reader configurations
    ///
    /// This constructor initializes a reader with shards from the provided sharding config.
    ///
    /// # Arguments
    /// * `sharding_config` - Configuration for creating and managing shards
    /// * `config` - Configuration for the parallel reader
    ///
    /// # Returns
    /// A new ParallelReader instance
    pub fn new(
        sharding_config: ShardingConfig<Source>,
        config: ParallelReaderConfig,
    ) -> Result<Self> {
        let task_queue = Arc::new(TaskQueue::new());
        let reader_pool = Arc::new(ResourcePool::new());
        
        let reader = Self {
            task_queue,
            reader_pool,
            config,
            sharding_config,
        };
        
        // Create initial shards using the count from sharding_config
        for _ in 0..reader.sharding_config.shards {
            match reader.get_new_shard() {
                Ok(_) => {},
                Err(DiskyError::NoMoreShards) => {
                    // No more shards available, that's okay
                    break;
                },
                Err(e) => {
                    // Actual error
                    return Err(e);
                }
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
            Task::DrainResource { byte_queue, completion } => {
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
                match resource.reader.next_record() {
                    Ok(DiskyPiece::Record(bytes)) => {
                        // Successfully read a record
                        Ok(DiskyParallelPiece::Record(bytes))
                    },
                    Ok(DiskyPiece::EOF) => {
                        // This reader reached EOF, remove it from the pool
                        resource.forget();
                        
                        // Try to get a new shard
                        match self.get_new_shard() {
                            Ok(_) | Err(DiskyError::NoMoreShards) => {
                                // Signal that this shard is finished, but we can try another
                                Ok(DiskyParallelPiece::ShardFinished)
                            },
                            Err(e) => {
                                // Error creating a new shard
                                Err(e)
                            }
                        }
                    },
                    Err(e) => Err(e),
                }
            },
            Err(DiskyError::PoolExhausted) => {
                // No more resources in the pool and we've already tried to create new shards,
                // so we are truly at EOF
                Ok(DiskyParallelPiece::EOF)
            },
            Err(e) => Err(e),
        }
    }
    
    // Iterator functionality removed - will be implemented in a future update
    
    /// Close the reader
    ///
    /// This method closes the task queue and resource pool.
    ///
    /// # Returns
    /// Ok(()) if the reader was closed successfully, or an error
    pub fn close(&self) -> Result<()> {
        // First, try to close the resource pool
        let resource_close_result = self.reader_pool.close();
        
        // Then, try to close the task queue, regardless of whether the resource close succeeded
        let task_close_result = self.task_queue.close();
        
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
                    match resource.reader.next_record() {
                        Ok(DiskyPiece::Record(bytes)) => {
                            // Successfully read a record, add to the queue
                            byte_queue.push_back(DiskyParallelPiece::Record(bytes))?;
                        },
                        Ok(DiskyPiece::EOF) => {
                            // This reader reached EOF, remove it from the pool
                            resource.forget();
                            
                            // Try to get a new shard
                            match self.get_new_shard() {
                                Ok(_) | Err(DiskyError::NoMoreShards) => {
                                    // Signal that this shard is finished
                                    byte_queue.push_back(DiskyParallelPiece::ShardFinished)?;
                                    return Ok(());
                                },
                                Err(e) => {
                                    // Error creating a new shard
                                    return Err(e);
                                }
                            }
                        },
                        Err(e) => return Err(e),
                    }
                }
            },
            Err(DiskyError::PoolExhausted) => {
                // No more resources in the pool and we've already tried to create new shards,
                // so we are truly at EOF
                byte_queue.push_back(DiskyParallelPiece::EOF)?;
                Ok(())
            },
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
    pub fn drain_resource_async(&self, byte_queue: Arc<ByteQueue>) -> Result<Arc<Promise<Result<()>>>> {
        let completion = Arc::new(Promise::new());
        
        let task = Task::DrainResource {
            byte_queue: Arc::clone(&byte_queue),
            completion: Arc::clone(&completion),
        };
        
        self.task_queue.push_back(task)?;
        
        Ok(completion)
    }
}

impl<Source: Read + Seek + Send + 'static> Drop for ParallelReader<Source> {
    fn drop(&mut self) {
        // Close resources directly, ignoring any errors during drop
        let _ = self.reader_pool.close();
        let _ = self.task_queue.close();
    }
}

// Iterator implementation removed - will be implemented in a future update

#[cfg(test)]
mod tests {
    // Tests will be added in a separate PR
}