// Multi-threaded reader implementation for Disky
//
// This module implements a reader that manages its own thread pool for parallel processing
// of Disky records, utilizing the underlying ParallelReader for resource management.

use std::io::{Read, Seek};
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::thread::{self, JoinHandle};

use bytes::Bytes;
use log::{debug, error, trace};

use crate::error::{DiskyError, Result};
use crate::parallel::byte_queue::ByteQueue;
use crate::parallel::reader::{DiskyParallelPiece, ParallelReader, ParallelReaderConfig, ShardingConfig};

/// Configuration for the multi-threaded reader
#[derive(Debug, Clone)]
pub struct MultiThreadedReaderConfig {
    /// Configuration for the underlying parallel reader
    pub reader_config: ParallelReaderConfig,
    
    /// Number of worker threads to spawn
    pub worker_threads: usize,
    
    /// Size of the byte queue in bytes
    pub queue_size_bytes: usize,
}

impl Default for MultiThreadedReaderConfig {
    fn default() -> Self {
        // Default to using the number of available CPUs for worker threads
        let worker_threads = match std::thread::available_parallelism() {
            Ok(num) => num.get(),
            Err(_) => 2, // Fallback if we can't determine parallelism
        };
        
        Self {
            reader_config: ParallelReaderConfig::default(),
            worker_threads,
            queue_size_bytes: 8 * 1024 * 1024, // Default to 8MB queue size
        }
    }
}

impl MultiThreadedReaderConfig {
    /// Creates a new configuration with custom settings
    pub fn new(
        reader_config: ParallelReaderConfig,
        worker_threads: usize,
        queue_size_bytes: usize,
    ) -> Self {
        Self {
            reader_config,
            worker_threads: worker_threads.max(1), // Ensure at least one worker
            queue_size_bytes: queue_size_bytes.max(1024), // Ensure reasonable minimum queue size
        }
    }
}

/// Worker thread state
struct Worker {
    /// Thread handle
    handle: JoinHandle<Result<()>>,
    
    /// Flag to indicate if this worker should continue running
    running: Arc<AtomicBool>,
}

/// A multi-threaded reader for Disky files
///
/// This reader manages its own thread pool to process read operations in parallel.
/// It leverages the ParallelReader for resource management and the ByteQueue for
/// transferring data between worker threads and the consumer.
pub struct MultiThreadedReader<Source: Read + Seek + Send + 'static> {
    /// The underlying parallel reader
    reader: Arc<ParallelReader<Source>>,
    
    /// Queue for passing records between threads
    byte_queue: Arc<ByteQueue>,
    
    /// Worker threads
    workers: Vec<Worker>,
    
    /// Flag to indicate if the reader has been closed
    closed: AtomicBool,
}

impl<Source: Read + Seek + Send + 'static> MultiThreadedReader<Source> {
    /// Creates a new multi-threaded reader
    ///
    /// This constructor initializes the reader and starts worker threads
    /// to process read operations.
    ///
    /// # Arguments
    /// * `sharding_config` - Configuration for creating and managing shards
    /// * `config` - Configuration for the multi-threaded reader
    ///
    /// # Returns
    /// A new MultiThreadedReader instance
    pub fn new(
        sharding_config: ShardingConfig<Source>,
        config: MultiThreadedReaderConfig,
    ) -> Result<Self> {
        // Create the underlying parallel reader
        let reader = Arc::new(ParallelReader::new(
            sharding_config,
            config.reader_config,
        )?);
        
        // Create the byte queue
        let byte_queue = Arc::new(ByteQueue::new(config.queue_size_bytes));
        
        // Start worker threads
        let mut workers = Vec::with_capacity(config.worker_threads);
        for i in 0..config.worker_threads {
            let reader_clone = Arc::clone(&reader);
            let byte_queue_clone = Arc::clone(&byte_queue);
            let running = Arc::new(AtomicBool::new(true));
            let running_clone = Arc::clone(&running);
            
            // Spawn a worker thread
            let handle = thread::spawn(move || {
                Self::worker_loop(
                    i,
                    reader_clone,
                    byte_queue_clone,
                    running_clone,
                )
            });
            
            workers.push(Worker {
                handle,
                running,
            });
        }
        
        Ok(Self {
            reader,
            byte_queue,
            workers,
            closed: AtomicBool::new(false),
        })
    }
    
    /// Worker thread main loop
    ///
    /// This function runs in each worker thread and continuously drains
    /// resources from the parallel reader into the byte queue.
    fn worker_loop(
        id: usize,
        reader: Arc<ParallelReader<Source>>,
        byte_queue: Arc<ByteQueue>,
        running: Arc<AtomicBool>,
    ) -> Result<()> {
        debug!("Worker thread {} starting", id);
        
        // Loop until signaled to stop
        while running.load(Ordering::Acquire) {
            // Drain a resource into the byte queue
            trace!("Worker {} attempting to drain a resource", id);
            match reader.drain_resource(Arc::clone(&byte_queue)) {
                Ok(()) => {
                    // Successfully drained a resource
                    trace!("Worker {} successfully drained a resource", id);
                }
                Err(DiskyError::PoolExhausted) => {
                    // No more resources available, exit worker thread
                    trace!("Worker {} found no resources, exiting", id);
                    break;
                }
                Err(e) => {
                    // Log the error and exit the worker
                    error!("Worker {} encountered error, exiting: {}", id, e);
                    
                    // Exit on error without sending EOF
                    // (other workers may still be able to read data successfully)
                    break;
                }
            }
        }
        
        debug!("Worker thread {} exiting", id);
        Ok(())
    }
    
    /// Reads the next record
    ///
    /// This method reads a record from the byte queue. It will block until
    /// a record is available or the reader is closed.
    ///
    /// # Returns
    /// The next record or EOF
    pub fn read(&self) -> Result<DiskyParallelPiece> {
        // Check if we're closed
        if self.closed.load(Ordering::Acquire) {
            return Err(DiskyError::ReaderClosed);
        }
        
        // Try to read from the byte queue
        match self.byte_queue.read_front() {
            Ok(DiskyParallelPiece::ShardFinished) => {
                // Skip shard finished markers and read the next record
                self.read()
            }
            Ok(piece) => Ok(piece),
            Err(DiskyError::QueueClosed(_)) => {
                // Queue closed, return EOF
                Ok(DiskyParallelPiece::EOF)
            }
            Err(e) => Err(e),
        }
    }
    
    /// Tries to read the next record without blocking
    ///
    /// This method attempts to read a record from the byte queue without blocking.
    ///
    /// # Returns
    /// Some(record) if a record was available, None if no records were available
    pub fn try_read(&self) -> Result<Option<DiskyParallelPiece>> {
        // Check if we're closed
        if self.closed.load(Ordering::Acquire) {
            return Err(DiskyError::ReaderClosed);
        }
        
        // Try to read from the byte queue
        match self.byte_queue.try_read_front()? {
            Some(DiskyParallelPiece::ShardFinished) => {
                // Skip shard finished markers and try again
                self.try_read()
            }
            Some(piece) => Ok(Some(piece)),
            None => Ok(None),
        }
    }
    
    /// Closes the reader
    ///
    /// This method shuts down all worker threads and closes the underlying resources.
    pub fn close(&self) -> Result<()> {
        // Set the closed flag
        self.closed.store(true, Ordering::Release);
        
        // Signal all workers to stop
        for worker in &self.workers {
            worker.running.store(false, Ordering::Release);
        }
        
        // Close the byte queue to unblock any readers
        self.byte_queue.close()?;
        
        // Close the underlying reader
        self.reader.close()?;
        
        Ok(())
    }
    
    /// Waits for all worker threads to join
    ///
    /// This method waits for all worker threads to complete and returns
    /// any errors that occurred.
    pub fn join(mut self) -> Result<()> {
        // Close if not already closed
        if !self.closed.load(Ordering::Acquire) {
            self.close()?;
        }
        
        // Take the workers out of self to avoid borrow checker issues
        let workers = std::mem::take(&mut self.workers);
        
        // Wait for all workers to complete
        for (i, worker) in workers.into_iter().enumerate() {
            match worker.handle.join() {
                Ok(result) => {
                    if let Err(e) = result {
                        error!("Worker {} returned error: {}", i, e);
                    }
                }
                Err(e) => {
                    error!("Failed to join worker {}: {:?}", i, e);
                }
            }
        }
        
        Ok(())
    }
    
    /// Returns the number of records in the queue
    pub fn queued_records(&self) -> Result<usize> {
        self.byte_queue.len()
    }
    
    /// Returns the number of bytes in the queue
    pub fn queued_bytes(&self) -> Result<usize> {
        self.byte_queue.bytes_used()
    }
    
    /// Returns the queue size limit in bytes
    pub fn queue_size_limit(&self) -> Result<usize> {
        self.byte_queue.bytes_block_limit()
    }
    
    /// Sets the queue size limit in bytes
    pub fn set_queue_size_limit(&self, limit: usize) -> Result<()> {
        self.byte_queue.set_bytes_block_limit(limit)
    }
}

/// Iterator implementation for MultiThreadedReader
impl<Source: Read + Seek + Send + 'static> Iterator for MultiThreadedReader<Source> {
    type Item = Result<Bytes>;
    
    fn next(&mut self) -> Option<Self::Item> {
        match self.read() {
            Ok(DiskyParallelPiece::Record(bytes)) => Some(Ok(bytes)),
            Ok(DiskyParallelPiece::EOF) => None,
            Ok(_) => self.next(), // Skip other control messages
            Err(e) => Some(Err(e)),
        }
    }
}

impl<Source: Read + Seek + Send + 'static> Drop for MultiThreadedReader<Source> {
    fn drop(&mut self) {
        // Close the reader, but ignore any errors since we can't return them
        let _ = self.close();
    }
}
