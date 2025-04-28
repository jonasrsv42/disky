// Multi-threaded writer implementation for Disky
//
// This module implements a writer that manages its own thread pool for parallel processing
// of Disky records, utilizing the underlying ParallelWriter for resource management.

use std::io::{Seek, Write};
use std::mem::ManuallyDrop;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread::{self, JoinHandle};

use bytes::Bytes;
use log::{debug, error, trace};

use crate::parallel::promise::Promise;

use crate::error::{DiskyError, Result};
use crate::parallel::writer::{ParallelWriter, ParallelWriterConfig, ShardingConfig};

/// Configuration for the multi-threaded writer
#[derive(Debug, Clone)]
pub struct MultiThreadedWriterConfig {
    /// Configuration for the underlying parallel writer
    pub writer_config: ParallelWriterConfig,

    /// Number of worker threads to spawn
    pub worker_threads: usize,
}

impl Default for MultiThreadedWriterConfig {
    fn default() -> Self {
        // Default to using the number of available CPUs for worker threads
        let worker_threads = match std::thread::available_parallelism() {
            Ok(num) => num.get(),
            Err(_) => 2, // Fallback if we can't determine parallelism
        };

        Self {
            writer_config: ParallelWriterConfig::default(),
            worker_threads,
        }
    }
}

impl MultiThreadedWriterConfig {
    /// Creates a new configuration with custom settings
    pub fn new(writer_config: ParallelWriterConfig, worker_threads: usize) -> Self {
        Self {
            writer_config,
            worker_threads: worker_threads.max(1), // Ensure at least one worker
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

/// A multi-threaded writer for Disky files
///
/// This writer manages its own thread pool to process write operations in parallel.
/// It leverages the ParallelWriter for resource management.
pub struct MultiThreadedWriter<Sink: Write + Seek + Send + 'static> {
    /// The underlying parallel writer
    writer: Arc<ParallelWriter<Sink>>,

    /// Worker threads wrapped in ManuallyDrop so they can be taken in drop
    workers: ManuallyDrop<Vec<Worker>>,

    /// Flag to indicate if the writer has been closed
    closed: AtomicBool,
}

impl<Sink: Write + Seek + Send + 'static> MultiThreadedWriter<Sink> {
    /// Creates a new multi-threaded writer
    ///
    /// This constructor initializes the writer and starts worker threads
    /// to process write operations.
    ///
    /// # Arguments
    /// * `sharding_config` - Configuration for creating and managing shards
    /// * `config` - Configuration for the multi-threaded writer
    ///
    /// # Returns
    /// A new MultiThreadedWriter instance
    pub fn new(
        sharding_config: ShardingConfig<Sink>,
        config: MultiThreadedWriterConfig,
    ) -> Result<Self> {
        // Create the underlying parallel writer
        let writer = Arc::new(ParallelWriter::new(sharding_config, config.writer_config)?);

        // Start worker threads
        let mut workers = Vec::with_capacity(config.worker_threads);
        for i in 0..config.worker_threads {
            let writer_clone = Arc::clone(&writer);
            let running = Arc::new(AtomicBool::new(true));
            let running_clone = Arc::clone(&running);

            // Spawn a worker thread
            let handle = thread::spawn(move || Self::worker_loop(i, writer_clone, running_clone));

            workers.push(Worker { handle, running });
        }

        Ok(Self {
            writer,
            workers: ManuallyDrop::new(workers),
            closed: AtomicBool::new(false),
        })
    }

    /// Worker thread main loop
    ///
    /// This function runs in each worker thread and continuously processes
    /// tasks from the parallel writer's task queue.
    fn worker_loop(
        id: usize,
        writer: Arc<ParallelWriter<Sink>>,
        running: Arc<AtomicBool>,
    ) -> Result<()> {
        debug!("Worker thread {} starting", id);

        // Loop until signaled to stop
        while running.load(Ordering::Acquire) {
            // Process next task from the queue
            trace!("Worker {} attempting to process a task", id);
            match writer.process_next_task() {
                Ok(()) => {
                    // Successfully processed a task
                    trace!("Worker {} successfully processed a task", id);
                }
                Err(DiskyError::QueueClosed(_)) => {
                    // Queue is closed, exit worker thread
                    trace!("Worker {} found queue closed, exiting", id);
                    break;
                }
                Err(e) => {
                    // Log the error but continue - might be transient
                    error!("Worker {} encountered error: {}", id, e);
                }
            }
        }

        debug!("Worker thread {} exiting", id);
        Ok(())
    }

    /// Writes a record asynchronously
    ///
    /// This method writes a record using the underlying parallel writer.
    /// It queues the write asynchronously to be handled by worker threads.
    ///
    /// # Arguments
    /// * `data` - The record data to write as Bytes
    ///
    /// # Returns
    /// A Result containing a Promise that will be fulfilled when the write completes
    pub fn write_record(&self, data: Bytes) -> Result<Arc<Promise<Result<()>>>> {
        // Check if we're closed
        if self.closed.load(Ordering::Acquire) {
            return Err(DiskyError::WritingClosedFile);
        }

        // Write the record asynchronously
        self.writer.write_record_async(data)
    }

    /// Writes a record and waits for completion
    ///
    /// This method writes a record asynchronously using the underlying parallel writer
    /// and then waits for the operation to complete.
    ///
    /// # Arguments
    /// * `data` - The record data to write as Bytes
    ///
    /// # Returns
    /// A Result indicating success or failure
    pub fn write_record_blocking(&self, data: Bytes) -> Result<()> {
        // Check if we're closed
        if self.closed.load(Ordering::Acquire) {
            return Err(DiskyError::WritingClosedFile);
        }

        // Write the record asynchronously
        let promise = self.writer.write_record_async(data)?;

        // Wait for the write to complete
        promise.wait()?
    }

    /// Writes a record from a slice and waits for completion
    ///
    /// This method is a convenience wrapper that converts a slice to Bytes
    /// and writes it asynchronously, waiting for completion.
    ///
    /// # Arguments
    /// * `data` - The record data as a byte slice
    ///
    /// # Returns
    /// A Result indicating success or failure
    pub fn write_slice_blocking(&self, data: &[u8]) -> Result<()> {
        // Check if we're closed
        if self.closed.load(Ordering::Acquire) {
            return Err(DiskyError::WritingClosedFile);
        }

        // Convert to Bytes
        let bytes_data = Bytes::copy_from_slice(data);

        // Write the record blocking
        self.write_record_blocking(bytes_data)
    }

    /// Flushes all writers asynchronously
    ///
    /// This method queues a flush operation to be processed by worker threads,
    /// and returns a Promise that will be fulfilled when the operation completes.
    ///
    /// # Returns
    /// A Result containing a Promise that will be fulfilled when the flush completes
    pub fn flush_async(&self) -> Result<Arc<Promise<Result<()>>>> {
        // Check if we're closed
        if self.closed.load(Ordering::Acquire) {
            return Err(DiskyError::WritingClosedFile);
        }

        // Queue a flush operation
        self.writer.flush_async()
    }

    /// Flushes all writers and waits for completion
    ///
    /// This method queues a flush operation and waits for it to complete.
    ///
    /// # Returns
    /// A Result indicating success or failure
    pub fn flush(&self) -> Result<()> {
        // Check if we're closed
        if self.closed.load(Ordering::Acquire) {
            return Err(DiskyError::WritingClosedFile);
        }

        // Queue a flush operation and wait for it to complete
        let promise = self.writer.flush_async()?;
        promise.wait()?
    }

    /// Closes the writer asynchronously
    ///
    /// This method signals all worker threads to stop and initiates the closing
    /// of the underlying resources. It returns a Promise that will be fulfilled
    /// when the close operation completes.
    ///
    /// # Returns
    /// A Result containing a Promise that will be fulfilled when the close completes
    pub fn close_async(&self) -> Result<Arc<Promise<Result<()>>>> {
        // Set the closed flag
        self.closed.store(true, Ordering::Release);

        // Signal all workers to stop
        for worker in self.workers.iter() {
            worker.running.store(false, Ordering::Release);
        }

        // Close the underlying writer
        self.writer.close_async()
    }

    /// Closes the writer and waits for completion
    ///
    /// This method shuts down all worker threads and closes the underlying resources.
    /// It blocks until the close operation is complete.
    ///
    /// # Returns
    /// A Result indicating success or failure
    pub fn close(&self) -> Result<()> {
        // Set the closed flag
        self.closed.store(true, Ordering::Release);

        // Signal all workers to stop
        for worker in self.workers.iter() {
            worker.running.store(false, Ordering::Release);
        }

        // Close the underlying writer
        let promise = self.writer.close_async()?;

        // Wait for the close to complete
        promise.wait()?
    }


    /// Returns the number of pending tasks
    pub fn pending_tasks(&self) -> Result<usize> {
        self.writer.pending_task_count()
    }

    /// Returns the number of available writer resources
    pub fn available_writers(&self) -> Result<usize> {
        self.writer.available_resource_count()
    }
}

impl<Sink: Write + Seek + Send + 'static> Drop for MultiThreadedWriter<Sink> {
    fn drop(&mut self) {
        // First, close the writer to signal shutdown
        let _ = self.close();
        
        // Now, take ownership of the workers and join them
        // SAFETY: This is safe because we're in Drop, so the struct is being destroyed
        // and we need to join the threads to prevent resource leaks
        let workers = unsafe { ManuallyDrop::take(&mut self.workers) };
        
        // Join all worker threads
        for (i, worker) in workers.into_iter().enumerate() {
            match worker.handle.join() {
                Ok(result) => {
                    if let Err(e) = result {
                        error!("Worker {} returned error in drop: {}", i, e);
                    }
                }
                Err(e) => {
                    error!("Failed to join worker {} in drop: {:?}", i, e);
                }
            }
        }
    }
}

