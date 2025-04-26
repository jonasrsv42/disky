//! Thread-safe resource queue with state management
//! 
//! This module provides a thread-safe resource queue implementation with support
//! for tracking active resources, pausing/resuming operations, and safely
//! processing resources using the visitor pattern.
//! 
//! # Visitor Pattern
//! 
//! The `ResourceQueue` uses the visitor pattern via its `process_resource` method
//! to ensure proper resource management. This approach has several advantages:
//! 
//! - **Resource Safety**: Resources are automatically returned to the queue after
//!   processing, preventing leaks even if errors occur
//! 
//! - **Deadlock Prevention**: The resource lifecycle is managed within an atomic
//!   operation, significantly reducing the risk of deadlocks
//! 
//! - **State Management**: The queue can safely handle state transitions with
//!   proper resource tracking
//! 
//! - **Simplified API**: Clients don't need to manually check out and return
//!   resources, reducing the risk of programmer error

use crate::error::{DiskyError, Result};
use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};

/// Represents the operational state of the resource queue
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ResourceQueueState {
    /// Normal operation - resources can be checked out and returned
    Normal,
    /// Queue is paused - resources can be returned but not checked out
    Paused,
    /// Queue is closed - no more operations are allowed
    Closed,
}

/// A queue of resources with tracking for resources that are currently in use
///
/// This specialized queue is designed for managing a pool of resources in a thread-safe
/// manner using the visitor pattern. Instead of exposing direct checkout/return methods
/// that could lead to resource leaks, it provides a `process_resource` method that
/// handles the complete resource lifecycle:
///
/// ```rust,no_run
/// # use disky::parallel::resource_queue::ResourceQueue;
/// # use disky::error::Result;
/// # 
/// let queue = ResourceQueue::new();
/// queue.add_resource("resource1".to_string()).unwrap();
///
/// // Process a resource using the visitor pattern
/// let result = queue.process_resource(|resource| {
///     // Work with the resource here
///     println!("Processing {}", resource);
///     // The resource is automatically returned to the queue when this closure completes
///     Ok(42) // Return any type you want
/// }).unwrap();
/// ```
///
/// # Benefits of the Visitor Pattern
///
/// - **Automatic Resource Management**: Resources are automatically returned to the queue
///   after processing, even if errors occur during processing
///
/// - **Deadlock Avoidance**: Resources cannot be "forgotten" outside the queue, which
///   reduces the risk of deadlocks in concurrent code
///
/// - **State Safety**: State transitions (Normal → Paused → Closed) are properly coordinated
///   with active resource tracking
///
/// - **Concurrency Support**: Multiple threads can safely process resources from the same queue
///
/// Key features:
/// - Thread-safe processing of resources using the visitor pattern
/// - Atomic tracking of active resources
/// - Pause/resume operations with clean state transitions
/// - Support for different operational states (Normal, Paused, Closed)
#[derive(Debug)]
pub struct ResourceQueue<T> {
    /// Internal state protected by mutex
    inner: Mutex<ResourceQueueInner<T>>,
    /// Condition variable for signaling state changes
    signal: Condvar,
}

/// Inner state of the resource queue, protected by a mutex
#[derive(Debug)]
struct ResourceQueueInner<T> {
    /// Queue of available resources
    queue: VecDeque<T>,
    /// Count of resources that have been checked out
    active_count: usize,
    /// Operational state of the queue
    state: ResourceQueueState,
}

impl<T> ResourceQueue<T> {
    /// Create a new empty resource queue
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(ResourceQueueInner {
                queue: VecDeque::new(),
                active_count: 0,
                state: ResourceQueueState::Normal,
            }),
            signal: Condvar::new(),
        }
    }

    /// Add a resource to the queue
    ///
    /// Resources can only be added in Normal state.
    /// Attempting to add resources in Paused or Closed states will return an error.
    pub fn add_resource(&self, resource: T) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))?;

        // Handle different queue states
        match inner.state {
            ResourceQueueState::Normal => {
                // Add the resource to the queue
                inner.queue.push_back(resource);
                self.signal.notify_one();
                Ok(())
            }
            ResourceQueueState::Closed => Err(DiskyError::QueueClosed(
                "Cannot add resources to a closed queue".to_string(),
            )),
            ResourceQueueState::Paused => Err(DiskyError::Other(
                "Cannot add resources while queue is paused".to_string(),
            )),
        }
    }

    /// Process a single resource from the queue using the visitor pattern
    ///
    /// This is the core method of the `ResourceQueue` implementing the visitor pattern.
    /// Rather than exposing separate checkout/return methods, this single method handles
    /// the complete resource lifecycle:
    ///
    /// 1. Waits for an available resource in Normal state
    /// 2. Marks the resource as active
    /// 3. Calls the provided function with the resource 
    /// 4. Ensures the resource is returned to the queue, even if processing fails
    /// 5. Returns the result of the processing function
    ///
    /// # How the Visitor Pattern Prevents Deadlocks
    ///
    /// The visitor pattern is especially valuable when implementing functionality like the 
    /// `pause_and_wait_for_all` method. Without the visitor pattern, clients could forget
    /// to return resources to the queue, leading to:
    ///
    /// - Resource leaks (resources never returned to the queue)
    /// - Deadlocks (when `pause_and_wait_for_all` waits indefinitely for resources)
    /// - Incorrect resource counts (when tracking active resources)
    ///
    /// By encapsulating the entire checkout/process/return cycle in a single method call,
    /// the visitor pattern eliminates these risks.
    ///
    /// # Arguments
    /// * `f` - Function to apply to the resource; receives a mutable reference
    ///
    /// # Returns
    /// * The result of applying the function to the resource
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use disky::parallel::resource_queue::ResourceQueue;
    /// # use disky::error::Result;
    /// #
    /// let queue = ResourceQueue::new();
    /// queue.add_resource(1).unwrap();
    ///
    /// // Process a resource and get a transformed result
    /// let doubled = queue.process_resource(|num| {
    ///     Ok(*num * 2)
    /// }).unwrap();
    ///
    /// assert_eq!(doubled, 2);
    /// ```
    pub fn process_resource<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut T) -> Result<R>,
    {
        // First scope: get a resource and mark it as active
        let mut resource = {
            let mut inner = self
                .inner
                .lock()
                .map_err(|e| DiskyError::Other(e.to_string()))?;

            // Wait until we have a resource and are in Normal state
            loop {
                // Check queue state first
                match inner.state {
                    ResourceQueueState::Closed => {
                        return Err(DiskyError::QueueClosed(
                            "Resource queue is closed".to_string(),
                        ));
                    }
                    ResourceQueueState::Paused => {
                        // Wait for state change
                        inner = self
                            .signal
                            .wait(inner)
                            .map_err(|e| DiskyError::Other(e.to_string()))?;
                        continue;
                    }
                    ResourceQueueState::Normal => {
                        // Check if resources are available
                        if !inner.queue.is_empty() {
                            break;
                        }
                        // Wait for resources
                        inner = self
                            .signal
                            .wait(inner)
                            .map_err(|e| DiskyError::Other(e.to_string()))?;
                    }
                }
            }

            // Get a resource from the queue (we should have one now)
            let resource = inner.queue.pop_front().ok_or_else(|| {
                DiskyError::Other("Empty queue after checking. Race condition?".to_string())
            })?;

            // Increment active count
            inner.active_count += 1;

            // Return the resource outside the lock scope
            resource
        };

        // Process the resource outside the lock to avoid deadlocks
        let processing_result = f(&mut resource);

        // Second scope: return the resource and update state
        {
            let mut inner = self
                .inner
                .lock()
                .map_err(|e| DiskyError::Other(e.to_string()))?;

            // Return resource to queue if not closed
            if inner.state != ResourceQueueState::Closed {
                inner.queue.push_back(resource);
            }

            // Decrement active count
            inner.active_count = inner.active_count.saturating_sub(1);

            // Notify waiters if needed
            if inner.state == ResourceQueueState::Paused && inner.active_count == 0 {
                // All resources returned during Paused state - notify waiters
                self.signal.notify_all();
            } else {
                // Normal notification for new resource availability
                self.signal.notify_one();
            }
        }

        // Return the result of processing
        processing_result
    }

    /// Set the queue to paused state and wait for all active resources to return
    ///
    /// This operation will:
    /// 1. If already in Paused state, wait until state changes to Normal or Closed
    /// 2. If state becomes Normal, set to Paused; if Closed, return error
    /// 3. Wait until active_count reaches 0
    /// 4. Return when all resources are back in the queue
    pub fn pause_and_wait_for_all(&self) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))?;

        // Handle different initial states
        match inner.state {
            ResourceQueueState::Closed => {
                return Err(DiskyError::QueueClosed(
                    "Cannot pause a closed queue".to_string(),
                ));
            }
            ResourceQueueState::Paused => {
                // Already paused, wait until state changes to Normal or Closed
                loop {
                    inner = self
                        .signal
                        .wait(inner)
                        .map_err(|e| DiskyError::Other(e.to_string()))?;

                    match inner.state {
                        ResourceQueueState::Normal => {
                            // State changed to Normal, now we can set it to Paused
                            inner.state = ResourceQueueState::Paused;
                            break;
                        }
                        ResourceQueueState::Closed => {
                            return Err(DiskyError::QueueClosed(
                                "Cannot pause a closed queue".to_string(),
                            ));
                        }
                        ResourceQueueState::Paused => {
                            // Still paused, continue waiting
                            continue;
                        }
                    }
                }
            }
            ResourceQueueState::Normal => {
                // Set state to Paused
                inner.state = ResourceQueueState::Paused;
            }
        }

        // Wait until all active resources have been returned
        while inner.active_count > 0 {
            inner = self
                .signal
                .wait(inner)
                .map_err(|e| DiskyError::Other(e.to_string()))?;
        }

        Ok(())
    }

    /// Resume normal operation after a pause
    ///
    /// This function will:
    /// 1. Check if the queue is in Paused state, and return error if not
    /// 2. Set the state to Normal
    /// 3. Notify all waiting threads
    pub fn resume(&self) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))?;

        match inner.state {
            ResourceQueueState::Paused => {
                inner.state = ResourceQueueState::Normal;
                self.signal.notify_all();
                Ok(())
            }
            ResourceQueueState::Closed => Err(DiskyError::QueueClosed(
                "Cannot resume a closed queue".to_string(),
            )),
            ResourceQueueState::Normal => Err(DiskyError::Other(
                "Queue is already in Normal state".to_string(),
            )),
        }
    }

    /// Set the queue to closed state
    ///
    /// This will prevent further checkouts and returns.
    /// Cannot close an already closed queue.
    pub fn close(&self) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))?;

        match inner.state {
            ResourceQueueState::Closed => Err(DiskyError::QueueClosed(
                "Queue is already closed".to_string(),
            )),
            _ => {
                // Both Normal and Paused states can transition to Closed
                inner.state = ResourceQueueState::Closed;
                self.signal.notify_all();
                Ok(())
            }
        }
    }

    /// Process all resources in the queue using the provided function
    ///
    /// This safely operates on all resources while maintaining proper state tracking.
    /// The resources never leave the queue, preventing race conditions or leaks.
    ///
    /// This function can operate on queues in Normal or Paused state,
    /// but will return an error if the queue is Closed.
    ///
    /// # Arguments
    /// * `f` - Function to apply to each resource
    ///
    /// # Returns
    /// * `Ok(())` if all operations succeeded
    /// * `Err(...)` containing the last error encountered if any operations failed
    pub fn process_all_resources<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&mut T) -> Result<()>,
    {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))?;

        // Handle different queue states
        match inner.state {
            ResourceQueueState::Closed => {
                return Err(DiskyError::QueueClosed(
                    "Resource queue is closed".to_string(),
                ));
            }
            ResourceQueueState::Normal | ResourceQueueState::Paused => {
                // Processing is allowed in both Normal and Paused states
                let mut last_error = None;

                // Process each resource in the queue
                for resource in &mut inner.queue {
                    if let Err(e) = f(resource) {
                        last_error = Some(e);
                    }
                }

                // Return the last error if any
                if let Some(e) = last_error {
                    return Err(e);
                }

                Ok(())
            }
        }
    }

    /// Get the current number of resources in the queue
    pub fn available_count(&self) -> Result<usize> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))?;

        Ok(inner.queue.len())
    }

    /// Check if the queue is empty (no resources available)
    #[cfg(test)]
    pub fn is_empty(&self) -> Result<bool> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))?;

        Ok(inner.queue.is_empty())
    }

    /// Get the current state of the queue
    #[cfg(test)]
    pub(crate) fn get_state(&self) -> Result<ResourceQueueState> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))?;

        Ok(inner.state)
    }

    /// Get the current number of active resources (checked out)
    #[cfg(test)]
    pub(crate) fn active_count(&self) -> Result<usize> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))?;

        Ok(inner.active_count)
    }

    /// Get all resources from the queue, emptying it in the process
    ///
    /// This method is intended for testing purposes only.
    /// It will only work if the queue is in Normal state.
    #[cfg(test)]
    pub fn drain_all_resources(&self) -> Result<Vec<T>> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))?;

        // Only allow draining in Normal state
        if inner.state != ResourceQueueState::Normal {
            return Err(DiskyError::Other(format!(
                "Cannot drain resources when queue is in {:?} state",
                inner.state
            )));
        }

        // Take all resources
        let mut resources = Vec::new();
        while let Some(resource) = inner.queue.pop_front() {
            resources.push(resource);
        }

        Ok(resources)
    }

    /// Get the total number of resources (in queue + active)
    #[cfg(test)]
    pub fn total_count(&self) -> Result<usize> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))?;

        Ok(inner.queue.len() + inner.active_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_basic_resource_queue_operations() {
        let queue = ResourceQueue::new();

        // Test initial state
        assert_eq!(queue.get_state().unwrap(), ResourceQueueState::Normal);
        assert_eq!(queue.available_count().unwrap(), 0);
        assert_eq!(queue.active_count().unwrap(), 0);
        assert!(queue.is_empty().unwrap());

        // Add resources
        queue.add_resource(1).unwrap();
        queue.add_resource(2).unwrap();
        queue.add_resource(3).unwrap();

        assert_eq!(queue.available_count().unwrap(), 3);
        assert_eq!(queue.total_count().unwrap(), 3);
        assert!(!queue.is_empty().unwrap());

        // Process a resource
        let result = queue
            .process_resource(|n| {
                assert!(*n >= 1 && *n <= 3);
                Ok(*n * 2)
            })
            .unwrap();

        assert!(result >= 2 && result <= 6);
        assert_eq!(queue.available_count().unwrap(), 3); // Resource returned to queue
        assert_eq!(queue.active_count().unwrap(), 0);

        // Drain all resources
        let resources = queue.drain_all_resources().unwrap();
        assert_eq!(resources.len(), 3);
        assert_eq!(queue.available_count().unwrap(), 0);
        assert!(queue.is_empty().unwrap());
    }

    #[test]
    fn test_process_all_resources() {
        let queue = ResourceQueue::new();

        // Add resources
        queue.add_resource(1).unwrap();
        queue.add_resource(2).unwrap();
        queue.add_resource(3).unwrap();

        // Process all resources
        queue
            .process_all_resources(|n| {
                *n *= 2;
                Ok(())
            })
            .unwrap();

        // Verify all resources were processed
        let resources = queue.drain_all_resources().unwrap();
        assert_eq!(resources, vec![2, 4, 6]);
    }

    #[test]
    fn test_pause_and_resume() {
        let queue = ResourceQueue::new();

        // Add resources
        queue.add_resource(1).unwrap();
        queue.add_resource(2).unwrap();

        // Pause the queue
        queue.pause_and_wait_for_all().unwrap();
        assert_eq!(queue.get_state().unwrap(), ResourceQueueState::Paused);

        // Should fail to add resources while paused
        assert!(queue.add_resource(3).is_err());

        // Process all resources should still work in paused state
        queue
            .process_all_resources(|n| {
                *n *= 2;
                Ok(())
            })
            .unwrap();

        // Resume
        queue.resume().unwrap();
        assert_eq!(queue.get_state().unwrap(), ResourceQueueState::Normal);

        // Should be able to add resources again
        queue.add_resource(3).unwrap();

        // Verify all resources
        let resources = queue.drain_all_resources().unwrap();
        assert_eq!(resources.len(), 3);
        assert!(resources.contains(&2)); // 1*2
        assert!(resources.contains(&4)); // 2*2
        assert!(resources.contains(&3)); // Added after resume
    }

    #[test]
    fn test_close() {
        let queue = ResourceQueue::new();

        // Add a resource
        queue.add_resource(1).unwrap();

        // Close the queue
        queue.close().unwrap();
        assert_eq!(queue.get_state().unwrap(), ResourceQueueState::Closed);

        // Operations should fail on closed queue
        assert!(queue.add_resource(2).is_err());
        assert!(queue.process_resource(|_| Ok(())).is_err());
        assert!(queue.process_all_resources(|_| Ok(())).is_err());
        assert!(queue.pause_and_wait_for_all().is_err());
        assert!(queue.resume().is_err());
        assert!(queue.close().is_err()); // Cannot close an already closed queue
    }

    #[test]
    fn test_multi_threaded_processing() {
        let queue = Arc::new(ResourceQueue::new());
        let worker_count = 3;
        let tasks_per_worker = 3;

        // Add resources
        for i in 1..=10 {
            queue.add_resource(i).unwrap();
        }

        // Create a channel for workers to signal completion of each task
        let (task_done_tx, task_done_rx) = std::sync::mpsc::channel();

        // Create threads to process resources
        let mut handles = vec![];
        for _ in 0..worker_count {
            let queue_clone = Arc::clone(&queue);
            let task_done_tx = task_done_tx.clone();

            let handle = thread::spawn(move || {
                for _ in 0..tasks_per_worker {
                    let result = queue_clone.process_resource(|n| {
                        // Process the resource
                        Ok(*n)
                    });
                    assert!(result.is_ok());

                    // Signal task completion
                    task_done_tx.send(()).unwrap();
                }
            });
            handles.push(handle);
        }

        // Drop the original sender to avoid deadlock
        drop(task_done_tx);

        // Wait for all tasks to complete
        for _ in 0..(worker_count * tasks_per_worker) {
            task_done_rx.recv().unwrap();
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Resources should all be back in the queue
        assert_eq!(queue.available_count().unwrap(), 10);
        assert_eq!(queue.active_count().unwrap(), 0);
    }

    #[test]
    fn test_concurrent_pause_and_process() {
        let queue = Arc::new(ResourceQueue::new());

        // Add resources
        for i in 1..=5 {
            queue.add_resource(i).unwrap();
        }

        // Create channels for synchronization
        let (processing_started_tx, processing_started_rx) = std::sync::mpsc::channel();
        let (pause_complete_tx, pause_complete_rx) = std::sync::mpsc::channel();

        // Spawn a thread to process resources
        let queue_clone = Arc::clone(&queue);
        let process_handle = thread::spawn(move || {
            // First process should start and signal
            match queue_clone.process_resource(|n| {
                // Signal that processing has started
                processing_started_tx.send(()).unwrap();

                Ok(*n)
            }) {
                Ok(_) => {} // Successfully processed
                Err(e) => {
                    panic!("First process_resource should succeed: {:?}", e);
                }
            }

            // Wait until pause is complete before finishing processing
            pause_complete_rx.recv().unwrap();

            // Try more processing after pause is complete
            for _ in 0..2 {
                if let Err(e) = queue_clone.process_resource(|n| Ok(*n)) {
                    // Error is acceptable if queue was closed
                    if let DiskyError::QueueClosed(_) = e {
                        return;
                    }
                    panic!("Unexpected error: {:?}", e);
                }
            }
        });

        // Wait for processing to start
        processing_started_rx.recv().unwrap();

        // Pause the queue - this should wait for active resources
        let pause_result = queue.pause_and_wait_for_all();

        // Now that we're paused, signal the processing thread to complete
        pause_complete_tx.send(()).unwrap();

        // Verify pause succeeded
        assert!(pause_result.is_ok());

        // At this point, all resources should be back in the queue and no active resources
        assert_eq!(queue.active_count().unwrap(), 0);
        assert_eq!(queue.get_state().unwrap(), ResourceQueueState::Paused);

        // Resume the queue to let the processing thread finish
        queue.resume().unwrap();

        // Wait for processing thread to complete
        process_handle.join().unwrap();
    }

    #[test]
    fn test_pause_when_already_paused() {
        let queue = Arc::new(ResourceQueue::new());

        // Add a resource
        queue.add_resource(1).unwrap();

        // First pause
        queue.pause_and_wait_for_all().unwrap();
        assert_eq!(queue.get_state().unwrap(), ResourceQueueState::Paused);

        // Create a clone for the pause thread
        let pause_queue = Arc::clone(&queue);

        // Use channels for synchronization
        let (pause_started_tx, pause_started_rx) = std::sync::mpsc::channel();
        let (close_done_tx, close_done_rx) = std::sync::mpsc::channel();

        // Create a thread that will attempt to pause again (will wait for unpaused state)
        let pause_thread = thread::spawn(move || {
            // Signal we're about to call pause_and_wait_for_all
            pause_started_tx.send(()).unwrap();

            // This call will block in the paused state waiting loop until the queue is closed
            let result = pause_queue.pause_and_wait_for_all();

            // When the main thread closes the queue, this call should return with an error
            assert!(result.is_err());
            if let Err(DiskyError::QueueClosed(_)) = result {
                // Expected error
            } else {
                panic!("Expected QueueClosed error, got {:?}", result);
            }

            // Signal that we've verified the error
            close_done_rx.recv().unwrap();
        });

        // Wait for pause thread to start its pause operation
        pause_started_rx.recv().unwrap();

        // Close the queue, which should cause the paused wait to fail with an error
        queue.close().unwrap();

        // Signal that close is done
        close_done_tx.send(()).unwrap();

        // Wait for the pause thread to complete
        pause_thread.join().unwrap();
    }
}
