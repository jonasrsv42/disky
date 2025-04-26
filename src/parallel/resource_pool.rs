//! Thread-safe resource queue with state management
//!
//! This module provides a thread-safe resource queue implementation with support
//! for tracking active resources, pausing/resuming operations, and safely
//! processing resources using the visitor pattern.
//!
//! # Visitor Pattern
//!
//! The `ResourcePool` uses the visitor pattern via its `process_resource` method
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
use std::sync::{Condvar, Mutex, MutexGuard};

/// Represents the operational state of the resource queue
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ResourcePoolState {
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
/// # use disky::parallel::resource_pool::ResourcePool;
/// # use disky::error::Result;
/// #
/// let queue = ResourcePool::new();
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
pub struct ResourcePool<T> {
    /// Internal state protected by mutex
    inner: Mutex<ResourcePoolInner<T>>,
    /// Condition variable for signaling state changes
    signal: Condvar,
}

/// Inner state of the resource queue, protected by a mutex
#[derive(Debug)]
struct ResourcePoolInner<T> {
    /// Queue of available resources
    queue: VecDeque<T>,
    /// Count of resources that have been checked out
    active_count: usize,
    /// Operational state of the queue
    state: ResourcePoolState,
}

impl<T> ResourcePool<T> {
    /// Create a new empty resource queue
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(ResourcePoolInner {
                queue: VecDeque::new(),
                active_count: 0,
                state: ResourcePoolState::Normal,
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
            ResourcePoolState::Normal => {
                // Add the resource to the queue
                inner.queue.push_back(resource);
                self.signal.notify_one();
                Ok(())
            }
            ResourcePoolState::Closed => Err(DiskyError::QueueClosed(
                "Cannot add resources to a closed queue".to_string(),
            )),
            ResourcePoolState::Paused => Err(DiskyError::Other(
                "Cannot add resources while queue is paused".to_string(),
            )),
        }
    }

    /// Process a single resource from the queue using the visitor pattern
    ///
    /// This is the core method of the `ResourcePool` implementing the visitor pattern.
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
    /// # use disky::parallel::resource_pool::ResourcePool;
    /// # use disky::error::Result;
    /// #
    /// let queue = ResourcePool::new();
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
                    ResourcePoolState::Closed => {
                        return Err(DiskyError::QueueClosed(
                            "Resource queue is closed".to_string(),
                        ));
                    }
                    ResourcePoolState::Paused => {
                        // Wait for state change
                        inner = self
                            .signal
                            .wait(inner)
                            .map_err(|e| DiskyError::Other(e.to_string()))?;
                        continue;
                    }
                    ResourcePoolState::Normal => {
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
            if inner.state != ResourcePoolState::Closed {
                inner.queue.push_back(resource);
            }

            // Decrement active count
            inner.active_count = inner.active_count.saturating_sub(1);

            // Notify waiters if needed
            if inner.state == ResourcePoolState::Paused && inner.active_count == 0 {
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

    fn wait_for_all<'a>(
        &'a self,
        mut inner: MutexGuard<'a, ResourcePoolInner<T>>,
    ) -> Result<MutexGuard<'a, ResourcePoolInner<T>>> {
        // Handle different initial states
        match inner.state {
            ResourcePoolState::Closed => {
                return Err(DiskyError::QueueClosed(
                    "Cannot pause a closed queue".to_string(),
                ));
            }
            ResourcePoolState::Paused => {
                // Already paused, wait until state changes to Normal or Closed
                loop {
                    inner = self
                        .signal
                        .wait(inner)
                        .map_err(|e| DiskyError::Other(e.to_string()))?;

                    match inner.state {
                        ResourcePoolState::Normal => {
                            // State changed to Normal, now we can set it to Paused
                            inner.state = ResourcePoolState::Paused;
                            break;
                        }
                        ResourcePoolState::Closed => {
                            return Err(DiskyError::QueueClosed(
                                "Cannot pause a closed queue".to_string(),
                            ));
                        }
                        ResourcePoolState::Paused => {
                            // Still paused, continue waiting
                            continue;
                        }
                    }
                }
            }
            ResourcePoolState::Normal => {
                // Set state to Paused
                inner.state = ResourcePoolState::Paused;
            }
        }

        // Wait until all active resources have been returned
        while inner.active_count > 0 {
            inner = self
                .signal
                .wait(inner)
                .map_err(|e| DiskyError::Other(e.to_string()))?;
        }

        Ok(inner)
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
            ResourcePoolState::Closed => Err(DiskyError::QueueClosed(
                "Queue is already closed".to_string(),
            )),
            _ => {
                // Both Normal and Paused states can transition to Closed
                inner.state = ResourcePoolState::Closed;
                self.signal.notify_all();
                Ok(())
            }
        }
    }

    pub fn process_then_close<F>(&self, f: F) -> Result<()>
    where
        F: FnMut(&mut T) -> Result<()>,
    {
        // First check if the queue is already closed
        {
            let inner = self
                .inner
                .lock()
                .map_err(|e| DiskyError::Other(e.to_string()))?;

            match inner.state {
                ResourcePoolState::Closed => {
                    return Err(DiskyError::QueueClosed(
                        "Cannot process resources; queue is already closed".to_string(),
                    ));
                }
                ResourcePoolState::Normal | ResourcePoolState::Paused => {
                    // These states are allowed to proceed
                    // We drop the lock here and call with_pause_process_resources below
                }
            }
        }

        // If not closed, proceed with the process-then-close operation
        self.internal_process_all_resources(f, ResourcePoolState::Closed)
    }

    fn internal_process_all_resources<F>(
        &self,
        mut f: F,
        final_state: ResourcePoolState,
    ) -> Result<()>
    where
        F: FnMut(&mut T) -> Result<()>,
    {
        // Get the lock for the entire operation
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))?;

        // Step 1: Wait for all active resources to return
        inner = self.wait_for_all(inner)?;

        // Step 2: Process each resource in the queue
        let mut process_result = Ok(());

        if inner.state != ResourcePoolState::Closed {
            let mut last_error = None;

            // Process each resource in the queue
            for resource in &mut inner.queue {
                if let Err(e) = f(resource) {
                    last_error = Some(e);
                }
            }

            // Store the processing result
            if let Some(e) = last_error {
                process_result = Err(e);
            }
        } else {
            process_result = Err(DiskyError::QueueClosed(
                "Resource queue is closed".to_string(),
            ));
        }

        // Step 3: Set final state (unless there was an error and we're trying to close)
        if process_result.is_ok() || final_state != ResourcePoolState::Closed {
            inner.state = final_state;

            // Notify waiters about the state change
            self.signal.notify_all();
        }

        // Return any error from processing
        process_result
    }

    pub fn process_all_resources<F>(&self, f: F) -> Result<()>
    where
        F: FnMut(&mut T) -> Result<()>,
    {
        self.internal_process_all_resources(f, ResourcePoolState::Normal)
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
    pub(crate) fn get_state(&self) -> Result<ResourcePoolState> {
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
        if inner.state != ResourcePoolState::Normal {
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
    fn test_basic_resource_pool_operations() {
        let queue = ResourcePool::new();

        // Test initial state
        assert_eq!(queue.get_state().unwrap(), ResourcePoolState::Normal);
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
        let queue = ResourcePool::new();

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
    fn test_with_pause_process_all_resources() {
        let queue = Arc::new(ResourcePool::new());

        // Add resources
        queue.add_resource(1).unwrap();
        queue.add_resource(2).unwrap();
        queue.add_resource(3).unwrap();

        // Spawn a thread to try to grab a resource while we process all
        let queue_clone = Arc::clone(&queue);
        let (resource_attempt_tx, resource_attempt_rx) = std::sync::mpsc::channel();
        let handle = thread::spawn(move || {
            // Signal we're about to try processing
            resource_attempt_tx.send(()).unwrap();

            // This should block until the with_pause_process_all_resources completes
            let result = queue_clone.process_resource(|n| {
                *n *= 10; // Different multiplier to verify which processing happened
                Ok(())
            });

            assert!(result.is_ok(), "Process after pause/resume should succeed");
        });

        // Wait for the thread to be ready to process
        resource_attempt_rx.recv().unwrap();

        // Small sleep to ensure the thread gets to the processing point
        thread::sleep(std::time::Duration::from_millis(10));

        // Process all resources with pause/resume handling
        queue
            .process_all_resources(|n| {
                *n *= 2; // Double the values
                Ok(())
            })
            .unwrap();

        // Wait for the other thread to complete after our processing
        handle.join().unwrap();

        // Verify the state is Normal after processing
        assert_eq!(queue.get_state().unwrap(), ResourcePoolState::Normal);

        // Verify resources - some might be multiplied by 2, some by 10 (if processed after)
        let resources = queue.drain_all_resources().unwrap();
        assert_eq!(resources.len(), 3);

        // All resources should have been processed at least once (either *2 or *10 or both)
        for r in &resources {
            assert!(*r == 2 || *r == 4 || *r == 6 || *r == 10 || *r == 20 || *r == 30);
        }
    }

    #[test]
    fn test_with_pause_process_all_resources_error_handling() {
        let queue = ResourcePool::new();

        // Add resources
        queue.add_resource(1).unwrap();
        queue.add_resource(2).unwrap();
        queue.add_resource(3).unwrap();

        // Test error during processing
        let result = queue.process_all_resources(|n| {
            if *n == 2 {
                return Err(DiskyError::Other("Test error".to_string()));
            }
            *n *= 2;
            Ok(())
        });

        // Verify error was returned
        assert!(result.is_err());

        // Verify state is back to Normal even after error
        assert_eq!(queue.get_state().unwrap(), ResourcePoolState::Normal);

        // Some resources should still be processed
        let resources = queue.drain_all_resources().unwrap();
        assert_eq!(resources.len(), 3);
    }

    #[test]
    fn test_multi_threaded_processing() {
        let queue = Arc::new(ResourcePool::new());
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
}
