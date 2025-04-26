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
/// This specialized queue is designed for managing a pool of resources that can be
/// checked out and returned. It tracks both the resources in the queue and the
/// resources that have been checked out but not yet returned.
///
/// Key features:
/// - Atomic checkout/return of resources
/// - Tracking of both queued and active resources
/// - Ability to pause and wait for all resources to be returned
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
    pub fn add_resource(&self, resource: T) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))?;

        // Cannot add resources if the queue is closed
        if inner.state == ResourceQueueState::Closed {
            return Err(DiskyError::QueueClosed("Resource queue is closed".to_string()));
        }

        // Add the resource to the queue
        inner.queue.push_back(resource);
        self.signal.notify_one();

        Ok(())
    }

    /// Check out a resource from the queue
    ///
    /// This operation will:
    /// 1. Wait until a resource is available and the queue is in Normal state
    /// 2. Remove the resource from the queue
    /// 3. Increment the active count
    /// 4. Return the resource
    pub fn checkout_resource(&self) -> Result<T> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))?;

        // Wait until resources are available and queue is in normal state
        while inner.queue.is_empty() || inner.state != ResourceQueueState::Normal {
            // If closed and empty, return error
            if inner.state == ResourceQueueState::Closed {
                return Err(DiskyError::QueueClosed("Resource queue is closed".to_string()));
            }

            // If paused or empty, wait for state change
            inner = self.signal.wait(inner).unwrap();
        }

        // Get resource from queue
        match inner.queue.pop_front() {
            Some(resource) => {
                // Increment active count
                inner.active_count += 1;
                Ok(resource)
            }
            None => Err(DiskyError::Other(
                "Empty resource queue. Race condition?".to_string(),
            )),
        }
    }

    /// Return a resource to the queue
    ///
    /// This operation will:
    /// 1. Add the resource back to the queue
    /// 2. Decrement the active count
    /// 3. Notify waiting threads if we're in paused state and active count is now 0
    pub fn return_resource(&self, resource: T) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))?;

        // If closed, reject the return
        if inner.state == ResourceQueueState::Closed {
            return Err(DiskyError::QueueClosed("Resource queue is closed".to_string()));
        }

        // Add resource back to queue
        inner.queue.push_back(resource);

        // Decrement active count
        if inner.active_count > 0 {
            inner.active_count -= 1;
        }

        // If we're in paused state and all resources have been returned, notify all waiters
        if inner.state == ResourceQueueState::Paused && inner.active_count == 0 {
            self.signal.notify_all();
        } else {
            // Normal notification for new resources
            self.signal.notify_one();
        }

        Ok(())
    }

    /// Set the queue to paused state and wait for all active resources to return
    ///
    /// This operation will:
    /// 1. Set the queue state to Paused
    /// 2. Wait until active_count reaches 0
    /// 3. Return when all resources are back in the queue
    pub fn pause_and_wait_for_all(&self) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))?;

        // Set state to Paused
        inner.state = ResourceQueueState::Paused;

        // Wait until all active resources have been returned
        while inner.active_count > 0 {
            inner = self.signal.wait(inner).unwrap();
        }

        Ok(())
    }

    /// Resume normal operation after a pause
    pub fn resume(&self) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))?;

        inner.state = ResourceQueueState::Normal;
        self.signal.notify_all();

        Ok(())
    }

    /// Set the queue to closed state
    ///
    /// This will prevent further checkouts and returns
    pub fn close(&self) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))?;

        inner.state = ResourceQueueState::Closed;
        self.signal.notify_all();

        Ok(())
    }

    /// Process all resources in the queue using the provided function
    ///
    /// This safely operates on all resources while maintaining proper state tracking.
    /// The resources never leave the queue, preventing race conditions or leaks.
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

        if inner.state == ResourceQueueState::Closed {
            return Err(DiskyError::QueueClosed("Resource queue is closed".to_string()));
        }

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
        let mut inner = self.inner.lock()
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
    use std::time::Duration;
    
    #[test]
    fn test_drain_all_resources() {
        let queue = ResourceQueue::new();
        
        // Add some resources
        queue.add_resource(1).unwrap();
        queue.add_resource(2).unwrap();
        queue.add_resource(3).unwrap();
        
        // Drain all resources
        let resources = queue.drain_all_resources().unwrap();
        
        // Check that we got all resources
        assert_eq!(resources.len(), 3);
        assert!(resources.contains(&1));
        assert!(resources.contains(&2));
        assert!(resources.contains(&3));
        
        // Queue should be empty
        assert_eq!(queue.available_count().unwrap(), 0);
        
        // Try to drain in non-Normal state
        queue.pause_and_wait_for_all().unwrap();
        assert!(queue.drain_all_resources().is_err());
        
        // Resume and try again
        queue.resume().unwrap();
        let empty_resources = queue.drain_all_resources().unwrap();
        assert_eq!(empty_resources.len(), 0);
    }

    #[test]
    fn test_basic_checkout_return() {
        let queue = ResourceQueue::new();

        // Add resources
        queue.add_resource(1).unwrap();
        queue.add_resource(2).unwrap();

        // Check available resources
        assert_eq!(queue.available_count().unwrap(), 2);

        // Check out resources
        let r1 = queue.checkout_resource().unwrap();
        assert_eq!(r1, 1);
        assert_eq!(queue.active_count().unwrap(), 1);

        let r2 = queue.checkout_resource().unwrap();
        assert_eq!(r2, 2);
        assert_eq!(queue.active_count().unwrap(), 2);

        // Check empty queue
        assert_eq!(queue.available_count().unwrap(), 0);
        assert!(queue.is_empty().unwrap());

        // Get total resource count (active + available)
        assert_eq!(queue.total_count().unwrap(), 2);

        // Return resources
        queue.return_resource(r1).unwrap();
        assert_eq!(queue.active_count().unwrap(), 1);

        queue.return_resource(r2).unwrap();
        assert_eq!(queue.active_count().unwrap(), 0);
    }

    #[test]
    fn test_pause_and_wait() {
        let queue = Arc::new(ResourceQueue::new());

        // Add resources
        queue.add_resource(1).unwrap();
        queue.add_resource(2).unwrap();

        // Check out one resource
        let r1 = queue.checkout_resource().unwrap();
        assert_eq!(queue.active_count().unwrap(), 1);

        // Pause in a separate thread
        let queue_clone = Arc::clone(&queue);
        let handle = thread::spawn(move || {
            queue_clone.pause_and_wait_for_all().unwrap();
            assert_eq!(queue_clone.active_count().unwrap(), 0);
        });

        // Give the thread time to start waiting
        thread::sleep(Duration::from_millis(10));

        // Return the resource
        queue.return_resource(r1).unwrap();

        // The thread should complete
        handle.join().unwrap();

        // Check that we're still in paused state
        assert_eq!(queue.get_state().unwrap(), ResourceQueueState::Paused);

        // Resume
        queue.resume().unwrap();
        assert_eq!(queue.get_state().unwrap(), ResourceQueueState::Normal);
    }

    #[test]
    fn test_process_all_resources() {
        let queue = ResourceQueue::new();

        // Add some resources
        queue.add_resource(1).unwrap();
        queue.add_resource(2).unwrap();
        queue.add_resource(3).unwrap();

        // Track processed values
        let mut processed = Vec::new();

        // Process all resources by collecting their values
        queue
            .process_all_resources(|resource| {
                processed.push(*resource);
                *resource += 10; // Modify the resource
                Ok(())
            })
            .unwrap();

        // Resources should still be in the queue
        assert_eq!(queue.available_count().unwrap(), 3);

        // Check we processed all resources
        assert_eq!(processed, vec![1, 2, 3]);

        // Check out a resource to verify its value was modified
        let r1 = queue.checkout_resource().unwrap();
        assert_eq!(r1, 11); // 1 + 10

        // Return the resource
        queue.return_resource(r1).unwrap();
    }

    #[test]
    fn test_close_state() {
        let queue = ResourceQueue::new();

        // Add a resource
        queue.add_resource(42).unwrap();

        // Close the queue
        queue.close().unwrap();

        // Get state
        assert_eq!(queue.get_state().unwrap(), ResourceQueueState::Closed);

        // Try to add a resource after closing (should fail)
        assert!(queue.add_resource(99).is_err());

        // Cannot process resources on a closed queue
        assert!(queue.process_all_resources(|_| Ok(())).is_err());

        // Also cannot check out resources after closing
        assert!(queue.checkout_resource().is_err());

        // Closing the queue makes it unusable for resource management
    }
}

