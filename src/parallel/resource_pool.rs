
use crate::error::{DiskyError, Result};
use std::collections::VecDeque;
use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
/// Represents the operational state of a ResourcePool
///
/// The state determines what operations are allowed on the pool.
pub enum ResourcePoolState {
    /// Active operation - resources can be checked out and returned
    /// This is the normal operating state of the pool.
    Active,
    /// Suspended state - resources can be returned but not checked out
    /// This state is used during operations that need exclusive access to all resources.
    Suspended,
    /// Shutdown state - no more operations are allowed
    /// Once a pool is shutdown, it cannot be reopened.
    Shutdown,
}

#[derive(Debug)]
/// A thread-safe pool of reusable resources
///
/// The ResourcePool provides a way to manage a collection of resources that can be
/// checked out, used, and then returned to the pool. It supports three operational states:
/// - Normal: resources can be checked out and returned
/// - Paused: resources can be returned but not checked out (used during batch processing)
/// - Closed: no operations are allowed (used when shutting down)
///
/// The pool tracks both available resources (in the queue) and active resources
/// (checked out but not yet returned). It uses a condition variable to coordinate
/// access and notify waiters when resources become available or the state changes.
pub struct ResourcePool<T> {
    /// Internal state protected by mutex
    inner: Arc<Mutex<ResourcePoolInner<T>>>,
    /// Condition variable for signaling state changes
    signal: Arc<Condvar>,
}

#[derive(Debug)]
/// Internal state of the resource pool, protected by a mutex
struct ResourcePoolInner<T> {
    /// Queue of available resources
    queue: VecDeque<T>,
    /// Count of resources that have been checked out
    active_count: usize,
    /// Operational state of the pool
    state: ResourcePoolState,
}

/// A wrapper around a resource from the pool
///
/// When this struct is dropped, the wrapped resource is automatically
/// returned to the resource pool unless `forget()` has been called.
pub struct Resource<T> {
    /// The actual resource being managed, wrapped in ManuallyDrop to control drop behavior
    resource: ManuallyDrop<T>,

    /// Whether to forget this resource (not return it to the pool) when dropped
    forget: bool,

    /// Reference to the resource pool's inner state
    inner: Arc<Mutex<ResourcePoolInner<T>>>,
    
    /// Reference to the condition variable for signaling resource availability
    signal: Arc<Condvar>,
}

impl<T> Resource<T> {
    /// Create a new Resource wrapper
    ///
    /// This is an internal method used by the ResourcePool to create
    /// a new resource wrapper when checking out a resource.
    fn new(resource: T, inner: Arc<Mutex<ResourcePoolInner<T>>>, signal: Arc<Condvar>) -> Self {
        Self {
            resource: ManuallyDrop::new(resource),
            forget: false,
            inner,
            signal,
        }
    }

    /// Mark this resource to be forgotten (not returned to the pool) when dropped
    ///
    /// This is useful when a resource is in an invalid state and should not be reused.
    pub fn forget(&mut self) {
        self.forget = true;
    }
}

impl<T> Deref for Resource<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.resource
    }
}

impl<T> DerefMut for Resource<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.resource
    }
}

impl<T> Drop for Resource<T> {
    fn drop(&mut self) {
        // Second scope: return the resource and update state
        {
            let mut inner = self
                .inner
                .lock()
                .expect("Unable to drop `Resource` due to MutexError");

            // Decrement active count
            inner.active_count = inner.active_count.saturating_sub(1);

            // SAFETY: this is safe because we do not access `self.member` any more
            let resource = unsafe { ManuallyDrop::take(&mut self.resource) };

            if !self.forget {
                inner.queue.push_back(resource)
            }

            // Notify waiters if needed
            if inner.state == ResourcePoolState::Suspended && inner.active_count == 0 {
                // All resources returned during Suspended state - notify waiters
                self.signal.notify_all();
            } else {
                // Normal notification for new resource availability
                self.signal.notify_one();
            }
        }
    }
}

impl<T> ResourcePool<T> {
    /// Acquires the inner lock and maps mutex errors to DiskyError
    ///
    /// This is a helper function to reduce code duplication for lock acquisition.
    fn acquire_lock(&self) -> Result<MutexGuard<ResourcePoolInner<T>>> {
        self.inner
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))
    }
    
    /// Create a new empty resource pool
    ///
    /// Initializes a new ResourcePool in the Active state with no resources.
    /// Resources can be added to the pool using the `add_resource` method.
    ///
    /// # Example
    /// ```
    /// use disky::parallel::resource_pool::ResourcePool;
    ///
    /// // Create a new resource pool for integers
    /// let pool = ResourcePool::<i32>::new();
    ///
    /// // Add some resources to the pool
    /// pool.add_resource(1).unwrap();
    /// pool.add_resource(2).unwrap();
    /// pool.add_resource(3).unwrap();
    /// ```
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(ResourcePoolInner {
                queue: VecDeque::new(),
                active_count: 0,
                state: ResourcePoolState::Active,
            })),
            signal: Arc::new(Condvar::new()),
        }
    }

    /// Add a resource to the pool
    ///
    /// Adds a new resource to the resource pool. This will fail if the pool
    /// is in any state other than Active.
    ///
    /// # Arguments
    /// * `resource` - The resource to add to the pool
    ///
    /// # Returns
    /// * `Ok(())` if the resource was added successfully
    /// * `Err` if the pool is shutdown or suspended
    pub fn add_resource(&self, resource: T) -> Result<()> {
        let mut inner = self.acquire_lock()?;

        // Handle different queue states
        match inner.state {
            ResourcePoolState::Active => {
                // Add the resource to the queue
                inner.queue.push_back(resource);
                self.signal.notify_one();
                Ok(())
            }
            ResourcePoolState::Shutdown => Err(DiskyError::QueueClosed(
                "Cannot add resources to a shutdown queue".to_string(),
            )),
            ResourcePoolState::Suspended => Err(DiskyError::Other(
                "Cannot add resources while queue is suspended".to_string(),
            )),
        }
    }

    /// Get a resource from the pool
    ///
    /// This method will attempt to get a resource from the pool. If the pool
    /// is empty, it will block until a resource becomes available. If the pool 
    /// is suspended, it will block until the pool returns to Active state. If the
    /// pool is shutdown, it will return an error immediately.
    ///
    /// # Returns
    /// * `Ok(Resource<T>)` if a resource was successfully obtained
    /// * `Err` if the pool is shutdown or an error occurred
    ///
    /// # Blocking behavior
    /// This method will block the current thread if:
    /// * The pool is empty (until a resource is returned)
    /// * The pool is suspended (until it returns to Active state)
    pub fn get_resource(&self) -> Result<Resource<T>> {
        // First scope: get a resource and mark it as active
        let mut inner = self.acquire_lock()?;

        // Wait until we have a resource and are in Active state
        loop {
            // Check queue state first
            match inner.state {
                ResourcePoolState::Shutdown => {
                    return Err(DiskyError::QueueClosed(
                        "Resource queue is shutdown".to_string(),
                    ));
                }
                ResourcePoolState::Suspended => {
                    // Wait for state change
                    inner = self
                        .signal
                        .wait(inner)
                        .map_err(|e| DiskyError::Other(e.to_string()))?;
                    continue;
                }
                ResourcePoolState::Active => {
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

        Ok(Resource::new(
            resource,
            self.inner.clone(),
            self.signal.clone(),
        ))
    }

    /// Wait for all resources to be returned to the pool and set state to Suspended
    ///
    /// This internal method is used to transition the pool to the Suspended state and
    /// wait until all checked-out resources have been returned. It is used as part
    /// of the implementation of operations that need exclusive access to all resources,
    /// such as process_all_resources and process_then_close.
    ///
    /// # Arguments
    /// * `inner` - A MutexGuard holding the lock on the pool's inner state
    ///
    /// # Returns
    /// * `Ok(MutexGuard)` - The MutexGuard with the inner state set to Suspended
    /// * `Err` - If the pool is already shutdown or another error occurs
    ///
    /// # Blocking behavior
    /// This method blocks until all resources have been returned to the pool.
    fn wait_for_all_resources<'a>(
        &'a self,
        mut inner: MutexGuard<'a, ResourcePoolInner<T>>,
    ) -> Result<MutexGuard<'a, ResourcePoolInner<T>>> {
        // Handle different initial states
        match inner.state {
            ResourcePoolState::Shutdown => {
                return Err(DiskyError::QueueClosed(
                    "Cannot suspend a shutdown queue".to_string(),
                ));
            }
            ResourcePoolState::Suspended => {
                // Already suspended, wait until state changes to Active or Shutdown
                loop {
                    inner = self
                        .signal
                        .wait(inner)
                        .map_err(|e| DiskyError::Other(e.to_string()))?;

                    match inner.state {
                        ResourcePoolState::Active => {
                            // State changed to Active, now we can set it to Suspended
                            inner.state = ResourcePoolState::Suspended;
                            break;
                        }
                        ResourcePoolState::Shutdown => {
                            return Err(DiskyError::QueueClosed(
                                "Cannot suspend a shutdown queue".to_string(),
                            ));
                        }
                        ResourcePoolState::Suspended => {
                            // Still suspended, continue waiting
                            continue;
                        }
                    }
                }
            }
            ResourcePoolState::Active => {
                // Set state to Suspended
                inner.state = ResourcePoolState::Suspended;
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

    /// Set the pool to shutdown state
    ///
    /// This will prevent further checkouts and returns.
    /// Cannot shutdown an already shutdown pool.
    pub fn close(&self) -> Result<()> {
        let mut inner = self.acquire_lock()?;

        match inner.state {
            ResourcePoolState::Shutdown => Err(DiskyError::QueueClosed(
                "Pool is already shutdown".to_string(),
            )),
            _ => {
                // Both Active and Suspended states can transition to Shutdown
                inner.state = ResourcePoolState::Shutdown;
                self.signal.notify_all();
                Ok(())
            }
        }
    }

    /// Process all resources in the pool and then shutdown the pool
    ///
    /// This method suspends the pool, applies the given function to each resource,
    /// and then transitions the pool to the Shutdown state regardless of whether
    /// the processing succeeded or failed.
    ///
    /// # Arguments
    /// * `f` - A function that will be applied to each resource in the pool
    ///
    /// # Returns
    /// * `Ok(())` if all resources were processed successfully
    /// * `Err` if an error occurred during processing or the pool was already shutdown
    ///
    /// # Blocking behavior
    /// This method blocks until all checked-out resources have been returned to the pool.
    pub fn process_then_close<F>(&self, f: F) -> Result<()>
    where
        F: FnMut(&mut T) -> Result<()>,
    {
        // Acquire lock for the entire operation
        let inner = self.acquire_lock()?;

        // Check if the queue is already shutdown
        match inner.state {
            ResourcePoolState::Shutdown => {
                return Err(DiskyError::QueueClosed(
                    "Cannot process resources; pool is already shutdown".to_string(),
                ));
            }
            ResourcePoolState::Active | ResourcePoolState::Suspended => {
                // These states are allowed to proceed
            }
        }

        // If not shutdown, proceed with the process-then-shutdown operation
        self.internal_process_all_resources(f, ResourcePoolState::Shutdown, inner)
    }

    /// Internal method to process all resources in the pool
    ///
    /// This method handles the common logic for processing all resources in the pool
    /// and transitioning to a specified final state. It's used by both process_all_resources
    /// and process_then_close.
    ///
    /// # Arguments
    /// * `f` - A function that will be applied to each resource in the pool
    /// * `final_state` - The state to transition the pool to after processing
    /// * `inner` - A MutexGuard holding the lock on the pool's inner state
    ///
    /// # Returns
    /// * `Ok(())` if all resources were processed successfully
    /// * `Err` if an error occurred during processing or the pool was already closed
    ///
    /// # Blocking behavior
    /// This method blocks until all checked-out resources have been returned to the pool.
    fn internal_process_all_resources<'a, F>(
        &'a self,
        mut f: F,
        final_state: ResourcePoolState,
        mut inner: MutexGuard<'a, ResourcePoolInner<T>>,
    ) -> Result<()>
    where
        F: FnMut(&mut T) -> Result<()>,
    {
        // Step 1: Wait for all active resources to return
        inner = self.wait_for_all_resources(inner)?;

        // Step 2: Process each resource in the queue
        let mut process_result = Ok(());

        if inner.state != ResourcePoolState::Shutdown {
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
                "Resource pool is shutdown".to_string(),
            ));
        }

        // Step 3: Set final state (unless there was an error and we're trying to shutdown)
        if process_result.is_ok() || final_state != ResourcePoolState::Shutdown {
            inner.state = final_state;

            // Notify waiters about the state change
            self.signal.notify_all();
        }

        // Return any error from processing
        process_result
    }

    /// Process all resources in the pool
    ///
    /// This method suspends the pool, applies the given function to each resource,
    /// and then transitions the pool back to the Active state.
    ///
    /// # Arguments
    /// * `f` - A function that will be applied to each resource in the pool
    ///
    /// # Returns
    /// * `Ok(())` if all resources were processed successfully
    /// * `Err` if an error occurred during processing or the pool was already shutdown
    ///
    /// # Blocking behavior
    /// This method blocks until all checked-out resources have been returned to the pool.
    pub fn process_all_resources<F>(&self, f: F) -> Result<()>
    where
        F: FnMut(&mut T) -> Result<()>,
    {
        // Acquire lock for the entire operation
        let inner = self.acquire_lock()?;
            
        self.internal_process_all_resources(f, ResourcePoolState::Active, inner)
    }

    /// Get the current number of resources available in the pool
    ///
    /// # Returns
    /// * `Ok(usize)` - The number of available resources
    /// * `Err` - If an error occurred while accessing the pool
    pub fn available_count(&self) -> Result<usize> {
        let inner = self.acquire_lock()?;

        Ok(inner.queue.len())
    }

    /// Check if the pool is empty (no resources available)
    ///
    /// # Returns
    /// * `Ok(bool)` - True if the pool is empty, false otherwise
    /// * `Err` - If an error occurred while accessing the pool
    #[cfg(test)]
    pub fn is_empty(&self) -> Result<bool> {
        let inner = self.acquire_lock()?;

        Ok(inner.queue.is_empty())
    }

    /// Get the current state of the pool
    ///
    /// # Returns
    /// * `Ok(ResourcePoolState)` - The current state of the pool
    /// * `Err` - If an error occurred while accessing the pool
    #[cfg(test)]
    pub(crate) fn get_state(&self) -> Result<ResourcePoolState> {
        let inner = self.acquire_lock()?;

        Ok(inner.state)
    }

    /// Get the current number of active resources (checked out)
    ///
    /// # Returns
    /// * `Ok(usize)` - The number of currently checked out resources
    /// * `Err` - If an error occurred while accessing the pool
    #[cfg(test)]
    pub(crate) fn active_count(&self) -> Result<usize> {
        let inner = self.acquire_lock()?;

        Ok(inner.active_count)
    }

    /// Get all resources from the pool, emptying it in the process
    ///
    /// This method is intended for testing purposes only.
    /// It will only work if the pool is in Active state.
    ///
    /// # Returns
    /// * `Ok(Vec<T>)` - A vector containing all resources from the pool
    /// * `Err` - If the pool is not in Active state or another error occurred
    #[cfg(test)]
    pub fn drain_all_resources(&self) -> Result<Vec<T>> {
        let mut inner = self.acquire_lock()?;

        // Only allow draining in Active state
        if inner.state != ResourcePoolState::Active {
            return Err(DiskyError::Other(format!(
                "Cannot drain resources when pool is in {:?} state",
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
    ///
    /// # Returns
    /// * `Ok(usize)` - The total number of resources managed by the pool
    /// * `Err` - If an error occurred while accessing the pool
    #[cfg(test)]
    pub fn total_count(&self) -> Result<usize> {
        let inner = self.acquire_lock()?;

        Ok(inner.queue.len() + inner.active_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, mpsc};
    use std::thread;

    #[test]
    fn test_basic_resource_pool_operations() {
        let queue = ResourcePool::new();

        // Test initial state
        assert_eq!(queue.get_state().unwrap(), ResourcePoolState::Active);
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

        assert_eq!(*queue.get_resource().unwrap(), 1);
        assert_eq!(queue.available_count().unwrap(), 3); // Resource returned to queue
        assert_eq!(queue.active_count().unwrap(), 0);
        assert_eq!(*queue.get_resource().unwrap(), 2);
        assert_eq!(*queue.get_resource().unwrap(), 3);

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
            let result = queue_clone.get_resource();

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

        // Verify the state is Active after processing
        assert_eq!(queue.get_state().unwrap(), ResourcePoolState::Active);

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

        // Verify state is back to Active even after error
        assert_eq!(queue.get_state().unwrap(), ResourcePoolState::Active);

        // Some resources should still be processed
        let resources = queue.drain_all_resources().unwrap();
        assert_eq!(resources.len(), 3);
    }

    #[test]
    fn test_state_transitions() {
        let pool = ResourcePool::<i32>::new();
        
        // Add some resources
        pool.add_resource(1).unwrap();
        pool.add_resource(2).unwrap();
        
        // Test transition to Suspended
        let (tx1, rx1) = mpsc::channel();
        let (tx2, rx2) = mpsc::channel();
        
        let pool_clone = Arc::new(pool);
        let thread_pool = Arc::clone(&pool_clone);
        
        // Thread that will attempt to get a resource while we're in Suspended state
        let handle = thread::spawn(move || {
            // Signal that we're ready to try to get a resource
            tx1.send(()).unwrap();
            
            // Wait for main thread to signal it's moved to Suspended state
            rx2.recv().unwrap();
            
            // This will block until the pool returns to Active state
            let result = thread_pool.get_resource();
            
            // Return whether we got a resource successfully
            result.is_ok()
        });
        
        // Wait for the thread to be ready
        rx1.recv().unwrap();
        
        // Process resources, which will move to Suspended temporarily
        pool_clone.process_all_resources(|n| {
            *n *= 10;
            Ok(())
        }).unwrap();
        
        // Signal the thread to continue
        tx2.send(()).unwrap();
        
        // Thread should succeed in getting a resource
        assert!(handle.join().unwrap());
        
        // Check resources were processed
        let resources = pool_clone.drain_all_resources().unwrap();
        for r in &resources {
            assert!(*r == 10 || *r == 20);
        }
        
        // Test transition to Shutdown
        pool_clone.close().unwrap();
        assert_eq!(pool_clone.get_state().unwrap(), ResourcePoolState::Shutdown);
        
        // Operations should fail when shutdown
        assert!(pool_clone.add_resource(3).is_err());
        assert!(pool_clone.get_resource().is_err());
        assert!(pool_clone.process_all_resources(|_| Ok(())).is_err());
    }
    
    #[test]
    fn test_resource_forget() {
        let pool = ResourcePool::new();
        
        // Add resources
        pool.add_resource(1).unwrap();
        pool.add_resource(2).unwrap();
        pool.add_resource(3).unwrap();
        
        assert_eq!(pool.available_count().unwrap(), 3);
        
        // Get a resource and forget it
        {
            let mut resource = pool.get_resource().unwrap();
            assert_eq!(*resource, 1);
            resource.forget();
            // Resource will be dropped here but not returned to pool
        }
        
        // Get another resource and let it auto-return
        {
            let resource = pool.get_resource().unwrap();
            assert_eq!(*resource, 2);
            // Resource will be dropped here and returned to pool
        }
        
        // Check counts
        assert_eq!(pool.available_count().unwrap(), 2);
        assert_eq!(pool.total_count().unwrap(), 2);
        
        // The forgotten resource should be gone, and the remaining ones should be 2 and 3
        let resources = pool.drain_all_resources().unwrap();
        assert_eq!(resources.len(), 2);
        assert!(resources.contains(&2));
        assert!(resources.contains(&3));
        assert!(!resources.contains(&1));
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
                    let result = queue_clone.get_resource();
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
