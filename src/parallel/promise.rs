//! Simple Promise implementation for parallel operations.
//!
//! This module provides a Promise type that can be used to wait
//! for asynchronous operations to complete and consume the value once.

use crate::error::{DiskyError, Result};
use std::sync::{Condvar, Mutex};

/// State of a Promise
#[derive(Debug)]
enum PromiseState<T> {
    /// No value has been provided yet
    Waiting,
    /// A value has been provided and is ready to be consumed
    Fulfilled(T),
    /// The value was previously provided but has already been taken
    Consumed,
}

/// A one-time-use Promise that allows a value to be produced in one thread
/// and consumed in another. Once the value is consumed, the Promise is no longer valid.
///
/// Note: If you need to share this Promise across threads, wrap it in an Arc.
#[derive(Debug)]
pub struct Promise<T> {
    /// The current state of the promise
    state: Mutex<PromiseState<T>>,

    /// Condition variable used to signal fulfillment
    condvar: Condvar,
}

impl<T> Promise<T>
where
    T: Send + 'static,
{
    /// Create a new, unfulfilled promise
    pub fn new() -> Self {
        Self {
            state: Mutex::new(PromiseState::Waiting),
            condvar: Condvar::new(),
        }
    }

    /// Fulfill the promise with the given value
    pub fn fulfill(&self, value: T) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|e| DiskyError::Other(format!("Failed to lock state mutex: {}", e)))?;

        match *state {
            PromiseState::Waiting => {
                *state = PromiseState::Fulfilled(value);
                self.condvar.notify_all();
                Ok(())
            }
            PromiseState::Fulfilled(_) => {
                Err(DiskyError::Other("Promise already fulfilled".to_string()))
            }
            PromiseState::Consumed => {
                Err(DiskyError::Other("Promise already consumed".to_string()))
            }
        }
    }

    /// Wait for the promise to be fulfilled and consume the value
    /// This can only be called once - subsequent calls will return an error
    pub fn wait(&self) -> Result<T> {
        let mut state = self
            .state
            .lock()
            .map_err(|e| DiskyError::Other(format!("Failed to lock state mutex: {}", e)))?;

        // Early-return on Consumed.
        if let PromiseState::Consumed = *state {
            return Err(DiskyError::Other(
                "Cannot `wait` for a PromiseState::Consumed Promise.".to_string(),
            ));
        }

        // Wait until the promise is fulfilled
        while let PromiseState::Waiting = *state {
            state = self
                .condvar
                .wait(state)
                .map_err(|e| DiskyError::Other(format!("Condvar wait failed: {}", e)))?;
        }

        // Take the value if it's available
        match std::mem::replace(&mut *state, PromiseState::Consumed) {
            PromiseState::Fulfilled(value) => Ok(value),
            PromiseState::Consumed => Err(DiskyError::Other(
                "Promise value already consumed".to_string(),
            )),
            PromiseState::Waiting => Err(DiskyError::Other("Promise in invalid state".to_string())),
        }
    }

    /// Check if the promise has been fulfilled
    pub fn is_fulfilled(&self) -> Result<bool> {
        let state = self
            .state
            .lock()
            .map_err(|e| DiskyError::Other(format!("Failed to lock state mutex: {}", e)))?;

        match *state {
            PromiseState::Fulfilled(_) => Ok(true),
            _ => Ok(false),
        }
    }

    /// Check if the promise has been consumed
    pub fn is_consumed(&self) -> Result<bool> {
        let state = self
            .state
            .lock()
            .map_err(|e| DiskyError::Other(format!("Failed to lock state mutex: {}", e)))?;

        match *state {
            PromiseState::Consumed => Ok(true),
            _ => Ok(false),
        }
    }

    /// Try to get the value without waiting
    /// This consumes the Promise, so it can only be called once
    pub fn try_get(&self) -> Result<Option<T>> {
        let mut state = self
            .state
            .lock()
            .map_err(|e| DiskyError::Other(format!("Failed to lock state mutex: {}", e)))?;

        match std::mem::replace(&mut *state, PromiseState::Consumed) {
            PromiseState::Fulfilled(value) => Ok(Some(value)),
            PromiseState::Waiting => {
                // Restore the waiting state since we didn't consume anything
                *state = PromiseState::Waiting;
                Ok(None)
            }
            PromiseState::Consumed => {
                // Already consumed, just return None
                Ok(None)
            }
        }
    }
}

// Clone is intentionally not implemented for Promise.
// If you need to share a Promise, wrap it in an Arc.

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::thread;

    #[test]
    fn test_promise_fulfillment() {
        let promise = Promise::new();
        promise.fulfill(42).unwrap();
        assert!(promise.is_fulfilled().unwrap());
        // The promise should be consumed after try_get
        assert_eq!(promise.try_get().unwrap(), Some(42));
        // Second try_get should report consumed
        let another_promise = Promise::new();
        another_promise.fulfill(42).unwrap();
        assert_eq!(another_promise.try_get().unwrap(), Some(42));
        assert_eq!(another_promise.try_get().unwrap(), None);
    }

    #[test]
    fn test_double_fulfill_fails() {
        let promise = Promise::new();
        promise.fulfill(42).unwrap();
        let result = promise.fulfill(84);
        assert!(result.is_err());
    }

    #[test]
    fn test_promise_wait() {
        let promise = Arc::new(Promise::new());
        let promise_clone = Arc::clone(&promise);

        // Channel for thread synchronization
        let (ready_tx, ready_rx) = mpsc::channel();

        let handle = thread::spawn(move || {
            // Wait for the main thread to be ready
            ready_rx.recv().unwrap();
            promise_clone.fulfill(42).unwrap();
        });

        // Signal worker thread to fulfill the promise
        ready_tx.send(()).unwrap();

        // Wait for the promise to be fulfilled - no need to unwrap the Arc
        let result = promise.wait().unwrap();
        assert_eq!(result, 42);

        handle.join().unwrap();
    }

    #[test]
    fn test_single_consumer() {
        let promise = Arc::new(Promise::new());
        promise.fulfill(84).unwrap();

        // Consume the promise - no need to extract from Arc
        let result = promise.wait().unwrap();
        assert_eq!(result, 84);
    }

    #[test]
    fn test_try_get_before_completion() {
        let promise = Promise::<i32>::new();
        assert_eq!(promise.try_get().unwrap(), None);
    }

    #[test]
    fn test_try_get_after_completion() {
        let promise = Promise::new();
        promise.fulfill("hello").unwrap();

        // try_get consumes the promise
        assert_eq!(promise.try_get().unwrap(), Some("hello"));

        // Second try_get should find nothing
        let another_promise = Promise::new();
        another_promise.fulfill("hello").unwrap();
        assert_eq!(another_promise.try_get().unwrap(), Some("hello"));
        assert_eq!(another_promise.try_get().unwrap(), None);
    }

    #[test]
    fn test_multiple_waiters_one_gets_value() {
        // Create a shared promise
        let promise = Arc::new(Promise::new());

        // Channel for threads to signal they're ready
        let (threads_ready_tx, threads_ready_rx) = mpsc::channel();

        // Use separate channels for each thread to avoid Receiver cloning
        let (start_tx1, start_rx1) = mpsc::channel();
        let (start_tx2, start_rx2) = mpsc::channel();
        let (start_tx3, start_rx3) = mpsc::channel();

        // Thread coordination counter
        let thread_count = 3;
        let mut ready_count = 0;

        // Create 3 threads that all try to consume the value
        let promise1 = Arc::clone(&promise);
        let threads_ready_tx1 = threads_ready_tx.clone();
        let handle1 = thread::spawn(move || {
            // Signal that this thread is ready
            threads_ready_tx1.send(()).unwrap();
            // Wait for the start signal
            start_rx1.recv().unwrap();

            // Try to consume the promise directly
            // If another thread has already consumed it, this will fail
            match promise1.wait() {
                Ok(v) => Some(v),
                Err(_) => None,
            }
        });

        let promise2 = Arc::clone(&promise);
        let threads_ready_tx2 = threads_ready_tx.clone();
        let handle2 = thread::spawn(move || {
            // Signal that this thread is ready
            threads_ready_tx2.send(()).unwrap();
            // Wait for the start signal
            start_rx2.recv().unwrap();

            match promise2.wait() {
                Ok(v) => Some(v),
                Err(_) => None,
            }
        });

        let promise3 = Arc::clone(&promise);
        let threads_ready_tx3 = threads_ready_tx;
        let handle3 = thread::spawn(move || {
            // Signal that this thread is ready
            threads_ready_tx3.send(()).unwrap();
            // Wait for the start signal
            start_rx3.recv().unwrap();

            match promise3.wait() {
                Ok(v) => Some(v),
                Err(_) => None,
            }
        });

        // Wait for all threads to be ready
        for _ in 0..thread_count {
            threads_ready_rx.recv().unwrap();
            ready_count += 1;
        }
        assert_eq!(ready_count, thread_count, "All threads should be ready");

        // Signal all threads to start trying to consume the promise
        start_tx1.send(()).unwrap();
        start_tx2.send(()).unwrap();
        start_tx3.send(()).unwrap();

        // Fulfill the promise
        promise.fulfill(42).unwrap();

        // Get results from all threads
        let result1 = handle1.join().unwrap();
        let result2 = handle2.join().unwrap();
        let result3 = handle3.join().unwrap();

        // Only one thread should get the value, others should get None
        let successful_results = [result1, result2, result3]
            .iter()
            .filter(|&r| r.is_some())
            .count();

        assert_eq!(
            successful_results, 1,
            "Only one thread should have successfully consumed the promise"
        );
    }
}
