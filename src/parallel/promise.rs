//! Simple Promise implementation for parallel operations.
//!
//! This module provides a Promise type that can be used to wait
//! for asynchronous operations to complete and consume the value once.

use crate::error::{DiskyError, Result};
use std::sync::{Arc, Condvar, Mutex};

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
#[derive(Debug)]
pub struct Promise<T> {
    /// The current state of the promise
    state: Arc<Mutex<PromiseState<T>>>,

    /// Condition variable used to signal fulfillment
    condvar: Arc<Condvar>,
}

impl<T> Promise<T>
where
    T: Send + 'static,
{
    /// Create a new, unfulfilled promise
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(PromiseState::Waiting)),
            condvar: Arc::new(Condvar::new()),
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
    pub fn wait(self) -> Result<T> {
        let mut state = self
            .state
            .lock()
            .map_err(|e| DiskyError::Other(format!("Failed to lock state mutex: {}", e)))?;

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
    pub fn try_get(self) -> Result<Option<T>> {
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

impl<T> Clone for Promise<T> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            condvar: Arc::clone(&self.condvar),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_promise_fulfillment() {
        let promise = Promise::new();
        promise.fulfill(42).unwrap();
        assert!(promise.is_fulfilled().unwrap());
        assert_eq!(promise.clone().try_get().unwrap(), Some(42));
        // The original promise should be consumed
        assert_eq!(promise.try_get().unwrap(), None);
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
        let promise = Promise::new();
        let promise_clone = promise.clone();

        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            promise_clone.fulfill(42).unwrap();
        });

        let result = promise.wait().unwrap();
        assert_eq!(result, 42);

        handle.join().unwrap();
    }

    #[test]
    fn test_single_consumer() {
        let promise = Promise::new();
        promise.fulfill(84).unwrap();

        // First wait gets the value
        let clone1 = promise.clone();
        let result1 = clone1.wait().unwrap();
        assert_eq!(result1, 84);

        // Second wait should fail because value was consumed
        let clone2 = promise.clone();
        let result2 = clone2.wait();
        assert!(result2.is_err());

        // Original should report consumed
        assert!(promise.is_consumed().unwrap());
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

        // First try_get succeeds
        let clone1 = promise.clone();
        assert_eq!(clone1.try_get().unwrap(), Some("hello"));

        // Second try_get fails because value was consumed
        let clone2 = promise.clone();
        assert_eq!(clone2.try_get().unwrap(), None);
    }

    #[test]
    fn test_multiple_waiters_one_gets_value() {
        // Create a promise
        let promise = Promise::new();

        // Create 3 threads that all try to consume the value
        let promise1 = promise.clone();
        let handle1 = thread::spawn(move || {
            let result = promise1.wait();
            result.ok()
        });

        let promise2 = promise.clone();
        let handle2 = thread::spawn(move || {
            let result = promise2.wait();
            result.ok()
        });

        let promise3 = promise.clone();
        let handle3 = thread::spawn(move || {
            let result = promise3.wait();
            result.ok()
        });

        // Give all threads time to start waiting
        thread::sleep(Duration::from_millis(10));

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

        assert_eq!(successful_results, 1);
    }
}



