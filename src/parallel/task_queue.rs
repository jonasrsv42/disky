use crate::error::{DiskyError, Result};
use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};

/// Represents the operational state of the task queue
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TaskQueueState {
    /// Normal operation - tasks can be added and processed
    Normal,
    /// Queue is closed - no more tasks can be added
    Closed,
}

/// Inner state of the task queue, protected by a mutex
#[derive(Debug)]
struct TaskQueueInner<T> {
    /// Queue of pending tasks
    queue: VecDeque<T>,
    /// Operational state of the queue
    state: TaskQueueState,
}

/// A thread-safe queue for tasks
///
/// This queue is designed for managing pending tasks. It supports
/// adding tasks, processing tasks, and checking queue status.
#[derive(Debug)]
pub struct TaskQueue<T> {
    /// Inner state protected by mutex
    inner: Mutex<TaskQueueInner<T>>,
    /// Condition variable for signaling new tasks
    signal: Condvar,
}

impl<T> TaskQueue<T> {
    /// Create a new empty task queue
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(TaskQueueInner {
                queue: VecDeque::new(),
                state: TaskQueueState::Normal,
            }),
            signal: Condvar::new(),
        }
    }

    /// Add a task to the back of the queue
    pub fn push_back(&self, t: T) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))?;

        // Check if queue is closed
        if inner.state == TaskQueueState::Closed {
            return Err(DiskyError::QueueClosed(
                "Cannot add to a closed queue".to_string(),
            ));
        }

        inner.queue.push_back(t);
        self.signal.notify_one();

        Ok(())
    }

    /// Read a task from the front of the queue
    ///
    /// This will block until a task is available or the queue is closed.
    pub fn read_front(&self) -> Result<T> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))?;

        // Wait until we have items or the queue is closed
        loop {
            // If we have items, return one
            if !inner.queue.is_empty() {
                return match inner.queue.pop_front() {
                    Some(task) => Ok(task),
                    None => Err(DiskyError::Other(
                        "non-empty queue with no tasks. Race condition?".to_string(),
                    )),
                };
            }

            // If closed and empty, return error
            if inner.state == TaskQueueState::Closed {
                return Err(DiskyError::QueueClosed(
                    "Queue is closed and empty".to_string(),
                ));
            }

            // Wait for more tasks
            inner = self.signal.wait(inner).unwrap();
        }
    }

    /// Read all tasks from the queue
    ///
    /// This will block until at least one task is available or the queue is closed.
    pub fn read_all(&self) -> Result<Vec<T>> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))?;

        // Wait until we have items or the queue is closed
        loop {
            // If we have items, return all of them
            if !inner.queue.is_empty() {
                let mut all = Vec::new();
                while let Some(task) = inner.queue.pop_front() {
                    all.push(task);
                }
                return Ok(all);
            }

            // If closed and empty, return empty vec
            if inner.state == TaskQueueState::Closed {
                return Ok(Vec::new());
            }

            // Wait for more tasks
            inner = self.signal.wait(inner).unwrap();
        }
    }

    /// Close the queue
    ///
    /// This prevents new tasks from being added, but allows existing tasks
    /// to be processed. This is useful for graceful shutdown.
    pub fn close(&self) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))?;

        inner.state = TaskQueueState::Closed;

        // Notify all waiters so they can check the closed state
        self.signal.notify_all();

        Ok(())
    }

    /// Check if the queue is empty
    pub fn is_empty(&self) -> Result<bool> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))?;

        Ok(inner.queue.is_empty())
    }

    /// Check if the queue is done (empty and closed)
    #[cfg(test)]
    pub fn is_done(&self) -> Result<bool> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))?;

        Ok(inner.queue.is_empty() && inner.state == TaskQueueState::Closed)
    }

    /// Get the number of tasks in the queue
    pub fn len(&self) -> Result<usize> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))?;

        Ok(inner.queue.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_basic_operations() {
        let queue = TaskQueue::new();

        // Add tasks
        queue.push_back(1).unwrap();
        queue.push_back(2).unwrap();
        queue.push_back(3).unwrap();

        assert_eq!(queue.len().unwrap(), 3);
        assert_eq!(queue.is_empty().unwrap(), false);

        // Read tasks
        let task = queue.read_front().unwrap();
        assert_eq!(task, 1);
        assert_eq!(queue.len().unwrap(), 2);

        let tasks = queue.read_all().unwrap();
        assert_eq!(tasks, vec![2, 3]);
        assert_eq!(queue.len().unwrap(), 0);
        assert_eq!(queue.is_empty().unwrap(), true);
    }

    #[test]
    fn test_close_queue() {
        let queue = TaskQueue::new();

        // Add tasks
        queue.push_back(1).unwrap();
        queue.push_back(2).unwrap();

        // Close the queue
        queue.close().unwrap();

        // Can still read existing tasks
        let task = queue.read_front().unwrap();
        assert_eq!(task, 1);

        // Cannot add new tasks
        assert!(matches!(
            queue.push_back(3),
            Err(DiskyError::QueueClosed(_))
        ));

        // Read the last task
        let task = queue.read_front().unwrap();
        assert_eq!(task, 2);

        // Now the queue is empty and closed
        assert!(queue.is_done().unwrap());

        // Reading from an empty closed queue should return QueueClosed error
        assert!(matches!(
            queue.read_front(),
            Err(DiskyError::QueueClosed(_))
        ));

        // Reading all from an empty closed queue should return empty vec
        let tasks = queue.read_all().unwrap();
        assert_eq!(tasks.len(), 0);
    }

    #[test]
    fn test_waiting_for_tasks() {
        let queue = Arc::new(TaskQueue::<i32>::new());

        // Channel to signal that reader thread is waiting
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();

        // Start a thread that will read a task
        let queue_clone = Arc::clone(&queue);
        let handle = thread::spawn(move || {
            // Signal that we're about to start reading (which will block)
            ready_tx.send(()).unwrap();

            // This will block until a task is available
            queue_clone.read_front().unwrap()
        });

        // Wait for reader thread to signal it's ready to read
        ready_rx.recv().unwrap();

        // Add a task
        queue.push_back(42).unwrap();

        // The thread should complete with the task
        let result = handle.join().unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn test_close_unblocks_readers() {
        let queue = Arc::new(TaskQueue::<i32>::new());

        // Channel to signal that reader thread is waiting
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();

        // Start a thread that will try to read from an empty queue
        let queue_clone = Arc::clone(&queue);
        let handle = thread::spawn(move || {
            // Signal that we're about to start reading (which will block)
            ready_tx.send(()).unwrap();

            match queue_clone.read_front() {
                Ok(val) => Ok(val),
                Err(e) => match e {
                    DiskyError::QueueClosed(_) => Err("queue closed"),
                    _ => Err("other error"),
                },
            }
        });

        // Wait for reader thread to signal it's ready to read
        ready_rx.recv().unwrap();

        // Close the queue - this should unblock the reader
        queue.close().unwrap();

        // The thread should complete with a QueueClosed error
        let result = handle.join().unwrap();
        assert_eq!(result, Err("queue closed"));
    }

    #[test]
    fn test_concurrent_operations() {
        let queue = Arc::new(TaskQueue::<i32>::new());

        // Channel to synchronize task production
        let (task_produced_tx, task_produced_rx) = std::sync::mpsc::channel();
        let (task_consumed_tx, task_consumed_rx) = std::sync::mpsc::channel();

        // Producer thread - adds tasks
        let queue_clone = Arc::clone(&queue);
        let producer = thread::spawn(move || {
            for i in 0..10 {
                queue_clone.push_back(i).unwrap();

                // Signal that a task was produced
                task_produced_tx.send(()).unwrap();

                // Wait for consumer to process before adding next item
                task_consumed_rx.recv().unwrap();
            }
        });

        // Consumer thread - processes tasks
        let queue_clone = Arc::clone(&queue);
        let consumer = thread::spawn(move || {
            let mut sum = 0;
            for _ in 0..10 {
                // Wait for producer to add a task
                task_produced_rx.recv().unwrap();

                // Process the task
                let task = queue_clone.read_front().unwrap();
                sum += task;

                // Signal that a task was consumed
                task_consumed_tx.send(()).unwrap();
            }
            sum
        });

        // Wait for threads to complete
        producer.join().unwrap();
        let sum = consumer.join().unwrap();

        // Sum of integers from 0 to 9
        assert_eq!(sum, 45);
        assert_eq!(queue.is_empty().unwrap(), true);
    }

    #[test]
    fn test_blocking_read() {
        let queue = Arc::new(TaskQueue::<i32>::new());

        // Channel to signal that reader thread is waiting
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();

        // Start a thread that will read a task
        let queue_clone = Arc::clone(&queue);
        let handle = thread::spawn(move || {
            // Signal that we're about to start reading (which will block)
            ready_tx.send(()).unwrap();

            // This should block until a task is available
            let task = queue_clone.read_front().unwrap();
            task * 2
        });

        // Wait for reader thread to signal it's ready to read
        ready_rx.recv().unwrap();

        // Add a task to unblock the thread
        queue.push_back(42).unwrap();

        // Thread should complete and return doubled value
        let result = handle.join().unwrap();
        assert_eq!(result, 84);
    }

    #[test]
    fn test_read_all_blocking() {
        let queue = Arc::new(TaskQueue::<i32>::new());

        // Channel to signal that reader thread is waiting
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();

        // Thread that will read all tasks
        let queue_clone = Arc::clone(&queue);
        let handle = thread::spawn(move || {
            // Signal that we're about to start reading (which will block)
            ready_tx.send(()).unwrap();

            // This should block until at least one task is available
            queue_clone.read_all().unwrap()
        });

        // Wait for reader thread to signal it's ready to read
        ready_rx.recv().unwrap();

        // Add a few tasks
        queue.push_back(1).unwrap();
        queue.push_back(2).unwrap();
        queue.push_back(3).unwrap();

        // Thread should complete with all tasks
        let tasks = handle.join().unwrap();
        assert_eq!(tasks, vec![1, 2, 3]);
        assert_eq!(queue.is_empty().unwrap(), true);
    }
}

