use crate::error::{DiskyError, Result};
use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};

#[derive(Debug)]
pub struct Queue<T> {
    buffer: Mutex<VecDeque<T>>,
    signal: Condvar,
}
impl<T> Queue<T> {
    /// Create empty blocking queue
    pub fn new() -> Self {
        Self {
            buffer: Mutex::new(VecDeque::new()),
            signal: Condvar::new(),
        }
    }
    /// push input on back of queue
    pub fn push_back(&self, t: T) -> Result<()> {
        let mut buffer = self
            .buffer
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))?;

        buffer.push_back(t);
        self.signal.notify_one();
        Ok(())
    }
    /// read element from front of queue
    pub fn read_front(&self) -> Result<T> {
        let mut buffer = self
            .buffer
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))?;

        while buffer.len() == 0 {
            buffer = self.signal.wait(buffer).unwrap();
        }

        match buffer.pop_front() {
            Some(element) => Ok(element),
            None => Err(DiskyError::Other(
                "non-empty queue with no element. Race condition?".to_string(),
            )),
        }
    }

    /// poll element from front of queue
    pub fn poll(&self) -> Result<Option<T>> {
        let mut buffer = self
            .buffer
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))?;
        Ok(buffer.pop_front())
    }

    /// read element from front of queue
    pub fn read_all(&self) -> Result<Vec<T>> {
        let mut buffer = self
            .buffer
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))?;

        while buffer.len() == 0 {
            buffer = self.signal.wait(buffer).unwrap();
        }

        let mut all: Vec<T> = Vec::new();
        while buffer.front().is_some() {
            all.push(buffer.pop_front().unwrap());
        }

        Ok(all)
    }

    /// wait for element from front of queue
    pub fn wait_front(&self) -> Result<()> {
        let mut buffer = self
            .buffer
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))?;

        while buffer.len() == 0 {
            buffer = self.signal.wait(buffer).unwrap();
        }

        Ok(())
    }

    /// return number of elements in queue
    pub fn len(&self) -> Result<usize> {
        let buffer = self
            .buffer
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))?;
        Ok(buffer.len())
    }

    /// Check if queue is empty.
    pub fn is_empty(&self) -> Result<bool> {
        let length = self.len()?;
        return Ok(length == 0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_len() {
        let queue = Queue::<f64>::new();
        assert_eq!(queue.len().unwrap(), 0);
    }
    #[test]
    fn test_push() {
        let queue = Queue::<f64>::new();
        queue.push_back(3.5).unwrap();
        assert_eq!(queue.len().unwrap(), 1);
    }
    #[test]
    fn test_read() {
        let queue = Queue::<f64>::new();
        queue.push_back(3.5).unwrap();
        assert_eq!(queue.read_front().unwrap(), 3.5);
        assert_eq!(queue.len().unwrap(), 0);
    }

    #[test]
    fn test_poll() {
        let queue = Queue::<f64>::new();
        assert_eq!(queue.poll().unwrap(), None);
        queue.push_back(3.5).unwrap();
        assert_eq!(queue.poll().unwrap(), Some(3.5));
        assert_eq!(queue.poll().unwrap(), None);
    }

    #[test]
    fn test_read_all() {
        let queue = Queue::<f64>::new();
        queue.push_back(3.5).unwrap();
        queue.push_back(4.0).unwrap();

        let all = queue.read_all().unwrap();
        assert_eq!(all[0], 3.5);
        assert_eq!(all[1], 4.0);
    }
}
