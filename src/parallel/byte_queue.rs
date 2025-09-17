use crate::error::{DiskyError, Result};
use crate::parallel::reader::DiskyParallelPiece;
use std::collections::VecDeque;
use std::sync::{Condvar, Mutex, MutexGuard};

/// Represents the operational state of the byte queue
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ByteQueueState {
    /// Normal operation - records can be added and processed
    Normal,
    /// Queue is closed - no more records can be added
    Closed,
}

/// Inner state of the byte queue, protected by a mutex
#[derive(Debug)]
struct ByteQueueInner {
    /// Queue of pending records
    queue: VecDeque<DiskyParallelPiece>,
    /// Operational state of the queue
    state: ByteQueueState,
    /// Current total bytes used by records in the queue
    current_bytes: usize,
    /// Byte threshold after which we start blocking on push
    bytes_block_limit: usize,
}

/// A thread-safe queue for DiskyParallelPiece objects with byte-size limiting
///
/// This queue is designed for managing DiskyParallelPiece objects with a limit on the total size
/// of Bytes objects stored. When the queue reaches the bytes_block_limit, new push
/// operations will block until space becomes available. However, it will always accept
/// at least one element regardless of size.
#[derive(Debug)]
pub struct ByteQueue {
    /// Inner state protected by mutex
    inner: Mutex<ByteQueueInner>,
    /// Condition variable for signaling queue state changes
    signal: Condvar,
}

// Helper functions
#[inline]
fn queue_closed_err(msg: &str) -> DiskyError {
    DiskyError::QueueClosed(msg.to_string())
}

impl ByteQueue {
    /// Acquires the inner lock and maps mutex errors to DiskyError
    #[inline]
    fn acquire_lock(&self) -> Result<MutexGuard<'_, ByteQueueInner>> {
        self.inner
            .lock()
            .map_err(|e| DiskyError::Other(e.to_string()))
    }

    /// Wait on condition variable and map errors
    #[inline]
    fn await_signal<'a>(
        &'a self,
        inner: MutexGuard<'a, ByteQueueInner>,
    ) -> Result<MutexGuard<'a, ByteQueueInner>> {
        self.signal
            .wait(inner)
            .map_err(|e| DiskyError::Other(e.to_string()))
    }

    /// Check if queue is closed and return appropriate error
    #[inline]
    fn check_closed(inner: &ByteQueueInner, msg: &str) -> Result<()> {
        if inner.state == ByteQueueState::Closed {
            Err(queue_closed_err(msg))
        } else {
            Ok(())
        }
    }

    /// Create a new empty byte queue with the specified byte threshold for blocking
    pub fn new(bytes_block_limit: usize) -> Self {
        Self {
            inner: Mutex::new(ByteQueueInner {
                queue: VecDeque::new(),
                state: ByteQueueState::Normal,
                current_bytes: 0,
                bytes_block_limit,
            }),
            signal: Condvar::new(),
        }
    }

    /// Calculate the size of a DiskyParallelPiece in bytes
    fn calculate_size(result: &DiskyParallelPiece) -> usize {
        match result {
            DiskyParallelPiece::Record(bytes) => bytes.len(),
            // Control messages don't consume storage space
            DiskyParallelPiece::ShardFinished | DiskyParallelPiece::EOF => 0,
        }
    }

    /// Add a record to the back of the queue, blocking if the queue exceeds the byte limit
    ///
    /// Will always accept at least one element even if it's larger than bytes_block_limit,
    /// but will block if the queue already contains data that exceeds the limit.
    pub fn push_back(&self, result: DiskyParallelPiece) -> Result<()> {
        let size = Self::calculate_size(&result);
        let mut inner = self.acquire_lock()?;

        // Check if queue is closed
        Self::check_closed(&inner, "Cannot add to a closed queue")?;

        // wait until there's enough space (we're below the block limit)
        while inner.current_bytes >= inner.bytes_block_limit {
            inner = self.await_signal(inner)?;

            // After waking up, check if the queue is closed
            Self::check_closed(&inner, "Queue was closed while waiting for space")?;
        }

        // Add the record and update byte count
        inner.queue.push_back(result);
        inner.current_bytes += size;
        self.signal.notify_all();

        Ok(())
    }

    /// Try to add a record to the back of the queue without blocking
    ///
    /// Returns Ok(true) if the record was added, Ok(false) if there wasn't enough space.
    /// Will always accept at least one element if the queue is empty.
    pub fn try_push_back(&self, result: DiskyParallelPiece) -> Result<bool> {
        let size = Self::calculate_size(&result);
        let mut inner = self.acquire_lock()?;

        // Check if queue is closed
        Self::check_closed(&inner, "Cannot add to a closed queue")?;

        // Check if we can add without blocking
        if inner.current_bytes >= inner.bytes_block_limit {
            return Ok(false);
        }

        // Add the record and update byte count
        inner.queue.push_back(result);
        inner.current_bytes += size;
        self.signal.notify_all();

        Ok(true)
    }

    /// Read a record from the front of the queue
    ///
    /// This will block until a record is available or the queue is closed.
    pub fn read_front(&self) -> Result<DiskyParallelPiece> {
        let mut inner = self.acquire_lock()?;

        // Wait until we have items or the queue is closed
        loop {
            // Try to get an item directly
            match inner.queue.pop_front() {
                Some(result) => {
                    // Update byte count
                    let size = Self::calculate_size(&result);
                    inner.current_bytes -= size;

                    // Notify waiters since the queue state has changed
                    self.signal.notify_all();

                    return Ok(result);
                }
                None => {
                    // No items, check if queue is closed
                    Self::check_closed(&inner, "Queue is closed and empty")?;

                    // Wait for more records
                    inner = self.await_signal(inner)?;
                }
            }
        }
    }

    /// Try to read a record from the front of the queue without blocking
    ///
    /// Returns Ok(Some(record)) if a record was read, Ok(None) if the queue is empty
    pub fn try_read_front(&self) -> Result<Option<DiskyParallelPiece>> {
        let mut inner = self.acquire_lock()?;

        // Try to get an item directly
        match inner.queue.pop_front() {
            Some(result) => {
                // Update byte count
                let size = Self::calculate_size(&result);
                inner.current_bytes -= size;

                // Notify waiters since the queue state has changed
                self.signal.notify_all();

                Ok(Some(result))
            }
            None => {
                // No items, check if queue is closed
                Self::check_closed(&inner, "Queue is closed and empty")?;
                Ok(None)
            }
        }
    }

    /// Read all records from the queue
    ///
    /// This will block until at least one record is available or the queue is closed.
    pub fn read_all(&self) -> Result<Vec<DiskyParallelPiece>> {
        let mut inner = self.acquire_lock()?;

        // Wait until we have items or the queue is closed
        loop {
            // If we have items, return all of them
            if !inner.queue.is_empty() {
                let mut results = Vec::with_capacity(inner.queue.len());

                while let Some(result) = inner.queue.pop_front() {
                    results.push(result);
                }

                // Update byte count
                inner.current_bytes = 0;

                // Notify waiters since the queue state has changed
                self.signal.notify_all();

                return Ok(results);
            }

            // If closed and empty, return empty vec
            if inner.state == ByteQueueState::Closed {
                return Ok(Vec::new());
            }

            // Wait for more records
            inner = self.await_signal(inner)?;
        }
    }

    /// Close the queue
    ///
    /// This prevents new records from being added, but allows existing records
    /// to be processed. This is useful for graceful shutdown.
    pub fn close(&self) -> Result<()> {
        let mut inner = self.acquire_lock()?;
        inner.state = ByteQueueState::Closed;

        // Notify all waiters so they can check the closed state
        self.signal.notify_all();

        Ok(())
    }

    /// Check if the queue is empty
    pub fn is_empty(&self) -> Result<bool> {
        let inner = self.acquire_lock()?;
        Ok(inner.queue.is_empty())
    }

    /// Check if the queue is done (empty and closed)
    pub fn is_done(&self) -> Result<bool> {
        let inner = self.acquire_lock()?;
        Ok(inner.queue.is_empty() && inner.state == ByteQueueState::Closed)
    }

    /// Get the number of records in the queue
    pub fn len(&self) -> Result<usize> {
        let inner = self.acquire_lock()?;
        Ok(inner.queue.len())
    }

    /// Get the current total size in bytes of all records in the queue
    pub fn bytes_used(&self) -> Result<usize> {
        let inner = self.acquire_lock()?;
        Ok(inner.current_bytes)
    }

    /// Get the byte threshold at which pushing will block
    pub fn bytes_block_limit(&self) -> Result<usize> {
        let inner = self.acquire_lock()?;
        Ok(inner.bytes_block_limit)
    }

    /// Set the byte threshold at which pushing will block
    pub fn set_bytes_block_limit(&self, bytes_block_limit: usize) -> Result<()> {
        let mut inner = self.acquire_lock()?;
        inner.bytes_block_limit = bytes_block_limit;

        // Notify waiters if we now have space available
        if inner.current_bytes < inner.bytes_block_limit {
            self.signal.notify_all();
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    // Helper to create a DiskyParallelPiece::Record with the specified size
    fn create_record(size: usize) -> DiskyParallelPiece {
        let bytes = vec![0u8; size];
        DiskyParallelPiece::Record(Bytes::from(bytes))
    }

    #[test]
    fn test_basic_operations() {
        let queue = ByteQueue::new(1000);

        // Add records
        queue.push_back(create_record(100)).unwrap();
        queue.push_back(create_record(200)).unwrap();
        queue.push_back(create_record(300)).unwrap();

        assert_eq!(queue.len().unwrap(), 3);
        assert_eq!(queue.bytes_used().unwrap(), 600);
        assert_eq!(queue.is_empty().unwrap(), false);

        // Read records
        match queue.read_front().unwrap() {
            DiskyParallelPiece::Record(bytes) => assert_eq!(bytes.len(), 100),
            _ => panic!("Expected Record"),
        }
        assert_eq!(queue.len().unwrap(), 2);
        assert_eq!(queue.bytes_used().unwrap(), 500);

        let results = queue.read_all().unwrap();
        assert_eq!(results.len(), 2);
        match &results[0] {
            DiskyParallelPiece::Record(bytes) => assert_eq!(bytes.len(), 200),
            _ => panic!("Expected Record"),
        }
        match &results[1] {
            DiskyParallelPiece::Record(bytes) => assert_eq!(bytes.len(), 300),
            _ => panic!("Expected Record"),
        }
        assert_eq!(queue.len().unwrap(), 0);
        assert_eq!(queue.bytes_used().unwrap(), 0);
        assert_eq!(queue.is_empty().unwrap(), true);
    }

    #[test]
    fn test_large_item_blocks_try_push_back() {
        let queue = ByteQueue::new(500);

        // Add a large record, this should be accepted even though it's larger than the limit
        queue.push_back(create_record(600)).unwrap();
        assert_eq!(queue.bytes_used().unwrap(), 600);

        // Try to add another record, should fail because we're over the limit
        assert_eq!(queue.try_push_back(create_record(100)).unwrap(), false);

        // After reading the large record, try_push_back should succeed
        match queue.read_front().unwrap() {
            DiskyParallelPiece::Record(bytes) => assert_eq!(bytes.len(), 600),
            _ => panic!("Expected Record"),
        }
        assert_eq!(queue.bytes_used().unwrap(), 0);

        // Now we can add more records
        assert_eq!(queue.try_push_back(create_record(100)).unwrap(), true);
        assert_eq!(queue.bytes_used().unwrap(), 100);
    }

    #[test]
    fn test_multiple_readers_and_writers() {
        let queue = Arc::new(ByteQueue::new(1000));
        let reader_count = 3;
        let writer_count = 2;
        let records_per_writer = 10;
        let record_size = 50;

        // Channel to track completion
        let (tx, rx) = std::sync::mpsc::channel();

        // Spawn reader threads
        let mut reader_handles = vec![];
        for _ in 0..reader_count {
            let queue_clone = Arc::clone(&queue);
            let tx_clone = tx.clone();
            let handle = thread::spawn(move || {
                let mut read_count = 0;
                let mut _total_bytes = 0; // Using underscore to avoid warning

                loop {
                    match queue_clone.read_front() {
                        Ok(result) => {
                            match &result {
                                DiskyParallelPiece::Record(bytes) => {
                                    _total_bytes += bytes.len();
                                    read_count += 1;
                                }
                                DiskyParallelPiece::EOF => {
                                    // End marker, we're done
                                    break;
                                }
                                _ => {}
                            }
                        }
                        Err(DiskyError::QueueClosed(_)) => {
                            // Queue closed, we're done
                            break;
                        }
                        Err(e) => panic!("Unexpected error: {:?}", e),
                    }
                }

                // Return how many records this thread read
                tx_clone.send(read_count).unwrap();
                read_count
            });
            reader_handles.push(handle);
        }

        // Spawn writer threads
        let mut writer_handles = vec![];
        for _ in 0..writer_count {
            let queue_clone = Arc::clone(&queue);
            let handle = thread::spawn(move || {
                for _ in 0..records_per_writer {
                    queue_clone.push_back(create_record(record_size)).unwrap();
                    thread::sleep(Duration::from_millis(5)); // Small delay to interleave operations
                }
                true
            });
            writer_handles.push(handle);
        }

        // Wait for writers to finish
        for handle in writer_handles {
            handle.join().unwrap();
        }

        // Send EOF markers for all readers and then close the queue
        for _ in 0..reader_count {
            queue.push_back(DiskyParallelPiece::EOF).unwrap();
        }
        queue.close().unwrap();

        // Wait for readers to finish
        for handle in reader_handles {
            handle.join().unwrap();
        }

        // Drop the original sender to close the channel
        drop(tx);

        // Collect results
        let mut total_records_read = 0;
        while let Ok(count) = rx.recv() {
            total_records_read += count;
        }

        // Verify that all records were read
        assert_eq!(total_records_read, writer_count * records_per_writer);
    }

    #[test]
    fn test_close_queue() {
        let queue = ByteQueue::new(1000);

        // Add records
        queue.push_back(create_record(100)).unwrap();
        queue.push_back(create_record(100)).unwrap();

        // Close the queue
        queue.close().unwrap();

        // Can still read existing records
        let result = queue.read_front().unwrap();
        match result {
            DiskyParallelPiece::Record(bytes) => assert_eq!(bytes.len(), 100),
            _ => panic!("Expected Record"),
        }

        // Cannot add new records
        let err = queue.push_back(create_record(100)).unwrap_err();
        match err {
            DiskyError::QueueClosed(_) => {}
            _ => panic!("Expected QueueClosed error"),
        }

        // Read the last record
        let result = queue.read_front().unwrap();
        match result {
            DiskyParallelPiece::Record(bytes) => assert_eq!(bytes.len(), 100),
            _ => panic!("Expected Record"),
        }

        // Now the queue is empty and closed
        assert!(queue.is_done().unwrap());

        // Reading from an empty closed queue should return QueueClosed error
        let err = queue.read_front().unwrap_err();
        match err {
            DiskyError::QueueClosed(_) => {}
            _ => panic!("Expected QueueClosed error"),
        }

        // Reading all from an empty closed queue should return empty vec
        let results = queue.read_all().unwrap();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_control_messages() {
        let queue = ByteQueue::new(100);

        // Control messages don't count toward the byte total
        queue.push_back(DiskyParallelPiece::ShardFinished).unwrap();
        queue.push_back(DiskyParallelPiece::EOF).unwrap();
        queue.push_back(create_record(100)).unwrap(); // This still fits

        assert_eq!(queue.len().unwrap(), 3);
        assert_eq!(queue.bytes_used().unwrap(), 100);

        // Read the control messages
        match queue.read_front().unwrap() {
            DiskyParallelPiece::ShardFinished => {}
            _ => panic!("Expected ShardFinished"),
        }

        match queue.read_front().unwrap() {
            DiskyParallelPiece::EOF => {}
            _ => panic!("Expected EOF"),
        }

        assert_eq!(queue.len().unwrap(), 1);
        assert_eq!(queue.bytes_used().unwrap(), 100);
    }

    /// Test that verifies the queue properly handles wake ordering
    ///
    /// Rather than try to test that notify_all wakes multiple waiters, which is
    /// very hard to test deterministically, this test checks the behavior of the queue
    /// when multiple threads are pushing data in a specific pattern that confirms
    /// proper ordering and waking without relying on timing.
    #[test]
    fn test_multiple_waiters_and_wakeups() {
        // Create a queue with a capacity big enough for 2 items
        let item_size = 50;
        let queue_capacity = item_size * 2;
        let queue = Arc::new(ByteQueue::new(queue_capacity));

        // Fill the queue completely to force readers to block
        queue.push_back(create_record(item_size)).unwrap();
        queue.push_back(create_record(item_size)).unwrap();

        // Create channels for synchronization
        let (push_tx, push_rx) = std::sync::mpsc::channel();
        let (pop_tx, pop_rx) = std::sync::mpsc::channel();

        // First, start a thread that will be blocked trying to push
        let q1 = Arc::clone(&queue);
        let push_thread = thread::spawn(move || {
            // Signal we're about to push
            push_tx.send(1).unwrap();

            // This will block until there's space
            q1.push_back(create_record(item_size)).unwrap();

            // Signal we've completed the push
            push_tx.send(2).unwrap();
        });

        // Wait for the push thread to start
        assert_eq!(push_rx.recv().unwrap(), 1);

        // Start a thread that will read from the queue
        let q2 = Arc::clone(&queue);
        let pop_thread = thread::spawn(move || {
            // Signal we're about to start popping
            pop_tx.send(1).unwrap();

            // Pop two items to make room
            let item1 = q2.read_front().unwrap();
            pop_tx.send(2).unwrap();

            let item2 = q2.read_front().unwrap();
            pop_tx.send(3).unwrap();

            // Wait for the pusher to have pushed an item
            assert_eq!(push_rx.recv().unwrap(), 2);

            // Read the item that was pushed
            let item3 = q2.read_front().unwrap();
            pop_tx.send(4).unwrap();

            (item1, item2, item3)
        });

        // Wait for pop thread to signal it's starting
        assert_eq!(pop_rx.recv().unwrap(), 1);

        // Wait for pop thread to signal it popped the first item
        assert_eq!(pop_rx.recv().unwrap(), 2);

        // Wait for pop thread to signal it popped the second item
        assert_eq!(pop_rx.recv().unwrap(), 3);

        // By this point, the push thread should have been unblocked

        // Wait for pop thread to signal it read the pushed item
        assert_eq!(pop_rx.recv().unwrap(), 4);

        // Wait for threads to complete
        push_thread.join().unwrap();
        let (item1, item2, item3) = pop_thread.join().unwrap();

        // Verify items were records with correct size
        match item1 {
            DiskyParallelPiece::Record(bytes) => assert_eq!(bytes.len(), item_size),
            _ => panic!("Expected Record"),
        }

        match item2 {
            DiskyParallelPiece::Record(bytes) => assert_eq!(bytes.len(), item_size),
            _ => panic!("Expected Record"),
        }

        match item3 {
            DiskyParallelPiece::Record(bytes) => assert_eq!(bytes.len(), item_size),
            _ => panic!("Expected Record"),
        }

        // Queue should now be empty
        assert_eq!(queue.len().unwrap(), 0);
        assert_eq!(queue.bytes_used().unwrap(), 0);
    }
}
