//! Threaded node for offloading subtrees to dedicated threads.
//!
//! This module provides [`ThreadedNodeConfig`] which wraps a child node and runs it
//! on a dedicated thread, communicating results back via a bounded channel.
//!
//! # Lifecycle
//!
//! ```text
//! ┌─────────────────┐                      ┌─────────────────┐
//! │  Parent Thread  │                      │  Child Thread   │
//! │                 │                      │                 │
//! │  ThreadedReader │    bounded channel   │  child.make()   │
//! │  [recv_ch] ◄────┼──────────────────────┼─ [send_ch]      │
//! │                 │                      │                 │
//! │  calls next()   │                      │  iterates and   │
//! │  to receive     │                      │  sends records  │
//! └─────────────────┘                      └─────────────────┘
//! ```
//!
//! 1. **Build**: `ThreadedNodeConfig::build()` spawns a child thread that owns
//!    the child node and the send end of a channel (`send_ch`).
//!
//! 2. **Normal operation**: Child thread calls `child.make()` to build the iterator,
//!    then iterates and sends each `Result<Bytes>` through `send_ch`. Parent thread
//!    calls `next()` which receives from `recv_ch`.
//!
//! 3. **Clean shutdown (parent drops `ThreadedReader`)**:
//!    - `drop()` first drops `recv_ch` (the receive end)
//!    - This causes any blocked or future `send_ch.send()` to return `Err`
//!    - Child thread sees the error, breaks its loop, and exits cleanly
//!    - `drop()` then calls `join()` on the thread handle (which succeeds
//!      immediately since the child already exited)
//!
//! 4. **Child error**: If `child.make()` fails, the error is sent through the
//!    channel and the child thread exits. Parent receives the error on first `next()`.
//!
//! 5. **Child panic**: If the child thread panics, `send_ch` is dropped, so
//!    `recv_ch.recv()` returns `Err` and parent sees `None` from the iterator.

use std::sync::mpsc::{self, Receiver};
use std::thread::{self, JoinHandle};

use bytes::Bytes;

use crate::error::Result;
use crate::tree::reader::{Node, Reader};

/// Default channel buffer size.
pub const DEFAULT_BUFFER_SIZE: usize = 64;

/// Builder for [`ThreadedReader`].
///
/// This is a tree node that runs a child node on a dedicated thread, allowing
/// the subtree to execute independently while the parent reads from a channel.
///
/// This is useful for:
/// - Parallelizing I/O-bound subtrees
/// - Decoupling producer (child tree) from consumer
/// - Keeping the main thread responsive while subtrees do work
///
/// # Example
///
/// ```ignore
/// use disky::parallel::node::ThreadedNodeConfig;
/// use disky::reader::RecordReaderConfig;
///
/// // Offload a reader to a dedicated thread
/// let threaded = ThreadedNodeConfig::new(RecordReaderConfig::new(file))
///     .with_buffer_size(128)
///     .build()?;
///
/// for record in threaded {
///     let bytes = record?;
///     // Process record (reader runs on separate thread)
/// }
/// ```
pub struct ThreadedNodeConfig {
    child: Box<dyn Node>,
    buffer_size: usize,
}

impl ThreadedNodeConfig {
    /// Creates a new config with the given child node.
    ///
    /// Uses the default buffer size of 64.
    pub fn new(child: impl Node + 'static) -> Self {
        Self {
            child: Box::new(child),
            buffer_size: DEFAULT_BUFFER_SIZE,
        }
    }

    /// Sets the channel buffer size.
    ///
    /// A larger buffer allows more records to be prefetched, which can improve
    /// throughput but uses more memory. A smaller buffer provides better
    /// backpressure.
    pub fn with_buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    /// Builds the [`ThreadedReader`].
    ///
    /// This spawns a child thread that will start iterating the child node immediately.
    /// See module-level docs for the full lifecycle description.
    pub fn build(self) -> Result<ThreadedReader> {
        let buffer_size = self.buffer_size.max(1); // At least 1
        let (send_ch, recv_ch) = mpsc::sync_channel::<Result<Bytes>>(buffer_size);

        let child = self.child;

        let handle = thread::spawn(move || {
            // Build the child reader on this thread
            let reader = match child.make() {
                Ok(r) => r,
                Err(e) => {
                    // Send the error and exit
                    let _ = send_ch.send(Err(e));
                    return;
                }
            };

            // Iterate and send each result through the channel.
            // If send fails, it means recv_ch was dropped (parent is done),
            // so we break and let the thread exit cleanly.
            for result in reader {
                if send_ch.send(result).is_err() {
                    break;
                }
            }
            // Thread exits, send_ch is dropped, recv_ch.recv() will return Err
        });

        Ok(ThreadedReader {
            recv_ch: Some(recv_ch),
            handle: Some(handle),
        })
    }
}

impl Node for ThreadedNodeConfig {
    fn make(self: Box<Self>) -> Result<Reader> {
        Ok(Box::new(self.build()?))
    }
}

/// An iterator that reads from a child node running on a dedicated thread.
///
/// The child node is iterated on a spawned thread, and results are communicated
/// back through a bounded channel. This allows the child subtree to run
/// independently while the parent consumes results at its own pace.
///
/// See module-level docs for the full lifecycle description.
pub struct ThreadedReader {
    /// Receive end of the channel. Wrapped in Option so we can drop it
    /// before joining the thread handle (see Drop impl).
    recv_ch: Option<Receiver<Result<Bytes>>>,

    /// Handle to the child thread.
    handle: Option<JoinHandle<()>>,
}

impl Iterator for ThreadedReader {
    type Item = Result<Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        // Receive the next result from the child thread.
        // Returns None if the channel is closed (child finished or errored).
        self.recv_ch.as_ref().and_then(|ch| ch.recv().ok())
    }
}

impl Drop for ThreadedReader {
    fn drop(&mut self) {
        // IMPORTANT: Drop recv_ch FIRST to signal the child thread to stop.
        //
        // If the child thread is blocked on send_ch.send() (buffer full),
        // dropping recv_ch causes that send to return Err immediately.
        // The child sees the error, breaks its loop, and exits cleanly.
        //
        // If we joined first without dropping recv_ch, we'd deadlock:
        // - Child blocked on send (buffer full)
        // - Parent blocked on join (waiting for child)
        // - Neither can proceed
        self.recv_ch.take();

        // Now the child thread can exit, so join will succeed.
        if let Some(handle) = self.handle.take() {
            // Ignore join result - child may have panicked, but we still
            // want to clean up without propagating the panic here.
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::DiskyError;
    use std::collections::HashSet;

    /// A simple node that produces a sequence of bytes for testing
    struct SequenceNode {
        count: usize,
    }

    impl SequenceNode {
        fn new(count: usize) -> Self {
            Self { count }
        }
    }

    impl Node for SequenceNode {
        fn make(self: Box<Self>) -> Result<Reader> {
            Ok(Box::new(
                (0..self.count).map(|i| Ok(Bytes::from(format!("item{}", i)))),
            ))
        }
    }

    /// A node that produces an error at a specific position
    struct ErrorAtNode {
        count: usize,
        error_at: usize,
    }

    impl Node for ErrorAtNode {
        fn make(self: Box<Self>) -> Result<Reader> {
            let error_at = self.error_at;
            Ok(Box::new((0..self.count).map(move |i| {
                if i == error_at {
                    Err(DiskyError::Other(format!("Error at {}", i)))
                } else {
                    Ok(Bytes::from(format!("item{}", i)))
                }
            })))
        }
    }

    /// A node that fails during make()
    struct FailingNode;

    impl Node for FailingNode {
        fn make(self: Box<Self>) -> Result<Reader> {
            Err(DiskyError::Other("Failed to build".to_string()))
        }
    }

    #[test]
    fn test_basic_iteration() {
        let node = SequenceNode::new(10);
        let reader = ThreadedNodeConfig::new(node).build().unwrap();

        let results: Vec<Bytes> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(results.len(), 10);

        for (i, bytes) in results.iter().enumerate() {
            assert_eq!(bytes.as_ref(), format!("item{}", i).as_bytes());
        }
    }

    #[test]
    fn test_empty_node() {
        let node = SequenceNode::new(0);
        let reader = ThreadedNodeConfig::new(node).build().unwrap();

        let results: Vec<Bytes> = reader.map(|r| r.unwrap()).collect();
        assert!(results.is_empty());
    }

    #[test]
    fn test_custom_buffer_size() {
        let node = SequenceNode::new(100);
        let reader = ThreadedNodeConfig::new(node)
            .with_buffer_size(8)
            .build()
            .unwrap();

        let results: Vec<Bytes> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(results.len(), 100);
    }

    #[test]
    fn test_error_propagation() {
        let node = ErrorAtNode {
            count: 10,
            error_at: 5,
        };
        let reader = ThreadedNodeConfig::new(node).build().unwrap();

        let results: Vec<Result<Bytes>> = reader.collect();
        assert_eq!(results.len(), 10);

        // First 5 should be Ok
        for i in 0..5 {
            assert!(results[i].is_ok());
        }
        // 6th should be error
        assert!(results[5].is_err());
        // Rest should be Ok
        for i in 6..10 {
            assert!(results[i].is_ok());
        }
    }

    #[test]
    fn test_make_error_propagation() {
        let node = FailingNode;
        let reader = ThreadedNodeConfig::new(node).build().unwrap();

        // First next() should return the error
        let mut iter = reader;
        let result = iter.next();
        assert!(result.is_some());
        assert!(result.unwrap().is_err());

        // Subsequent calls should return None
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_as_tree_node() {
        let inner = SequenceNode::new(5);
        let threaded = ThreadedNodeConfig::new(inner);

        // Use it as a Node
        let reader = Box::new(threaded).make().unwrap();
        let results: Vec<Bytes> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn test_drop_before_exhaustion() {
        let node = SequenceNode::new(1000);
        let mut reader = ThreadedNodeConfig::new(node)
            .with_buffer_size(4)
            .build()
            .unwrap();

        // Read only a few items
        for _ in 0..5 {
            let _ = reader.next();
        }

        // Drop the reader - should not hang
        drop(reader);
    }

    #[test]
    fn test_preserves_all_items() {
        let node = SequenceNode::new(50);
        let reader = ThreadedNodeConfig::new(node)
            .with_buffer_size(8)
            .build()
            .unwrap();

        let results: HashSet<String> = reader
            .map(|r| String::from_utf8(r.unwrap().to_vec()).unwrap())
            .collect();

        let expected: HashSet<String> = (0..50).map(|i| format!("item{}", i)).collect();
        assert_eq!(results, expected);
    }

    // ==================== Deadlock Prevention Tests ====================
    //
    // These tests verify that the ThreadedReader does not deadlock in
    // various edge cases. Each test has a timeout implicit in the test
    // runner - if these hang, it indicates a deadlock bug.

    /// Test: Child thread exits early (e.g., empty iterator), parent keeps reading.
    ///
    /// Expected: Parent should receive None immediately, not block forever.
    /// This verifies that when send_ch is dropped (child exits), recv_ch.recv()
    /// returns Err and we convert that to None.
    #[test]
    fn test_no_deadlock_child_exits_early() {
        let node = SequenceNode::new(0); // Empty - child exits immediately
        let mut reader = ThreadedNodeConfig::new(node).build().unwrap();

        // These should all return None without blocking
        assert!(reader.next().is_none());
        assert!(reader.next().is_none());
        assert!(reader.next().is_none());
    }

    /// Test: Child thread finishes normally, parent reads past the end.
    ///
    /// Expected: After exhausting the iterator, further next() calls return None.
    #[test]
    fn test_no_deadlock_read_past_end() {
        let node = SequenceNode::new(3);
        let mut reader = ThreadedNodeConfig::new(node).build().unwrap();

        // Exhaust the iterator
        assert!(reader.next().is_some());
        assert!(reader.next().is_some());
        assert!(reader.next().is_some());

        // These should return None without blocking
        assert!(reader.next().is_none());
        assert!(reader.next().is_none());
    }

    /// Test: Child thread errors during make(), parent tries to read.
    ///
    /// Expected: First read returns the error, subsequent reads return None.
    /// This verifies we don't block when the child thread exits after sending an error.
    #[test]
    fn test_no_deadlock_child_make_fails() {
        let node = FailingNode;
        let mut reader = ThreadedNodeConfig::new(node).build().unwrap();

        // First call gets the error
        let first = reader.next();
        assert!(first.is_some());
        assert!(first.unwrap().is_err());

        // Subsequent calls should return None without blocking
        assert!(reader.next().is_none());
        assert!(reader.next().is_none());
    }

    /// Test: Buffer fills up, then parent drops the reader.
    ///
    /// Expected: Drop completes without deadlock.
    ///
    /// Scenario:
    /// 1. Child produces many items with a tiny buffer
    /// 2. Child blocks on send() because buffer is full
    /// 3. Parent drops reader without consuming all items
    /// 4. Drop must: (a) drop recv_ch to unblock child's send(), (b) join thread
    ///
    /// If we joined before dropping recv_ch, we'd deadlock:
    /// - Child blocked on send (buffer full)
    /// - Parent blocked on join (waiting for child)
    #[test]
    fn test_no_deadlock_buffer_full_parent_drops() {
        let node = SequenceNode::new(10_000); // Many items
        let mut reader = ThreadedNodeConfig::new(node)
            .with_buffer_size(2) // Tiny buffer - will fill immediately
            .build()
            .unwrap();

        // Read just one item - buffer will be full with child blocked on send
        let _ = reader.next();

        // Give child thread time to fill buffer and block
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Drop should complete without deadlock
        // (If this hangs, the test will timeout)
        drop(reader);
    }

    /// Test: Parent drops reader immediately after build, before any reads.
    ///
    /// Expected: Drop completes without deadlock.
    #[test]
    fn test_no_deadlock_drop_immediately() {
        let node = SequenceNode::new(100);
        let reader = ThreadedNodeConfig::new(node)
            .with_buffer_size(4)
            .build()
            .unwrap();

        // Drop without reading anything
        drop(reader);
    }

    /// A node that blocks for a while before producing items.
    /// Used to test scenarios where the child is slow to start.
    struct SlowStartNode {
        delay_ms: u64,
        count: usize,
    }

    impl Node for SlowStartNode {
        fn make(self: Box<Self>) -> Result<Reader> {
            std::thread::sleep(std::time::Duration::from_millis(self.delay_ms));
            Ok(Box::new(
                (0..self.count).map(|i| Ok(Bytes::from(format!("item{}", i)))),
            ))
        }
    }

    /// Test: Parent drops reader while child is still in make().
    ///
    /// Expected: Drop completes without deadlock.
    #[test]
    fn test_no_deadlock_drop_during_make() {
        let node = SlowStartNode {
            delay_ms: 50,
            count: 10,
        };
        let reader = ThreadedNodeConfig::new(node).build().unwrap();

        // Drop immediately - child is still in make()
        drop(reader);
    }

    /// A node where each item takes time to produce.
    struct SlowProducerNode {
        delay_ms: u64,
        count: usize,
    }

    impl Node for SlowProducerNode {
        fn make(self: Box<Self>) -> Result<Reader> {
            let delay = self.delay_ms;
            Ok(Box::new((0..self.count).map(move |i| {
                std::thread::sleep(std::time::Duration::from_millis(delay));
                Ok(Bytes::from(format!("item{}", i)))
            })))
        }
    }

    /// Test: Parent drops reader while child is iterating slowly.
    ///
    /// Expected: Drop completes without deadlock.
    #[test]
    fn test_no_deadlock_drop_during_iteration() {
        let node = SlowProducerNode {
            delay_ms: 20,
            count: 100,
        };
        let mut reader = ThreadedNodeConfig::new(node)
            .with_buffer_size(4)
            .build()
            .unwrap();

        // Read a couple items
        let _ = reader.next();
        let _ = reader.next();

        // Drop while child is still producing
        drop(reader);
    }
}
