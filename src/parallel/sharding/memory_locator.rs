use std::io::Cursor;
// The Cursor type implements the required Read+Seek trait bounds
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::error::{DiskyError, Result};
use crate::parallel::sharding::traits::ShardLocator;

/// A memory-based shard locator that can be used for testing.
///
/// This locator provides in-memory Cursors as shards, which is useful for
/// tests or when data is already in memory.
pub struct MemoryShardLocator<F>
where
    F: Fn() -> Result<Cursor<Vec<u8>>> + Send + Sync + 'static,
{
    /// Function that creates shard sources
    source_factory: F,

    /// Number of shards to create
    shard_count: usize,

    /// Index of the next shard to return
    next_index: AtomicUsize,
}

impl<F> MemoryShardLocator<F>
where
    F: Fn() -> Result<Cursor<Vec<u8>>> + Send + Sync + 'static,
{
    /// Create a new MemoryShardLocator with the given factory function.
    ///
    /// # Arguments
    /// * `source_factory` - Function that creates cursor sources
    /// * `shard_count` - Number of shards to create
    ///
    /// # Returns
    /// A new MemoryShardLocator instance
    pub fn new(source_factory: F, shard_count: usize) -> Self {
        Self {
            source_factory,
            shard_count,
            next_index: AtomicUsize::new(0),
        }
    }
}

impl<F> ShardLocator<Cursor<Vec<u8>>> for MemoryShardLocator<F>
where
    F: Fn() -> Result<Cursor<Vec<u8>>> + Send + Sync + 'static,
{
    fn next_shard(&self) -> Result<Cursor<Vec<u8>>> {
        // Get the current index and increment it atomically
        let index = self.next_index.fetch_add(1, Ordering::SeqCst);

        // Check if we have any more shards
        if index >= self.shard_count {
            return Err(DiskyError::NoMoreShards);
        }

        // Create a new cursor
        (self.source_factory)()
    }

    fn estimated_shard_count(&self) -> Option<usize> {
        // Return the total count of shards
        Some(self.shard_count)
    }
}
