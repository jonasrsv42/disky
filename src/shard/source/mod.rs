//! Shard source implementations.
//!
//! The [`Shards`] trait provides indexed access to a collection of shards.
//! Iteration strategies are separate types that wrap a `Shards` implementation:
//!
//! - [`SequentialShardSource`] - finite, in-order iteration with `ExactSizeIterator`
//! - [`RandomRepeatingShardSource`] - infinite, reshuffling iteration

mod file;
mod memory;
#[cfg(feature = "sampling")]
mod random;
mod sequential;

#[cfg(test)]
pub(crate) mod tests;

pub use file::FileShards;
pub use memory::MemoryShards;
#[cfg(feature = "sampling")]
pub use random::RandomRepeatingShardSource;
pub use sequential::SequentialShardSource;

use std::io::{Read, Seek};

use crate::error::Result;

/// A shard returned by a shard source.
///
/// Contains the readable source and an identifier for debugging/error messages.
#[derive(Debug)]
pub struct Shard<Source> {
    /// The source to read from.
    pub source: Source,

    /// Identifier for this shard (e.g., file path).
    /// Used in error messages to identify which shard failed.
    pub id: String,
}

/// Indexed access to a collection of shards.
///
/// Implementations know how to list and open shards for a specific backend
/// (files, S3, GCS, etc.). Iteration order is controlled separately by
/// wrapping a `Shards` in a strategy like [`SequentialShardSource`] or
/// [`RandomRepeatingShardSource`].
///
/// `open` takes `&self` so multiple iterators can share one index without
/// synchronization.
pub trait Shards {
    type Source: Read + Seek;

    /// The number of shards in this collection.
    fn count(&self) -> usize;

    /// Open the shard at the given index.
    fn open(&self, index: usize) -> Result<Shard<Self::Source>>;
}
