//! Shard source implementations.
//!
//! The [`Shards`] trait provides indexed access to a collection of shards.
//! Iteration strategies are separate types that wrap a `Shards` implementation:
//!
//! - [`SequentialShardSource`] - finite, in-order iteration with `ExactSizeIterator`
//! - [`RandomRepeatingShardSource`] - infinite, reshuffling iteration
//!
//! # Dynamic Dispatch
//!
//! The `Shards` trait uses dynamic dispatch (no generics) to simplify FFI.
//! Implementations create [`RecordReader`](crate::reader::RecordReader) internally
//! and return a boxed iterator. This means:
//!
//! - No generic type parameter propagation through the codebase
//! - Single Python wrapper class instead of one per source type
//! - `RecordReader<File>` reads directly (no vtable overhead on reads)
//! - Only `Iterator::next()` goes through dynamic dispatch

mod file;
mod memory;
#[cfg(feature = "random")]
mod random;
mod sequential;

#[cfg(test)]
pub(crate) mod tests;

pub use file::FileShards;
pub use memory::MemoryShards;
#[cfg(feature = "random")]
pub use random::RandomRepeatingShardSource;
pub use sequential::SequentialShardSource;

use crate::error::Result;
use crate::tree::reader::Reader;

/// A shard returned by a shard source.
///
/// Contains a record iterator and an identifier for debugging/error messages.
/// The `reader` field is a boxed iterator that yields `Result<Bytes>`.
pub struct Shard {
    /// The record iterator for this shard.
    /// Created by the [`Shards`] implementation using [`RecordReader`](crate::reader::RecordReader).
    pub reader: Reader,

    /// Identifier for this shard (e.g., file path).
    /// Used in error messages to identify which shard failed.
    pub id: String,
}

/// Indexed access to a collection of shards.
///
/// Implementations know how to list and open shards for a specific backend
/// (files, S3, GCS, etc.). The `open` method creates a [`RecordReader`](crate::reader::RecordReader)
/// internally and returns a [`Shard`] containing the boxed iterator.
///
/// Iteration order is controlled separately by wrapping a `Shards` in a strategy
/// like [`SequentialShardSource`] or [`RandomRepeatingShardSource`].
///
/// `open` takes `&self` so multiple iterators can share one index without
/// synchronization.
///
/// # Example
///
/// ```ignore
/// use disky::shard::source::{FileShards, Shards};
///
/// let shards = FileShards::from_prefix("/data/shard_")?;
/// for i in 0..shards.count() {
///     let shard = shards.open(i)?;
///     for record in shard.reader {
///         let bytes = record?;
///         // process bytes
///     }
/// }
/// ```
pub trait Shards: Send + Sync {
    /// The number of shards in this collection.
    fn count(&self) -> usize;

    /// Open the shard at the given index.
    ///
    /// This creates a [`RecordReader`](crate::reader::RecordReader) for the shard
    /// and returns it as a boxed iterator.
    fn open(&self, index: usize) -> Result<Shard>;
}
