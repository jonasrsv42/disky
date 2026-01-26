use std::io::{Read, Seek, Write};

use crate::error::Result;

/// Describes the shard count and behavior of a shard locator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardCount {
    /// A finite number of shards - will return `NoMoreShards` after exhausting
    Finite(usize),
    /// A repeating locator with the given number of unique shards - never exhausts
    Repeating(usize),
}

/// A trait defining the common interface for shard creation strategies.
///
/// A Sharder is responsible for providing new sinks (Write + Seek) when requested.
/// The consumer is responsible for wrapping these sinks in appropriate writer types.
///
/// Note: This trait uses `&mut self` to allow implementations to maintain internal
/// state without requiring interior mutability. Consumers that need thread-safe
/// access should wrap the sharder in appropriate synchronization primitives
/// (e.g., `Mutex<S>` where `S: Sharder`).
pub trait Sharder<Sink: Write + Seek + Send + 'static> {
    /// Create a new sink.
    /// This is called when a new shard is needed.
    fn create_sink(&mut self) -> Result<Sink>;
}

/// A shard returned by a `ShardLocator`.
///
/// Contains the source to read from and an identifier for debugging/error messages.
#[derive(Debug)]
pub struct Shard<Source> {
    /// The source to read from.
    pub source: Source,

    /// Identifier for this shard (e.g., file path, ordinal index).
    /// Used in error messages to identify which shard failed.
    pub id: String,
}

/// A trait for locating and opening existing shards for reading.
///
/// This trait provides methods to incrementally retrieve shards for reading.
/// It can be used by parallel readers to locate and open shards created
/// by compatible sharders.
///
/// Note: This trait uses `&mut self` to allow implementations to maintain internal
/// state without requiring interior mutability. Consumers that need thread-safe
/// access should wrap the locator in appropriate synchronization primitives
/// (e.g., `Mutex<L>` where `L: ShardLocator`).
pub trait ShardLocator<Source: Read + Seek + Send + 'static> {
    /// Returns the next available shard.
    ///
    /// This method is called repeatedly to get all available shards.
    /// When no more shards are available, it returns Err(DiskyError::NoMoreShards).
    ///
    /// # Returns
    /// - Ok(shard) if a shard was successfully located and opened
    /// - Err(DiskyError::NoMoreShards) if no more shards are available
    /// - Err(...) if some other error occurred while trying to locate or open a shard
    fn next_shard(&mut self) -> Result<Shard<Source>>;

    /// Returns the shard count and behavior of this locator.
    fn shard_count(&self) -> ShardCount;
}
