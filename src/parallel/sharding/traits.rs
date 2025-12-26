use std::io::{Read, Seek, Write};

use crate::error::Result;

/// A trait defining the common interface for shard creation strategies.
///
/// A Sharder is responsible for providing new sinks (Write + Seek) when requested.
/// The consumer is responsible for wrapping these sinks in appropriate writer types.
pub trait Sharder<Sink: Write + Seek + Send + 'static> {
    /// Create a new sink.
    /// This is called when a new shard is needed.
    fn create_sink(&self) -> Result<Sink>;
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
pub trait ShardLocator<Source: Read + Seek + Send + 'static> {
    /// Returns the next available shard.
    ///
    /// This method is called repeatedly to get all available shards.
    /// When no more shards are available, it returns Err(DiskyError::NoMoreShards).
    ///
    /// This method is thread-safe and does not require mutable access to self.
    ///
    /// # Returns
    /// - Ok(shard) if a shard was successfully located and opened
    /// - Err(DiskyError::NoMoreShards) if no more shards are available
    /// - Err(...) if some other error occurred while trying to locate or open a shard
    fn next_shard(&self) -> Result<Shard<Source>>;

    /// Returns the estimated total number of shards, if known.
    ///
    /// This is an optional method that can provide a hint about the total
    /// number of shards that might be available. The actual number might
    /// differ if shards are added or removed during reading.
    ///
    /// # Returns
    /// Some(count) if the count is known, None otherwise.
    fn estimated_shard_count(&self) -> Option<usize> {
        None
    }
}
