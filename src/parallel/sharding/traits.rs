use std::io::{Seek, Write};

use crate::error::Result;

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
