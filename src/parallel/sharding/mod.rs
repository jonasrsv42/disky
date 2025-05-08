//! Sharding module for handling distribution of data across multiple files or memory buffers.
//!
//! This module provides traits and implementations for creating and locating shards,
//! which are individual storage units (files or memory) that can be written to or read from.
//! Sharding is particularly useful for parallel processing and handling large datasets.

mod traits;
mod utils;
mod auto_sharder;
mod file_sharder;
mod file_locator;
mod random_locator;
mod memory_locator;

// Re-export the main types for easier access
pub use traits::{Sharder, ShardLocator};
pub use auto_sharder::Autosharder;
pub use file_sharder::{FileSharder, FileSharderConfig};
pub use file_locator::{FileShardLocator, MultiPathShardLocator};
pub use random_locator::{RandomRepeatingFileShardLocator, RandomMultiPathShardLocator};
pub use memory_locator::MemoryShardLocator;