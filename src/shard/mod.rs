//! Shard module for reading from and writing to sharded data.
//!
//! - `source` - Shard backends and iteration strategies
//! - `reader` - Shard readers that compose as `Iterator<Item = Result<Bytes>>`

pub mod reader;
pub mod source;

pub use reader::{RoundRobinShardReader, SequentialShardReader};
pub use source::{Shard, Shards};
