//! Shard module for reading from and writing to sharded data.
//!
//! - `source` - Shard backends and iteration strategies for reading
//! - `sink` - Shard backends for writing
//! - `reader` - Shard readers that compose as `Iterator<Item = Result<Bytes>>`
//! - `writer` - Shard writers`

pub mod reader;
pub mod sink;
pub mod source;
pub mod writer;

pub use reader::{RoundRobinShardReader, SequentialShardReader};
pub use source::{Shard, Shards};
pub use writer::{SequentialShardWriter, SequentialShardWriterConfig};
