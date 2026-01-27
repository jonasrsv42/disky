//! Shard readers that compose as `Iterator<Item = Result<Bytes>>`.
//!
//! - [`SequentialShardReader`] — drains each shard fully before moving to the next
//! - [`RoundRobinShardReader`] — interleaves reads across multiple active shards

mod round_robin;
mod sequential;

pub use round_robin::RoundRobinShardReader;
pub use sequential::SequentialShardReader;
