//! Shard readers that compose as `Iterator<Item = Result<Bytes>>`.
//!
//! - [`SequentialShardReader`] — drains each shard fully before moving to the next
//! - [`RoundRobinShardReader`] — interleaves reads across multiple active shards
//!
//! Each reader has a corresponding config builder that implements [`Node`](crate::tree::reader::Node)
//! for use in reader trees:
//!
//! - [`SequentialShardReaderConfig`]
//! - [`RoundRobinShardReaderConfig`]

mod round_robin;
mod sequential;

pub use round_robin::{RoundRobinShardReader, RoundRobinShardReaderConfig};
pub use sequential::{SequentialShardReader, SequentialShardReaderConfig};
