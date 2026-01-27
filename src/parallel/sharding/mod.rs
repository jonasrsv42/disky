//! Sharding module for handling distribution of data across multiple files or memory buffers.
//!
//! This module provides traits and implementations for creating shards,
//! which are individual storage units (files or memory) that can be written to.

mod auto_sharder;
mod file_sharder;
mod traits;

pub use auto_sharder::Autosharder;
pub use file_sharder::{FileSharder, FileSharderConfig};
pub use traits::Sharder;
