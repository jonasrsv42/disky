//! Block-level functionality for Riegeli files.
//!
//! This module contains the low-level building blocks for the Riegeli file format.
//! At the block level, Riegeli divides files into fixed-size blocks, with block headers
//! inserted at block boundaries (by default, every 64 KiB).
//!
//! # Key Components
//!
//! - [`writer::BlockWriter`]: Handles writing data with automatic insertion of block headers
//!   at block boundaries.
//! - [`writer::BlockWriterConfig`]: Configuration options for block writers.
//!
//! # Block Structure
//!
//! Each block in a Riegeli file starts with a 24-byte header at block boundaries:
//!
//! ```text
//! +---------------+----------------+----------------+
//! |  header_hash  | previous_chunk |   next_chunk   |
//! |    (8 bytes)  |    (8 bytes)   |    (8 bytes)   |
//! +---------------+----------------+----------------+
//! ```
//!
//! - `header_hash`: A 64-bit HighwayHash of the rest of the header
//! - `previous_chunk`: Distance from chunk beginning to block boundary 
//! - `next_chunk`: Distance from block boundary to chunk end

pub mod writer;
pub mod reader;

#[cfg(test)]
mod tests;
