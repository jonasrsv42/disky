//! Shard sink implementations.
//!
//! The [`Shards`] trait provides writable shards on demand via
//! `next(&mut self)`.
//!
//! Unlike [`source::Shards`](crate::shard::source::Shards) which gives indexed
//! access to a fixed collection, sink `Shards` is a factory — each call to
//! `next` yields the next shard to write into.

mod closure;
mod file;
mod memory;

pub use closure::ClosureShards;
pub use file::{FileShards, FileShardsBuilder};
pub use memory::MemoryShards;

use std::io::{Seek, Write};

use crate::error::Result;

/// A shard returned by a shard sink.
///
/// Contains the writable sink and an identifier for debugging/error messages.
pub struct Shard<Sink> {
    /// The sink to write into.
    pub sink: Sink,

    /// Identifier for this shard (e.g., file path).
    /// Used in error messages to identify which shard failed.
    pub id: String,
}

/// Producer of writable shards.
///
/// Each call to [`next`](Shards::next) yields a shard to write into.
/// The trait is deliberately agnostic about *how* the shard is obtained —
/// implementations may create new files, open existing files for appending,
/// overwrite existing files, start cloud multipart uploads, etc. Consumers
/// must not assume any particular strategy; that responsibility belongs to
/// the implementation and its constructor.
///
/// `next` takes `&mut self` because producing a shard typically mutates
/// internal state (e.g., incrementing a counter). Consumers that need
/// thread-safe access should wrap in `Mutex`.
pub trait Shards {
    type Sink: Write + Seek;

    /// Yield the next shard to write into.
    fn next(&mut self) -> Result<Shard<Self::Sink>>;
}
