//! Memory-based shard sink.

use std::io::Cursor;

use crate::error::Result;
use crate::shard::sink::{Shard, Shards};

/// A memory-based shard factory. Implements [`Shards`].
///
/// Each call to `next` returns a fresh `Cursor<Vec<u8>>`.
/// Useful for testing.
pub struct MemoryShards {
    counter: usize,
}

impl MemoryShards {
    pub fn new() -> Self {
        Self { counter: 0 }
    }
}

impl Default for MemoryShards {
    fn default() -> Self {
        Self::new()
    }
}

impl Shards for MemoryShards {
    type Sink = Cursor<Vec<u8>>;

    fn next(&mut self) -> Result<Shard<Cursor<Vec<u8>>>> {
        let id = format!("memory_shard_{}", self.counter);
        self.counter += 1;

        Ok(Shard {
            sink: Cursor::new(Vec::new()),
            id,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    #[test]
    fn creates_writable_shards() {
        let mut shards = MemoryShards::new();

        let mut shard = shards.next().unwrap();
        shard.sink.write_all(b"hello").unwrap();

        let data = shard.sink.into_inner();
        assert_eq!(data, b"hello");
    }

    #[test]
    fn increments_ids() {
        let mut shards = MemoryShards::new();

        let s0 = shards.next().unwrap();
        let s1 = shards.next().unwrap();
        let s2 = shards.next().unwrap();

        assert_eq!(s0.id, "memory_shard_0");
        assert_eq!(s1.id, "memory_shard_1");
        assert_eq!(s2.id, "memory_shard_2");
    }

    #[test]
    fn each_shard_is_independent() {
        let mut shards = MemoryShards::new();

        let mut s0 = shards.next().unwrap();
        let mut s1 = shards.next().unwrap();

        s0.sink.write_all(b"aaa").unwrap();
        s1.sink.write_all(b"bbb").unwrap();

        assert_eq!(s0.sink.into_inner(), b"aaa");
        assert_eq!(s1.sink.into_inner(), b"bbb");
    }
}
