//! Memory-based shard backend.

use std::io::Cursor;

use crate::error::Result;
use crate::shard::source::{Shard, Shards};

/// A collection of memory-based shards. Implements [`Shards`].
///
/// Uses a factory function to create shard sources on demand.
/// Useful for testing or when data is already in memory.
pub struct MemoryShards<F>
where
    F: Fn(usize) -> Result<Vec<u8>>,
{
    factory: F,
    count: usize,
}

impl<F> MemoryShards<F>
where
    F: Fn(usize) -> Result<Vec<u8>>,
{
    /// Create a new MemoryShards with the given factory function and count.
    ///
    /// # Arguments
    /// * `factory` - Function that creates shard data for a given index
    /// * `count` - Number of shards
    pub fn new(factory: F, count: usize) -> Self {
        Self { factory, count }
    }
}

impl<F> Shards for MemoryShards<F>
where
    F: Fn(usize) -> Result<Vec<u8>>,
{
    type Source = Cursor<Vec<u8>>;

    fn count(&self) -> usize {
        self.count
    }

    fn open(&self, index: usize) -> Result<Shard<Cursor<Vec<u8>>>> {
        let data = (self.factory)(index)?;
        Ok(Shard {
            source: Cursor::new(data),
            id: format!("memory_shard_{}", index),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;

    #[test]
    fn opens_all() {
        let shards = MemoryShards::new(|i| Ok(format!("data {}", i).into_bytes()), 3);
        assert_eq!(shards.count(), 3);

        for i in 0..3 {
            assert!(shards.open(i).is_ok());
        }
    }

    #[test]
    fn returns_correct_data() {
        let shards = MemoryShards::new(|i| Ok(format!("shard {} data", i).into_bytes()), 2);

        let shard = shards.open(0).unwrap();
        let mut contents = String::new();
        let mut source = shard.source;
        source.read_to_string(&mut contents).unwrap();
        assert_eq!(contents, "shard 0 data");
    }

    #[test]
    fn has_correct_id() {
        let shards = MemoryShards::new(|_| Ok(vec![]), 1);
        let shard = shards.open(0).unwrap();
        assert_eq!(shard.id, "memory_shard_0");
    }

    #[test]
    fn reopens_same_index() {
        let shards = MemoryShards::new(|i| Ok(format!("data {}", i).into_bytes()), 1);

        let s1 = shards.open(0).unwrap();
        let s2 = shards.open(0).unwrap();

        let read = |mut s: Cursor<Vec<u8>>| {
            let mut out = String::new();
            s.read_to_string(&mut out).unwrap();
            out
        };

        assert_eq!(read(s1.source), read(s2.source));
    }
}
