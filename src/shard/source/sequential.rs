//! Sequential shard iteration.

use std::iter::FusedIterator;

use crate::error::Result;
use crate::shard::source::{Shard, Shards};

/// Iterates over shards in order, 0..count. Finite.
///
/// Implements `ExactSizeIterator` so consumers can query remaining count.
pub struct SequentialShardSource<ShardsType: Shards> {
    shards: ShardsType,
    next_index: usize,
}

impl<ShardsType: Shards> SequentialShardSource<ShardsType> {
    pub fn new(shards: ShardsType) -> Self {
        Self {
            shards,
            next_index: 0,
        }
    }
}

impl<ShardsType: Shards> Iterator for SequentialShardSource<ShardsType> {
    type Item = Result<Shard<ShardsType::Source>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_index >= self.shards.count() {
            return None;
        }

        let index = self.next_index;
        self.next_index += 1;

        Some(self.shards.open(index))
    }
}

impl<ShardsType: Shards> FusedIterator for SequentialShardSource<ShardsType> {}

impl<ShardsType: Shards> ExactSizeIterator for SequentialShardSource<ShardsType> {
    fn len(&self) -> usize {
        self.shards.count() - self.next_index
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use tempfile::TempDir;

    use crate::shard::source::FileShards;
    use crate::shard::source::tests::create_test_shards;

    use super::*;

    #[test]
    fn iterates_all_in_order() {
        let dir = TempDir::new().unwrap();
        create_test_shards(&dir, "shard", 3);

        let shards = FileShards::from_pattern(dir.path().to_path_buf(), "shard").unwrap();
        let source = SequentialShardSource::new(shards);

        let ids: Vec<_> = source.map(|s| s.unwrap().id).collect();
        assert_eq!(ids.len(), 3);

        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(ids, sorted);
    }

    #[test]
    fn exact_size_decreases() {
        let dir = TempDir::new().unwrap();
        create_test_shards(&dir, "shard", 5);

        let shards = FileShards::from_pattern(dir.path().to_path_buf(), "shard").unwrap();
        let mut source = SequentialShardSource::new(shards);
        assert_eq!(source.len(), 5);

        source.next();
        assert_eq!(source.len(), 4);

        source.next();
        source.next();
        assert_eq!(source.len(), 2);
    }

    #[test]
    fn returns_none_when_exhausted() {
        let dir = TempDir::new().unwrap();
        create_test_shards(&dir, "shard", 1);

        let shards = FileShards::from_pattern(dir.path().to_path_buf(), "shard").unwrap();
        let mut source = SequentialShardSource::new(shards);

        assert!(source.next().is_some());
        assert!(source.next().is_none());
        assert!(source.next().is_none());
    }

    #[test]
    fn shards_are_readable() {
        let dir = TempDir::new().unwrap();
        create_test_shards(&dir, "shard", 1);

        let shards = FileShards::from_pattern(dir.path().to_path_buf(), "shard").unwrap();
        let mut source = SequentialShardSource::new(shards);

        let shard = source.next().unwrap().unwrap();
        let mut contents = String::new();
        let mut file = shard.source;
        file.read_to_string(&mut contents).unwrap();
        assert_eq!(contents, "shard 0 data");
    }
}
