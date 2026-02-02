//! Sequential shard iteration.

use std::iter::FusedIterator;

use crate::error::Result;
use crate::shard::source::{Shard, Shards};

/// Iterates over shards in order, 0..count. Finite.
///
/// Implements `ExactSizeIterator` so consumers can query remaining count.
pub struct SequentialShardSource {
    shards: Box<dyn Shards>,
    next_index: usize,
}

impl SequentialShardSource {
    pub fn new(shards: impl Shards + 'static) -> Self {
        Self {
            shards: Box::new(shards),
            next_index: 0,
        }
    }
}

impl Iterator for SequentialShardSource {
    type Item = Result<Shard>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_index >= self.shards.count() {
            return None;
        }

        let index = self.next_index;
        self.next_index += 1;

        Some(self.shards.open(index))
    }
}

impl FusedIterator for SequentialShardSource {}

impl ExactSizeIterator for SequentialShardSource {
    fn len(&self) -> usize {
        self.shards.count() - self.next_index
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::shard::source::FileShards;
    use crate::shard::source::tests::create_test_shards;

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
    fn shards_return_records() {
        let dir = TempDir::new().unwrap();
        create_test_shards(&dir, "shard", 1);

        let shards = FileShards::from_pattern(dir.path().to_path_buf(), "shard").unwrap();
        let mut source = SequentialShardSource::new(shards);

        let shard = source.next().unwrap().unwrap();
        let records: Vec<_> = shard.reader.map(|r| r.unwrap()).collect();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].as_ref(), b"record_0_0");
        assert_eq!(records[1].as_ref(), b"record_0_1");
    }
}
