//! Random repeating shard iteration.

use std::iter::FusedIterator;

use rand::rngs::StdRng;
use rand::{SeedableRng, seq::SliceRandom};

use crate::error::Result;
use crate::shard::source::{Shard, Shards};

/// Iterates over shards in random order, reshuffling after each full pass.
/// Never returns None - infinite iterator.
pub struct RandomRepeatingShardSource<ShardsType: Shards> {
    shards: ShardsType,
    indices: Vec<usize>,
    position: usize,
    rng: StdRng,
}

impl<ShardsType: Shards> RandomRepeatingShardSource<ShardsType> {
    /// Create with a random seed.
    pub fn new(shards: ShardsType) -> Self {
        let mut rng = StdRng::from_entropy();
        let mut indices: Vec<usize> = (0..shards.count()).collect();
        indices.shuffle(&mut rng);

        Self {
            shards,
            indices,
            position: 0,
            rng,
        }
    }

    /// Create with a specific seed for reproducible ordering.
    pub fn with_seed(shards: ShardsType, seed: u64) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut indices: Vec<usize> = (0..shards.count()).collect();
        indices.shuffle(&mut rng);

        Self {
            shards,
            indices,
            position: 0,
            rng,
        }
    }
}

impl<ShardsType: Shards> Iterator for RandomRepeatingShardSource<ShardsType> {
    type Item = Result<Shard<ShardsType::Source>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.indices.len() {
            self.position = 0;
            self.indices.shuffle(&mut self.rng);
        }

        let index = self.indices[self.position];
        self.position += 1;

        Some(self.shards.open(index))
    }
}

impl<ShardsType: Shards> FusedIterator for RandomRepeatingShardSource<ShardsType> {}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use tempfile::TempDir;

    use crate::shard::source::FileShards;
    use crate::shard::source::tests::create_test_shards;

    use super::*;

    #[test]
    fn never_returns_none() {
        let dir = TempDir::new().unwrap();
        create_test_shards(&dir, "shard", 3);

        let shards = FileShards::from_pattern(dir.path().to_path_buf(), "shard").unwrap();
        let mut source = RandomRepeatingShardSource::with_seed(shards, 42);

        for _ in 0..20 {
            assert!(source.next().unwrap().is_ok());
        }
    }

    #[test]
    fn visits_all_per_epoch() {
        let dir = TempDir::new().unwrap();
        create_test_shards(&dir, "shard", 5);

        let shards = FileShards::from_pattern(dir.path().to_path_buf(), "shard").unwrap();
        let mut source = RandomRepeatingShardSource::with_seed(shards, 42);

        let ids: HashSet<_> = (0..5).map(|_| source.next().unwrap().unwrap().id).collect();
        assert_eq!(ids.len(), 5);
    }

    #[test]
    fn seeded_is_deterministic() {
        let dir = TempDir::new().unwrap();
        create_test_shards(&dir, "shard", 5);

        let shards1 = FileShards::from_pattern(dir.path().to_path_buf(), "shard").unwrap();
        let shards2 = FileShards::from_pattern(dir.path().to_path_buf(), "shard").unwrap();

        let mut source1 = RandomRepeatingShardSource::with_seed(shards1, 42);
        let mut source2 = RandomRepeatingShardSource::with_seed(shards2, 42);

        let ids1: Vec<_> = (0..10)
            .map(|_| source1.next().unwrap().unwrap().id)
            .collect();
        let ids2: Vec<_> = (0..10)
            .map(|_| source2.next().unwrap().unwrap().id)
            .collect();
        assert_eq!(ids1, ids2);
    }

    #[test]
    fn reshuffles_after_epoch() {
        let dir = TempDir::new().unwrap();
        create_test_shards(&dir, "shard", 5);

        let shards = FileShards::from_pattern(dir.path().to_path_buf(), "shard").unwrap();
        let mut source = RandomRepeatingShardSource::with_seed(shards, 42);

        let epoch1: Vec<_> = (0..5).map(|_| source.next().unwrap().unwrap().id).collect();
        let epoch2: Vec<_> = (0..5).map(|_| source.next().unwrap().unwrap().id).collect();

        // Both epochs visit all shards
        let set1: HashSet<_> = epoch1.iter().collect();
        let set2: HashSet<_> = epoch2.iter().collect();
        assert_eq!(set1.len(), 5);
        assert_eq!(set2.len(), 5);

        // Seed 42 produces different orderings across epochs
        assert_ne!(epoch1, epoch2);
    }
}
