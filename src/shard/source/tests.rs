use std::io::Write;
use std::path::PathBuf;

use tempfile::TempDir;

pub fn create_test_shards(dir: &TempDir, prefix: &str, count: usize) -> Vec<PathBuf> {
    (0..count)
        .map(|i| {
            let path = dir.path().join(format!("{}_{}", prefix, i));
            let mut file = std::fs::File::create(&path).unwrap();
            write!(file, "shard {} data", i).unwrap();
            path
        })
        .collect()
}
