use std::path::PathBuf;

use tempfile::TempDir;

use crate::writer::RecordWriterConfig;

/// Create test shards with valid disky format containing records.
/// Each shard contains 2 records: "record_{shard_idx}_0" and "record_{shard_idx}_1".
pub fn create_test_shards(dir: &TempDir, prefix: &str, count: usize) -> Vec<PathBuf> {
    (0..count)
        .map(|i| {
            let path = dir.path().join(format!("{}_{}", prefix, i));
            let file = std::fs::File::create(&path).unwrap();
            let mut writer = RecordWriterConfig::new(file).build().unwrap();
            writer
                .write_record(format!("record_{}_0", i).as_bytes())
                .unwrap();
            writer
                .write_record(format!("record_{}_1", i).as_bytes())
                .unwrap();
            writer.close().unwrap();
            path
        })
        .collect()
}
