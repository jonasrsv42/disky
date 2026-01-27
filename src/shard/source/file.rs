//! File-based shard backend.

use std::fs::File;
use std::path::PathBuf;

use crate::error::{DiskyError, Result};
use crate::shard::source::{Shard, Shards};

/// Returns all files in `dir` whose name starts with `prefix`.
fn expand_pattern(dir: &std::path::Path, prefix: &str) -> Result<Vec<PathBuf>> {
    if prefix.is_empty() {
        return Err(DiskyError::Other(
            "Cannot expand pattern with empty prefix".to_string(),
        ));
    }

    let paths: Vec<_> = std::fs::read_dir(dir)
        .map_err(DiskyError::Io)?
        .filter_map(|entry| {
            let path = entry.ok()?.path();

            // Is a file and filename starts with the prefix.
            let matches = path.is_file() && path.file_name()?.to_str()?.starts_with(prefix);

            // If condition holds we return path else filter.
            matches.then_some(path)
        })
        .collect();

    if paths.is_empty() {
        return Err(DiskyError::Other(format!(
            "No files found matching prefix '{}' in '{}'",
            prefix,
            dir.display()
        )));
    }

    Ok(paths)
}

fn validate_paths(paths: &[PathBuf]) -> Result<()> {
    if paths.is_empty() {
        return Err(DiskyError::Other("No shard paths provided".to_string()));
    }

    for path in paths {
        if !path.exists() {
            return Err(DiskyError::Other(format!(
                "Shard file does not exist: {}",
                path.display()
            )));
        }
    }

    Ok(())
}

/// A collection of file-based shards. Implements [`Shards`].
///
/// Construct from explicit paths with `new`, a path prefix with `from_prefix`,
/// or a directory + prefix with `from_pattern`.
pub struct FileShards {
    paths: Vec<PathBuf>,
}

impl FileShards {
    /// Create from an explicit list of file paths.
    pub fn new(paths: Vec<PathBuf>) -> Result<Self> {
        validate_paths(&paths)?;
        Ok(Self { paths })
    }

    /// Create by matching all files starting with the given path prefix.
    ///
    /// The last component of the path is used as the filename prefix.
    /// For example, `/tmp/data/shard_` matches `shard_0`, `shard_1`, `shard_train`, etc.
    /// Results are returned in sorted order.
    pub fn from_prefix(prefix: impl AsRef<std::path::Path>) -> Result<Self> {
        let prefix = prefix.as_ref();
        let dir = prefix.parent().ok_or_else(|| {
            DiskyError::Other(format!(
                "Prefix has no parent directory: {}",
                prefix.display()
            ))
        })?;
        let file_prefix = prefix.file_name().and_then(|n| n.to_str()).ok_or_else(|| {
            DiskyError::Other(format!(
                "Prefix has no file component: {}",
                prefix.display()
            ))
        })?;
        Self::from_pattern(dir.to_path_buf(), file_prefix)
    }

    /// Create by discovering files with the given prefix in a directory.
    /// Results are returned in sorted order.
    pub fn from_pattern(dir: PathBuf, prefix: impl Into<String>) -> Result<Self> {
        let prefix = prefix.into();
        if prefix.is_empty() {
            return Err(DiskyError::Other(
                "Cannot expand pattern with empty prefix".to_string(),
            ));
        }
        let mut paths = expand_pattern(&dir, &prefix)?;
        // We sort because pattern expansion cannot guarantee order across file system
        // implementations. To ensure determinism we sort.
        //
        // To avoid the sorted path one can initiate directly with `new`.
        paths.sort();
        Self::new(paths)
    }
}

impl Shards for FileShards {
    type Source = File;

    fn count(&self) -> usize {
        self.paths.len()
    }

    fn open(&self, index: usize) -> Result<Shard<File>> {
        if index >= self.paths.len() {
            return Err(DiskyError::Other(format!(
                "Cannot open shard at index {} when there is only {} shards",
                index,
                self.paths.len()
            )));
        }
        let path = &self.paths[index];
        let id = path.display().to_string();
        let source = File::open(path).map_err(DiskyError::Io)?;
        Ok(Shard { source, id })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shard::source::tests::create_test_shards;
    use tempfile::TempDir;

    #[test]
    fn new_opens_all() {
        let dir = TempDir::new().unwrap();
        let paths = create_test_shards(&dir, "data", 3);

        let shards = FileShards::new(paths).unwrap();
        assert_eq!(shards.count(), 3);

        for i in 0..3 {
            assert!(shards.open(i).is_ok());
        }
    }

    #[test]
    fn new_empty_paths_error() {
        let result = FileShards::new(vec![]);
        assert!(matches!(result, Err(DiskyError::Other(msg)) if msg.contains("No shard paths")));
    }

    #[test]
    fn new_nonexistent_path_error() {
        let result = FileShards::new(vec!["/nonexistent/path".into()]);
        assert!(matches!(result, Err(DiskyError::Other(msg)) if msg.contains("does not exist")));
    }

    #[test]
    fn from_prefix_finds_matching() {
        let dir = TempDir::new().unwrap();
        create_test_shards(&dir, "shard", 3);

        let prefix = dir.path().join("shard_");
        let shards = FileShards::from_prefix(&prefix).unwrap();
        assert_eq!(shards.count(), 3);
    }

    #[test]
    fn from_prefix_ignores_non_matching() {
        let dir = TempDir::new().unwrap();
        create_test_shards(&dir, "shard", 2);
        std::fs::File::create(dir.path().join("other_file")).unwrap();

        let prefix = dir.path().join("shard_");
        let shards = FileShards::from_prefix(&prefix).unwrap();
        assert_eq!(shards.count(), 2);
    }

    #[test]
    fn from_prefix_no_match() {
        let dir = TempDir::new().unwrap();
        let prefix = dir.path().join("nonexistent_");
        let result = FileShards::from_prefix(&prefix);
        assert!(matches!(result, Err(DiskyError::Other(msg)) if msg.contains("No files found")));
    }

    #[test]
    fn from_pattern_finds_matching() {
        let dir = TempDir::new().unwrap();
        create_test_shards(&dir, "shard", 3);

        let shards = FileShards::from_pattern(dir.path().to_path_buf(), "shard").unwrap();
        // "shard" matches shard_0, shard_1, shard_2
        assert_eq!(shards.count(), 3);
    }

    #[test]
    fn from_pattern_sorted_order() {
        let dir = TempDir::new().unwrap();
        create_test_shards(&dir, "shard", 3);

        let shards = FileShards::from_pattern(dir.path().to_path_buf(), "shard").unwrap();
        let ids: Vec<_> = (0..3).map(|i| shards.open(i).unwrap().id).collect();

        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(ids, sorted);
    }

    #[test]
    fn from_pattern_no_shards_found() {
        let dir = TempDir::new().unwrap();
        let result = FileShards::from_pattern(dir.path().to_path_buf(), "nonexistent");
        assert!(matches!(result, Err(DiskyError::Other(msg)) if msg.contains("No files found")));
    }

    #[test]
    fn from_pattern_empty_prefix_error() {
        let dir = TempDir::new().unwrap();
        let result = FileShards::from_pattern(dir.path().to_path_buf(), "");
        assert!(matches!(result, Err(DiskyError::Other(msg)) if msg.contains("empty prefix")));
    }

    #[test]
    fn open_returns_readable_file() {
        let dir = TempDir::new().unwrap();
        create_test_shards(&dir, "data", 1);

        let shards = FileShards::new(vec![dir.path().join("data_0")]).unwrap();
        let shard = shards.open(0).unwrap();

        use std::io::Read;
        let mut contents = String::new();
        let mut source = shard.source;
        source.read_to_string(&mut contents).unwrap();
        assert_eq!(contents, "shard 0 data");
    }
}
