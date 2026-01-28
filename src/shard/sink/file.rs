//! File-based shard sink.

use std::fs::File;
use std::path::PathBuf;

use crate::error::{DiskyError, Result};
use crate::shard::sink::{Shard, Shards};

/// Write mode for [`FileShardsBuilder`].
enum Mode {
    /// Start at index 0, overwriting any existing files.
    Overwrite,
    /// Scan the directory for existing shards and start after the highest index.
    Append,
}

/// Builder for [`FileShards`].
///
/// # Examples
///
/// ```no_run
/// use disky::shard::sink::FileShardsBuilder;
///
/// // Fresh write — shard_0, shard_1, ...
/// let shards = FileShardsBuilder::new("/tmp/data", "shard").build()?;
///
/// // From a combined prefix path
/// let shards = FileShardsBuilder::from_prefix("/tmp/data/shard")?.build()?;
///
/// // Append after existing shards
/// let shards = FileShardsBuilder::new("/tmp/data", "shard").append().build()?;
/// # Ok::<(), disky::error::DiskyError>(())
/// ```
pub struct FileShardsBuilder {
    dir: PathBuf,
    prefix: String,
    mode: Mode,
}

impl FileShardsBuilder {
    /// Create a builder with an explicit directory and prefix.
    pub fn new(dir: impl Into<PathBuf>, prefix: impl Into<String>) -> Self {
        Self {
            dir: dir.into(),
            prefix: prefix.into(),
            mode: Mode::Overwrite,
        }
    }

    /// Create a builder from a path whose last component is the prefix.
    ///
    /// For example, `/tmp/data/shard` splits into dir `/tmp/data` and
    /// prefix `shard`.
    pub fn from_prefix(prefix: impl AsRef<std::path::Path>) -> Result<Self> {
        let prefix = prefix.as_ref();
        if prefix.is_dir() {
            return Err(DiskyError::Other(format!(
                "Path is a directory, not a prefix: '{}'. \
                 Pass a prefix like '{}/shard' instead.",
                prefix.display(),
                prefix.display()
            )));
        }
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
        Ok(Self::new(dir, file_prefix))
    }

    /// Append after existing shards instead of overwriting from index 0.
    ///
    /// On [`build`](Self::build), the directory is scanned for files matching
    /// `{prefix}_{N}` and the counter starts at `max(N) + 1`.
    /// If no matching files exist, starts at 0.
    pub fn append(mut self) -> Self {
        self.mode = Mode::Append;
        self
    }

    /// Build the [`FileShards`].
    ///
    /// Creates the output directory if it doesn't exist.
    /// Fails if the prefix is empty, the directory cannot be created,
    /// or append mode cannot scan the directory.
    pub fn build(self) -> Result<FileShards> {
        if self.prefix.is_empty() {
            return Err(DiskyError::Other("Prefix must not be empty".to_string()));
        }

        let counter = match self.mode {
            Mode::Overwrite => 0,
            Mode::Append => find_next_index(&self.dir, &self.prefix)?,
        };

        std::fs::create_dir_all(&self.dir).map_err(DiskyError::Io)?;

        Ok(FileShards {
            dir: self.dir,
            prefix: self.prefix,
            counter,
        })
    }
}

/// Scan `dir` for files named `{prefix}_{N}` and return `max(N) + 1`.
/// Returns 0 if the directory doesn't exist or contains no matching files.
fn find_next_index(dir: &std::path::Path, prefix: &str) -> Result<usize> {
    if !dir.exists() {
        return Ok(0);
    }

    let expected_prefix = format!("{}_", prefix);
    let max_index = std::fs::read_dir(dir)
        .map_err(DiskyError::Io)?
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            let name = path.file_name()?.to_str()?;
            let suffix = name.strip_prefix(&expected_prefix)?;
            suffix.parse::<usize>().ok()
        })
        .max();

    Ok(max_index.map_or(0, |m| m + 1))
}

/// A file-based shard factory. Implements [`Shards`].
///
/// Creates sequentially numbered files: `{prefix}_{0}`, `{prefix}_{1}`, etc.
///
/// Construct via [`FileShardsBuilder`].
pub struct FileShards {
    dir: PathBuf,
    prefix: String,
    counter: usize,
}

impl FileShards {
    fn next_path(&mut self) -> PathBuf {
        let index = self.counter;
        self.counter += 1;
        self.dir.join(format!("{}_{}", self.prefix, index))
    }
}

impl Shards for FileShards {
    type Sink = File;

    fn next(&mut self) -> Result<Shard<File>> {
        let path = self.next_path();
        let id = path.display().to_string();
        let sink = File::create(&path).map_err(DiskyError::Io)?;

        Ok(Shard { sink, id })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::TempDir;

    use super::*;

    #[test]
    fn build_creates_sequential_files() {
        let dir = TempDir::new().unwrap();
        let mut shards = FileShardsBuilder::new(dir.path(), "shard").build().unwrap();

        let s0 = shards.next().unwrap();
        let s1 = shards.next().unwrap();
        let s2 = shards.next().unwrap();

        assert!(s0.id.contains("shard_0"));
        assert!(s1.id.contains("shard_1"));
        assert!(s2.id.contains("shard_2"));
    }

    #[test]
    fn build_from_prefix() {
        let dir = TempDir::new().unwrap();
        let prefix_path = dir.path().join("shard");
        let mut shards = FileShardsBuilder::from_prefix(&prefix_path)
            .unwrap()
            .build()
            .unwrap();

        let s = shards.next().unwrap();
        assert!(s.id.contains("shard_0"));
        assert!(dir.path().join("shard_0").exists());
    }

    #[test]
    fn from_prefix_invalid_path_errors() {
        let result = FileShardsBuilder::from_prefix("/");
        assert!(result.is_err());
    }

    #[test]
    fn from_prefix_directory_errors() {
        let dir = TempDir::new().unwrap();
        let result = FileShardsBuilder::from_prefix(dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn files_are_writable() {
        let dir = TempDir::new().unwrap();
        let mut shards = FileShardsBuilder::new(dir.path(), "data").build().unwrap();

        let mut shard = shards.next().unwrap();
        shard.sink.write_all(b"hello").unwrap();
        drop(shard);

        let contents = std::fs::read_to_string(dir.path().join("data_0")).unwrap();
        assert_eq!(contents, "hello");
    }

    #[test]
    fn creates_directory_if_missing() {
        let dir = TempDir::new().unwrap();
        let nested = dir.path().join("sub").join("dir");
        let mut shards = FileShardsBuilder::new(&nested, "shard").build().unwrap();

        let shard = shards.next().unwrap();
        assert!(shard.id.contains("shard_0"));
        assert!(nested.join("shard_0").exists());
    }

    #[test]
    fn each_shard_has_unique_id() {
        let dir = TempDir::new().unwrap();
        let mut shards = FileShardsBuilder::new(dir.path(), "shard").build().unwrap();

        let ids: Vec<_> = (0..5).map(|_| shards.next().unwrap().id).collect();
        let unique: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(ids.len(), unique.len());
    }

    #[test]
    fn append_no_existing_starts_at_zero() {
        let dir = TempDir::new().unwrap();
        let mut shards = FileShardsBuilder::new(dir.path(), "shard")
            .append()
            .build()
            .unwrap();

        let s = shards.next().unwrap();
        assert!(s.id.contains("shard_0"));
    }

    #[test]
    fn append_starts_after_highest() {
        let dir = TempDir::new().unwrap();
        // Create some existing shards
        std::fs::write(dir.path().join("shard_0"), b"data").unwrap();
        std::fs::write(dir.path().join("shard_1"), b"data").unwrap();
        std::fs::write(dir.path().join("shard_2"), b"data").unwrap();

        let mut shards = FileShardsBuilder::new(dir.path(), "shard")
            .append()
            .build()
            .unwrap();

        let s = shards.next().unwrap();
        assert!(s.id.contains("shard_3"));
    }

    #[test]
    fn append_handles_gaps() {
        let dir = TempDir::new().unwrap();
        // shard_0, shard_5 — gap in the middle
        std::fs::write(dir.path().join("shard_0"), b"data").unwrap();
        std::fs::write(dir.path().join("shard_5"), b"data").unwrap();

        let mut shards = FileShardsBuilder::new(dir.path(), "shard")
            .append()
            .build()
            .unwrap();

        let s = shards.next().unwrap();
        assert!(s.id.contains("shard_6"));
    }

    #[test]
    fn append_ignores_non_matching_files() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("shard_0"), b"data").unwrap();
        std::fs::write(dir.path().join("other_file"), b"data").unwrap();
        std::fs::write(dir.path().join("shard_notes.txt"), b"data").unwrap();

        let mut shards = FileShardsBuilder::new(dir.path(), "shard")
            .append()
            .build()
            .unwrap();

        let s = shards.next().unwrap();
        assert!(s.id.contains("shard_1"));
    }

    #[test]
    fn append_nonexistent_dir_starts_at_zero() {
        let dir = TempDir::new().unwrap();
        let missing = dir.path().join("does_not_exist");

        let mut shards = FileShardsBuilder::new(&missing, "shard")
            .append()
            .build()
            .unwrap();

        let s = shards.next().unwrap();
        assert!(s.id.contains("shard_0"));
    }

    #[test]
    fn empty_prefix_errors() {
        let dir = TempDir::new().unwrap();
        let result = FileShardsBuilder::new(dir.path(), "").build();
        assert!(result.is_err());
    }

    #[test]
    fn overwrite_replaces_existing_file() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("shard_0"), b"old data").unwrap();

        let mut shards = FileShardsBuilder::new(dir.path(), "shard").build().unwrap();
        let mut shard = shards.next().unwrap();
        shard.sink.write_all(b"new").unwrap();
        drop(shard);

        let contents = std::fs::read_to_string(dir.path().join("shard_0")).unwrap();
        assert_eq!(contents, "new");
    }

    #[test]
    fn from_prefix_append_combined() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("shard_0"), b"data").unwrap();
        std::fs::write(dir.path().join("shard_1"), b"data").unwrap();

        let prefix_path = dir.path().join("shard");
        let mut shards = FileShardsBuilder::from_prefix(&prefix_path)
            .unwrap()
            .append()
            .build()
            .unwrap();

        let s = shards.next().unwrap();
        assert!(s.id.contains("shard_2"));
    }
}
