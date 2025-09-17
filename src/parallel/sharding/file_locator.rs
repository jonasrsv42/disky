use std::fs::File;
// The File type implements the required Read+Seek trait bounds
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::error::{DiskyError, Result};
use crate::parallel::sharding::traits::ShardLocator;
use crate::parallel::sharding::utils::find_shard_paths;

/// A locator for finding and opening sharded files created by a FileSharder.
/// This locator returns shards in sequential order.
pub struct FileShardLocator {
    /// List of shard file paths
    shard_paths: Vec<PathBuf>,

    /// Index of the next shard to return
    next_index: AtomicUsize,
}

impl FileShardLocator {
    /// Create a new FileShardLocator to read shards with the given prefix.
    ///
    /// # Arguments
    /// * `output_dir` - Directory containing the shard files
    /// * `file_prefix` - Prefix for shard file names
    ///
    /// # Returns
    /// A new FileShardLocator instance
    pub fn new(output_dir: PathBuf, file_prefix: impl Into<String>) -> Result<Self> {
        let file_prefix = file_prefix.into();
        let mut shard_paths = find_shard_paths(&output_dir, &file_prefix)?;

        // Sort the paths to ensure consistent order
        shard_paths.sort();

        Ok(Self {
            shard_paths,
            next_index: AtomicUsize::new(0),
        })
    }
}

impl ShardLocator<File> for FileShardLocator {
    fn next_shard(&self) -> Result<File> {
        // Get the current index and increment it atomically
        let index = self.next_index.fetch_add(1, Ordering::SeqCst);

        // Check if we have any more shards
        if index >= self.shard_paths.len() {
            return Err(DiskyError::NoMoreShards);
        }

        // Get the next shard path
        let file_path = &self.shard_paths[index];

        // Open the file for reading
        File::open(file_path).map_err(|e| DiskyError::Io(e))
    }

    fn estimated_shard_count(&self) -> Option<usize> {
        // Return the actual count of shards we found
        Some(self.shard_paths.len())
    }
}

/// A shard locator that uses an explicit list of file paths.
///
/// This locator allows directly specifying the file paths to use as shards,
/// rather than discovering them through glob patterns or directory traversal.
#[derive(Debug)]
pub struct MultiPathShardLocator {
    /// List of shard file paths
    shard_paths: Vec<PathBuf>,

    /// Index of the next shard to return
    next_index: AtomicUsize,
}

impl MultiPathShardLocator {
    /// Create a new MultiPathShardLocator with the given file paths.
    ///
    /// # Arguments
    /// * `file_paths` - List of file paths to use as shards
    ///
    /// # Returns
    /// A new MultiPathShardLocator instance
    pub fn new(file_paths: Vec<PathBuf>) -> Result<Self> {
        // Verify that we have at least one file path
        if file_paths.is_empty() {
            return Err(DiskyError::Other("No shard paths provided".to_string()));
        }

        // Validate that all files exist
        for path in &file_paths {
            if !path.exists() {
                return Err(DiskyError::Other(format!(
                    "Shard file does not exist: {}",
                    path.display()
                )));
            }
        }

        Ok(Self {
            shard_paths: file_paths,
            next_index: AtomicUsize::new(0),
        })
    }
}

impl ShardLocator<File> for MultiPathShardLocator {
    fn next_shard(&self) -> Result<File> {
        // Get the current index and increment it atomically
        let index = self.next_index.fetch_add(1, Ordering::SeqCst);

        // Check if we have any more shards
        if index >= self.shard_paths.len() {
            return Err(DiskyError::NoMoreShards);
        }

        // Get the next shard path
        let file_path = &self.shard_paths[index];

        // Open the file for reading
        File::open(file_path).map_err(|e| DiskyError::Io(e))
    }

    fn estimated_shard_count(&self) -> Option<usize> {
        // Return the actual count of shards we have
        Some(self.shard_paths.len())
    }
}
