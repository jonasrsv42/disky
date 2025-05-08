use std::fs::File;
// The File type implements the required Write+Seek trait bounds
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use log::info;

use crate::error::{DiskyError, Result};
use crate::parallel::sharding::traits::Sharder;

/// Configuration for a FileSharder
#[derive(Debug, Clone)]
pub struct FileSharderConfig {
    /// Prefix for file names (default: "shard")
    pub file_prefix: String,

    /// Starting index for file numbering (default: 0)
    pub start_index: usize,

    /// Whether to append to existing shards (default: false)
    pub append: bool,
}

impl FileSharderConfig {
    /// Create a new configuration with default values
    pub fn new(file_prefix: impl Into<String>) -> Self {
        Self {
            file_prefix: file_prefix.into(),
            start_index: 0,
            append: false,
        }
    }

    /// Set the starting index for file numbering
    pub fn with_start_index(mut self, start_index: usize) -> Self {
        self.start_index = start_index;
        self
    }

    /// Enable or disable append mode
    pub fn with_append(mut self, append: bool) -> Self {
        self.append = append;
        self
    }
}

impl Default for FileSharderConfig {
    fn default() -> Self {
        Self {
            file_prefix: "shard".into(),
            start_index: 0,
            append: false,
        }
    }
}

/// A file-based implementation of the Sharder that creates sequentially numbered files.
///
/// This implementation creates files with names like "prefix_0", "prefix_1", etc.
/// It can also operate in append mode, where it will skip existing files and create
/// new files at the next available index.
///
/// # Example with default configuration
/// ```no_run
/// use disky::parallel::sharding::{FileSharder, Sharder};
/// use std::path::PathBuf;
///
/// let output_dir = PathBuf::from("/tmp/records");
/// let file_sharder = FileSharder::new(output_dir);
///
/// // Create a new file sink
/// let sink = file_sharder.create_sink().unwrap();
/// ```
///
/// # Example with custom prefix
/// ```no_run
/// use disky::parallel::sharding::{FileSharder, Sharder};
/// use std::path::PathBuf;
///
/// let output_dir = PathBuf::from("/tmp/records");
/// let file_sharder = FileSharder::with_prefix(
///     output_dir,
///     "my_shard"
/// );
///
/// // Create a new file sink
/// let sink = file_sharder.create_sink().unwrap();
/// ```
///
/// # Example with append mode
/// ```no_run
/// use disky::parallel::sharding::{FileSharder, FileSharderConfig, Sharder};
/// use std::path::PathBuf;
///
/// let output_dir = PathBuf::from("/tmp/records");
/// let config = FileSharderConfig::default().with_append(true);
/// let file_sharder = FileSharder::with_config(output_dir, config);
///
/// // Create a new file sink (will skip existing shards)
/// let sink = file_sharder.create_sink().unwrap();
/// ```
pub struct FileSharder {
    /// Directory to create files in
    output_dir: PathBuf,

    /// Configuration for this sharder
    config: FileSharderConfig,

    /// Counter for generating sequential file names
    counter: AtomicUsize,
}

impl FileSharder {
    /// Create a new FileSharder with the given directory and default configuration.
    ///
    /// # Arguments
    /// * `output_dir` - Directory where shard files will be created
    ///
    /// # Returns
    /// A new FileSharder instance
    pub fn new(output_dir: PathBuf) -> Self {
        Self::with_config(output_dir, FileSharderConfig::default())
    }

    /// Create a new FileSharder with the given directory and configuration.
    ///
    /// # Arguments
    /// * `output_dir` - Directory where shard files will be created
    /// * `config` - Configuration for the file sharder
    ///
    /// # Returns
    /// A new FileSharder instance
    pub fn with_config(output_dir: PathBuf, config: FileSharderConfig) -> Self {
        Self {
            output_dir,
            counter: AtomicUsize::new(config.start_index),
            config,
        }
    }

    /// Create a new FileSharder with the given directory and prefix.
    ///
    /// # Arguments
    /// * `output_dir` - Directory where shard files will be created
    /// * `file_prefix` - Prefix for shard file names
    ///
    /// # Returns
    /// A new FileSharder instance
    pub fn with_prefix(output_dir: PathBuf, file_prefix: impl Into<String>) -> Self {
        let config = FileSharderConfig::new(file_prefix);
        Self::with_config(output_dir, config)
    }

    /// Create a new FileSharder with the given directory, prefix, and starting index.
    ///
    /// # Arguments
    /// * `output_dir` - Directory where shard files will be created
    /// * `file_prefix` - Prefix for shard file names
    /// * `start_index` - The starting index for file numbering
    ///
    /// # Returns
    /// A new FileSharder instance
    pub fn with_start_index(
        output_dir: PathBuf,
        file_prefix: impl Into<String>,
        start_index: usize,
    ) -> Self {
        let config = FileSharderConfig::new(file_prefix).with_start_index(start_index);
        Self::with_config(output_dir, config)
    }

    /// Get the next file path for a new shard
    fn next_file_path(&self) -> PathBuf {
        let file_num = self.counter.fetch_add(1, Ordering::SeqCst);
        self.output_dir
            .join(format!("{}_{}", self.config.file_prefix, file_num))
    }
}

impl Sharder<File> for FileSharder {
    fn create_sink(&self) -> Result<File> {
        // In append mode, keep trying paths until finding one that doesn't exist,
        // or if all existing files have been checked, create a new one at the next index
        let mut file_path = self.next_file_path();

        // Create the directory if it doesn't exist
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| DiskyError::Io(e))?;
        }

        // Check if we're in append mode
        if self.config.append {
            // If the file exists, we need to increment and try again
            // until we find a new file to create
            while file_path.exists() {
                info!("Skipping existing shard file: {}", file_path.display());
                file_path = self.next_file_path();
            }
        }

        // Create the file
        File::create(&file_path).map_err(|e| DiskyError::Io(e))
    }
}