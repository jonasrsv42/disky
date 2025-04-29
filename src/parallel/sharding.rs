use std::fs::File;
use std::io::{Read, Seek, Write};
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use glob::glob;
use log::warn;

use crate::error::{DiskyError, Result};

/// A trait defining the common interface for shard creation strategies.
///
/// A Sharder is responsible for providing new sinks (Write + Seek) when requested.
/// The consumer is responsible for wrapping these sinks in appropriate writer types.
pub trait Sharder<Sink: Write + Seek + Send + 'static> {
    /// Create a new sink.
    /// This is called when a new shard is needed.
    fn create_sink(&self) -> Result<Sink>;
}

/// A trait for locating and opening existing shards for reading.
///
/// This trait provides methods to incrementally retrieve shards for reading.
/// It can be used by parallel readers to locate and open shards created
/// by compatible sharders.
pub trait ShardLocator<Source: Read + Seek + Send + 'static> {
    /// Returns the next available shard source.
    ///
    /// This method is called repeatedly to get all available shards.
    /// When no more shards are available, it returns Err(DiskyError::NoMoreShards).
    ///
    /// This method is thread-safe and does not require mutable access to self.
    ///
    /// # Returns
    /// - Ok(source) if a shard was successfully located and opened
    /// - Err(DiskyError::NoMoreShards) if no more shards are available
    /// - Err(...) if some other error occurred while trying to locate or open a shard
    fn next_shard(&self) -> Result<Source>;
    
    /// Returns the estimated total number of shards, if known.
    ///
    /// This is an optional method that can provide a hint about the total
    /// number of shards that might be available. The actual number might
    /// differ if shards are added or removed during reading.
    ///
    /// # Returns
    /// Some(count) if the count is known, None otherwise.
    fn estimated_shard_count(&self) -> Option<usize> {
        None
    }
    
    /// Resets the locator to start reading shards from the beginning.
    ///
    /// This allows the locator to be reused to read the same shards again.
    ///
    /// # Returns
    /// Ok(()) if the reset was successful, Err(...) otherwise.
    fn reset(&self) -> Result<()>;
}

/// A general-purpose auto sharder that can create new sink instances on demand.
///
/// The `Autosharder` implements the `Sharder` trait and provides a mechanism
/// for creating new sinks when needed, based on a factory function.
///
/// # Example
/// ```no_run
/// use std::fs::File;
/// use std::io::Cursor;
/// use std::path::PathBuf;
/// use disky::parallel::sharding::{Autosharder, Sharder};
///
/// // Memory-based sharder using Cursor<Vec<u8>>
/// let mem_sharder = Autosharder::new(|| {
///     Ok(Cursor::new(Vec::new()))
/// });
///
/// // File-based sharder with auto-incrementing file names
/// let base_path = PathBuf::from("/tmp/records");
/// let counter = std::sync::atomic::AtomicUsize::new(0);
/// let file_sharder = Autosharder::new(move || {
///     let file_num = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
///     let file_path = base_path.join(format!("shard_{}.bin", file_num));
///     let file = File::create(file_path)?;
///     Ok(file)
/// });
/// ```
pub struct Autosharder<Sink, F>
where
    Sink: Write + Seek + Send + 'static,
    F: Fn() -> Result<Sink> + Send + Sync + 'static,
{
    /// Factory function for creating new sinks
    sink_factory: F,

    /// Phantom data for the Sink type
    _phantom: PhantomData<Sink>,
}

impl<Sink, F> Autosharder<Sink, F>
where
    Sink: Write + Seek + Send + 'static,
    F: Fn() -> Result<Sink> + Send + Sync + 'static,
{
    /// Create a new Autosharder with the given sink factory.
    ///
    /// # Arguments
    /// * `sink_factory` - A function that creates new sink instances
    ///
    /// # Returns
    /// A new Autosharder instance
    pub fn new(sink_factory: F) -> Self {
        Self {
            sink_factory,
            _phantom: PhantomData,
        }
    }
}

impl<Sink, F> Sharder<Sink> for Autosharder<Sink, F>
where
    Sink: Write + Seek + Send + 'static,
    F: Fn() -> Result<Sink> + Send + Sync + 'static,
{
    fn create_sink(&self) -> Result<Sink> {
        // Create a new sink using the factory
        (self.sink_factory)()
            .map_err(|e| DiskyError::Other(format!("Failed to create sink: {}", e)))
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
    pub fn with_prefix(
        output_dir: PathBuf,
        file_prefix: impl Into<String>,
    ) -> Self {
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
        self.output_dir.join(format!(
            "{}_{}",
            self.config.file_prefix, file_num
        ))
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
                log::info!("Skipping existing shard file: {}", file_path.display());
                file_path = self.next_file_path();
            }
        }

        // Create the file
        File::create(&file_path).map_err(|e| DiskyError::Io(e))
    }
}

/// A locator for finding and opening sharded files created by a FileSharder.
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
        let glob_pattern = format!("{}_*", file_prefix);
        let pattern = output_dir.join(glob_pattern);
        let pattern_str = pattern.to_string_lossy();
        
        // Get a list of all matching files
        let mut shard_paths: Vec<PathBuf> = Vec::new();
        
        for entry in glob(&pattern_str).map_err(|e| DiskyError::Other(format!("Invalid glob pattern: {}", e)))? {
            match entry {
                Ok(path) => shard_paths.push(path),
                Err(e) => warn!("Error with glob entry: {}", e),
            }
        }
        
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
    
    fn reset(&self) -> Result<()> {
        // Reset the index to start reading from the beginning
        self.next_index.store(0, Ordering::SeqCst);
        Ok(())
    }
    
    fn estimated_shard_count(&self) -> Option<usize> {
        // Return the actual count of shards we found
        Some(self.shard_paths.len())
    }
}

/// A memory-based shard locator that can be used for testing.
///
/// This locator provides in-memory Cursors as shards, which is useful for 
/// tests or when data is already in memory.
pub struct MemoryShardLocator<F>
where
    F: Fn() -> Result<std::io::Cursor<Vec<u8>>> + Send + Sync + 'static,
{
    /// Function that creates shard sources
    source_factory: F,
    
    /// Number of shards to create
    shard_count: usize,
    
    /// Index of the next shard to return
    next_index: AtomicUsize,
}

impl<F> MemoryShardLocator<F>
where
    F: Fn() -> Result<std::io::Cursor<Vec<u8>>> + Send + Sync + 'static,
{
    /// Create a new MemoryShardLocator with the given factory function.
    ///
    /// # Arguments
    /// * `source_factory` - Function that creates cursor sources
    /// * `shard_count` - Number of shards to create
    ///
    /// # Returns
    /// A new MemoryShardLocator instance
    pub fn new(source_factory: F, shard_count: usize) -> Self {
        Self {
            source_factory,
            shard_count,
            next_index: AtomicUsize::new(0),
        }
    }
}

impl<F> ShardLocator<std::io::Cursor<Vec<u8>>> for MemoryShardLocator<F>
where
    F: Fn() -> Result<std::io::Cursor<Vec<u8>>> + Send + Sync + 'static,
{
    fn next_shard(&self) -> Result<std::io::Cursor<Vec<u8>>> {
        // Get the current index and increment it atomically
        let index = self.next_index.fetch_add(1, Ordering::SeqCst);
        
        // Check if we have any more shards
        if index >= self.shard_count {
            return Err(DiskyError::NoMoreShards);
        }
        
        // Create a new cursor
        (self.source_factory)()
    }
    
    fn reset(&self) -> Result<()> {
        // Reset the index to start from the beginning
        self.next_index.store(0, Ordering::SeqCst);
        Ok(())
    }
    
    fn estimated_shard_count(&self) -> Option<usize> {
        // Return the total count of shards
        Some(self.shard_count)
    }
}

