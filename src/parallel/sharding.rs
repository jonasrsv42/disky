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
    /// # Returns
    /// - Ok(source) if a shard was successfully located and opened
    /// - Err(DiskyError::NoMoreShards) if no more shards are available
    /// - Err(...) if some other error occurred while trying to locate or open a shard
    fn next_shard(&mut self) -> Result<Source>;
    
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
    fn reset(&mut self) -> Result<()>;
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
///
/// # Example
/// ```no_run
/// use disky::parallel::sharding::{FileSharder, Sharder};
/// use std::path::PathBuf;
///
/// let output_dir = PathBuf::from("/tmp/records");
/// let file_sharder = FileSharder::new(
///     output_dir,
///     "shard"
/// );
///
/// // Create a new file sink
/// let sink = file_sharder.create_sink().unwrap();
/// ```
pub struct FileSharder {
    /// Directory to create files in
    output_dir: PathBuf,

    /// Prefix for file names
    file_prefix: String,

    /// Counter for generating sequential file names
    counter: AtomicUsize,
}

impl FileSharder {
    /// Create a new FileSharder with the given directory and prefix.
    ///
    /// # Arguments
    /// * `output_dir` - Directory where shard files will be created
    /// * `file_prefix` - Prefix for shard file names
    ///
    /// # Returns
    /// A new FileSharder instance
    pub fn new(
        output_dir: PathBuf,
        file_prefix: impl Into<String>,
    ) -> Self {
        Self {
            output_dir,
            file_prefix: file_prefix.into(),
            counter: AtomicUsize::new(0),
        }
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
        Self {
            output_dir,
            file_prefix: file_prefix.into(),
            counter: AtomicUsize::new(start_index),
        }
    }

    /// Get the next file path for a new shard
    fn next_file_path(&self) -> PathBuf {
        let file_num = self.counter.fetch_add(1, Ordering::SeqCst);
        self.output_dir.join(format!(
            "{}_{}",
            self.file_prefix, file_num
        ))
    }
}

impl Sharder<File> for FileSharder {
    fn create_sink(&self) -> Result<File> {
        let file_path = self.next_file_path();

        // Create the directory if it doesn't exist
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| DiskyError::Io(e))?;
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
    next_index: usize,
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
            next_index: 0,
        })
    }
}

impl ShardLocator<File> for FileShardLocator {
    fn next_shard(&mut self) -> Result<File> {
        // Check if we have any more shards
        if self.next_index >= self.shard_paths.len() {
            return Err(DiskyError::NoMoreShards);
        }
        
        // Get the next shard path
        let file_path = &self.shard_paths[self.next_index];
        self.next_index += 1;
        
        // Open the file for reading
        File::open(file_path).map_err(|e| DiskyError::Io(e))
    }
    
    fn reset(&mut self) -> Result<()> {
        // Reset the index to start reading from the beginning
        self.next_index = 0;
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
    next_index: usize,
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
            next_index: 0,
        }
    }
}

impl<F> ShardLocator<std::io::Cursor<Vec<u8>>> for MemoryShardLocator<F>
where
    F: Fn() -> Result<std::io::Cursor<Vec<u8>>> + Send + Sync + 'static,
{
    fn next_shard(&mut self) -> Result<std::io::Cursor<Vec<u8>>> {
        // Check if we have any more shards
        if self.next_index >= self.shard_count {
            return Err(DiskyError::NoMoreShards);
        }
        
        // Increment index
        self.next_index += 1;
        
        // Create a new cursor
        (self.source_factory)()
    }
    
    fn reset(&mut self) -> Result<()> {
        // Reset the index to start from the beginning
        self.next_index = 0;
        Ok(())
    }
    
    fn estimated_shard_count(&self) -> Option<usize> {
        // Return the total count of shards
        Some(self.shard_count)
    }
}

