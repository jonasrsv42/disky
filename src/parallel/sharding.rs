use std::io::{Seek, Write};
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::fs::File;

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
/// This implementation creates files with names like "prefix_0.bin", "prefix_1.bin", etc.
///
/// # Example
/// ```no_run
/// use disky::parallel::sharding::{FileSharder, Sharder};
/// use std::path::PathBuf;
///
/// let output_dir = PathBuf::from("/tmp/records");
/// let file_sharder = FileSharder::new(
///     output_dir,
///     "shard",
///     ".bin"
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
    
    /// Suffix for file names (file extension)
    file_suffix: String,
    
    /// Counter for generating sequential file names
    counter: AtomicUsize,
}

impl FileSharder {
    /// Create a new FileSharder with the given directory, prefix, and suffix.
    ///
    /// # Arguments
    /// * `output_dir` - Directory where shard files will be created
    /// * `file_prefix` - Prefix for shard file names
    /// * `file_suffix` - Suffix for shard file names (typically a file extension)
    ///
    /// # Returns
    /// A new FileSharder instance
    pub fn new(
        output_dir: PathBuf,
        file_prefix: impl Into<String>,
        file_suffix: impl Into<String>,
    ) -> Self {
        Self {
            output_dir,
            file_prefix: file_prefix.into(),
            file_suffix: file_suffix.into(),
            counter: AtomicUsize::new(0),
        }
    }
    
    /// Create a new FileSharder with the given directory, prefix, suffix, and starting index.
    ///
    /// # Arguments
    /// * `output_dir` - Directory where shard files will be created
    /// * `file_prefix` - Prefix for shard file names
    /// * `file_suffix` - Suffix for shard file names (typically a file extension)
    /// * `start_index` - The starting index for file numbering
    ///
    /// # Returns
    /// A new FileSharder instance
    pub fn with_start_index(
        output_dir: PathBuf,
        file_prefix: impl Into<String>,
        file_suffix: impl Into<String>,
        start_index: usize,
    ) -> Self {
        Self {
            output_dir,
            file_prefix: file_prefix.into(),
            file_suffix: file_suffix.into(),
            counter: AtomicUsize::new(start_index),
        }
    }
    
    /// Get the next file path for a new shard
    fn next_file_path(&self) -> PathBuf {
        let file_num = self.counter.fetch_add(1, Ordering::SeqCst);
        self.output_dir.join(format!("{}_{}{}", self.file_prefix, file_num, self.file_suffix))
    }
}

impl Sharder<File> for FileSharder {
    fn create_sink(&self) -> Result<File> {
        let file_path = self.next_file_path();
        
        // Create the directory if it doesn't exist
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| DiskyError::Io(e))?;
        }
        
        // Create the file
        File::create(&file_path)
            .map_err(|e| DiskyError::Io(e))
    }
}

