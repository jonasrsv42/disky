use std::io::{Seek, Write};
use std::marker::PhantomData;

use crate::error::{DiskyError, Result};
use crate::parallel::sharding::traits::Sharder;

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