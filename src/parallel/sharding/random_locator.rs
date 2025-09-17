use std::fs::File;
// The File type implements the required Read+Seek trait bounds
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use rand::rngs::StdRng;
use rand::{SeedableRng, seq::SliceRandom};

use crate::error::{DiskyError, Result};
use crate::parallel::sharding::traits::ShardLocator;
use crate::parallel::sharding::utils::find_shard_paths;

/// A shard locator that returns shards in a random order, exhausting all shards before repeating.
///
/// This locator randomizes the order of shards but ensures all shards are visited
/// before repeating. It's helpful for scenarios where you want random access but still
/// need to process all shards, such as training on all data in random order.
///
/// # Non-deterministic behavior
///
/// IMPORTANT: Even when using a fixed seed with `with_seed()`, the random shard locator
/// may not provide a deterministic ordering when used with parallel or multi-threaded readers.
/// This is because thread scheduling and racing conditions can affect the order in which
/// shards are requested and processed, potentially leading to different results between runs.
///
/// # Example
/// ```no_run
/// use disky::parallel::sharding::{RandomRepeatingFileShardLocator, ShardLocator};
/// use std::path::PathBuf;
///
/// // Create a locator for shards with the given prefix
/// let locator = RandomRepeatingFileShardLocator::new(
///     PathBuf::from("/tmp/records"),
///     "shard"
/// ).unwrap();
///
/// // Get shards in random order (will repeat after all shards are exhausted)
/// let shard1 = locator.next_shard().unwrap();
/// let shard2 = locator.next_shard().unwrap();
/// // Will continue to provide all shards in randomized batches
/// ```
#[derive(Debug)]
pub struct RandomRepeatingFileShardLocator {
    /// List of all shard file paths
    shard_paths: Vec<PathBuf>,

    /// Current position in the shuffled indices
    position: AtomicUsize,

    /// Current randomized order of indices - shuffled when exhausted
    indices: Mutex<Vec<usize>>,

    /// RNG for shuffling
    rng: Mutex<StdRng>,
}

impl RandomRepeatingFileShardLocator {
    /// Create a new RandomRepeatingFileShardLocator to read shards with the given prefix.
    ///
    /// # Arguments
    /// * `output_dir` - Directory containing the shard files
    /// * `file_prefix` - Prefix for shard file names
    ///
    /// # Returns
    /// A new RandomRepeatingFileShardLocator instance
    pub fn new(output_dir: PathBuf, file_prefix: impl Into<String>) -> Result<Self> {
        let file_prefix = file_prefix.into();
        let shard_paths = find_shard_paths(&output_dir, &file_prefix)?;

        // Create initial randomized indices
        let mut indices: Vec<usize> = (0..shard_paths.len()).collect();

        // Create a new RNG with a random seed
        let mut rng = StdRng::from_entropy();

        // Shuffle the indices
        indices.shuffle(&mut rng);

        Ok(Self {
            shard_paths,
            position: AtomicUsize::new(0),
            indices: Mutex::new(indices),
            rng: Mutex::new(rng),
        })
    }

    /// Create a new RandomRepeatingFileShardLocator with a specific seed.
    ///
    /// This is useful for reproducible randomization, such as in testing
    /// or when you want the same random sequence across runs.
    ///
    /// # Arguments
    /// * `output_dir` - Directory containing the shard files
    /// * `file_prefix` - Prefix for shard file names
    /// * `seed` - Seed for the random number generator
    ///
    /// # Returns
    /// A new RandomRepeatingFileShardLocator instance with a deterministic random sequence
    pub fn with_seed(
        output_dir: PathBuf,
        file_prefix: impl Into<String>,
        seed: u64,
    ) -> Result<Self> {
        let file_prefix = file_prefix.into();
        let shard_paths = find_shard_paths(&output_dir, &file_prefix)?;

        // Create initial randomized indices
        let mut indices: Vec<usize> = (0..shard_paths.len()).collect();

        // Create a new RNG with the given seed
        let mut rng = StdRng::seed_from_u64(seed);

        // Shuffle the indices
        indices.shuffle(&mut rng);

        Ok(Self {
            shard_paths,
            position: AtomicUsize::new(0),
            indices: Mutex::new(indices),
            rng: Mutex::new(rng),
        })
    }

    /// Reshuffles the indices when all shards have been exhausted
    ///
    /// Takes a mutable reference to the already locked indices
    fn reshuffle_if_needed(&self, indices: &mut Vec<usize>) -> Result<()> {
        let position = self.position.load(Ordering::Acquire);

        // If we've gone through all shards, reshuffle
        if position >= indices.len() {
            // Reset position counter
            self.position.store(0, Ordering::Release);

            // Lock RNG for shuffling
            let mut rng = self
                .rng
                .lock()
                .map_err(|_| DiskyError::Other("Failed to lock RNG".to_string()))?;

            // Shuffle the indices
            indices.shuffle(&mut *rng);
        }

        Ok(())
    }
}

impl ShardLocator<File> for RandomRepeatingFileShardLocator {
    fn next_shard(&self) -> Result<File> {
        // Lock the indices to check/reshuffle if needed
        let mut indices = self
            .indices
            .lock()
            .map_err(|_| DiskyError::Other("Failed to lock indices".to_string()))?;

        // Call reshuffle_if_needed with the locked indices
        self.reshuffle_if_needed(&mut indices)?;

        // Get the current position and increment it
        let position = self.position.fetch_add(1, Ordering::SeqCst);

        // Get the file path for the current position
        let index = indices[position % indices.len()];
        let file_path = &self.shard_paths[index];

        // Open the file for reading
        File::open(file_path).map_err(|e| DiskyError::Io(e))
    }

    fn estimated_shard_count(&self) -> Option<usize> {
        // We know the exact count, but since we repeat indefinitely,
        // we return the number of unique shards
        Some(self.shard_paths.len())
    }
}

/// A random access variant of the MultiPathShardLocator.
///
/// This locator returns shards in a random order, exhausting all shards before repeating.
/// It's useful for scenarios like training on data in random order while ensuring all
/// records are processed.
///
/// # Non-deterministic behavior
///
/// IMPORTANT: Even when using a fixed seed with `with_seed()`, the random shard locator
/// may not provide a deterministic ordering when used with parallel or multi-threaded readers.
/// This is because thread scheduling and racing conditions can affect the order in which
/// shards are requested and processed, potentially leading to different results between runs.
#[derive(Debug)]
pub struct RandomMultiPathShardLocator {
    /// List of all shard file paths
    shard_paths: Vec<PathBuf>,

    /// Current position in the shuffled indices
    position: AtomicUsize,

    /// Current randomized order of indices - shuffled when exhausted
    indices: Mutex<Vec<usize>>,

    /// RNG for shuffling
    rng: Mutex<StdRng>,
}

impl RandomMultiPathShardLocator {
    /// Create a new RandomMultiPathShardLocator with the given file paths.
    ///
    /// # Arguments
    /// * `file_paths` - List of file paths to use as shards
    ///
    /// # Returns
    /// A new RandomMultiPathShardLocator instance with a random seed
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

        // Create initial randomized indices
        let mut indices: Vec<usize> = (0..file_paths.len()).collect();

        // Create a new RNG with a random seed
        let mut rng = StdRng::from_entropy();

        // Shuffle the indices
        indices.shuffle(&mut rng);

        Ok(Self {
            shard_paths: file_paths,
            position: AtomicUsize::new(0),
            indices: Mutex::new(indices),
            rng: Mutex::new(rng),
        })
    }

    /// Create a new RandomMultiPathShardLocator with a specific seed.
    ///
    /// This is useful for reproducible randomization, such as in testing
    /// or when you want the same random sequence across runs.
    ///
    /// # Arguments
    /// * `file_paths` - List of file paths to use as shards
    /// * `seed` - Seed for the random number generator
    ///
    /// # Returns
    /// A new RandomMultiPathShardLocator instance with a deterministic random sequence
    pub fn with_seed(file_paths: Vec<PathBuf>, seed: u64) -> Result<Self> {
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

        // Create initial randomized indices
        let mut indices: Vec<usize> = (0..file_paths.len()).collect();

        // Create a new RNG with the given seed
        let mut rng = StdRng::seed_from_u64(seed);

        // Shuffle the indices
        indices.shuffle(&mut rng);

        Ok(Self {
            shard_paths: file_paths,
            position: AtomicUsize::new(0),
            indices: Mutex::new(indices),
            rng: Mutex::new(rng),
        })
    }

    /// Reshuffles the indices when all shards have been exhausted
    ///
    /// Takes a mutable reference to the already locked indices
    fn reshuffle_if_needed(&self, indices: &mut Vec<usize>) -> Result<()> {
        let position = self.position.load(Ordering::Acquire);

        // If we've gone through all shards, reshuffle
        if position >= indices.len() {
            // Reset position counter
            self.position.store(0, Ordering::Release);

            // Lock RNG for shuffling
            let mut rng = self
                .rng
                .lock()
                .map_err(|_| DiskyError::Other("Failed to lock RNG".to_string()))?;

            // Shuffle the indices
            indices.shuffle(&mut *rng);
        }

        Ok(())
    }
}

impl ShardLocator<File> for RandomMultiPathShardLocator {
    fn next_shard(&self) -> Result<File> {
        // Lock the indices to check/reshuffle if needed
        let mut indices = self
            .indices
            .lock()
            .map_err(|_| DiskyError::Other("Failed to lock indices".to_string()))?;

        // Call reshuffle_if_needed with the locked indices
        self.reshuffle_if_needed(&mut indices)?;

        // Get the current position and increment it
        let position = self.position.fetch_add(1, Ordering::SeqCst);

        // Get the file path for the current position
        let index = indices[position % indices.len()];
        let file_path = &self.shard_paths[index];

        // Open the file for reading
        File::open(file_path).map_err(|e| DiskyError::Io(e))
    }

    fn estimated_shard_count(&self) -> Option<usize> {
        // We know the exact count, but since we repeat indefinitely,
        // we return the number of unique shards
        Some(self.shard_paths.len())
    }
}
