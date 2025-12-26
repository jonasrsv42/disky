use std::fs::File;
// The File type implements the required Read+Seek trait bounds
use std::path::PathBuf;
use std::sync::Mutex;

use rand::rngs::StdRng;
use rand::{SeedableRng, seq::SliceRandom};

use crate::error::{DiskyError, Result};
use crate::parallel::sharding::traits::{Shard, ShardLocator};
use crate::parallel::sharding::utils::find_shard_paths;

/// Internal state protected by a single mutex to avoid race conditions
#[derive(Debug)]
struct ShardLocatorState {
    /// Current position in the shuffled indices
    position: usize,
    /// Current randomized order of indices - shuffled when exhausted
    indices: Vec<usize>,
    /// RNG for shuffling
    rng: StdRng,
}

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

    /// All mutable state protected by a single mutex
    state: Mutex<ShardLocatorState>,
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

        let state = ShardLocatorState {
            position: 0,
            indices,
            rng,
        };

        Ok(Self {
            shard_paths,
            state: Mutex::new(state),
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

        let state = ShardLocatorState {
            position: 0,
            indices,
            rng,
        };

        Ok(Self {
            shard_paths,
            state: Mutex::new(state),
        })
    }
}

impl ShardLocator<File> for RandomRepeatingFileShardLocator {
    fn next_shard(&self) -> Result<Shard<File>> {
        // Lock all state - position check, increment, and reshuffle are atomic
        let mut state = self
            .state
            .lock()
            .map_err(|_| DiskyError::Other("Failed to lock state".to_string()))?;

        // Destructure to get separate mutable references (satisfies borrow checker)
        let ShardLocatorState {
            position,
            indices,
            rng,
        } = &mut *state;

        // If we've gone through all shards, reshuffle before getting next
        if *position >= indices.len() {
            *position = 0;
            indices.shuffle(rng);
        }

        // Get the current position and increment it
        let current_position = *position;
        *position += 1;

        // Get the file path for the current position
        let index = indices[current_position];
        let file_path = &self.shard_paths[index];
        let id = file_path.display().to_string();

        // Open the file for reading
        let source = File::open(file_path).map_err(DiskyError::Io)?;

        Ok(Shard { source, id })
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

    /// All mutable state protected by a single mutex
    state: Mutex<ShardLocatorState>,
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

        let state = ShardLocatorState {
            position: 0,
            indices,
            rng,
        };

        Ok(Self {
            shard_paths: file_paths,
            state: Mutex::new(state),
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

        let state = ShardLocatorState {
            position: 0,
            indices,
            rng,
        };

        Ok(Self {
            shard_paths: file_paths,
            state: Mutex::new(state),
        })
    }
}

impl ShardLocator<File> for RandomMultiPathShardLocator {
    fn next_shard(&self) -> Result<Shard<File>> {
        // Lock all state - position check, increment, and reshuffle are atomic
        let mut state = self
            .state
            .lock()
            .map_err(|_| DiskyError::Other("Failed to lock state".to_string()))?;

        // Destructure to get separate mutable references (satisfies borrow checker)
        let ShardLocatorState {
            position,
            indices,
            rng,
        } = &mut *state;

        // If we've gone through all shards, reshuffle before getting next
        if *position >= indices.len() {
            *position = 0;
            indices.shuffle(rng);
        }

        // Get the current position and increment it
        let current_position = *position;
        *position += 1;

        // Get the file path for the current position
        let index = indices[current_position];
        let file_path = &self.shard_paths[index];
        let id = file_path.display().to_string();

        // Open the file for reading
        let source = File::open(file_path).map_err(DiskyError::Io)?;

        Ok(Shard { source, id })
    }

    fn estimated_shard_count(&self) -> Option<usize> {
        // We know the exact count, but since we repeat indefinitely,
        // we return the number of unique shards
        Some(self.shard_paths.len())
    }
}
