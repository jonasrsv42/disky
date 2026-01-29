//! Sampling Reader for Disky.
//!
//! This module provides a reader that samples from multiple source iterators based on their weights.
//! It's useful for situations where you want to combine multiple data sources with controlled
//! proportions, such as when balancing training data from different sources.

use bytes::Bytes;
use log::debug;
use rand::SeedableRng;
use rand::distributions::WeightedIndex;
use rand::prelude::Distribution;
use rand::rngs::StdRng;

use crate::error::{DiskyError, Result};
use crate::reader::DiskyPiece;
use crate::tree::reader::{Node, Reader};

/// Options for configuring a [`SamplingReader`].
///
/// Controls the random seed used for sampling.
#[derive(Debug, Clone, Copy, Default)]
pub struct SamplingReaderOptions {
    /// Seed for random number generation.
    /// If `None`, a random seed will be used.
    pub seed: Option<u64>,
}

impl SamplingReaderOptions {
    /// Sets the random seed.
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = Some(seed);
        self
    }
}

/// Builder for [`SamplingReader`].
///
/// This is a tree node that samples from multiple child nodes based on weights.
///
/// # Example
///
/// ```ignore
/// use disky::tree::sampling::SamplingReaderConfig;
/// use disky::reader::RecordReaderConfig;
///
/// // Build a sampling tree from weighted nodes
/// let sampler = SamplingReaderConfig::new()
///     .append(2.0, RecordReaderConfig::new(file_a))  // 2x weight
///     .append(1.0, RecordReaderConfig::new(file_b))  // 1x weight
///     .with_seed(42)
///     .build()?;
///
/// for record in sampler {
///     let bytes = record?;
///     // ...
/// }
/// ```
pub struct SamplingReaderConfig {
    sources: Vec<(f64, Box<dyn Node>)>,
    options: SamplingReaderOptions,
}

impl SamplingReaderConfig {
    /// Creates a new empty builder.
    pub fn new() -> Self {
        Self {
            sources: Vec::new(),
            options: SamplingReaderOptions::default(),
        }
    }

    /// Appends a weighted source node.
    ///
    /// The weight must be positive. Higher weights mean the source is sampled more frequently.
    pub fn append(mut self, weight: f64, node: impl Node + 'static) -> Self {
        self.sources.push((weight, Box::new(node)));
        self
    }

    /// Sets the options.
    pub fn options(mut self, options: SamplingReaderOptions) -> Self {
        self.options = options;
        self
    }

    /// Sets the random seed.
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.options.seed = Some(seed);
        self
    }

    /// Builds the [`SamplingReader`].
    ///
    /// # Errors
    ///
    /// Returns an error if sources is empty or contains non-positive weights.
    pub fn build(self) -> Result<SamplingReader> {
        if self.sources.is_empty() {
            return Err(DiskyError::Other(
                "Cannot create SamplingReader with empty sources".to_string(),
            ));
        }

        // Separate weights and nodes
        let (weights, nodes): (Vec<_>, Vec<_>) = self.sources.into_iter().unzip();

        // Check for non-positive weights
        if weights.iter().any(|&w| w <= 0.0) {
            return Err(DiskyError::Other(
                "Non-positive weights are not allowed".to_string(),
            ));
        }

        // Build all nodes into readers
        let iterators: Vec<Reader> = nodes
            .into_iter()
            .map(|node| node.make())
            .collect::<Result<Vec<_>>>()?;

        // Create the active indices (initially, all iterators are active)
        let active_indices = (0..iterators.len()).collect();

        // Create the distribution
        let distribution = WeightedIndex::new(&weights).map_err(|e| {
            DiskyError::Other(format!("Failed to create weighted distribution: {}", e))
        })?;

        // Initialize random number generator
        let rng = match self.options.seed {
            Some(seed) => StdRng::seed_from_u64(seed),
            None => StdRng::from_entropy(),
        };

        Ok(SamplingReader {
            iterators,
            weights,
            active_indices,
            distribution,
            rng,
        })
    }
}

impl Default for SamplingReaderConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl Node for SamplingReaderConfig {
    fn make(self: Box<Self>) -> Result<Reader> {
        Ok(Box::new(self.build()?))
    }
}

/// A reader that samples from multiple readers based on weights.
///
/// The SamplingReader takes a set of (weight, reader) pairs and samples from them
/// based on the provided weights. For each read operation, it selects a reader
/// probabilistically according to the weights, reads one record from it, and returns
/// that record. This continues until all readers are exhausted.
///
/// This is useful for situations where you want to combine data from multiple
/// sources with specific proportions.
///
/// # Example
///
/// ```ignore
/// use disky::tree::sampling::SamplingReaderConfig;
///
/// let sampler = SamplingReaderConfig::new()
///     .append(2.0, node_a)
///     .append(1.0, node_b)
///     .with_seed(42)
///     .build()?;
///
/// for record in sampler {
///     let bytes = record?;
///     // ...
/// }
/// ```
pub struct SamplingReader {
    /// The readers to sample from
    iterators: Vec<Reader>,

    /// The weights for each iterator
    weights: Vec<f64>,

    /// Active indices that map to non-exhausted iterators
    active_indices: Vec<usize>,

    /// The weighted distribution for sampling
    distribution: WeightedIndex<f64>,

    /// Random number generator
    rng: StdRng,
}

impl SamplingReader {
    /// Updates the weighted distribution based on current active indices.
    fn update_distribution(&mut self) -> Result<()> {
        let active_weights: Vec<f64> = self
            .active_indices
            .iter()
            .map(|&idx| self.weights[idx])
            .collect();

        self.distribution = WeightedIndex::new(&active_weights).map_err(|e| {
            DiskyError::Other(format!("Failed to create weighted distribution: {}", e))
        })?;

        Ok(())
    }

    /// Reads the next record from one of the sources.
    ///
    /// This method samples a source based on weights and reads a record from it.
    /// If the sampled source is exhausted, it updates the distribution and tries again.
    ///
    /// # Returns
    ///
    /// - `DiskyPiece::Record(bytes)` if a record was read
    /// - `DiskyPiece::EOF` if all sources are exhausted
    pub fn read(&mut self) -> Result<DiskyPiece> {
        if self.active_indices.is_empty() {
            return Ok(DiskyPiece::EOF);
        }

        let sampled_idx = self.distribution.sample(&mut self.rng);
        let iter_idx = self.active_indices[sampled_idx];

        match self.iterators[iter_idx].next() {
            Some(Ok(bytes)) => Ok(DiskyPiece::Record(bytes)),
            Some(Err(e)) => Err(e),
            None => {
                debug!("Iterator {} is exhausted", iter_idx);
                self.active_indices.swap_remove(sampled_idx);

                if !self.active_indices.is_empty() {
                    self.update_distribution()?;
                    self.read()
                } else {
                    Ok(DiskyPiece::EOF)
                }
            }
        }
    }

    /// Returns the number of active (non-exhausted) sources.
    pub fn active_sources(&self) -> usize {
        self.active_indices.len()
    }

    /// Returns the total number of sources.
    pub fn total_sources(&self) -> usize {
        self.iterators.len()
    }

    /// Returns the weights of all sources.
    pub fn weights(&self) -> &[f64] {
        &self.weights
    }
}

impl Iterator for SamplingReader {
    type Item = Result<Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read() {
            Ok(DiskyPiece::Record(bytes)) => Some(Ok(bytes)),
            Ok(DiskyPiece::EOF) => None,
            Err(e) => Some(Err(e)),
        }
    }
}
