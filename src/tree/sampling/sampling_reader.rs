// Copyright 2024
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
/// # Example
///
/// ```ignore
/// use disky::tree::sampling::SamplingReaderConfig;
/// use disky::reader::RecordReaderConfig;
///
/// let reader_a = RecordReaderConfig::new(file_a).build()?;
/// let reader_b = RecordReaderConfig::new(file_b).build()?;
///
/// let sampler = SamplingReaderConfig::new(vec![
///     (2.0, reader_a),  // 2x weight
///     (1.0, reader_b),  // 1x weight
/// ])
/// .with_seed(42)
/// .build()?;
///
/// for record in sampler {
///     let bytes = record?;
///     // ...
/// }
/// ```
pub struct SamplingReaderConfig<I> {
    sources: Vec<(f64, I)>,
    options: SamplingReaderOptions,
}

impl<I> SamplingReaderConfig<I>
where
    I: Iterator<Item = Result<Bytes>>,
{
    /// Creates a new builder with the given weighted sources.
    ///
    /// Each source is a `(weight, iterator)` pair. Weights must be positive.
    pub fn new(sources: Vec<(f64, I)>) -> Self {
        Self {
            sources,
            options: SamplingReaderOptions::default(),
        }
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
    pub fn build(self) -> Result<SamplingReader<I>> {
        if self.sources.is_empty() {
            return Err(DiskyError::Other(
                "Cannot create SamplingReader with empty sources".to_string(),
            ));
        }

        // Separate weights and iterators
        let (weights, iterators): (Vec<_>, Vec<_>) = self.sources.into_iter().unzip();

        // Check for non-positive weights
        if weights.iter().any(|&w| w <= 0.0) {
            return Err(DiskyError::Other(
                "Non-positive weights are not allowed".to_string(),
            ));
        }

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

/// A reader that samples from multiple readers based on weights.
///
/// The SamplingReader takes a set of (weight, iterator) pairs and samples from them
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
/// let sampler = SamplingReaderConfig::new(vec![
///     (2.0, reader_a),
///     (1.0, reader_b),
/// ])
/// .with_seed(42)
/// .build()?;
///
/// for record in sampler {
///     let bytes = record?;
///     // ...
/// }
/// ```
pub struct SamplingReader<I>
where
    I: Iterator<Item = Result<Bytes>>,
{
    /// The iterators to sample from
    iterators: Vec<I>,

    /// The weights for each iterator
    weights: Vec<f64>,

    /// Active indices that map to non-exhausted iterators
    active_indices: Vec<usize>,

    /// The weighted distribution for sampling
    distribution: WeightedIndex<f64>,

    /// Random number generator
    rng: StdRng,
}

impl<I> SamplingReader<I>
where
    I: Iterator<Item = Result<Bytes>>,
{
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

impl<I> Iterator for SamplingReader<I>
where
    I: Iterator<Item = Result<Bytes>>,
{
    type Item = Result<Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read() {
            Ok(DiskyPiece::Record(bytes)) => Some(Ok(bytes)),
            Ok(DiskyPiece::EOF) => None,
            Err(e) => Some(Err(e)),
        }
    }
}
