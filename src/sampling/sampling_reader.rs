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

/// Configuration for the sampling reader
#[derive(Debug, Clone)]
pub struct SamplingReaderConfig {
    /// Seed for random number generation (optional)
    /// If not provided, a random seed will be used
    pub seed: Option<u64>,
}

impl Default for SamplingReaderConfig {
    fn default() -> Self {
        Self { seed: None }
    }
}

impl SamplingReaderConfig {
    /// Creates a new configuration with a specific random seed
    pub fn with_seed(seed: u64) -> Self {
        Self { seed: Some(seed) }
    }
}

/// A reader that samples from multiple readers based on weights
///
/// The SamplingReader takes a set of (weight, iterator) pairs and samples from them
/// based on the provided weights. For each read operation, it selects a reader
/// probabilistically according to the weights, reads one record from it, and returns
/// that record. This continues until all readers are exhausted.
///
/// This is useful for situations where you want to combine data from multiple
/// sources with specific proportions.
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
    /// Creates a new SamplingReader with default configuration
    ///
    /// # Arguments
    ///
    /// * `sources` - A vector of (weight, iterator) pairs
    ///
    /// # Returns
    ///
    /// A new SamplingReader
    ///
    /// # Errors
    ///
    /// Returns an error if the weights vector is empty or contains non-positive values
    pub fn new(sources: Vec<(f64, I)>) -> Result<Self> {
        Self::with_config(sources, SamplingReaderConfig::default())
    }

    /// Creates a new SamplingReader with custom configuration
    ///
    /// # Arguments
    ///
    /// * `sources` - A vector of (weight, iterator) pairs
    /// * `config` - Configuration options for the sampling reader
    ///
    /// # Returns
    ///
    /// A new SamplingReader
    ///
    /// # Errors
    ///
    /// Returns an error if the weights vector is empty or contains non-positive values
    pub fn with_config(sources: Vec<(f64, I)>, config: SamplingReaderConfig) -> Result<Self> {
        if sources.is_empty() {
            return Err(DiskyError::Other(
                "Cannot create SamplingReader with empty sources".to_string(),
            ));
        }

        // Separate weights and iterators
        let (weights, iterators): (Vec<_>, Vec<_>) = sources.into_iter().unzip();

        // Check for non-positive weights
        if weights.iter().any(|&w| w <= 0.0) {
            return Err(DiskyError::Other(
                "Non-positive weights are not allowed".to_string(),
            ));
        }

        // Create the active indices (initially, all iterators are active)
        let active_indices = (0..iterators.len()).collect();

        // Create the distribution - this should always succeed since we checked for non-positive weights
        let distribution = match WeightedIndex::new(&weights) {
            Ok(dist) => dist,
            Err(e) => {
                return Err(DiskyError::Other(format!(
                    "Failed to create weighted distribution: {}",
                    e
                )));
            }
        };

        // Initialize random number generator
        let rng = match config.seed {
            Some(seed) => StdRng::seed_from_u64(seed),
            None => StdRng::from_entropy(),
        };

        Ok(Self {
            iterators,
            weights,
            active_indices,
            distribution,
            rng,
        })
    }

    /// Updates the weighted distribution based on current active indices
    ///
    /// This should be called whenever the active_indices list changes.
    fn update_distribution(&mut self) -> Result<()> {
        // Get weights for active iterators
        let active_weights: Vec<f64> = self
            .active_indices
            .iter()
            .map(|&idx| self.weights[idx])
            .collect();

        // Create new distribution
        match WeightedIndex::new(&active_weights) {
            Ok(dist) => {
                self.distribution = dist;
                Ok(())
            }
            Err(e) => {
                // This should not happen since we disallow non-positive weights
                Err(DiskyError::Other(format!(
                    "Failed to create weighted distribution: {}",
                    e
                )))
            }
        }
    }

    /// Reads the next record from one of the sources
    ///
    /// This method samples a source based on weights and reads a record from it.
    /// If the sampled source is exhausted, it updates the distribution and tries again.
    ///
    /// # Returns
    ///
    /// - DiskyPiece::Record(bytes) if a record was read
    /// - DiskyPiece::EOF if all sources are exhausted
    pub fn read(&mut self) -> Result<DiskyPiece> {
        // Base case: no active iterators means we're at EOF
        if self.active_indices.is_empty() {
            return Ok(DiskyPiece::EOF);
        }

        // Sample an iterator using the current distribution
        let sampled_idx = self.distribution.sample(&mut self.rng);
        let iter_idx = self.active_indices[sampled_idx];

        match self.iterators[iter_idx].next() {
            Some(Ok(bytes)) => {
                // Successfully read a record
                Ok(DiskyPiece::Record(bytes))
            }
            Some(Err(e)) => {
                // Error reading from the iterator
                Err(e)
            }
            None => {
                // Iterator is exhausted - remove it from active indices
                debug!("Iterator {} is exhausted", iter_idx);
                self.active_indices.swap_remove(sampled_idx);

                // If we still have active iterators, update the distribution
                if !self.active_indices.is_empty() {
                    self.update_distribution()?;
                    self.read() // Recursively try to read from another iterator
                } else {
                    // No more active iterators, we're at EOF
                    Ok(DiskyPiece::EOF)
                }
            }
        }
    }

    /// Returns the number of active (non-exhausted) sources
    pub fn active_sources(&self) -> usize {
        self.active_indices.len()
    }

    /// Returns the total number of sources
    pub fn total_sources(&self) -> usize {
        self.iterators.len()
    }

    /// Returns the weights of all sources
    pub fn weights(&self) -> &[f64] {
        &self.weights
    }
}

/// Iterator implementation for SamplingReader
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

#[cfg(test)]
mod tests {
    // Tests will be added in a future PR
}
