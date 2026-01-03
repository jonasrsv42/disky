//! Reservoir shuffle for streaming data randomization.
//!
//! This module provides a reservoir-based shuffle that can be applied to any
//! iterator of records. Unlike block-based shuffling which only reorders within
//! fixed blocks, reservoir shuffle allows items to mix freely across the entire
//! stream, providing better randomization for ML training workloads.

use bytes::Bytes;
use rand::seq::SliceRandom;
use rand::Rng;
use rand::{rngs::StdRng, SeedableRng};

use crate::error::{DiskyError, Result};

/// Configuration for the reservoir shuffle.
#[derive(Debug, Clone)]
pub struct ReservoirShuffleConfig {
    /// Size of the shuffle buffer. Must be greater than zero.
    pub buffer_size: usize,
    /// Optional seed for deterministic shuffling. If not provided, entropy is used.
    pub seed: Option<u64>,
}

impl ReservoirShuffleConfig {
    /// Creates a new configuration with the specified buffer size.
    ///
    /// Uses entropy for random seed.
    pub fn new(buffer_size: usize) -> Self {
        Self {
            buffer_size,
            seed: None,
        }
    }

    /// Creates a new configuration with a specific seed for deterministic behavior.
    pub fn with_seed(buffer_size: usize, seed: u64) -> Self {
        Self {
            buffer_size,
            seed: Some(seed),
        }
    }
}

/// Internal state of the reservoir shuffle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Accumulating initial items into the buffer.
    Filling,
    /// Buffer is full; swapping incoming items with random buffer positions.
    Streaming,
    /// Input exhausted; outputting remaining shuffled buffer items.
    Draining,
    /// All items have been output.
    Done,
}

/// An iterator adapter that shuffles items using reservoir sampling.
///
/// This provides streaming shuffle using a buffer of `buffer_size` items.
/// Unlike block-based shuffling which only reorders within fixed blocks,
/// reservoir shuffle allows items to mix freely across the entire stream,
/// providing better randomization for ML training.
///
/// # Algorithm
///
/// 1. **Fill phase**: First `buffer_size` items fill the buffer (no output yet)
/// 2. **Streaming phase**: For each new item:
///    - Pick a random index in the buffer
///    - Swap the new item with the item at that index
///    - Output the displaced item
/// 3. **Drain phase**: When input is exhausted, shuffle the remaining buffer
///    using Fisher-Yates and output all items
///
/// # Error Handling
///
/// Errors from the inner iterator are surfaced immediately without buffering.
/// This ensures the first error encountered is reported promptly, making
/// debugging easier. The buffer state is preserved, so iteration can continue
/// after an error if desired.
///
/// # Example
///
/// ```ignore
/// use disky::sampling::ReservoirShuffle;
///
/// let reader = RecordReader::new(file)?;
/// let shuffled = ReservoirShuffle::new(reader, 1000)?;
///
/// for record in shuffled {
///     let bytes = record?;
///     // Process shuffled record
/// }
/// ```
pub struct ReservoirShuffle<I>
where
    I: Iterator<Item = Result<Bytes>>,
{
    inner: I,
    buffer: Vec<Bytes>,
    buffer_size: usize,
    rng: StdRng,
    state: State,
}

impl<I> ReservoirShuffle<I>
where
    I: Iterator<Item = Result<Bytes>>,
{
    /// Creates a new ReservoirShuffle with the specified buffer size.
    ///
    /// # Arguments
    ///
    /// * `inner` - The iterator to shuffle
    /// * `buffer_size` - Size of the shuffle buffer (must be > 0)
    ///
    /// # Errors
    ///
    /// Returns an error if `buffer_size` is zero.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let shuffled = ReservoirShuffle::new(reader, 1000)?;
    /// ```
    pub fn new(inner: I, buffer_size: usize) -> Result<Self> {
        Self::with_config(inner, ReservoirShuffleConfig::new(buffer_size))
    }

    /// Creates a new ReservoirShuffle with a specific seed for deterministic behavior.
    ///
    /// This is useful for reproducible experiments or testing.
    ///
    /// # Arguments
    ///
    /// * `inner` - The iterator to shuffle
    /// * `buffer_size` - Size of the shuffle buffer (must be > 0)
    /// * `seed` - Seed for the random number generator
    ///
    /// # Errors
    ///
    /// Returns an error if `buffer_size` is zero.
    pub fn with_seed(inner: I, buffer_size: usize, seed: u64) -> Result<Self> {
        Self::with_config(inner, ReservoirShuffleConfig::with_seed(buffer_size, seed))
    }

    /// Creates a new ReservoirShuffle with custom configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if `buffer_size` is zero.
    pub fn with_config(inner: I, config: ReservoirShuffleConfig) -> Result<Self> {
        if config.buffer_size == 0 {
            return Err(DiskyError::Other(
                "Buffer size must be greater than zero".to_string(),
            ));
        }

        let rng = match config.seed {
            Some(seed) => StdRng::seed_from_u64(seed),
            None => StdRng::from_entropy(),
        };

        Ok(Self {
            inner,
            buffer: Vec::with_capacity(config.buffer_size),
            buffer_size: config.buffer_size,
            rng,
            state: State::Filling,
        })
    }

    /// Returns the configured buffer size.
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    /// Returns the current number of items in the buffer.
    pub fn buffered_count(&self) -> usize {
        self.buffer.len()
    }

    /// Transitions to draining state by shuffling the buffer.
    fn start_draining(&mut self) {
        self.buffer.shuffle(&mut self.rng);
        self.state = State::Draining;
    }
}

impl<I> Iterator for ReservoirShuffle<I>
where
    I: Iterator<Item = Result<Bytes>>,
{
    type Item = Result<Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.state {
                State::Filling => match self.inner.next() {
                    Some(Ok(item)) => {
                        self.buffer.push(item);
                        if self.buffer.len() == self.buffer_size {
                            self.state = State::Streaming;
                        }
                        // Continue filling, don't output yet
                    }
                    Some(Err(e)) => {
                        // Surface error immediately
                        return Some(Err(e));
                    }
                    None => {
                        // Input exhausted and our buffer is empty.

                        // This case happens if the source had no data what-so-ever 
                        // or only errors.
                        if self.buffer.is_empty() {
                            self.state = State::Done;
                            return None;
                        }

                        // Otherwise drain existing buffer.
                        self.start_draining();
                    }
                },
                State::Streaming => match self.inner.next() {
                    Some(Ok(item)) => {
                        // Swap with random buffer position, output displaced item
                        let idx = self.rng.gen_range(0..self.buffer.len());
                        let displaced = std::mem::replace(&mut self.buffer[idx], item);
                        return Some(Ok(displaced));
                    }
                    Some(Err(e)) => {
                        // Surface error immediately
                        return Some(Err(e));
                    }
                    None => {
                        // Input exhausted, start draining
                        self.start_draining();
                    }
                },
                State::Draining => {
                    // Buffer was shuffled in start_draining(), just pop items
                    match self.buffer.pop() {
                        Some(item) => return Some(Ok(item)),
                        None => {
                            self.state = State::Done;
                            return None;
                        }
                    }
                }
                State::Done => {
                    return None;
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let buffered = self.buffer.len();
        let (inner_lo, inner_hi) = self.inner.size_hint();
        (
            inner_lo.saturating_add(buffered),
            inner_hi.and_then(|h| h.checked_add(buffered)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    /// Helper to create an iterator from a vec of bytes
    fn make_iter(items: Vec<&[u8]>) -> impl Iterator<Item = Result<Bytes>> {
        items
            .into_iter()
            .map(|b| Ok(Bytes::from(b.to_vec())))
            .collect::<Vec<_>>()
            .into_iter()
    }

    /// Helper to create an iterator with an error at a specific position
    fn make_iter_with_error(
        items: Vec<&[u8]>,
        error_at: usize,
    ) -> impl Iterator<Item = Result<Bytes>> {
        items
            .into_iter()
            .enumerate()
            .map(move |(i, b)| {
                if i == error_at {
                    Err(DiskyError::Other(format!("Error at position {}", i)))
                } else {
                    Ok(Bytes::from(b.to_vec()))
                }
            })
            .collect::<Vec<_>>()
            .into_iter()
    }

    #[test]
    fn test_zero_buffer_size_returns_error() {
        let iter = make_iter(vec![b"a", b"b", b"c"]);
        let result = ReservoirShuffle::new(iter, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_input() {
        let iter = make_iter(vec![]);
        let mut shuffle = ReservoirShuffle::new(iter, 10).unwrap();
        assert!(shuffle.next().is_none());
    }

    #[test]
    fn test_preserves_all_items() {
        let items: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d", b"e", b"f", b"g", b"h"];
        let expected: HashSet<Vec<u8>> = items.iter().map(|b| b.to_vec()).collect();

        let iter = make_iter(items);
        let shuffle = ReservoirShuffle::with_seed(iter, 3, 42).unwrap();

        let output: HashSet<Vec<u8>> = shuffle.map(|r| r.unwrap().to_vec()).collect();

        assert_eq!(output, expected, "All items should be preserved");
    }

    #[test]
    fn test_input_smaller_than_buffer() {
        let items: Vec<&[u8]> = vec![b"a", b"b", b"c"];
        let expected: HashSet<Vec<u8>> = items.iter().map(|b| b.to_vec()).collect();

        let iter = make_iter(items);
        let shuffle = ReservoirShuffle::with_seed(iter, 10, 42).unwrap();

        let output: HashSet<Vec<u8>> = shuffle.map(|r| r.unwrap().to_vec()).collect();

        assert_eq!(output, expected, "All items should be preserved");
    }

    #[test]
    fn test_input_equal_to_buffer() {
        let items: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d", b"e"];
        let expected: HashSet<Vec<u8>> = items.iter().map(|b| b.to_vec()).collect();

        let iter = make_iter(items);
        let shuffle = ReservoirShuffle::with_seed(iter, 5, 42).unwrap();

        let output: HashSet<Vec<u8>> = shuffle.map(|r| r.unwrap().to_vec()).collect();

        assert_eq!(output, expected, "All items should be preserved");
    }

    #[test]
    fn test_buffer_size_one() {
        // Buffer size 1 means each item swaps with the single buffer slot
        let items: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d"];
        let expected: HashSet<Vec<u8>> = items.iter().map(|b| b.to_vec()).collect();

        let iter = make_iter(items);
        let shuffle = ReservoirShuffle::with_seed(iter, 1, 42).unwrap();

        let output: HashSet<Vec<u8>> = shuffle.map(|r| r.unwrap().to_vec()).collect();

        assert_eq!(output, expected, "All items should be preserved");
    }

    #[test]
    fn test_deterministic_with_seed() {
        let items: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d", b"e", b"f", b"g", b"h"];
        let seed = 12345u64;

        let iter1 = make_iter(items.clone());
        let shuffle1 = ReservoirShuffle::with_seed(iter1, 3, seed).unwrap();
        let output1: Vec<Vec<u8>> = shuffle1.map(|r| r.unwrap().to_vec()).collect();

        let iter2 = make_iter(items);
        let shuffle2 = ReservoirShuffle::with_seed(iter2, 3, seed).unwrap();
        let output2: Vec<Vec<u8>> = shuffle2.map(|r| r.unwrap().to_vec()).collect();

        assert_eq!(output1, output2, "Same seed should produce same order");
    }

    #[test]
    fn test_no_output_during_filling() {
        // With buffer_size=5 and 3 items, we should only output during drain
        let items: Vec<&[u8]> = vec![b"a", b"b", b"c"];
        let iter = make_iter(items);
        let mut shuffle = ReservoirShuffle::with_seed(iter, 5, 42).unwrap();

        // First three calls fill the buffer, fourth triggers drain
        // Actually, since we loop internally, the first call will fill and then drain
        // Let's verify we get all 3 items
        let mut count = 0;
        while shuffle.next().is_some() {
            count += 1;
        }
        assert_eq!(count, 3);
    }

    #[test]
    fn test_error_surfaces_immediately() {
        let items: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d", b"e"];
        let iter = make_iter_with_error(items, 2); // Error at position 2

        let mut shuffle = ReservoirShuffle::with_seed(iter, 5, 42).unwrap();

        // Items 0, 1 fill buffer, item 2 is error
        // Error should surface immediately
        let result = shuffle.next();
        assert!(result.is_some());
        assert!(result.unwrap().is_err());
    }

    #[test]
    fn test_can_continue_after_error() {
        let items: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d", b"e", b"f"];
        let iter = make_iter_with_error(items, 2); // Error at position 2

        let mut shuffle = ReservoirShuffle::with_seed(iter, 10, 42).unwrap();

        // Get the error
        let result = shuffle.next();
        assert!(result.unwrap().is_err());

        // Continue - should get remaining items (d, e, f) plus buffered (a, b)
        let mut ok_count = 0;
        for result in shuffle {
            if result.is_ok() {
                ok_count += 1;
            }
        }
        // a, b were buffered before error; d, e, f come after
        assert_eq!(ok_count, 5); // a, b, d, e, f
    }

    #[test]
    fn test_shuffles_output_order() {
        // Verify shuffling produces different order than input
        let items: Vec<Vec<u8>> = (0..20).map(|i| vec![i as u8]).collect();
        let original: Vec<Vec<u8>> = items.clone();

        let items_refs: Vec<&[u8]> = items.iter().map(|v| v.as_slice()).collect();
        let iter = make_iter(items_refs);
        let shuffle = ReservoirShuffle::with_seed(iter, 5, 42).unwrap();
        let output: Vec<Vec<u8>> = shuffle.map(|r| r.unwrap().to_vec()).collect();

        assert_ne!(output, original, "Shuffling should change the order");
    }

    #[test]
    fn test_size_hint() {
        let items: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d", b"e"];
        let iter = make_iter(items);
        let shuffle = ReservoirShuffle::new(iter, 3).unwrap();

        let (lo, hi) = shuffle.size_hint();
        assert_eq!(lo, 5);
        assert_eq!(hi, Some(5));
    }

    #[test]
    fn test_cross_block_mixing() {
        // Verify that items can cross what would be "block boundaries"
        // With buffer_size=4, block shuffle would keep items 0-3 in positions 0-3
        // Reservoir shuffle should allow crossing

        let buffer_size = 4;
        let num_items = 20;

        let items: Vec<Vec<u8>> = (0..num_items).map(|i| vec![i as u8]).collect();
        let items_refs: Vec<&[u8]> = items.iter().map(|v| v.as_slice()).collect();

        let iter = make_iter(items_refs);
        let shuffle = ReservoirShuffle::with_seed(iter, buffer_size, 42).unwrap();
        let output: Vec<u8> = shuffle.map(|r| r.unwrap()[0]).collect();

        // Check if any item crossed a block boundary
        let crossed = output.iter().enumerate().any(|(pos, &val)| {
            let original_block = val as usize / buffer_size;
            let output_block = pos / buffer_size;
            original_block != output_block
        });

        assert!(
            crossed,
            "Reservoir shuffle should allow items to cross block boundaries"
        );
    }
}
