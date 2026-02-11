//! Reservoir shuffle for streaming data randomization.
//!
//! This module provides a reservoir-based shuffle that can be applied to any
//! reader tree node. Unlike block-based shuffling which only reorders within
//! fixed blocks, reservoir shuffle allows items to mix freely across the entire
//! stream, providing better randomization for ML training workloads.

use bytes::Bytes;
use rand::seq::SliceRandom;
use rand::{RngExt, SeedableRng, rngs::StdRng};

use crate::error::{DiskyError, Result};
use crate::tree::reader::{Node, Reader};

/// Default buffer size for reservoir shuffle.
pub const DEFAULT_BUFFER_SIZE: usize = 42;

/// Options for configuring a [`ReservoirShuffle`].
#[derive(Debug, Clone, Copy)]
pub struct ReservoirShuffleOptions {
    /// Size of the shuffle buffer. Must be greater than zero. Defaults to 32.
    pub buffer_size: usize,
    /// Optional seed for deterministic shuffling. If not provided, entropy is used.
    pub seed: Option<u64>,
}

impl Default for ReservoirShuffleOptions {
    fn default() -> Self {
        Self {
            buffer_size: DEFAULT_BUFFER_SIZE,
            seed: None,
        }
    }
}

impl ReservoirShuffleOptions {
    /// Sets the buffer size.
    pub fn with_buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    /// Sets the random seed for deterministic behavior.
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = Some(seed);
        self
    }
}

/// Builder for [`ReservoirShuffle`].
///
/// This is a tree node that shuffles records from a child node using reservoir sampling.
/// The default buffer size is 42.
///
/// # Example
///
/// ```no_run
/// use disky::tree::sampling::ReservoirShuffleConfig;
/// use disky::reader::RecordReaderConfig;
/// use std::fs::File;
///
/// # fn main() -> disky::error::Result<()> {
/// let file = File::open("data.disky")?;
/// let shuffled = ReservoirShuffleConfig::new(RecordReaderConfig::new(file))
///     .with_buffer_size(1000)
///     .with_seed(42)
///     .build()?;
///
/// for record in shuffled {
///     let bytes = record?;
///     // Process shuffled record
/// }
/// # Ok(())
/// # }
/// ```
pub struct ReservoirShuffleConfig {
    child: Box<dyn Node>,
    options: ReservoirShuffleOptions,
}

impl ReservoirShuffleConfig {
    /// Creates a new config with the given child node.
    ///
    /// Uses the default buffer size of 42.
    pub fn new(child: impl Node + 'static) -> Self {
        Self {
            child: Box::new(child),
            options: ReservoirShuffleOptions::default(),
        }
    }

    /// Sets the options.
    pub fn options(mut self, options: ReservoirShuffleOptions) -> Self {
        self.options = options;
        self
    }

    /// Sets the buffer size.
    pub fn with_buffer_size(mut self, buffer_size: usize) -> Self {
        self.options.buffer_size = buffer_size;
        self
    }

    /// Sets the random seed for deterministic behavior.
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.options.seed = Some(seed);
        self
    }

    /// Builds the [`ReservoirShuffle`].
    ///
    /// # Errors
    ///
    /// Returns an error if `buffer_size` is zero or if the child node fails to build.
    pub fn build(self) -> Result<ReservoirShuffle> {
        if self.options.buffer_size == 0 {
            return Err(DiskyError::Other(
                "Buffer size must be greater than zero".to_string(),
            ));
        }

        let inner = self.child.make()?;

        let rng = match self.options.seed {
            Some(seed) => StdRng::seed_from_u64(seed),
            None => StdRng::try_from_rng(&mut rand::rngs::SysRng).map_err(|e| {
                DiskyError::Other(format!("Failed to initialize RNG from OS entropy: {}", e))
            })?,
        };

        Ok(ReservoirShuffle {
            inner,
            buffer: Vec::with_capacity(self.options.buffer_size),
            buffer_size: self.options.buffer_size,
            rng,
            state: State::Filling,
        })
    }
}

impl Node for ReservoirShuffleConfig {
    fn make(self: Box<Self>) -> Result<Reader> {
        Ok(Box::new(self.build()?))
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
/// ```no_run
/// use disky::tree::sampling::ReservoirShuffleConfig;
/// use disky::reader::RecordReaderConfig;
/// use std::fs::File;
///
/// # fn main() -> disky::error::Result<()> {
/// let file = File::open("data.disky")?;
/// let shuffled = ReservoirShuffleConfig::new(RecordReaderConfig::new(file))
///     .with_buffer_size(1000)
///     .build()?;
///
/// for record in shuffled {
///     let bytes = record?;
///     // Process shuffled record
/// }
/// # Ok(())
/// # }
/// ```
pub struct ReservoirShuffle {
    inner: Reader,
    buffer: Vec<Bytes>,
    buffer_size: usize,
    rng: StdRng,
    state: State,
}

impl ReservoirShuffle {
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

impl Iterator for ReservoirShuffle {
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
                        let idx = self.rng.random_range(0..self.buffer.len());
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tree::sampling::tests::helpers::{BytesNode, ErrorNode};
    use std::collections::HashSet;

    #[test]
    fn test_zero_buffer_size_returns_error() {
        let node = BytesNode::new(vec![b"a", b"b", b"c"]);
        let result = ReservoirShuffleConfig::new(node)
            .with_buffer_size(0)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_input() {
        let node = BytesNode::new(vec![]);
        let mut shuffle = ReservoirShuffleConfig::new(node)
            .with_buffer_size(10)
            .build()
            .unwrap();
        assert!(shuffle.next().is_none());
    }

    #[test]
    fn test_preserves_all_items() {
        let items: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d", b"e", b"f", b"g", b"h"];
        let expected: HashSet<Vec<u8>> = items.iter().map(|b| b.to_vec()).collect();

        let node = BytesNode::new(items);
        let shuffle = ReservoirShuffleConfig::new(node)
            .with_buffer_size(3)
            .with_seed(42)
            .build()
            .unwrap();

        let output: HashSet<Vec<u8>> = shuffle.map(|r| r.unwrap().to_vec()).collect();

        assert_eq!(output, expected, "All items should be preserved");
    }

    #[test]
    fn test_input_smaller_than_buffer() {
        let items: Vec<&[u8]> = vec![b"a", b"b", b"c"];
        let expected: HashSet<Vec<u8>> = items.iter().map(|b| b.to_vec()).collect();

        let node = BytesNode::new(items);
        let shuffle = ReservoirShuffleConfig::new(node)
            .with_buffer_size(10)
            .with_seed(42)
            .build()
            .unwrap();

        let output: HashSet<Vec<u8>> = shuffle.map(|r| r.unwrap().to_vec()).collect();

        assert_eq!(output, expected, "All items should be preserved");
    }

    #[test]
    fn test_input_equal_to_buffer() {
        let items: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d", b"e"];
        let expected: HashSet<Vec<u8>> = items.iter().map(|b| b.to_vec()).collect();

        let node = BytesNode::new(items);
        let shuffle = ReservoirShuffleConfig::new(node)
            .with_buffer_size(5)
            .with_seed(42)
            .build()
            .unwrap();

        let output: HashSet<Vec<u8>> = shuffle.map(|r| r.unwrap().to_vec()).collect();

        assert_eq!(output, expected, "All items should be preserved");
    }

    #[test]
    fn test_buffer_size_one() {
        // Buffer size 1 means each item swaps with the single buffer slot
        let items: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d"];
        let expected: HashSet<Vec<u8>> = items.iter().map(|b| b.to_vec()).collect();

        let node = BytesNode::new(items);
        let shuffle = ReservoirShuffleConfig::new(node)
            .with_buffer_size(1)
            .with_seed(42)
            .build()
            .unwrap();

        let output: HashSet<Vec<u8>> = shuffle.map(|r| r.unwrap().to_vec()).collect();

        assert_eq!(output, expected, "All items should be preserved");
    }

    #[test]
    fn test_deterministic_with_seed() {
        let items: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d", b"e", b"f", b"g", b"h"];
        let seed = 12345u64;

        let node1 = BytesNode::new(items.clone());
        let shuffle1 = ReservoirShuffleConfig::new(node1)
            .with_buffer_size(3)
            .with_seed(seed)
            .build()
            .unwrap();
        let output1: Vec<Vec<u8>> = shuffle1.map(|r| r.unwrap().to_vec()).collect();

        let node2 = BytesNode::new(items);
        let shuffle2 = ReservoirShuffleConfig::new(node2)
            .with_buffer_size(3)
            .with_seed(seed)
            .build()
            .unwrap();
        let output2: Vec<Vec<u8>> = shuffle2.map(|r| r.unwrap().to_vec()).collect();

        assert_eq!(output1, output2, "Same seed should produce same order");
    }

    #[test]
    fn test_no_output_during_filling() {
        // With buffer_size=5 and 3 items, we should only output during drain
        let items: Vec<&[u8]> = vec![b"a", b"b", b"c"];
        let node = BytesNode::new(items);
        let mut shuffle = ReservoirShuffleConfig::new(node)
            .with_buffer_size(5)
            .with_seed(42)
            .build()
            .unwrap();

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
        let node = ErrorNode::new(items, 2); // Error at position 2

        let mut shuffle = ReservoirShuffleConfig::new(node)
            .with_buffer_size(5)
            .with_seed(42)
            .build()
            .unwrap();

        // Items 0, 1 fill buffer, item 2 is error
        // Error should surface immediately
        let result = shuffle.next();
        assert!(result.is_some());
        assert!(result.unwrap().is_err());
    }

    #[test]
    fn test_can_continue_after_error() {
        let items: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d", b"e", b"f"];
        let node = ErrorNode::new(items, 2); // Error at position 2

        let mut shuffle = ReservoirShuffleConfig::new(node)
            .with_buffer_size(10)
            .with_seed(42)
            .build()
            .unwrap();

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
        let node = BytesNode::new(items_refs);
        let shuffle = ReservoirShuffleConfig::new(node)
            .with_buffer_size(5)
            .with_seed(42)
            .build()
            .unwrap();
        let output: Vec<Vec<u8>> = shuffle.map(|r| r.unwrap().to_vec()).collect();

        assert_ne!(output, original, "Shuffling should change the order");
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

        let node = BytesNode::new(items_refs);
        let shuffle = ReservoirShuffleConfig::new(node)
            .with_buffer_size(buffer_size)
            .with_seed(42)
            .build()
            .unwrap();
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

    #[test]
    fn test_as_tree_node() {
        // Test that ReservoirShuffleConfig works as a Node
        let items: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d", b"e"];
        let expected: HashSet<Vec<u8>> = items.iter().map(|b| b.to_vec()).collect();

        let inner_node = BytesNode::new(items);
        let shuffle_node = ReservoirShuffleConfig::new(inner_node)
            .with_buffer_size(3)
            .with_seed(42);

        // Use it as a Node
        let reader = Box::new(shuffle_node).make().unwrap();
        let output: HashSet<Vec<u8>> = reader.map(|r| r.unwrap().to_vec()).collect();

        assert_eq!(
            output, expected,
            "All items should be preserved through Node interface"
        );
    }
}
