//! Shared test helpers for sampling tests.

use bytes::Bytes;

use crate::error::{DiskyError, Result};
use crate::tree::reader::{Node, Reader};

/// A simple node that produces a numbered sequence of bytes for testing.
///
/// Produces records like "A0", "A1", "A2", etc.
pub struct SequenceNode {
    prefix: &'static str,
    count: usize,
}

impl SequenceNode {
    /// Create a new sequence node.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Prefix for each record (e.g., "A" produces "A0", "A1", ...)
    /// * `count` - Number of records to produce
    pub fn new(prefix: &'static str, count: usize) -> Self {
        Self { prefix, count }
    }
}

impl Node for SequenceNode {
    fn make(self: Box<Self>) -> Result<Reader> {
        let prefix = self.prefix;
        let count = self.count;
        Ok(Box::new((0..count).map(move |i| {
            let data = format!("{}{}", prefix, i);
            Ok(Bytes::from(data))
        })))
    }
}

/// A node that produces items from a slice of bytes.
pub struct BytesNode {
    items: Vec<Vec<u8>>,
}

impl BytesNode {
    /// Create a new bytes node from a slice of byte slices.
    pub fn new(items: Vec<&[u8]>) -> Self {
        Self {
            items: items.into_iter().map(|b| b.to_vec()).collect(),
        }
    }
}

impl Node for BytesNode {
    fn make(self: Box<Self>) -> Result<Reader> {
        Ok(Box::new(self.items.into_iter().map(|b| Ok(Bytes::from(b)))))
    }
}

/// A node that produces an error at a specific position.
pub struct ErrorNode {
    items: Vec<Vec<u8>>,
    error_at: usize,
}

impl ErrorNode {
    /// Create a new error node.
    ///
    /// # Arguments
    ///
    /// * `items` - Items to produce
    /// * `error_at` - Position at which to produce an error (0-indexed)
    pub fn new(items: Vec<&[u8]>, error_at: usize) -> Self {
        Self {
            items: items.into_iter().map(|b| b.to_vec()).collect(),
            error_at,
        }
    }
}

impl Node for ErrorNode {
    fn make(self: Box<Self>) -> Result<Reader> {
        let error_at = self.error_at;
        Ok(Box::new(self.items.into_iter().enumerate().map(
            move |(i, b)| {
                if i == error_at {
                    Err(DiskyError::Other(format!("Error at position {}", i)))
                } else {
                    Ok(Bytes::from(b))
                }
            },
        )))
    }
}
