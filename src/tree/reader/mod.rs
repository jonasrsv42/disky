//! Tree-based reader construction.
//!
//! This module provides a composable tree-based reader architecture where nodes
//! describe the structure of a reader tree, and calling `make()` on the root
//! node builds the actual iterator tree.
//!
//! # The Node Trait
//!
//! All reader tree nodes implement the [`Node`] trait, which has a single method
//! `make()` that consumes the node and produces a boxed iterator.
//!
//! # Example
//!
//! ```ignore
//! use disky::tree::reader::*;
//!
//! // Build a tree that interleaves records from two shard sources
//! let tree = interleave(vec![
//!     sequential_shards(FileShards::from_pattern("/data/a", "shard")?),
//!     sequential_shards(FileShards::from_pattern("/data/b", "shard")?),
//! ]);
//!
//! // Build the actual iterator
//! let reader = tree.make()?;
//!
//! // Use it
//! for record in reader {
//!     let bytes = record?;
//!     // ...
//! }
//! ```

mod round_robin;

pub use round_robin::{RoundRobinNode, interleave};

use bytes::Bytes;

use crate::error::Result;

/// Type alias for the boxed iterator that nodes produce.
///
/// All nodes produce this type from their `make()` method, enabling
/// heterogeneous tree composition.
pub type Reader = Box<dyn Iterator<Item = Result<Bytes>> + Send>;

/// A node in a reader tree.
///
/// Nodes describe the structure of a reader tree without actually constructing
/// the iterators. This separation allows:
///
/// - Inspecting or modifying the tree structure before building
/// - Deferring resource acquisition (file opens, etc.) until `make()` is called
/// - Clean error handling at build time
///
/// Call [`Node::make()`] on the root node to recursively build the entire
/// iterator tree.
///
/// # Implementing Node
///
/// ```ignore
/// use disky::tree::reader::{Node, Reader};
/// use disky::error::Result;
///
/// struct MyCustomNode {
///     // ... configuration ...
/// }
///
/// impl Node for MyCustomNode {
///     fn make(self: Box<Self>) -> Result<Reader> {
///         // Build and return the actual iterator
///         Ok(Box::new(my_iterator))
///     }
/// }
/// ```
///
/// # Why `self: Box<Self>`?
///
/// The `make` method takes `self: Box<Self>` rather than `self` to maintain
/// object safety. This allows nodes to be stored as `Box<dyn Node>` in
/// collections, enabling heterogeneous tree composition where children
/// can be different concrete node types.
pub trait Node: Send {
    /// Consume this node and build the actual iterator.
    ///
    /// This method recursively builds the entire subtree rooted at this node.
    /// It may fail if resource acquisition (e.g., opening files) fails.
    fn make(self: Box<Self>) -> Result<Reader>;
}
