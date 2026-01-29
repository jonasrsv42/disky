//! Round-robin interleaving of multiple reader tree nodes.
//!
//! This module provides a [`RoundRobinNode`] that interleaves records from
//! multiple child nodes in round-robin fashion.

use bytes::Bytes;

use crate::error::Result;

use super::{Node, Reader};

/// A node that interleaves records from multiple children in round-robin order.
///
/// When built, this node constructs all child readers and then reads one record
/// from each in turn, cycling through until all are exhausted.
///
/// # Example
///
/// ```ignore
/// use disky::tree::reader::{RoundRobinNode, Node};
/// use disky::reader::RecordReaderConfig;
///
/// // Interleave records from two different sources
/// let tree = RoundRobinNode::new()
///     .append(RecordReaderConfig::new(file_a))
///     .append(RecordReaderConfig::new(file_b));
///
/// let reader = tree.build()?;
/// for record in reader {
///     let bytes = record?;
///     // Records arrive interleaved: a0, b0, a1, b1, ...
/// }
/// ```
pub struct RoundRobinNode {
    children: Vec<Box<dyn Node>>,
}

impl RoundRobinNode {
    /// Create an empty round-robin node.
    pub fn new() -> Self {
        Self {
            children: Vec::new(),
        }
    }

    /// Append a child node.
    pub fn append(mut self, node: impl Node + 'static) -> Self {
        self.children.push(Box::new(node));
        self
    }

    /// Build the reader.
    pub fn build(self) -> Result<Reader> {
        Box::new(self).make()
    }
}

impl Default for RoundRobinNode {
    fn default() -> Self {
        Self::new()
    }
}

impl Node for RoundRobinNode {
    fn make(self: Box<Self>) -> Result<Reader> {
        // Build all children into readers
        let readers: Vec<Reader> = self
            .children
            .into_iter()
            .map(|node| node.make())
            .collect::<Result<Vec<_>>>()?;

        Ok(Box::new(RoundRobinReader::new(readers)))
    }
}

/// Create a round-robin interleaving node from multiple children.
///
/// This is a convenience function for creating a [`RoundRobinNode`] when you
/// already have boxed nodes. For building from scratch, prefer using
/// `RoundRobinNode::new().append(...).append(...)`.
///
/// # Example
///
/// ```ignore
/// use disky::tree::reader::interleave;
///
/// let tree = interleave(vec![child1, child2, child3]);
/// let reader = tree.make()?;
/// ```
pub fn interleave(children: Vec<Box<dyn Node>>) -> Box<dyn Node> {
    Box::new(RoundRobinNode { children })
}

/// Iterator that reads from multiple readers in round-robin order.
struct RoundRobinReader {
    readers: Vec<Reader>,
    position: usize,
}

impl RoundRobinReader {
    fn new(readers: Vec<Reader>) -> Self {
        Self {
            readers,
            position: 0,
        }
    }
}

impl Iterator for RoundRobinReader {
    type Item = Result<Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.readers.is_empty() {
                return None;
            }

            let reader = &mut self.readers[self.position];

            match reader.next() {
                Some(Ok(bytes)) => {
                    self.position = (self.position + 1) % self.readers.len();
                    return Some(Ok(bytes));
                }
                Some(Err(e)) => {
                    // Propagate errors immediately
                    return Some(Err(e));
                }
                None => {
                    // This reader is exhausted, remove it
                    let _ = self.readers.swap_remove(self.position);

                    // Adjust position if we're now out of bounds
                    if !self.readers.is_empty() && self.position >= self.readers.len() {
                        self.position = 0;
                    }
                    // Loop to try next reader
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::reader::RecordReaderConfig;
    use crate::writer::RecordWriterConfig;

    /// Write records into an in-memory disky buffer.
    fn write_records(records: &[&[u8]]) -> Vec<u8> {
        let mut buffer = Vec::new();
        {
            let mut writer = RecordWriterConfig::new(Cursor::new(&mut buffer))
                .build()
                .unwrap();
            for record in records {
                writer.write_record(record).unwrap();
            }
            writer.close().unwrap();
        }
        buffer
    }

    /// Create a boxed Node from record data.
    fn node(data: Vec<u8>) -> Box<dyn Node> {
        Box::new(RecordReaderConfig::new(Cursor::new(data)))
    }

    #[test]
    fn empty_children_returns_none() {
        let mut reader = RoundRobinNode::new().build().unwrap();
        assert!(reader.next().is_none());
    }

    #[test]
    fn single_child_reads_all() {
        let data = write_records(&[b"aaa", b"bbb", b"ccc"]);
        let reader = RoundRobinNode::new()
            .append(RecordReaderConfig::new(Cursor::new(data)))
            .build()
            .unwrap();

        let records: Vec<Bytes> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(records.len(), 3);
        assert_eq!(&records[0][..], b"aaa");
        assert_eq!(&records[1][..], b"bbb");
        assert_eq!(&records[2][..], b"ccc");
    }

    #[test]
    fn interleaves_multiple_children() {
        let data0 = write_records(&[b"a0", b"a1"]);
        let data1 = write_records(&[b"b0", b"b1"]);
        let data2 = write_records(&[b"c0", b"c1"]);

        let reader = RoundRobinNode::new()
            .append(RecordReaderConfig::new(Cursor::new(data0)))
            .append(RecordReaderConfig::new(Cursor::new(data1)))
            .append(RecordReaderConfig::new(Cursor::new(data2)))
            .build()
            .unwrap();

        let records: Vec<Bytes> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(records.len(), 6);
        // Round-robin order
        assert_eq!(&records[0][..], b"a0");
        assert_eq!(&records[1][..], b"b0");
        assert_eq!(&records[2][..], b"c0");
        assert_eq!(&records[3][..], b"a1");
        assert_eq!(&records[4][..], b"b1");
        assert_eq!(&records[5][..], b"c1");
    }

    #[test]
    fn uneven_children_drain_correctly() {
        // data0: a0 (1 record)
        // data1: b0, b1, b2 (3 records)
        // data2: c0, c1 (2 records)
        let data0 = write_records(&[b"a0"]);
        let data1 = write_records(&[b"b0", b"b1", b"b2"]);
        let data2 = write_records(&[b"c0", b"c1"]);

        let reader = RoundRobinNode::new()
            .append(RecordReaderConfig::new(Cursor::new(data0)))
            .append(RecordReaderConfig::new(Cursor::new(data1)))
            .append(RecordReaderConfig::new(Cursor::new(data2)))
            .build()
            .unwrap();

        let records: Vec<Bytes> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(records.len(), 6);

        // Round-robin order with swap_remove:
        // 1. a0 (pos 0->1)
        // 2. b0 (pos 1->2)
        // 3. c0 (pos 2->0)
        // 4. data0 exhausted, swap_remove(0) moves data2 to index 0
        // 5. c1 (pos 0->1)
        // 6. b1 (pos 1->0)
        // 7. data2 exhausted, swap_remove(0) moves data1 to index 0
        // 8. b2 (pos 0->0)
        assert_eq!(&records[0][..], b"a0");
        assert_eq!(&records[1][..], b"b0");
        assert_eq!(&records[2][..], b"c0");
        assert_eq!(&records[3][..], b"c1");
        assert_eq!(&records[4][..], b"b1");
        assert_eq!(&records[5][..], b"b2");
    }

    #[test]
    fn interleave_helper_creates_node() {
        let data0 = write_records(&[b"x"]);
        let data1 = write_records(&[b"y"]);

        let tree = interleave(vec![node(data0), node(data1)]);
        let reader = tree.make().unwrap();

        let records: Vec<Bytes> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(records.len(), 2);
        assert_eq!(&records[0][..], b"x");
        assert_eq!(&records[1][..], b"y");
    }

    #[test]
    fn exhausted_iterator_stays_none() {
        let data = write_records(&[b"only"]);
        let mut reader = RoundRobinNode::new()
            .append(RecordReaderConfig::new(Cursor::new(data)))
            .build()
            .unwrap();

        assert!(reader.next().unwrap().is_ok());
        for _ in 0..5 {
            assert!(reader.next().is_none());
        }
    }

    #[test]
    fn nested_interleave() {
        // Test composability: nesting round-robin nodes
        let data_a0 = write_records(&[b"a0"]);
        let data_a1 = write_records(&[b"a1"]);
        let data_b0 = write_records(&[b"b0"]);

        let inner = RoundRobinNode::new()
            .append(RecordReaderConfig::new(Cursor::new(data_a0)))
            .append(RecordReaderConfig::new(Cursor::new(data_a1)));

        let reader = RoundRobinNode::new()
            .append(inner)
            .append(RecordReaderConfig::new(Cursor::new(data_b0)))
            .build()
            .unwrap();

        let records: Vec<Bytes> = reader.map(|r| r.unwrap()).collect();

        assert_eq!(records.len(), 3);
        // Inner interleave produces: a0, a1
        // Outer interleaves (a0, a1 interleaved) with b0
        // First record from inner (a0), first from outer second child (b0), then a1
        assert_eq!(&records[0][..], b"a0");
        assert_eq!(&records[1][..], b"b0");
        assert_eq!(&records[2][..], b"a1");
    }
}
