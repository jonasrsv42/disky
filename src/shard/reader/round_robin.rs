//! Round-robin shard reader.

use bytes::Bytes;

use crate::error::{DiskyError, Result};
use crate::shard::source::Shard;
use crate::tree::reader::{Node, Reader};

/// Builder for [`RoundRobinShardReader`].
///
/// Takes a shard source (any `Iterator<Item = Result<Shard>>`) and builds a reader
/// that round-robins across multiple active shards.
///
/// # Examples
///
/// ```no_run
/// use disky::shard::reader::RoundRobinShardReaderConfig;
/// use disky::shard::source::{FileShards, SequentialShardSource};
///
/// # fn main() -> disky::error::Result<()> {
/// // Read from up to 4 shards concurrently, round-robin style.
/// let reader = RoundRobinShardReaderConfig::new(
///     SequentialShardSource::new(FileShards::from_pattern("/data", "shard")?)
/// )
/// .max_active(4)
/// .build()?;
///
/// for record in reader {
///     let bytes = record?;
///     // ...
/// }
/// # Ok(())
/// # }
/// ```
pub struct RoundRobinShardReaderConfig<ShardSource> {
    source: ShardSource,
    max_active: Option<usize>,
}

impl<ShardSource> RoundRobinShardReaderConfig<ShardSource> {
    /// Create a config with the given shard source iterator.
    ///
    /// By default, `max_active` is `None`, meaning all shards are opened upfront.
    pub fn new(source: ShardSource) -> Self {
        Self {
            source,
            max_active: None,
        }
    }

    /// Set the maximum number of shards to keep open simultaneously.
    ///
    /// - `None` (default): open all shards upfront. Do not use with infinite shard sources.
    /// - `Some(n)`: keep at most `n` readers open, replacing exhausted shards on the fly.
    pub fn max_active(mut self, max: usize) -> Self {
        self.max_active = Some(max);
        self
    }
}

impl<ShardSource> RoundRobinShardReaderConfig<ShardSource>
where
    ShardSource: Iterator<Item = Result<Shard>>,
{
    /// Build the [`RoundRobinShardReader`].
    ///
    /// # Errors
    ///
    /// Returns an error if `max_active` was set to 0.
    pub fn build(self) -> Result<RoundRobinShardReader<ShardSource>> {
        if self.max_active == Some(0) {
            return Err(DiskyError::Other("max_active must be at least 1".into()));
        }

        Ok(RoundRobinShardReader {
            shards: self.source,
            max_active: self.max_active,
            state: ReaderState::Start,
        })
    }
}

impl<ShardSource> Node for RoundRobinShardReaderConfig<ShardSource>
where
    ShardSource: Iterator<Item = Result<Shard>> + Send + Sync + 'static,
{
    fn make(self: Box<Self>) -> Result<Reader> {
        Ok(Box::new(self.build()?))
    }
}

struct ActiveShard {
    reader: Reader,
    shard_id: String,
}

enum ReaderState {
    /// Not yet initialized — will open the initial batch of shards.
    Start,
    /// Actively round-robining across open shard readers.
    Reading {
        readers: Vec<ActiveShard>,
        position: usize,
    },
    /// Terminal — all shards exhausted or hit an error.
    Done,
}

/// Reads records by round-robining across multiple active shards.
///
/// Maintains up to `max_active` shard readers simultaneously and reads one
/// record from each in turn. When a shard is exhausted, the next shard is
/// pulled from the iterator to replace it.
///
/// Implements `Iterator<Item = Result<Bytes>>` so it composes with other
/// tree-based reader nodes.
///
/// # Construction
///
/// - `max_active: None` — opens all shards from the iterator upfront.
///   Do not use with infinite iterators.
/// - `max_active: Some(n)` — keeps at most `n` readers open at once,
///   replacing exhausted shards on the fly.
///
/// Errors from individual shards are wrapped in [`DiskyError::ShardError`]
/// with the shard's id for traceability.
pub struct RoundRobinShardReader<ShardIter: Iterator<Item = Result<Shard>>> {
    shards: ShardIter,
    max_active: Option<usize>,
    state: ReaderState,
}

impl<ShardIter: Iterator<Item = Result<Shard>>> RoundRobinShardReader<ShardIter> {
    /// Open the next shard from the iterator, returning an `ActiveShard`.
    fn next_reader(shards: &mut ShardIter) -> Result<Option<ActiveShard>> {
        let shard = match shards.next() {
            Some(Ok(shard)) => shard,
            Some(Err(e)) => return Err(e),
            None => return Ok(None),
        };

        Ok(Some(ActiveShard {
            reader: shard.reader,
            shard_id: shard.id,
        }))
    }

    /// Replace or remove an exhausted reader at `position`.
    ///
    /// If a replacement shard is available, swaps it in at the current position.
    /// Otherwise removes the exhausted reader and adjusts position.
    fn replace_or_remove(
        readers: &mut Vec<ActiveShard>,
        position: &mut usize,
        shards: &mut ShardIter,
    ) -> Result<()> {
        match Self::next_reader(shards)? {
            Some(replacement) => {
                readers[*position] = replacement;
            }
            None => {
                readers.swap_remove(*position);

                // Reset position if we are now out of bounds.
                if *position >= readers.len() {
                    *position = 0;
                }
            }
        }
        Ok(())
    }

    /// Open the initial batch of shards (up to `max_active`, or all if `None`).
    fn initialize(&mut self) -> Result<Vec<ActiveShard>> {
        let mut readers = Vec::new();

        loop {
            if let Some(max) = self.max_active {
                if readers.len() >= max {
                    break;
                }
            }

            match Self::next_reader(&mut self.shards)? {
                Some(active) => readers.push(active),
                None => break,
            }
        }

        Ok(readers)
    }
}

impl<ShardIter: Iterator<Item = Result<Shard>>> Iterator for RoundRobinShardReader<ShardIter> {
    type Item = Result<Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match &mut self.state {
                ReaderState::Done => return None,

                ReaderState::Start => match self.initialize() {
                    Ok(readers) => {
                        self.state = ReaderState::Reading {
                            readers,
                            position: 0,
                        };
                    }
                    Err(e) => {
                        self.state = ReaderState::Done;
                        return Some(Err(e));
                    }
                },

                ReaderState::Reading { readers, position } => {
                    if readers.is_empty() {
                        self.state = ReaderState::Done;
                        return None;
                    }

                    let active = &mut readers[*position];

                    match active.reader.next() {
                        Some(Ok(bytes)) => {
                            *position = (*position + 1) % readers.len();
                            return Some(Ok(bytes));
                        }
                        Some(Err(e)) => {
                            let id = active.shard_id.clone();
                            self.state = ReaderState::Done;
                            return Some(Err(DiskyError::ShardError {
                                shard_id: id,
                                source: Box::new(e),
                            }));
                        }
                        None => {
                            // Shard exhausted, try to replace
                            if let Err(e) =
                                Self::replace_or_remove(readers, position, &mut self.shards)
                            {
                                self.state = ReaderState::Done;
                                return Some(Err(e));
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use bytes::Bytes;

    use super::RoundRobinShardReaderConfig;
    use crate::error::DiskyError;
    use crate::shard::source::{MemoryShards, SequentialShardSource};
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

    #[test]
    fn empty_shards_returns_none() {
        let shards = MemoryShards::new(|_| Ok(write_records(&[])), 0);
        let source = SequentialShardSource::new(shards);
        let mut reader = RoundRobinShardReaderConfig::new(source).build().unwrap();
        assert!(reader.next().is_none());
    }

    #[test]
    fn single_shard_reads_all() {
        let data = write_records(&[b"aaa", b"bbb", b"ccc"]);

        let shards = MemoryShards::new(move |_| Ok(data.clone()), 1);
        let source = SequentialShardSource::new(shards);
        let reader = RoundRobinShardReaderConfig::new(source).build().unwrap();

        let records: Vec<Bytes> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(records.len(), 3);
        assert_eq!(&records[0][..], b"aaa");
        assert_eq!(&records[1][..], b"bbb");
        assert_eq!(&records[2][..], b"ccc");
    }

    #[test]
    fn interleaves_across_shards() {
        // 3 shards, each with 2 records. Expect round-robin order.
        let shard0 = write_records(&[b"s0r0", b"s0r1"]);
        let shard1 = write_records(&[b"s1r0", b"s1r1"]);
        let shard2 = write_records(&[b"s2r0", b"s2r1"]);

        let buffers = vec![shard0, shard1, shard2];
        let shards = MemoryShards::new(move |i| Ok(buffers[i].clone()), 3);
        let source = SequentialShardSource::new(shards);
        let reader = RoundRobinShardReaderConfig::new(source).build().unwrap();

        let records: Vec<Bytes> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(records.len(), 6);
        assert_eq!(&records[0][..], b"s0r0");
        assert_eq!(&records[1][..], b"s1r0");
        assert_eq!(&records[2][..], b"s2r0");
        assert_eq!(&records[3][..], b"s0r1");
        assert_eq!(&records[4][..], b"s1r1");
        assert_eq!(&records[5][..], b"s2r1");
    }

    #[test]
    fn uneven_shards_drain_correctly() {
        // Shards with different record counts.
        let shard0 = write_records(&[b"s0r0"]);
        let shard1 = write_records(&[b"s1r0", b"s1r1", b"s1r2"]);
        let shard2 = write_records(&[b"s2r0", b"s2r1"]);

        let buffers = vec![shard0, shard1, shard2];
        let shards = MemoryShards::new(move |i| Ok(buffers[i].clone()), 3);
        let source = SequentialShardSource::new(shards);
        let reader = RoundRobinShardReaderConfig::new(source).build().unwrap();

        let records: Vec<Bytes> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(records.len(), 6);
        // All records from all shards must appear.
        let as_strs: Vec<String> = records
            .iter()
            .map(|b| String::from_utf8(b.to_vec()).unwrap())
            .collect();
        assert!(as_strs.contains(&"s0r0".to_string()));
        assert!(as_strs.contains(&"s1r0".to_string()));
        assert!(as_strs.contains(&"s1r1".to_string()));
        assert!(as_strs.contains(&"s1r2".to_string()));
        assert!(as_strs.contains(&"s2r0".to_string()));
        assert!(as_strs.contains(&"s2r1".to_string()));
    }

    #[test]
    fn refills_exhausted_shards() {
        // 4 shards, max_active=2. All 4 should be read.
        let shard0 = write_records(&[b"s0r0"]);
        let shard1 = write_records(&[b"s1r0"]);
        let shard2 = write_records(&[b"s2r0"]);
        let shard3 = write_records(&[b"s3r0"]);

        let buffers = vec![shard0, shard1, shard2, shard3];
        let shards = MemoryShards::new(move |i| Ok(buffers[i].clone()), 4);
        let source = SequentialShardSource::new(shards);
        let reader = RoundRobinShardReaderConfig::new(source)
            .max_active(2)
            .build()
            .unwrap();

        let records: Vec<Bytes> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(records.len(), 4);
        let as_strs: Vec<String> = records
            .iter()
            .map(|b| String::from_utf8(b.to_vec()).unwrap())
            .collect();
        assert!(as_strs.contains(&"s0r0".to_string()));
        assert!(as_strs.contains(&"s1r0".to_string()));
        assert!(as_strs.contains(&"s2r0".to_string()));
        assert!(as_strs.contains(&"s3r0".to_string()));
    }

    #[test]
    fn max_active_none_opens_all() {
        // With None, all shards should be active from the start.
        // 3 shards each with 1 record — round-robin reads one from each.
        let shard0 = write_records(&[b"a"]);
        let shard1 = write_records(&[b"b"]);
        let shard2 = write_records(&[b"c"]);

        let buffers = vec![shard0, shard1, shard2];
        let shards = MemoryShards::new(move |i| Ok(buffers[i].clone()), 3);
        let source = SequentialShardSource::new(shards);
        let reader = RoundRobinShardReaderConfig::new(source).build().unwrap();

        let records: Vec<Bytes> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(records.len(), 3);
        // Round-robin order: shard0 first, then shard1, then shard2.
        assert_eq!(&records[0][..], b"a");
        assert_eq!(&records[1][..], b"b");
        assert_eq!(&records[2][..], b"c");
    }

    #[test]
    fn error_terminates_iterator() {
        let shards = MemoryShards::new(|_| Err(DiskyError::Other("always fails".into())), 1);
        let source = SequentialShardSource::new(shards);
        let reader = RoundRobinShardReaderConfig::new(source).build().unwrap();

        let results: Vec<_> = reader.take(10).collect();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_err());
    }

    #[test]
    fn shard_open_error_propagates() {
        let good_data = write_records(&[b"record"]);
        let shards = MemoryShards::new(
            move |i| {
                if i == 1 {
                    Err(DiskyError::Other("shard open failed".into()))
                } else {
                    Ok(good_data.clone())
                }
            },
            2,
        );
        let source = SequentialShardSource::new(shards);

        // max_active=None tries to open both; second fails during init.
        let mut reader = RoundRobinShardReaderConfig::new(source).build().unwrap();

        let err = reader.next().unwrap().unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("shard open failed"),
            "Expected shard open error, got: {}",
            msg,
        );

        assert!(reader.next().is_none());
    }

    #[test]
    fn exhausted_iterator_stays_none() {
        let data = write_records(&[b"only"]);
        let shards = MemoryShards::new(move |_| Ok(data.clone()), 1);
        let source = SequentialShardSource::new(shards);
        let mut reader = RoundRobinShardReaderConfig::new(source).build().unwrap();

        assert!(reader.next().unwrap().is_ok());
        for _ in 0..5 {
            assert!(reader.next().is_none());
        }
    }

    #[test]
    fn max_active_zero_returns_error() {
        let data = write_records(&[b"aaa"]);
        let shards = MemoryShards::new(move |_| Ok(data.clone()), 1);
        let source = SequentialShardSource::new(shards);
        let err = RoundRobinShardReaderConfig::new(source)
            .max_active(0)
            .build()
            .err()
            .expect("expected error for max_active=0");
        let msg = format!("{}", err);
        assert!(
            msg.contains("max_active"),
            "Expected max_active error, got: {}",
            msg
        );
    }

    #[test]
    fn max_active_one_behaves_like_sequential() {
        let shard0 = write_records(&[b"s0r0", b"s0r1"]);
        let shard1 = write_records(&[b"s1r0"]);
        let shard2 = write_records(&[b"s2r0", b"s2r1", b"s2r2"]);

        let buffers = vec![shard0, shard1, shard2];
        let shards = MemoryShards::new(move |i| Ok(buffers[i].clone()), 3);
        let source = SequentialShardSource::new(shards);
        let reader = RoundRobinShardReaderConfig::new(source)
            .max_active(1)
            .build()
            .unwrap();

        // With max_active=1, shards are drained one at a time in order.
        let records: Vec<Bytes> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(records.len(), 6);
        assert_eq!(&records[0][..], b"s0r0");
        assert_eq!(&records[1][..], b"s0r1");
        assert_eq!(&records[2][..], b"s1r0");
        assert_eq!(&records[3][..], b"s2r0");
        assert_eq!(&records[4][..], b"s2r1");
        assert_eq!(&records[5][..], b"s2r2");
    }

    #[test]
    fn replacement_shard_failure_terminates() {
        // 2 shards: first is valid, second fails to open.
        // max_active=1 so the second is a replacement, not in the initial batch.
        let good_data = write_records(&[b"record"]);
        let shards = MemoryShards::new(
            move |i| {
                if i == 1 {
                    Err(DiskyError::Other("replacement failed".into()))
                } else {
                    Ok(good_data.clone())
                }
            },
            2,
        );
        let source = SequentialShardSource::new(shards);
        let mut reader = RoundRobinShardReaderConfig::new(source)
            .max_active(1)
            .build()
            .unwrap();

        // First shard reads fine.
        let first = reader.next().unwrap().unwrap();
        assert_eq!(&first[..], b"record");

        // Replacement shard fails to open.
        let err = reader.next().unwrap().unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("replacement failed"),
            "Expected replacement error, got: {}",
            msg,
        );

        assert!(reader.next().is_none());
    }
}
