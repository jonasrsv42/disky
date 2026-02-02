//! Sequential shard reader.

use bytes::Bytes;

use crate::error::{DiskyError, Result};
use crate::shard::source::Shard;
use crate::tree::reader::{Node, Reader};

/// Builder for [`SequentialShardReader`].
///
/// Takes a shard source (any `Iterator<Item = Result<Shard>>`) and builds a reader
/// that drains each shard fully before moving to the next.
///
/// # Examples
///
/// ```no_run
/// use disky::shard::reader::SequentialShardReaderConfig;
/// use disky::shard::source::{FileShards, SequentialShardSource};
///
/// # fn main() -> disky::error::Result<()> {
/// let reader = SequentialShardReaderConfig::new(
///     SequentialShardSource::new(FileShards::from_pattern("/data", "shard")?)
/// ).build();
///
/// for record in reader {
///     let bytes = record?;
///     // ...
/// }
/// # Ok(())
/// # }
/// ```
pub struct SequentialShardReaderConfig<ShardSource> {
    source: ShardSource,
}

impl<ShardSource> SequentialShardReaderConfig<ShardSource> {
    /// Create a config with the given shard source iterator.
    pub fn new(source: ShardSource) -> Self {
        Self { source }
    }
}

impl<ShardSource> SequentialShardReaderConfig<ShardSource>
where
    ShardSource: Iterator<Item = Result<Shard>>,
{
    /// Build the [`SequentialShardReader`].
    pub fn build(self) -> SequentialShardReader<ShardSource> {
        SequentialShardReader {
            shards: self.source,
            state: ReaderState::Start,
        }
    }
}

impl<ShardSource> Node for SequentialShardReaderConfig<ShardSource>
where
    ShardSource: Iterator<Item = Result<Shard>> + Send + Sync + 'static,
{
    fn make(self: Box<Self>) -> Result<Reader> {
        Ok(Box::new(self.build()))
    }
}

enum ReaderState {
    /// Ready to open the next shard.
    Start,
    /// Actively reading records from a shard.
    Reading { reader: Reader, shard_id: String },
    /// Terminal — iterator exhausted or hit an error.
    Done,
}

/// Reads records from a sequence of shards, draining each shard fully
/// before moving to the next.
///
/// Implements `Iterator<Item = Result<Bytes>>` so it composes with other
/// tree-based reader nodes.
///
/// The shard iteration order is determined by the shard source passed at
/// construction — use [`SequentialShardSource`] for in-order,
/// [`RandomRepeatingShardSource`] for shuffled/infinite, or any other
/// `Iterator<Item = Result<Shard>>`.
///
/// Errors from individual shards are wrapped in [`DiskyError::ShardError`]
/// with the shard's id for traceability.
///
/// [`SequentialShardSource`]: crate::shard::source::SequentialShardSource
/// [`RandomRepeatingShardSource`]: crate::shard::source::RandomRepeatingShardSource
pub struct SequentialShardReader<ShardIter: Iterator<Item = Result<Shard>>> {
    shards: ShardIter,
    state: ReaderState,
}

impl<ShardIter: Iterator<Item = Result<Shard>>> Iterator for SequentialShardReader<ShardIter> {
    type Item = Result<Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match &mut self.state {
                ReaderState::Done => return None,

                ReaderState::Start => match self.shards.next() {
                    None => {
                        self.state = ReaderState::Done;
                        return None;
                    }
                    Some(Err(e)) => {
                        self.state = ReaderState::Done;
                        return Some(Err(e));
                    }
                    Some(Ok(shard)) => {
                        self.state = ReaderState::Reading {
                            reader: shard.reader,
                            shard_id: shard.id,
                        };
                    }
                },

                ReaderState::Reading { reader, shard_id } => match reader.next() {
                    Some(Ok(bytes)) => return Some(Ok(bytes)),
                    Some(Err(e)) => {
                        let id = shard_id.clone();
                        self.state = ReaderState::Done;
                        return Some(Err(DiskyError::ShardError {
                            shard_id: id,
                            source: Box::new(e),
                        }));
                    }
                    None => {
                        // Shard exhausted, move to next
                        self.state = ReaderState::Start;
                    }
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use bytes::Bytes;

    use super::SequentialShardReaderConfig;
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
        let mut reader =
            SequentialShardReaderConfig::new(SequentialShardSource::new(shards)).build();
        assert!(reader.next().is_none());
    }

    #[test]
    fn single_shard_reads_all_records() {
        let data = write_records(&[b"aaa", b"bbb", b"ccc"]);

        let shards = MemoryShards::new(move |_| Ok(data.clone()), 1);
        let reader = SequentialShardReaderConfig::new(SequentialShardSource::new(shards)).build();

        let records: Vec<Bytes> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(records.len(), 3);
        assert_eq!(&records[0][..], b"aaa");
        assert_eq!(&records[1][..], b"bbb");
        assert_eq!(&records[2][..], b"ccc");
    }

    #[test]
    fn multiple_shards_reads_in_shard_order() {
        let shard0 = write_records(&[b"s0r0", b"s0r1"]);
        let shard1 = write_records(&[b"s1r0"]);
        let shard2 = write_records(&[b"s2r0", b"s2r1", b"s2r2"]);

        let buffers = vec![shard0, shard1, shard2];
        let shards = MemoryShards::new(move |i| Ok(buffers[i].clone()), 3);
        let reader = SequentialShardReaderConfig::new(SequentialShardSource::new(shards)).build();

        let records: Vec<Bytes> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(records.len(), 6);
        // Shard 0
        assert_eq!(&records[0][..], b"s0r0");
        assert_eq!(&records[1][..], b"s0r1");
        // Shard 1
        assert_eq!(&records[2][..], b"s1r0");
        // Shard 2
        assert_eq!(&records[3][..], b"s2r0");
        assert_eq!(&records[4][..], b"s2r1");
        assert_eq!(&records[5][..], b"s2r2");
    }

    #[test]
    fn record_integrity() {
        let shard_data: Vec<Vec<u8>> = (0..3)
            .map(|shard_idx| {
                let records: Vec<Vec<u8>> = (0..5)
                    .map(|rec_idx| format!("shard{}rec{}", shard_idx, rec_idx).into_bytes())
                    .collect();
                let refs: Vec<&[u8]> = records.iter().map(|r| r.as_slice()).collect();
                write_records(&refs)
            })
            .collect();

        let shards = MemoryShards::new(move |i| Ok(shard_data[i].clone()), 3);
        let reader = SequentialShardReaderConfig::new(SequentialShardSource::new(shards)).build();

        let records: Vec<String> = reader
            .map(|r| String::from_utf8(r.unwrap().to_vec()).unwrap())
            .collect();

        assert_eq!(records.len(), 15);
        for shard_idx in 0..3 {
            for rec_idx in 0..5 {
                let idx = shard_idx * 5 + rec_idx;
                assert_eq!(records[idx], format!("shard{}rec{}", shard_idx, rec_idx));
            }
        }
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
        let mut reader =
            SequentialShardReaderConfig::new(SequentialShardSource::new(shards)).build();

        // First shard succeeds
        let first = reader.next().unwrap().unwrap();
        assert_eq!(&first[..], b"record");

        // Second shard fails to open
        let err = reader.next().unwrap().unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("shard open failed"),
            "Expected shard open error, got: {}",
            msg
        );

        // Iterator is done after error
        assert!(reader.next().is_none());
    }

    #[test]
    fn error_terminates_iterator() {
        let shards = MemoryShards::new(|_| Err(DiskyError::Other("always fails".into())), 1);
        let reader = SequentialShardReaderConfig::new(SequentialShardSource::new(shards)).build();

        // Must produce exactly one error then stop
        let results: Vec<_> = reader.take(10).collect();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_err());
    }

    #[test]
    fn exhausted_iterator_stays_none() {
        let data = write_records(&[b"only"]);
        let shards = MemoryShards::new(move |_| Ok(data.clone()), 1);
        let mut reader =
            SequentialShardReaderConfig::new(SequentialShardSource::new(shards)).build();

        assert!(reader.next().unwrap().is_ok());
        for _ in 0..5 {
            assert!(reader.next().is_none());
        }
    }
}
