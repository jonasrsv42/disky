use std::io::{Read, Seek};

use bytes::Bytes;

use crate::error::{DiskyError, Result};
use crate::reader::{DiskyPiece, RecordReader, RecordReaderConfig, RecordReaderOptions};
use crate::shard::source::Shard;
use crate::tree::reader::{Node, Reader};

/// Builder for [`SequentialShardReader`].
///
/// Takes a shard source (any `Iterator<Item = Result<Shard<Source>>>`) and optional
/// configuration. Call [`build()`](Self::build) to create the reader, or use as a
/// [`Node`] in a reader tree.
///
/// The shard source determines the order shards are visited. Use:
/// - [`SequentialShardSource`](crate::shard::source::SequentialShardSource) for in-order
/// - [`RandomRepeatingShardSource`](crate::shard::source::RandomRepeatingShardSource) for shuffled/infinite
///
/// # Examples
///
/// ```ignore
/// use disky::shard::reader::SequentialShardReaderConfig;
/// use disky::shard::source::{FileShards, SequentialShardSource};
///
/// let reader = SequentialShardReaderConfig::new(
///     SequentialShardSource::new(FileShards::from_pattern("/data", "shard")?)
/// ).build();
///
/// for record in reader {
///     let bytes = record?;
///     // ...
/// }
/// ```
pub struct SequentialShardReaderConfig<ShardSource> {
    source: ShardSource,
    reader_options: RecordReaderOptions,
}

impl<ShardSource> SequentialShardReaderConfig<ShardSource> {
    /// Create a config with the given shard source iterator.
    pub fn new(source: ShardSource) -> Self {
        Self {
            source,
            reader_options: RecordReaderOptions::default(),
        }
    }

    /// Set the [`RecordReaderOptions`] used for each shard's reader.
    pub fn reader_options(mut self, options: RecordReaderOptions) -> Self {
        self.reader_options = options;
        self
    }
}

impl<Source, ShardSource> SequentialShardReaderConfig<ShardSource>
where
    Source: Read + Seek,
    ShardSource: Iterator<Item = Result<Shard<Source>>>,
{
    /// Build the [`SequentialShardReader`].
    pub fn build(self) -> SequentialShardReader<Source, ShardSource> {
        SequentialShardReader {
            shards: self.source,
            options: self.reader_options,
            state: ReaderState::Start,
        }
    }
}

impl<Source, ShardSource> Node for SequentialShardReaderConfig<ShardSource>
where
    Source: Read + Seek + Send + 'static,
    ShardSource: Iterator<Item = Result<Shard<Source>>> + Send + 'static,
{
    fn make(self: Box<Self>) -> Result<Reader> {
        Ok(Box::new(self.build()))
    }
}

enum ReaderState<Source: Read + Seek> {
    /// Ready to open the next shard.
    Start,
    /// Actively reading records from a shard.
    Reading {
        reader: RecordReader<Source>,
        shard_id: String,
    },
    /// Terminal — iterator exhausted or hit an error.
    Done,
}

/// Reads records from a sequence of shards, draining each shard fully
/// before moving to the next.
///
/// Implements `Iterator<Item = Result<Bytes>>` so it composes with other
/// tree-based reader nodes.
///
/// The shard iteration order is determined by the `ShardIter` passed at
/// construction — use [`SequentialShardSource`] for in-order,
/// [`RandomRepeatingShardSource`] for shuffled/infinite, or any other
/// `Iterator<Item = Result<Shard<Source>>>`.
///
/// Errors from individual shards are wrapped in [`DiskyError::ShardError`]
/// with the shard's id for traceability.
///
/// [`SequentialShardSource`]: crate::shard::source::SequentialShardSource
/// [`RandomRepeatingShardSource`]: crate::shard::source::RandomRepeatingShardSource
pub struct SequentialShardReader<
    Source: Read + Seek,
    ShardIter: Iterator<Item = Result<Shard<Source>>>,
> {
    shards: ShardIter,
    options: RecordReaderOptions,
    state: ReaderState<Source>,
}

impl<Source: Read + Seek, ShardIter: Iterator<Item = Result<Shard<Source>>>> Iterator
    for SequentialShardReader<Source, ShardIter>
{
    type Item = Result<Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.state {
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
                    Some(Ok(shard)) => match RecordReaderConfig::new(shard.source)
                        .options(self.options)
                        .build()
                    {
                        Ok(reader) => {
                            self.state = ReaderState::Reading {
                                reader,
                                shard_id: shard.id,
                            };
                        }
                        Err(e) => {
                            self.state = ReaderState::Done;
                            return Some(Err(DiskyError::ShardError {
                                shard_id: shard.id,
                                source: Box::new(e),
                            }));
                        }
                    },
                },

                ReaderState::Reading {
                    ref mut reader,
                    ref shard_id,
                } => match reader.next_record() {
                    Ok(DiskyPiece::Record(bytes)) => return Some(Ok(bytes)),
                    Ok(DiskyPiece::EOF) => {
                        self.state = ReaderState::Start;
                    }
                    Err(e) => {
                        let id = shard_id.clone();
                        self.state = ReaderState::Done;
                        return Some(Err(DiskyError::ShardError {
                            shard_id: id,
                            source: Box::new(e),
                        }));
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

    use crate::error::DiskyError;
    use crate::shard::source::{MemoryShards, SequentialShardSource, Shard, Shards};
    use crate::writer::RecordWriterConfig;

    use super::SequentialShardReaderConfig;

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
        let shards = MemoryShards::new(|_| Ok(vec![]), 0);
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
    fn shard_open_error_is_wrapped() {
        struct FailOnSecond {
            good_data: Vec<u8>,
        }

        impl Shards for FailOnSecond {
            type Source = Cursor<Vec<u8>>;

            fn count(&self) -> usize {
                2
            }

            fn open(&self, index: usize) -> crate::error::Result<Shard<Cursor<Vec<u8>>>> {
                if index == 1 {
                    return Err(DiskyError::Other("shard open failed".into()));
                }
                Ok(Shard {
                    source: Cursor::new(self.good_data.clone()),
                    id: format!("test_shard_{}", index),
                })
            }
        }

        let data = write_records(&[b"record"]);
        let shards = FailOnSecond { good_data: data };
        let mut reader =
            SequentialShardReaderConfig::new(SequentialShardSource::new(shards)).build();

        // First shard succeeds.
        let first = reader.next().unwrap().unwrap();
        assert_eq!(&first[..], b"record");

        // Second shard fails to open — error passes through from the source.
        let err = reader.next().unwrap().unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("shard open failed"),
            "Expected shard open error, got: {}",
            msg
        );

        // Iterator is done after error.
        assert!(reader.next().is_none());
    }

    #[test]
    fn error_terminates_iterator() {
        // If errors didn't transition to Done, this test would spin forever.
        let shards = MemoryShards::new(|_| Err(DiskyError::Other("always fails".into())), 1);
        let reader = SequentialShardReaderConfig::new(SequentialShardSource::new(shards)).build();

        // Must produce exactly one error then stop.
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

    #[test]
    fn invalid_shard_data_wraps_in_shard_error() {
        // Source returns Ok(shard) but the data isn't valid disky —
        // RecordReader::with_config fails and the error is wrapped in ShardError.
        let shards = MemoryShards::new(|_| Ok(b"not valid disky data".to_vec()), 1);
        let mut reader =
            SequentialShardReaderConfig::new(SequentialShardSource::new(shards)).build();

        let err = reader.next().unwrap().unwrap_err();
        match &err {
            DiskyError::ShardError { shard_id, .. } => {
                assert!(!shard_id.is_empty(), "shard_id should identify the shard");
            }
            other => panic!("Expected ShardError, got: {}", other),
        }

        assert!(reader.next().is_none());
    }
}
