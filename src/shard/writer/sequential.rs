//! Sequential shard writer.
//!
//! Writes records across a sequence of shards, filling each shard until a
//! byte limit is reached, then closing it and opening the next one from the
//! factory.

use std::io::{Seek, Write};

use log::error;

use crate::error::{DiskyError, Result};
use crate::shard::sink;
use crate::writer::{RecordWriter, RecordWriterConfig};

/// Default maximum bytes per shard (1 GiB).
const DEFAULT_MAX_SHARD_BYTES: usize = 1 << 30;

enum WriterState<Sink: Write + Seek> {
    /// No shard opened yet (lazy — opens on first write).
    Start,
    /// Actively writing records to a shard.
    Writing {
        writer: RecordWriter<Sink>,
        shard_id: String,
        bytes_written: usize,
    },
    /// Terminal — closed or hit an unrecoverable error.
    Done,
}

/// Builder for [`SequentialShardWriter`].
///
/// Only the shard factory is required. Everything else has sensible defaults.
///
/// # Examples
///
/// ```
/// use disky::shard::sink::MemoryShards;
/// use disky::shard::writer::SequentialShardWriterConfig;
/// use disky::writer::RecordWriterConfig;
///
/// // Minimal — factory only, defaults for everything else.
/// let mut writer = SequentialShardWriterConfig::new(MemoryShards::new()).build();
///
/// writer.write_record(b"hello").unwrap();
/// writer.close().unwrap();
/// ```
///
/// ```
/// use disky::shard::sink::MemoryShards;
/// use disky::shard::writer::SequentialShardWriterConfig;
/// use disky::writer::RecordWriterConfig;
/// use disky::compression::CompressionType;
///
/// // Everything customised.
/// let mut writer = SequentialShardWriterConfig::new(MemoryShards::new())
///     .config(RecordWriterConfig::default().with_compression(CompressionType::None))
///     .max_shard_bytes(1024 * 1024)
///     .build();
///
/// writer.write_record(b"hello").unwrap();
/// writer.close().unwrap();
/// ```
pub struct SequentialShardWriterConfig<Factory> {
    factory: Factory,
    writer_config: RecordWriterConfig,
    max_bytes_per_shard: usize,
}

impl<Factory> SequentialShardWriterConfig<Factory> {
    /// Create a config with the given shard factory. All other options
    /// use their defaults (`RecordWriterConfig::default()`, no shard rotation).
    pub fn new(factory: Factory) -> Self {
        Self {
            factory,
            writer_config: RecordWriterConfig::default(),
            max_bytes_per_shard: DEFAULT_MAX_SHARD_BYTES,
        }
    }

    /// Set the [`RecordWriterConfig`] used for each shard's writer.
    pub fn config(mut self, config: RecordWriterConfig) -> Self {
        self.writer_config = config;
        self
    }

    /// Rotate to a new shard after `max_bytes` of record data have been
    /// written to the current one.
    pub fn max_shard_bytes(mut self, max_bytes: usize) -> Self {
        self.max_bytes_per_shard = max_bytes;
        self
    }

    /// Build the [`SequentialShardWriter`].
    pub fn build<Sink>(self) -> SequentialShardWriter<Sink, Factory>
    where
        Sink: Write + Seek,
        Factory: sink::Shards<Sink = Sink>,
    {
        SequentialShardWriter {
            factory: self.factory,
            config: self.writer_config,
            max_bytes_per_shard: self.max_bytes_per_shard,
            state: WriterState::Start,
        }
    }
}

/// Writes records sequentially across shards from a [`sink::Shards`] factory.
///
/// Construct via [`SequentialShardWriterConfig`].
///
/// Construction is lazy — no shards are opened until the first call to
/// [`write_record`](Self::write_record).
///
/// When `max_bytes_per_shard` is set, the writer closes the current shard
/// and opens a new one after the cumulative record bytes written to that
/// shard reach the limit.
///
/// Errors from the underlying [`RecordWriter`] and shard factory are wrapped
/// in [`DiskyError::ShardError`] with the shard's id for traceability.
pub struct SequentialShardWriter<Sink: Write + Seek, Factory: sink::Shards<Sink = Sink>> {
    factory: Factory,
    config: RecordWriterConfig,
    max_bytes_per_shard: usize,
    state: WriterState<Sink>,
}

impl<Sink: Write + Seek, Factory: sink::Shards<Sink = Sink>> SequentialShardWriter<Sink, Factory> {
    /// Write a record to the current shard.
    ///
    /// If no shard is open yet, one is obtained from the factory. If the
    /// byte limit is exceeded after this write, the current shard is closed
    /// and the next call will open a new one.
    pub fn write_record(&mut self, data: &[u8]) -> Result<()> {
        loop {
            match self.state {
                WriterState::Done => return Err(DiskyError::WritingClosedFile),

                WriterState::Start => {
                    let shard = self.factory.next()?;
                    match RecordWriter::with_config(shard.sink, self.config) {
                        Ok(writer) => {
                            self.state = WriterState::Writing {
                                writer,
                                shard_id: shard.id,
                                bytes_written: 0,
                            };
                            // Loop to hit the Writing arm.
                        }
                        Err(e) => {
                            self.state = WriterState::Done;
                            return Err(DiskyError::ShardError {
                                shard_id: shard.id,
                                source: Box::new(e),
                            });
                        }
                    }
                }

                WriterState::Writing {
                    ref mut writer,
                    ref shard_id,
                    ref mut bytes_written,
                } => {
                    // Try to write a record into shard.
                    if let Err(e) = writer.write_record(data) {
                        let id = shard_id.clone();
                        self.state = WriterState::Done;
                        return Err(DiskyError::ShardError {
                            shard_id: id,
                            source: Box::new(e),
                        });
                    }

                    // Successful write (hopefully) - increment written.
                    *bytes_written += data.len();

                    // If written enough - try rotate.
                    if *bytes_written >= self.max_bytes_per_shard {
                        if let Err(e) = writer.close() {
                            let id = shard_id.clone();
                            self.state = WriterState::Done;
                            return Err(DiskyError::ShardError {
                                shard_id: id,
                                source: Box::new(e),
                            });
                        }
                        self.state = WriterState::Start;
                    }

                    return Ok(());
                }
            }
        }
    }

    /// Close the current shard (if any) and transition to the terminal state.
    ///
    /// Idempotent — calling `close()` multiple times is safe.
    pub fn close(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, WriterState::Done) {
            WriterState::Start => Ok(()),
            WriterState::Done => Ok(()),
            WriterState::Writing {
                mut writer,
                shard_id,
                ..
            } => writer.close().map_err(|e| DiskyError::ShardError {
                shard_id,
                source: Box::new(e),
            }),
        }
    }
}

impl<Sink: Write + Seek, Factory: sink::Shards<Sink = Sink>> Drop
    for SequentialShardWriter<Sink, Factory>
{
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            error!("SequentialShardWriter::drop: close failed: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, SeekFrom};
    use std::sync::{Arc, Mutex};

    use crate::error::DiskyError;
    use crate::reader::{DiskyPiece, RecordReaderConfig};
    use crate::shard::sink::{ClosureShards, MemoryShards, Shards};

    use super::SequentialShardWriterConfig;

    /// Read all records from a disky buffer.
    fn read_records(data: &[u8]) -> Vec<Vec<u8>> {
        let cursor = Cursor::new(data.to_vec());
        let mut reader = RecordReaderConfig::new(cursor).build().unwrap();
        let mut records = Vec::new();
        loop {
            match reader.next_record().unwrap() {
                DiskyPiece::Record(bytes) => records.push(bytes.to_vec()),
                DiskyPiece::EOF => break,
            }
        }
        records
    }

    /// A sink factory that captures all written data for later inspection.
    struct CapturingShards {
        shards: Arc<Mutex<Vec<Arc<Mutex<Cursor<Vec<u8>>>>>>>,
        counter: usize,
    }

    impl CapturingShards {
        fn new() -> (Self, Arc<Mutex<Vec<Arc<Mutex<Cursor<Vec<u8>>>>>>>) {
            let shards = Arc::new(Mutex::new(Vec::new()));
            let factory = Self {
                shards: shards.clone(),
                counter: 0,
            };
            (factory, shards)
        }
    }

    /// A wrapper around Cursor that shares its inner buffer via Arc<Mutex>.
    struct SharedCursor {
        inner: Arc<Mutex<Cursor<Vec<u8>>>>,
    }

    impl std::io::Write for SharedCursor {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.inner.lock().unwrap().write(buf)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.inner.lock().unwrap().flush()
        }
    }

    impl std::io::Seek for SharedCursor {
        fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
            self.inner.lock().unwrap().seek(pos)
        }
    }

    impl Shards for CapturingShards {
        type Sink = SharedCursor;

        fn next(&mut self) -> crate::error::Result<crate::shard::sink::Shard<SharedCursor>> {
            let cursor = Arc::new(Mutex::new(Cursor::new(Vec::new())));
            self.shards.lock().unwrap().push(cursor.clone());
            let id = format!("capturing_shard_{}", self.counter);
            self.counter += 1;
            Ok(crate::shard::sink::Shard {
                sink: SharedCursor { inner: cursor },
                id,
            })
        }
    }

    #[test]
    fn single_shard_writes_all() {
        let (factory, shards) = CapturingShards::new();
        let mut writer = SequentialShardWriterConfig::new(factory).build();

        writer.write_record(b"aaa").unwrap();
        writer.write_record(b"bbb").unwrap();
        writer.write_record(b"ccc").unwrap();
        writer.close().unwrap();

        let shards = shards.lock().unwrap();
        assert_eq!(shards.len(), 1);

        let data = shards[0].lock().unwrap().get_ref().clone();
        let records = read_records(&data);
        assert_eq!(records.len(), 3);
        assert_eq!(records[0], b"aaa");
        assert_eq!(records[1], b"bbb");
        assert_eq!(records[2], b"ccc");
    }

    #[test]
    fn rotates_shards_on_byte_limit() {
        let (factory, shards) = CapturingShards::new();
        // Each record is 10 bytes; limit is 15 so after 2 records we rotate.
        let mut writer = SequentialShardWriterConfig::new(factory)
            .max_shard_bytes(15)
            .build();

        // Write 5 records of 10 bytes each.
        for i in 0..5 {
            let data = format!("record_{:03}", i);
            writer.write_record(data.as_bytes()).unwrap();
        }
        writer.close().unwrap();

        let shards = shards.lock().unwrap();
        // With 10-byte records and 15-byte limit: shard fills at 2 records (20 >= 15),
        // then rotates. So 5 records → shards of [2, 2, 1] = 3 shards.
        assert!(
            shards.len() >= 2,
            "Expected multiple shards, got {}",
            shards.len()
        );

        // Verify all records are readable across all shards.
        let mut all_records = Vec::new();
        for shard in shards.iter() {
            let data = shard.lock().unwrap().get_ref().clone();
            if data.is_empty() {
                continue;
            }
            all_records.extend(read_records(&data));
        }
        assert_eq!(all_records.len(), 5);
        for i in 0..5 {
            let expected = format!("record_{:03}", i);
            assert_eq!(all_records[i], expected.as_bytes());
        }
    }

    #[test]
    fn close_is_idempotent() {
        let mut writer = SequentialShardWriterConfig::new(MemoryShards::new()).build();

        writer.write_record(b"data").unwrap();
        writer.close().unwrap();
        writer.close().unwrap(); // Should not error.
    }

    #[test]
    fn write_after_close_fails() {
        let mut writer = SequentialShardWriterConfig::new(MemoryShards::new()).build();

        writer.write_record(b"data").unwrap();
        writer.close().unwrap();

        let err = writer.write_record(b"more").unwrap_err();
        assert!(
            matches!(err, DiskyError::WritingClosedFile),
            "Expected WritingClosedFile, got: {}",
            err
        );
    }

    #[test]
    fn empty_writer_close() {
        let mut writer = SequentialShardWriterConfig::new(MemoryShards::new()).build();
        writer.close().unwrap();
    }

    #[test]
    fn factory_error_propagates() {
        let mut writer = SequentialShardWriterConfig::new(ClosureShards::new(
            || -> crate::error::Result<Cursor<Vec<u8>>> {
                Err(DiskyError::Other("factory boom".into()))
            },
        ))
        .build();

        let err = writer.write_record(b"data").unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("factory boom"),
            "Expected factory error, got: {}",
            msg
        );
    }

    #[test]
    fn lazy_construction() {
        let call_count = Arc::new(Mutex::new(0usize));
        let call_count_clone = call_count.clone();

        let writer = SequentialShardWriterConfig::new(ClosureShards::new(move || {
            *call_count_clone.lock().unwrap() += 1;
            Ok(Cursor::new(Vec::new()))
        }))
        .build();

        // Factory should not have been called yet.
        assert_eq!(*call_count.lock().unwrap(), 0);

        drop(writer);
    }

    #[test]
    fn lazy_construction_calls_on_first_write() {
        let call_count = Arc::new(Mutex::new(0usize));
        let call_count_clone = call_count.clone();

        let mut writer = SequentialShardWriterConfig::new(ClosureShards::new(move || {
            *call_count_clone.lock().unwrap() += 1;
            Ok(Cursor::new(Vec::new()))
        }))
        .build();

        assert_eq!(*call_count.lock().unwrap(), 0);
        writer.write_record(b"first").unwrap();
        assert_eq!(*call_count.lock().unwrap(), 1);
        writer.close().unwrap();
    }

    #[test]
    fn bytes_tracking_triggers_rotation() {
        let call_count = Arc::new(Mutex::new(0usize));
        let call_count_clone = call_count.clone();

        let mut writer = SequentialShardWriterConfig::new(ClosureShards::new(move || {
            *call_count_clone.lock().unwrap() += 1;
            Ok(Cursor::new(Vec::new()))
        }))
        .max_shard_bytes(10)
        .build();

        // Write 5 bytes — under limit, no rotation.
        writer.write_record(b"aaaaa").unwrap();
        assert_eq!(*call_count.lock().unwrap(), 1);

        // Write 5 more bytes — now 10 >= 10, triggers rotation.
        writer.write_record(b"bbbbb").unwrap();
        // The write succeeded and the shard was closed. A new one isn't
        // created until the next write.
        assert_eq!(*call_count.lock().unwrap(), 1);

        // Next write triggers new shard creation.
        writer.write_record(b"ccccc").unwrap();
        assert_eq!(*call_count.lock().unwrap(), 2);

        writer.close().unwrap();
    }

    #[test]
    fn record_larger_than_max_shard_bytes() {
        let (factory, shards) = CapturingShards::new();
        let mut writer = SequentialShardWriterConfig::new(factory)
            .max_shard_bytes(5)
            .build();

        // Write a 10-byte record into a shard with 5-byte limit.
        // Should succeed and trigger rotation.
        writer.write_record(b"0123456789").unwrap();
        writer.write_record(b"abcde").unwrap();
        writer.close().unwrap();

        let shards = shards.lock().unwrap();
        assert_eq!(shards.len(), 2);

        let data0 = shards[0].lock().unwrap().get_ref().clone();
        let data1 = shards[1].lock().unwrap().get_ref().clone();
        assert_eq!(read_records(&data0), vec![b"0123456789"]);
        assert_eq!(read_records(&data1), vec![b"abcde"]);
    }

    #[test]
    fn factory_error_on_second_shard() {
        let call_count = Arc::new(Mutex::new(0usize));
        let cc = call_count.clone();

        let mut writer = SequentialShardWriterConfig::new(ClosureShards::new(move || {
            let mut count = cc.lock().unwrap();
            *count += 1;
            if *count > 1 {
                return Err(DiskyError::Other("second shard fails".into()));
            }
            Ok(Cursor::new(Vec::new()))
        }))
        .max_shard_bytes(5)
        .build();

        // First write succeeds and fills the shard (5 >= 5), rotation closes it.
        writer.write_record(b"aaaaa").unwrap();
        // Second write tries to open a new shard — factory fails.
        let err = writer.write_record(b"bbbbb").unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("second shard fails"), "Got: {}", msg);
    }

    #[test]
    fn drop_without_close() {
        let (factory, shards) = CapturingShards::new();
        {
            let mut writer = SequentialShardWriterConfig::new(factory).build();
            writer.write_record(b"hello").unwrap();
            // Drop without explicit close — should not panic.
        }

        let shards = shards.lock().unwrap();
        assert_eq!(shards.len(), 1);
        let data = shards[0].lock().unwrap().get_ref().clone();
        assert_eq!(read_records(&data), vec![b"hello"]);
    }

    #[test]
    fn exhausted_writer_stays_done() {
        let mut writer = SequentialShardWriterConfig::new(MemoryShards::new()).build();

        writer.close().unwrap();

        // Multiple writes after close should all fail consistently.
        for _ in 0..5 {
            assert!(writer.write_record(b"nope").is_err());
        }
    }
}
