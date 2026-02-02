//! Memory-based shard backend.

use std::io::Cursor;

use crate::error::Result;
use crate::reader::{RecordReaderConfig, RecordReaderOptions};
use crate::shard::source::{Shard, Shards};

/// A collection of memory-based shards. Implements [`Shards`].
///
/// Uses a factory function to create shard data on demand.
/// The factory must return valid disky-formatted data (use [`RecordWriter`](crate::writer::RecordWriter)).
/// Useful for testing or when data is already in memory.
///
/// # Example
///
/// ```ignore
/// use std::io::Cursor;
/// use disky::shard::source::MemoryShards;
/// use disky::writer::RecordWriterConfig;
///
/// // Factory that creates valid disky data
/// let factory = |i| {
///     let mut buffer = Vec::new();
///     let mut writer = RecordWriterConfig::new(Cursor::new(&mut buffer)).build()?;
///     writer.write_record(format!("record_{}", i).as_bytes())?;
///     writer.close()?;
///     Ok(buffer)
/// };
///
/// let shards = MemoryShards::new(factory, 3);
/// ```
pub struct MemoryShards<F>
where
    F: Fn(usize) -> Result<Vec<u8>> + Send + Sync,
{
    factory: F,
    count: usize,
    options: RecordReaderOptions,
}

impl<F> MemoryShards<F>
where
    F: Fn(usize) -> Result<Vec<u8>> + Send + Sync,
{
    /// Create a new MemoryShards with the given factory function and count.
    ///
    /// # Arguments
    /// * `factory` - Function that creates disky-formatted shard data for a given index
    /// * `count` - Number of shards
    pub fn new(factory: F, count: usize) -> Self {
        Self {
            factory,
            count,
            options: RecordReaderOptions::default(),
        }
    }

    /// Set the [`RecordReaderOptions`] used when opening shards.
    pub fn reader_options(mut self, options: RecordReaderOptions) -> Self {
        self.options = options;
        self
    }
}

impl<F> Shards for MemoryShards<F>
where
    F: Fn(usize) -> Result<Vec<u8>> + Send + Sync,
{
    fn count(&self) -> usize {
        self.count
    }

    fn open(&self, index: usize) -> Result<Shard> {
        let data = (self.factory)(index)?;
        let reader = RecordReaderConfig::new(Cursor::new(data))
            .options(self.options)
            .build()?;
        Ok(Shard {
            reader: Box::new(reader),
            id: format!("memory_shard_{}", index),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::RecordWriterConfig;

    /// Helper to create valid disky-formatted data with given records.
    fn make_disky_data(records: &[&[u8]]) -> Vec<u8> {
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
    fn opens_all() {
        let shards = MemoryShards::new(
            |i| Ok(make_disky_data(&[format!("data {}", i).as_bytes()])),
            3,
        );
        assert_eq!(shards.count(), 3);

        for i in 0..3 {
            assert!(shards.open(i).is_ok());
        }
    }

    #[test]
    fn returns_correct_data() {
        let shards = MemoryShards::new(
            |i| Ok(make_disky_data(&[format!("shard {} data", i).as_bytes()])),
            2,
        );

        let shard = shards.open(0).unwrap();
        let records: Vec<_> = shard.reader.map(|r| r.unwrap()).collect();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].as_ref(), b"shard 0 data");
    }

    #[test]
    fn has_correct_id() {
        let shards = MemoryShards::new(|_| Ok(make_disky_data(&[b"test"])), 1);
        let shard = shards.open(0).unwrap();
        assert_eq!(shard.id, "memory_shard_0");
    }

    #[test]
    fn reopens_same_index() {
        let shards = MemoryShards::new(
            |i| Ok(make_disky_data(&[format!("data {}", i).as_bytes()])),
            1,
        );

        let s1 = shards.open(0).unwrap();
        let s2 = shards.open(0).unwrap();

        let read = |mut shard: Shard| -> String {
            let bytes = shard.reader.next().unwrap().unwrap();
            String::from_utf8(bytes.to_vec()).unwrap()
        };

        assert_eq!(read(s1), read(s2));
    }
}
