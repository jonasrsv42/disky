use crate::error::Result;
use bytes::Bytes;

// Size of the records in the chunk, if they were to be written now.
pub(crate) struct RecordsSize(pub u64);

// Size of the chunk as written to the sink.
pub(crate) struct ChunkSize(pub u64);

/// The [ChunkWriter] will live for multiple chunks. [ChunkWriter::write_record] will be invoked N times
/// and then [ChunkWriter::serialize_chunk] once. This may then repeat indefinitly
pub(crate) trait ChunkWriter {
    /// Write a single record to in-memory buffer.
    fn write_record(&mut self, record: &[u8]) -> Result<RecordsSize>;
    /// Serialize all current records to a single chunk and reset state for next chunk.
    fn serialize_chunk(&mut self) -> Bytes;
}
