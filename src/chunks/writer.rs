use crate::error::Result;
use bytes::Bytes;

// Size of the records in the chunk, if they were to be written now.
pub struct RecordsSize(pub u64);

/// The [ChunkWriter] will live for multiple chunks. [ChunkWriter::write_record] will be invoked N times
/// and then [ChunkWriter::serialize_chunk] once. This may then repeat indefinitly
pub trait ChunkWriter {
    /// Serialize all current records to a single chunk and reset state for next chunk.
    fn serialize_chunk(&mut self) -> Result<Bytes>;
}
