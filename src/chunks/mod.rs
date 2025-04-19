mod simple_chunk;
mod writer;
pub mod signature_writer;
pub mod signature_parser;
pub mod signature;
pub mod header_writer;
pub mod header_parser;
pub mod header;
pub mod simple_chunk_writer;
pub mod simple_chunk_parser;
pub mod chunks_parser;

#[cfg(test)]
pub mod tests;

pub use simple_chunk_writer::SimpleChunkWriter;
pub use simple_chunk_parser::{SimpleChunkParser, RecordResult};
pub use writer::{ChunkWriter, RecordsSize};
pub use signature_writer::SignatureWriter;
pub use signature::{FILE_SIGNATURE_HEADER, SIGNATURE_HEADER_SIZE};
pub use signature_parser::validate_signature;
pub use header::{ChunkHeader, ChunkType, CHUNK_HEADER_SIZE};
pub use header_writer::write_chunk_header;
pub use header_parser::parse_chunk_header;
pub use chunks_parser::{ChunksParser, Chunk, SimpleChunk, SimpleChunkIterator};
