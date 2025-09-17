pub mod chunks_parser;
pub mod header;
pub mod header_parser;
pub mod header_writer;
pub mod signature;
pub mod signature_parser;
pub mod signature_writer;
mod simple_chunk;
pub mod simple_chunk_parser;
pub mod simple_chunk_writer;
mod writer;

#[cfg(test)]
pub mod tests;

pub use header::{CHUNK_HEADER_SIZE, ChunkHeader, ChunkType};
pub use header_parser::parse_chunk_header;
pub use header_writer::write_chunk_header;
pub use signature::{FILE_SIGNATURE_HEADER, SIGNATURE_HEADER_SIZE};
pub use signature_parser::validate_signature;
pub use signature_writer::SignatureWriter;
pub use simple_chunk_parser::{SimpleChunkParser, SimpleChunkPiece};
pub use simple_chunk_writer::SimpleChunkWriter;
pub use writer::{ChunkWriter, RecordsSize};
//pub use chunks_parser::{ChunksParser, Chunk, SimpleChunk, SimpleChunkIterator};
