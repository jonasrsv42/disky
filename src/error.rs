//! Error types for the Disky format.

use std::io;
use thiserror::Error;

/// The main error type for Disky operations.
#[derive(Debug, Error)]
pub enum DiskyError {
    /// An I/O error occurred.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// The file does not have a valid Disky file signature.
    #[error("Invalid file signature: {0}")]
    InvalidFileSignature(String),

    /// A block header hash does not match.
    #[error("Block header hash mismatch")]
    BlockHeaderHashMismatch,

    /// A chunk header hash does not match.
    #[error("Chunk header hash mismatch")]
    ChunkHeaderHashMismatch,

    /// The chunk data hash does not match.
    #[error("Chunk data hash mismatch")]
    ChunkDataHashMismatch,

    /// Reached an unexpected end of chunk when
    /// trying to parse it.
    #[error("Unexpected end of chunk: {0}")]
    UnexpectedEndOfChunk(String),

    /// Missing chunk data. There is not enough
    /// chunk data according to the chunk header.
    /// This is not recoverable.
    #[error("Missing chunk data: {0}")]
    MissingChunkData(String),

    /// Reached an unexpected end while parsing chunk header.
    #[error("Unexpected end of chunk header: {0}")]
    UnexpectedEndOfChunkHeader(String),

    /// Reached an unexpected end while parsing chunk header.
    #[error("Block header inconsistency: {0}")]
    BlockHeaderInconsistency(String),

    /// An error occurred while trying to read signature.
    /// We typically do not try to recover from these.
    #[error("Error while reading signature: {0}")]
    SignatureReadingError(String),

    /// Block header validation failed.
    #[error("Invalid block header: {0}")]
    InvalidBlockHeader(String),

    /// Tried to read a corrupted block, this originates from
    /// the block reader.
    #[error("Trying to read at a corrupted block: {0}")]
    ReadCorruptedBlock(String),

    /// Tried to read a corrupted chunk.
    #[error("Trying to read at a corrupted chunk: {0}")]
    ReadCorruptedChunk(String),

    /// Failed to parse varint
    #[error("Varint parse error: {0}")]
    VarintParseError(String),

    /// The file is corrupt and cannot be recovered.
    #[error("Unrecoverable file corruption: {0}")]
    UnrecoverableCorruption(String),

    /// Unsupported compression type.
    #[error("Disky has not been compiled with support for compression type: {0}")]
    UnsupportedCompressionType(u8),

    /// Unsupported compression type.
    #[error("Unsupported chunk type: {0}")]
    UnsupportedChunkType(u8),

    /// The chunk type is not recognized.
    #[error("Unknown chunk type: {0}")]
    UnknownChunkType(u8),

    /// End of file was reached unexpectedly.
    #[error("Unexpected end of file")]
    UnexpectedEof,

    /// The file is not a Disky file.
    #[error("Not a Disky file: {0}")]
    NotDiskyFile(String),

    /// Attempted to write to a closed file.
    #[error("Writing a closed file")]
    WritingClosedFile,

    /// We hit an invalid reader state.
    #[error("Invalid reader state: {0}")]
    InvalidReaderState(String),

    /// Occurs when attempting to interact with a closed queue.
    #[error("Queue is closed: {0}")]
    QueueClosed(String),

    /// Indicates that no more shards are available from a ShardLocator.
    #[error("No more shards available")]
    NoMoreShards,

    /// Indicates that a resource pool is completely exhausted with no resources.
    #[error("Resource pool is exhausted")]
    PoolExhausted,

    /// Indicates that an operation was attempted on a closed reader.
    #[error("Reader is closed")]
    ReaderClosed,

    /// A general error occurred.
    #[error("{0}")]
    Other(String),

    /// An error occurred while reading from a specific shard.
    #[error("shard '{shard_id}': {source}")]
    ShardError {
        shard_id: String,
        #[source]
        source: Box<DiskyError>,
    },
}

/// A specialized Result type for Disky operations.
pub type Result<T> = std::result::Result<T, DiskyError>;
