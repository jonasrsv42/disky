use crate::blocks::writer;
use crate::error::Result;
use bytes::{Bytes, BytesMut};
use std::io::{Read, Seek};

/// Bytes corresponding to a series of stacked chunks.
/// Configuration options for BlockWriter.
#[derive(Debug, Clone)]
pub(crate) struct BlockReaderConfig {
    /// Size of a block in bytes (default: 64 KiB).
    pub(crate) block_size: u64,
}

impl Default for BlockReaderConfig {
    fn default() -> Self {
        Self {
            block_size: writer::DEFAULT_BLOCK_SIZE,
        }
    }
}

impl BlockReaderConfig {
    pub(crate) fn with_block_size(block_size: u64) -> Result<Self> {
        // TODO implement and re-use validate block size from the reader.
        todo!()
    }
}

pub struct BlockReader<Source: Read + Seek> {
    source: Source,

    /// readr configuration.
    config: BlockReaderConfig,

    /// Current position in file.
    pos: usize,

    /// Current position in chunks buffer.
    chunks_pos: usize,

    /// Current chunk candidates. It may be one-or-more chunks as
    /// the block reader cannot in some circumstances, like when multiple chunks
    /// fit in a single block, know how many there are.
    ///
    /// The reader will read at-least one chunk into memory and then pass
    /// them all to a chunk parser that will treat it as a series of chunks, e.g. that right after parsing
    /// a full chunk it will expect a chunk header, if there's none it'll consider
    /// it to be a corruption.
    ///
    /// It is guaranteed that the reader returns a contiguous byte array that according to the
    /// block header contains a series of one or more **whole** uncorrupted chunks.
    ///
    /// However, the block reader can only detect corrupted blocks, so it'll drop chunks with
    /// corrupted blocks. Furhter corruption can be detected downstream in block parsing when
    /// we compare data hashes and would be managed there instead.
    chunks: BytesMut,
}

impl<Source: Read + Seek> BlockReader<Source> {
    fn read_next_block(&mut self) -> Result<Bytes> {
        // TODO reading the next block of bytes.
        todo!()
    }

    fn skip_block(&mut self) -> Result<()> {
        // TODO skip reading the candidate block.
        todo!()
    }

    /// Read the next chunks array from the `Source`, see the specification of [BlockReader::chunks]
    /// for what these [Bytes] contain.
    ///
    /// Will return an error on corruption
    fn read_next_chunk(&mut self) -> Result<Bytes> {
        todo!()
    }

    /// Drop and skip the current active chunks to the next possible chunks. Typically used
    /// to proceed in-case of corruption.
    fn skip_chunk(&mut self) -> Result<Bytes> {
        todo!()
    }
}
