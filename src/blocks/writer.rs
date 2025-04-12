use bytes::{BufMut, Bytes, BytesMut};
use std::io::{Seek, Write};

use crate::error::Result;
use crate::hash::highway_hash;

/// The size of a block header in bytes.
/// Always 24 bytes: 8 for header_hash, 8 for previous_chunk, 8 for next_chunk.
pub const BLOCK_HEADER_SIZE: u64 = 24;

/// Configuration options for BlockWriter.
#[derive(Debug, Clone)]
pub struct BlockWriterConfig {
    /// Size of a block in bytes (default: 64 KiB).
    pub block_size: u64,
}

impl Default for BlockWriterConfig {
    fn default() -> Self {
        // Default is 64 KiB which is well above the header size
        // This should never fail validation
        //
        // NOTE: It should ALWAYS be 64KiB for actual writing, it's only configurable
        // to allow for test injection to write "human-readable" tests
        Self {
            block_size: 1 << 16, // 64 KiB
        }
    }
}

impl BlockWriterConfig {
    /// Creates a new BlockWriterConfig with custom block size.
    ///
    /// Returns an error if block_size is too small compared to header size,
    /// which could cause cascading headers.
    pub fn with_block_size(block_size: u64) -> crate::error::Result<Self> {
        // Validate block size to prevent cascading headers
        Self::validate_block_size(block_size)?;

        Ok(Self { block_size })
    }

    /// Returns the usable block size (block size minus block header size).
    pub fn usable_block_size(&self) -> u64 {
        self.block_size - BLOCK_HEADER_SIZE
    }

    /// Validates that the block size is large enough to prevent cascading headers.
    ///
    /// The block size must be at least twice the header size to prevent cascading headers.
    /// This is because a header spans across a block boundary, another header would be needed,
    /// which could cause an infinite cascade of headers.
    fn validate_block_size(block_size: u64) -> crate::error::Result<()> {
        if block_size < BLOCK_HEADER_SIZE * 2 {
            return Err(crate::error::RiegeliError::Other(
                format!("Block size ({}) must be at least twice the header size ({}) to prevent cascading headers",
                        block_size, BLOCK_HEADER_SIZE)
            ));
        }
        Ok(())
    }
}

/// Writer for Riegeli blocks that handles block boundary interruptions.
///
/// According to the Riegeli specification, a chunk of logical data can be interrupted by
/// a block header at any block boundary. This writer handles those interruptions by
/// automatically inserting block headers at the appropriate positions.
///
/// # Structure of Riegeli Files
///
/// Riegeli files consist of a sequence of blocks, each starting with a 24-byte block header
/// when it falls on a block boundary (by default every 64 KiB). Each block header contains:
/// - `header_hash` (8 bytes): Hash of the rest of the header
/// - `previous_chunk` (8 bytes): Distance from chunk beginning to block beginning
/// - `next_chunk` (8 bytes): Distance from block beginning to chunk end
///
/// # Usage
///
/// The `BlockWriter` is typically used as part of a higher-level writer implementation,
/// such as a `ChunkWriter` or `RecordWriter`. It handles the low-level details of ensuring
/// block headers are inserted at block boundaries, while higher-level components manage the
/// logical chunking of data.
///
/// ```no_run
/// use disky::blocks::writer::{BlockWriter, BlockWriterConfig};
/// use std::fs::File;
/// use bytes::Bytes;
///
/// // Create a BlockWriter with a file as the underlying sink
/// let file = File::create("example.riegeli").unwrap();
/// let mut writer = BlockWriter::new(file).unwrap();
///
/// // Write a chunk of data
/// let data = Bytes::from(b"Some data to write".to_vec());
/// writer.write_chunk(data).unwrap();
///
/// // Always call flush when done
/// writer.flush().unwrap();
/// ```
pub struct BlockWriter<Sink: Write + Seek> {
    /// The underlying writer.
    sink: Sink,
    /// Current position in the file.
    pos: u64,
    /// Configuration options.
    pub config: BlockWriterConfig,
}

impl<Sink: Write + Seek> BlockWriter<Sink> {
    /// Creates a new BlockWriter with default configuration.
    pub fn new(sink: Sink) -> Result<Self> {
        Self::with_config(sink, BlockWriterConfig::default())
    }

    /// Creates a new BlockWriter with custom configuration.
    pub fn with_config(mut sink: Sink, config: BlockWriterConfig) -> Result<Self> {
        // Validate the configuration - ensure block size is reasonable
        BlockWriterConfig::validate_block_size(config.block_size)?;

        let pos = sink.stream_position()?;
        Ok(Self { sink, pos, config })
    }

    /// Creates a new BlockWriter with default configuration, for appending to existing data.
    ///
    /// This allows specifying an initial position, which is useful for appending to an existing file.
    ///
    /// # Parameters
    ///
    /// * `sink` - The sink to write to
    /// * `pos` - The current position in the sink (usually the file size)
    pub fn for_append(sink: Sink, pos: u64) -> Result<Self> {
        Self::for_append_with_config(sink, pos, BlockWriterConfig::default())
    }

    /// Creates a new BlockWriter with custom configuration, for appending to existing data.
    ///
    /// This allows specifying an initial position, which is useful for appending to an existing file.
    ///
    /// # Parameters
    ///
    /// * `sink` - The sink to write to
    /// * `pos` - The current position in the sink (usually the file size)
    /// * `config` - The configuration to use
    pub fn for_append_with_config(
        mut sink: Sink,
        pos: u64,
        config: BlockWriterConfig,
    ) -> Result<Self> {
        // Validate the configuration - ensure block size is reasonable
        BlockWriterConfig::validate_block_size(config.block_size)?;

        // Verify the sink position matches the expected position
        let actual_pos = sink.stream_position()?;
        if actual_pos != pos {
            sink.seek(std::io::SeekFrom::Start(pos))?;
        }

        Ok(Self { sink, pos, config })
    }

    /// Updates the current position from the underlying sink and returns it.
    ///
    /// This method synchronizes the internal position tracking with the actual position
    /// in the underlying sink, which may be necessary if other operations have been
    /// performed directly on the sink.
    ///
    /// # Returns
    ///
    /// * `Result<u64>` - The current position in the sink, or an error if the sink's
    ///   position could not be determined.
    pub fn update_position(&mut self) -> Result<u64> {
        self.pos = self.sink.stream_position()?;
        Ok(self.pos)
    }

    /// Writes a block header at the current position.
    ///
    /// According to the Riegeli specification, a block header contains:
    /// - header_hash (8 bytes) - hash of the rest of the header
    /// - previous_chunk (8 bytes) - distance from beginning of chunk to beginning of block
    /// - next_chunk (8 bytes) - distance from beginning of block to end of chunk
    ///
    /// Note: The header writing is not atomic. If a write operation fails in the middle of
    /// writing a header, it will leave a partial, invalid header in the file. The header hash
    /// included in each block header can be used by readers to detect corrupted headers.
    fn write_block_header(&mut self, previous_chunk: u64, next_chunk: u64) -> Result<()> {
        // Use a single buffer for the entire header (24 bytes)
        let mut header = BytesMut::with_capacity(BLOCK_HEADER_SIZE as usize);

        // Prepare the content portion of the header first (16 bytes)
        let mut content = [0u8; 16];
        (&mut content[0..8]).copy_from_slice(&previous_chunk.to_le_bytes());
        (&mut content[8..16]).copy_from_slice(&next_chunk.to_le_bytes());

        // Calculate the header hash from the content
        let header_hash = highway_hash(&content);

        // Now build the complete header in one go
        header.put_u64_le(header_hash); // First 8 bytes: header_hash
        header.put_slice(&content); // Next 16 bytes: previous_chunk and next_chunk

        // Write the header
        self.sink.write_all(&header)?;

        // Update position
        self.pos += BLOCK_HEADER_SIZE;

        Ok(())
    }

    /// Returns the number of bytes remaining in the current block.
    fn remaining_in_block(&self, pos: u64) -> u64 {
        self.config.block_size - pos % self.config.block_size
    }

    /// Checks if a position falls on a block boundary.
    ///
    /// According to the Riegeli specification, block boundaries occur at multiples of block_size,
    /// which includes position 0 (i.e., files always start with a header).
    fn is_block_boundary(&self, pos: u64) -> bool {
        pos % self.config.block_size == 0
    }

    /// Computes the absolute position of the chunk end based on the Riegeli specification.
    ///
    /// This calculates the total file position where the chunk will end,
    /// taking into account any additional block headers that must be inserted at block boundaries.
    ///
    /// The formula follows the Riegeli spec:
    /// ```text
    /// NumOverheadBlocks(pos, size) = (size + (pos + kUsableBlockSize - 1) % kBlockSize) / kUsableBlockSize;
    /// chunk_end = chunk_begin + chunk_size + NumOverheadBlocks * kBlockHeaderSize;
    /// ```
    pub(crate) fn compute_chunk_end(&self, chunk_begin: u64, chunk_size: u64) -> u64 {
        let usable_block_size = self.config.usable_block_size();

        let num_overhead_blocks = (chunk_size
            + (chunk_begin + usable_block_size - 1) % self.config.block_size)
            / usable_block_size;

        chunk_begin + chunk_size + num_overhead_blocks * BLOCK_HEADER_SIZE
    }

    /// Writes a chunk of data, handling block boundaries by inserting block headers as needed.
    ///
    /// Each call to this method is considered to be writing a new logical chunk, and the
    /// current position is treated as the beginning of that chunk. If a block boundary is
    /// encountered while writing the chunk, a block header will be inserted automatically.
    ///
    /// # Block Boundaries and Headers
    ///
    /// Block boundaries occur at multiples of the configured block_size. When a boundary is encountered:
    /// 1. The writer pauses writing chunk data
    /// 2. It inserts a 24-byte block header containing:
    ///    - `header_hash`: Hash of the next 16 bytes
    ///    - `previous_chunk`: Distance from chunk start to block boundary
    ///    - `next_chunk`: Distance from block boundary to chunk end
    /// 3. It then continues writing the remaining chunk data
    ///
    /// # I/O Error Handling
    ///
    /// This method relies on the `write_all` method of the underlying sink to ensure all data
    /// is written completely or an error is returned. Important considerations:
    ///
    /// - If an I/O error occurs during writing, the method will return an error immediately
    /// - The writer has no way to track how many bytes were successfully written before the error
    /// - There is no built-in mechanism to resume writing from the point of failure
    /// - The implementation assumes `write_all` either writes all data or returns an error,
    ///   which depends on the correctness of the underlying sink's implementation
    ///
    /// # Edge Cases
    ///
    /// - **Empty chunks**: No data or headers are written for empty chunks
    /// - **Position 0**: The beginning of the file is always a block boundary
    /// - **Exact block boundary**: If writing begins at a block boundary, a header is inserted first
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use disky::blocks::writer::BlockWriter;
    /// # use bytes::Bytes;
    /// # let file = std::fs::File::create("example.riegeli").unwrap();
    /// # let mut writer = BlockWriter::new(file).unwrap();
    /// // Write a single chunk
    /// let data = Bytes::from(b"Some data".to_vec());
    /// writer.write_chunk(data).unwrap();
    ///
    /// // Write multiple chunks (each gets its own logical boundary)
    /// writer.write_chunk(Bytes::from(b"Chunk 1".to_vec())).unwrap();
    /// writer.write_chunk(Bytes::from(b"Chunk 2".to_vec())).unwrap();
    /// writer.write_chunk(Bytes::from(b"Chunk 3".to_vec())).unwrap();
    /// ```
    ///
    /// # Arguments
    ///
    /// * `chunk_data` - The chunk data to write
    ///
    /// # Returns
    ///
    /// * `Result<()>` - Success or an error if the write failed
    pub fn write_chunk(&mut self, chunk_data: Bytes) -> Result<()> {
        // Special case: Empty chunks don't need any headers or writing
        if chunk_data.is_empty() {
            return Ok(());
        }

        // Update position to ensure it's current
        self.update_position()?;

        // Each call to write_chunk begins a new chunk
        let chunk_begin = self.pos;

        // Write the chunk data with block header interruptions as needed
        let mut data_pos = 0;
        while data_pos < chunk_data.len() {
            // Check if we're at a block boundary
            if self.is_block_boundary(self.pos) {
                // We're at a block boundary - write a block header
                let previous_chunk = self.pos - chunk_begin;

                // next_chunk should be the distance from the beginning of the current block 
                // to the end of the chunk, according to the Riegeli specification:
                // "distance from the beginning of the block to the end of the chunk interrupted by this block header"
                
                // First, calculate the absolute position of the chunk end
                let chunk_end = self.compute_chunk_end(chunk_begin, chunk_data.len() as u64);
                
                // Then calculate the distance from the current block to that end position
                let next_chunk = chunk_end - self.pos;

                self.write_block_header(previous_chunk, next_chunk)?;
                continue;
            }

            // Calculate how much data we can write before the next block boundary
            let remaining = self.remaining_in_block(self.pos);

            // Write as much data as fits before the next block boundary
            let bytes_to_write =
                std::cmp::min(remaining, (chunk_data.len() - data_pos) as u64) as usize;
            let end_pos = data_pos + bytes_to_write;

            self.sink.write_all(&chunk_data[data_pos..end_pos])?;
            data_pos = end_pos;
            self.pos += bytes_to_write as u64;
        }

        Ok(())
    }

    /// Flushes any buffered data to the underlying sink.
    ///
    /// # Important
    ///
    /// This method MUST be called after all writing is complete and before the writer
    /// is dropped to ensure all data is properly written to the underlying sink. Failing
    /// to call flush may result in data loss.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use disky::blocks::writer::BlockWriter;
    /// # use bytes::Bytes;
    /// # let file = std::fs::File::create("example.riegeli").unwrap();
    /// # let mut writer = BlockWriter::new(file).unwrap();
    /// // Write some chunks
    /// writer.write_chunk(Bytes::from(b"Chunk 1".to_vec())).unwrap();
    /// writer.write_chunk(Bytes::from(b"Chunk 2".to_vec())).unwrap();
    ///
    /// // Always flush when done
    /// writer.flush().unwrap();
    /// ```
    pub fn flush(&mut self) -> Result<()> {
        self.sink.flush()?;
        Ok(())
    }

    /// Returns the underlying sink, consuming self.
    pub fn into_inner(self) -> Sink {
        self.sink
    }

    /// Gets a reference to the underlying sink.
    pub fn get_ref(&self) -> &Sink {
        &self.sink
    }

    /// Gets a mutable reference to the underlying sink.
    pub fn get_mut(&mut self) -> &mut Sink {
        &mut self.sink
    }
}
