use bytes::{BufMut, BytesMut};
use std::io::{Result as IoResult, Seek, SeekFrom, Write};

use crate::blocks::utils::{self, BLOCK_HEADER_SIZE, DEFAULT_BLOCK_SIZE};
use crate::error::Result;
use crate::hash::highway_hash;

/// A wrapper around any `Write + Seek` sink that tracks the current position.
///
/// This tracks the current position automatically as writes and seeks occur,
/// eliminating the need to call `stream_position()` frequently or manually
/// maintain a position counter.
pub struct WritePositionTracker<Sink: Write + Seek> {
    /// The underlying sink to write to
    sink: Sink,

    /// The current position in the sink
    position: u64,
}

impl<Sink: Write + Seek> WritePositionTracker<Sink> {
    /// Create a new WritePositionTracker wrapping the given sink.
    pub fn new(mut sink: Sink) -> IoResult<Self> {
        // Get the initial position from the sink
        let position = sink.stream_position()?;

        Ok(Self { sink, position })
    }

    /// Create a new WritePositionTracker at a specific position.
    ///
    /// This is primarily intended for test use.
    #[cfg(test)]
    pub fn at_position(mut sink: Sink, position: u64) -> IoResult<Self> {
        // Set the position in the sink
        sink.seek(SeekFrom::Start(position))?;

        Ok(Self { sink, position })
    }

    /// Returns the current position in the sink.
    pub fn position(&self) -> u64 {
        self.position
    }

    /// Returns a reference to the underlying sink.
    ///
    /// # Warning
    ///
    /// This method is intended for test use only. It exposes the internal state
    /// of the tracker which can lead to inconsistencies if not used carefully.
    #[cfg(test)]
    pub fn get_ref(&self) -> &Sink {
        &self.sink
    }

    /// Returns a mutable reference to the underlying sink.
    ///
    /// # Warning
    ///
    /// This method is intended for test use only. It exposes the internal state
    /// of the tracker which can lead to inconsistencies if not used carefully.
    ///
    /// Directly manipulating the position of the underlying sink (e.g., using `seek()`)
    /// will cause the position tracking to become out of sync and can lead to data
    /// corruption or incorrect behavior.
    #[cfg(test)]
    pub fn get_mut(&mut self) -> &mut Sink {
        &mut self.sink
    }

    /// Returns the underlying sink, consuming self.
    ///
    /// # Warning
    ///
    /// This method is intended for test use only. It exposes the internal state
    /// of the tracker which can lead to inconsistencies if not used carefully.
    #[cfg(test)]
    pub fn into_inner(self) -> Sink {
        self.sink
    }
}

impl<Sink: Write + Seek> Write for WritePositionTracker<Sink> {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        let bytes_written = self.sink.write(buf)?;
        self.position += bytes_written as u64;
        Ok(bytes_written)
    }

    fn flush(&mut self) -> IoResult<()> {
        self.sink.flush()
    }
}

impl<Sink: Write + Seek> Seek for WritePositionTracker<Sink> {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        // Forward seek call to the sink
        let new_pos = self.sink.seek(pos)?;

        // Update our tracked position
        self.position = new_pos;

        Ok(new_pos)
    }

    fn stream_position(&mut self) -> IoResult<u64> {
        // We can return our tracked position directly without a system call
        Ok(self.position)
    }
}

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
            block_size: DEFAULT_BLOCK_SIZE, // 64 KiB
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
        utils::validate_block_size(block_size)?;
        Ok(Self { block_size })
    }

    /// Returns the usable block size (block size minus block header size).
    pub fn usable_block_size(&self) -> u64 {
        utils::usable_block_size(self.block_size)
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
///
/// // Create a BlockWriter with a file as the underlying sink
/// let file = File::create("example.riegeli").unwrap();
/// let mut writer = BlockWriter::new(file).unwrap();
///
/// // Write a chunk of data
/// let data = b"Some data to write";
/// writer.write_chunk(data).unwrap();
///
/// // Always call flush when done
/// writer.flush().unwrap();
/// ```
pub struct BlockWriter<Sink: Write + Seek> {
    /// The underlying writer, wrapped in a position tracker.
    sink: WritePositionTracker<Sink>,
    /// Configuration options.
    pub config: BlockWriterConfig,
}

impl<Sink: Write + Seek> BlockWriter<Sink> {
    /// Creates a new BlockWriter with default configuration.
    pub fn new(sink: Sink) -> Result<Self> {
        Self::with_config(sink, BlockWriterConfig::default())
    }

    /// Creates a new BlockWriter with custom configuration.
    pub fn with_config(sink: Sink, config: BlockWriterConfig) -> Result<Self> {
        // Validate the configuration - ensure block size is reasonable
        utils::validate_block_size(config.block_size)?;

        let sink = WritePositionTracker::new(sink).map_err(|e| crate::error::DiskyError::Io(e))?;

        Ok(Self { sink, config })
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
        utils::validate_block_size(config.block_size)?;

        // Verify the sink position matches the expected position
        let actual_pos = sink.stream_position()?;
        if actual_pos != pos {
            sink.seek(SeekFrom::Start(pos))?;
        }

        // Create the position tracker at the correct position
        let sink = WritePositionTracker::new(sink).map_err(|e| crate::error::DiskyError::Io(e))?;

        Ok(Self { sink, config })
    }

    /// Returns the current position in the underlying sink.
    ///
    /// The position is automatically tracked by the WritePositionTracker, so this
    /// method simply returns the current position without making a system call.
    ///
    /// # Returns
    ///
    /// * `u64` - The current position in the sink.
    pub fn position(&self) -> u64 {
        self.sink.position()
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

        // Position is automatically tracked by WritePositionTracker

        Ok(())
    }

    /// Returns the number of bytes remaining in the current block.
    fn remaining_in_block(&self) -> u64 {
        utils::remaining_in_block(self.sink.position, self.config.block_size)
    }

    /// Checks if a position falls on a block boundary.
    ///
    /// According to the Riegeli specification, block boundaries occur at multiples of block_size,
    /// which includes position 0 (i.e., files always start with a header).
    fn at_block_boundary(&self) -> bool {
        utils::is_block_boundary(self.sink.position, self.config.block_size)
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
    /// # let file = std::fs::File::create("example.riegeli").unwrap();
    /// # let mut writer = BlockWriter::new(file).unwrap();
    /// // Write a single chunk
    /// let data = b"Some data";
    /// writer.write_chunk(data).unwrap();
    ///
    /// // Write multiple chunks (each gets its own logical boundary)
    /// writer.write_chunk(b"Chunk 1").unwrap();
    /// writer.write_chunk(b"Chunk 2").unwrap();
    /// writer.write_chunk(b"Chunk 3").unwrap();
    /// ```
    ///
    /// # Arguments
    ///
    /// * `chunk_data` - The chunk data to write
    ///
    /// # Returns
    ///
    /// * `Result<()>` - Success or an error if the write failed
    pub fn write_chunk(&mut self, chunk_data: &[u8]) -> Result<()> {
        // Special case: Empty chunks don't need any headers or writing
        if chunk_data.is_empty() {
            return Ok(());
        }

        // Each call to write_chunk begins a new chunk
        let chunk_begin = self.position();

        // Write the chunk data with block header interruptions as needed
        let mut data_pos = 0;
        while data_pos < chunk_data.len() {
            // Check if we're at a block boundary
            if self.at_block_boundary() {
                // We're at a block boundary - write a block header
                let previous_chunk = self.position() - chunk_begin;

                // next_chunk should be the distance from the beginning of the current block
                // to the end of the chunk, according to the Riegeli specification:
                // "distance from the beginning of the block to the end of the chunk interrupted by this block header"

                // First, calculate the absolute position of the chunk end
                let chunk_end = self.compute_chunk_end(chunk_begin, chunk_data.len() as u64);

                // Then calculate the distance from the current block to that end position
                let next_chunk = chunk_end - self.position();

                self.write_block_header(previous_chunk, next_chunk)?;
                continue;
            }

            // Calculate how much data we can write before the next block boundary
            let remaining = self.remaining_in_block();

            // Write as much data as fits before the next block boundary
            let bytes_to_write =
                std::cmp::min(remaining, (chunk_data.len() - data_pos) as u64) as usize;
            let end_pos = data_pos + bytes_to_write;

            self.sink.write_all(&chunk_data[data_pos..end_pos])?;
            data_pos = end_pos;
            // Position is automatically tracked by WritePositionTracker
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
    /// # let file = std::fs::File::create("example.riegeli").unwrap();
    /// # let mut writer = BlockWriter::new(file).unwrap();
    /// // Write some chunks
    /// writer.write_chunk(b"Chunk 1").unwrap();
    /// writer.write_chunk(b"Chunk 2").unwrap();
    ///
    /// // Always flush when done
    /// writer.flush().unwrap();
    /// ```
    pub fn flush(&mut self) -> Result<()> {
        self.sink.flush()?;
        Ok(())
    }

    /// Returns the underlying sink, consuming self.
    ///
    /// # Warning
    ///
    /// This method is intended for test use only. It exposes the internal state
    /// of the writer which can lead to inconsistencies if not used carefully.
    #[cfg(test)]
    pub fn into_inner(self) -> Sink {
        self.sink.into_inner()
    }

    /// Gets a reference to the underlying sink.
    ///
    /// # Warning
    ///
    /// This method is intended for test use only. It exposes the internal state
    /// of the writer which can lead to inconsistencies if not used carefully.
    #[cfg(test)]
    pub fn get_ref(&self) -> &Sink {
        self.sink.get_ref()
    }

    /// Gets a mutable reference to the underlying sink.
    ///
    /// # Warning
    ///
    /// This method is intended for test use only. It exposes the internal state
    /// of the writer which can lead to inconsistencies if not used carefully.
    ///
    /// Directly manipulating the position of the underlying sink (e.g., using `seek()`)
    /// will cause the position tracking to become out of sync. If you need to change
    /// the position, use `BlockWriter::for_append_with_config()` to create a new writer
    /// at the desired position, rather than manipulating the sink directly.
    #[cfg(test)]
    pub fn get_mut(&mut self) -> &mut Sink {
        self.sink.get_mut()
    }

    /// Returns the current position in the underlying sink.
    ///
    /// This is primarily intended for testing purposes.
    #[cfg(test)]
    pub fn current_position(&self) -> u64 {
        self.position()
    }
}
