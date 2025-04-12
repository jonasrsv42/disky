use crate::blocks::utils::{self, BLOCK_HEADER_SIZE, DEFAULT_BLOCK_SIZE};
use crate::error::{Result, DiskyError};
use crate::hash::highway_hash;
use bytes::{Bytes, BytesMut};
use std::cmp::min;
use std::io::{Read, Seek};

/// Configuration options for BlockReader.
#[derive(Debug, Clone)]
pub struct BlockReaderConfig {
    /// Size of a block in bytes (default: 64 KiB).
    pub block_size: u64,
}

impl Default for BlockReaderConfig {
    fn default() -> Self {
        Self {
            block_size: DEFAULT_BLOCK_SIZE,
        }
    }
}

impl BlockReaderConfig {
    /// Creates a new BlockReaderConfig with custom block size.
    ///
    /// Returns an error if block_size is too small compared to header size,
    /// which could cause cascading headers.
    pub fn with_block_size(block_size: u64) -> Result<Self> {
        utils::validate_block_size(block_size)?;
        Ok(Self { block_size })
    }

    /// Returns the usable block size (block size minus block header size).
    pub fn usable_block_size(&self) -> u64 {
        utils::usable_block_size(self.block_size)
    }
}

/// Represents a block header in the Riegeli format.
struct BlockHeader {
    /// Distance from the beginning of the chunk to the beginning of the block.
    previous_chunk: u64,
    
    /// Distance from the beginning of the block to the end of the chunk.
    next_chunk: u64,
}

/// Reader for Disky blocks that handles block boundary interruptions.
///
/// According to the Riegeli specification, a chunk of logical data can be interrupted by
/// a block header at any block boundary. This reader handles those interruptions by
/// automatically skipping block headers at the appropriate positions and reconstructing
/// the original data.
///
/// # Important Limitation
///
/// This reader handles the Riegeli block format, which has some important behaviors to understand:
/// 
/// 1. When reading at a block boundary, the reader uses the block header information to
///    read exactly one logical chunk, as defined by the BlockWriter when `write_chunk()` was called.
///
/// 2. When reading in the middle of a block (not at a boundary), the reader reads all data
///    up to the next block boundary. This means:
///    - If multiple chunks fit within a block after the first chunk, they will all be returned
///      together in a single Bytes object on the next read.
///    - There's no way to separate individual chunks within this data without additional
///      chunk-level parsing.
///
/// For proper chunk separation, you need to use a higher-level chunk reader that understands
/// the chunk format (e.g., simple chunks, transposed chunks) which will include size
/// information. The chunk parser will need to process the Bytes returned by this reader
/// to separate and extract individual chunks.
///
/// # Structure of Riegeli Files
///
/// Riegeli files consist of a sequence of blocks, each starting with a 24-byte block header
/// when it falls on a block boundary (by default every 64 KiB). Each block header contains:
/// - `header_hash` (8 bytes): Hash of the rest of the header
/// - `previous_chunk` (8 bytes): Distance from chunk beginning to block beginning
/// - `next_chunk` (8 bytes): Distance from block beginning to chunk end
pub struct BlockReader<Source: Read + Seek> {
    /// The underlying source to read from.
    source: Source,

    /// Reader configuration.
    config: BlockReaderConfig,

    /// Current position in file.
    pos: u64,

    /// Current chunk start position (used for calculating chunk boundaries)
    chunk_begin: u64,

    /// Buffer for storing chunk data that has been read.
    buffer: BytesMut,
}

impl<Source: Read + Seek> BlockReader<Source> {
    /// Creates a new BlockReader with default configuration.
    pub fn new(source: Source) -> Result<Self> {
        Self::with_config(source, BlockReaderConfig::default())
    }

    /// Creates a new BlockReader with custom configuration.
    pub fn with_config(mut source: Source, config: BlockReaderConfig) -> Result<Self> {
        let pos = source.stream_position()?;
        
        Ok(Self {
            source,
            config,
            pos,
            chunk_begin: pos,
            buffer: BytesMut::new(),
        })
    }

    /// Updates the current position from the underlying source and returns it.
    fn update_position(&mut self) -> Result<u64> {
        self.pos = self.source.stream_position()?;
        Ok(self.pos)
    }

    /// Checks if a position falls on a block boundary.
    fn is_block_boundary(&self, pos: u64) -> bool {
        utils::is_block_boundary(pos, self.config.block_size)
    }

    /// Reads a block header from the current position.
    ///
    /// This function reads 24 bytes from the current position, interprets it as a block header,
    /// and validates the header hash. It also updates the current position.
    ///
    /// # Returns
    ///
    /// * `Result<BlockHeader>` - The parsed header if valid, or an error
    fn read_block_header(&mut self) -> Result<BlockHeader> {
        // Read the header bytes
        let mut header_bytes = [0u8; BLOCK_HEADER_SIZE as usize];
        let bytes_read = self.source.read(&mut header_bytes)?;
        
        if bytes_read < header_bytes.len() {
            return Err(DiskyError::UnexpectedEof);
        }

        // Extract header components
        let header_hash = u64::from_le_bytes([
            header_bytes[0], header_bytes[1], header_bytes[2], header_bytes[3],
            header_bytes[4], header_bytes[5], header_bytes[6], header_bytes[7],
        ]);

        let previous_chunk = u64::from_le_bytes([
            header_bytes[8], header_bytes[9], header_bytes[10], header_bytes[11],
            header_bytes[12], header_bytes[13], header_bytes[14], header_bytes[15],
        ]);

        let next_chunk = u64::from_le_bytes([
            header_bytes[16], header_bytes[17], header_bytes[18], header_bytes[19],
            header_bytes[20], header_bytes[21], header_bytes[22], header_bytes[23],
        ]);

        // Verify header hash
        let content = &header_bytes[8..24];
        let calculated_hash = highway_hash(content);
        
        if calculated_hash != header_hash {
            return Err(DiskyError::BlockHeaderHashMismatch);
        }

        // Validate header values 
        utils::validate_header_values(previous_chunk, next_chunk, self.config.block_size)?;

        // Update position
        self.pos += BLOCK_HEADER_SIZE;

        Ok(BlockHeader {
            previous_chunk,
            next_chunk,
        })
    }

    /// Reads a block of data up to the next block boundary or specified limit.
    ///
    /// # Arguments
    ///
    /// * `max_bytes` - Maximum number of bytes to read
    ///
    /// # Returns
    ///
    /// * `Result<usize>` - Number of bytes read, or an error
    fn read_data_block(&mut self, max_bytes: u64) -> Result<usize> {
        // Calculate how much data we can read before the next block boundary
        let to_next_boundary = utils::remaining_in_block(self.pos, self.config.block_size);
        let to_read = min(max_bytes, to_next_boundary) as usize;
        
        if to_read == 0 {
            return Ok(0);
        }
        
        // Read the data
        let mut buf = vec![0u8; to_read];
        let bytes_read = self.source.read(&mut buf)?;
        
        if bytes_read > 0 {
            // Add to our buffer and update position
            self.buffer.extend_from_slice(&buf[..bytes_read]);
            self.pos += bytes_read as u64;
        }
        
        Ok(bytes_read)
    }

    /// Reads chunks data, handling block headers.
    ///
    /// This method reads from the current position, skipping any block headers
    /// it encounters, until it has read all of the chunks from a logical section of
    /// the file. Note that this may return data containing multiple logical chunks 
    /// if they are stored within a single block.
    ///
    /// The returned data needs to be processed by a higher-level chunk parser to
    /// extract individual chunks based on their specific format.
    pub fn read_chunks(&mut self) -> Result<Bytes> {
        // Update position and clear buffer for new chunk
        self.update_position()?;
        self.buffer.clear();
        
        // Mark the beginning of this chunk
        self.chunk_begin = self.pos;
        
        // Start reading the chunk
        if self.is_block_boundary(self.pos) {
            self.read_chunks_at_boundary()
        } else {
            self.read_chunks_mid_block()
        }
    }

    /// Reads chunks starting at a block boundary.
    ///
    /// This is called when we're at a block boundary and we need to read new chunks.
    /// The block header tells us how far to read for the complete data section.
    ///
    /// # Returns
    ///
    /// * `Result<Bytes>` - The complete chunk data, or an error
    fn read_chunks_at_boundary(&mut self) -> Result<Bytes> {
        // Read the block header
        let header = self.read_block_header()?;
        
        // Verify previous_chunk is consistent (should be 0 at start of chunk)
        if header.previous_chunk != 0 {
            return Err(DiskyError::Corruption(format!(
                "Expected previous_chunk=0 at the beginning of a chunk, got {}", 
                header.previous_chunk
            )));
        }
        
        // Determine chunk size from next_chunk
        let chunk_size = header.next_chunk - BLOCK_HEADER_SIZE;
        
        // Read the chunk data, handling any additional headers
        self.read_complete_chunk_data(chunk_size)?;
        
        // Return the completed chunk
        Ok(self.buffer.split().freeze())
    }

    /// Reads chunks when we're in the middle of a block.
    ///
    /// This is called when our current position is not at a block boundary,
    /// so we need to read until the next block boundary and then use the header
    /// to determine how to proceed.
    ///
    /// # Returns
    ///
    /// * `Result<Bytes>` - The complete chunk data, or an error
    fn read_chunks_mid_block(&mut self) -> Result<Bytes> {
        // Since this can be the end of a chunk, we might just need to return what we have
        // If there's no remaining data to read, just return the current buffer
        if self.buffer.len() > 0 {
            return Ok(self.buffer.split().freeze());
        }
        
        // Read until the next block boundary
        let remaining_in_block = utils::remaining_in_block(self.pos, self.config.block_size);
        let bytes_read = self.read_data_block(remaining_in_block)?;
        
        if bytes_read == 0 {
            if self.buffer.is_empty() {
                return Err(DiskyError::UnexpectedEof);
            }
            return Ok(self.buffer.split().freeze());
        }
        
        // If we're at a block boundary, read the block header
        if self.is_block_boundary(self.pos) {
            // Read the block header
            let header = self.read_block_header()?;
            
            // If previous_chunk is 0, this is the start of a new chunk
            // If the buffer is empty, we haven't yet started reading a chunk
            if header.previous_chunk == 0 || self.buffer.is_empty() {
                // Start of a new chunk - push back the data we already read
                self.chunk_begin = self.pos - BLOCK_HEADER_SIZE; 
                
                // Calculate chunk size from next_chunk
                let chunk_size = header.next_chunk - BLOCK_HEADER_SIZE;
                
                // Read the complete chunk
                self.read_complete_chunk_data(chunk_size)?;
            } else {
                // This is a continuation of the current chunk
                // Verify the previous_chunk value is consistent
                let expected_previous_chunk = self.pos - BLOCK_HEADER_SIZE - self.chunk_begin;
                if header.previous_chunk != expected_previous_chunk {
                    return Err(DiskyError::Corruption(format!(
                        "Block header previous_chunk value mismatch: expected {}, got {}",
                        expected_previous_chunk, header.previous_chunk
                    )));
                }
                
                // Calculate remaining chunk size from next_chunk
                let remaining_chunk_size = header.next_chunk - BLOCK_HEADER_SIZE;
                
                // Read the rest of the chunk
                self.read_complete_chunk_data(remaining_chunk_size)?;
            }
        }
        
        // Return the completed chunk
        Ok(self.buffer.split().freeze())
    }

    /// Reads the remainder of a chunk, handling any block headers encountered.
    ///
    /// # Arguments
    ///
    /// * `remaining_size` - The remaining size of the chunk to read
    ///
    /// # Returns
    ///
    /// * `Result<()>` - Success or an error
    fn read_complete_chunk_data(&mut self, mut remaining_size: u64) -> Result<()> {
        while remaining_size > 0 {
            // Check if we're at a block boundary
            if self.is_block_boundary(self.pos) {
                // Read the block header
                let header = self.read_block_header()?;
                
                // Verify the previous_chunk value is consistent
                let expected_previous_chunk = self.pos - BLOCK_HEADER_SIZE - self.chunk_begin;
                if header.previous_chunk != expected_previous_chunk {
                    return Err(DiskyError::Corruption(format!(
                        "Block header previous_chunk value mismatch: expected {}, got {}",
                        expected_previous_chunk, header.previous_chunk
                    )));
                }
                
                // Update remaining size based on next_chunk if needed
                let recalculated_remaining = header.next_chunk - BLOCK_HEADER_SIZE;
                if recalculated_remaining < remaining_size {
                    remaining_size = recalculated_remaining;
                }
                
                continue;
            }
            
            // Read data until the next boundary or until we've read all remaining data
            let bytes_read = self.read_data_block(remaining_size)?;
            
            if bytes_read == 0 {
                // End of file reached unexpectedly
                return Err(DiskyError::UnexpectedEof);
            }
            
            // Update remaining size
            remaining_size -= bytes_read as u64;
        }
        
        Ok(())
    }

    /// Skips the current chunks and moves to the beginning of the next block section.
    ///
    /// This is useful for error recovery when chunks are corrupted.
    pub fn skip_chunks(&mut self) -> Result<()> {
        // Read the current chunks to determine their end position
        let _chunks = self.read_chunks()?;
        
        // Clear the buffer for the next chunk
        self.buffer.clear();
        
        // Start fresh at current position
        self.update_position()?;
        self.chunk_begin = self.pos;
        
        Ok(())
    }

    /// Returns the current position in the file.
    pub fn position(&self) -> u64 {
        self.pos
    }

    /// Returns the current chunk beginning position.
    pub fn chunk_begin(&self) -> u64 {
        self.chunk_begin
    }

    /// Returns a reference to the underlying source.
    pub fn get_ref(&self) -> &Source {
        &self.source
    }

    /// Returns a mutable reference to the underlying source.
    pub fn get_mut(&mut self) -> &mut Source {
        &mut self.source
    }

    /// Returns the underlying source, consuming self.
    pub fn into_inner(self) -> Source {
        self.source
    }
}