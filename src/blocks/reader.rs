use crate::blocks::utils::{self, BLOCK_HEADER_SIZE, DEFAULT_BLOCK_SIZE};
use crate::error::{DiskyError, Result};
use crate::hash::highway_hash;
use bytes::{Bytes, BytesMut};
use log::{debug, error, info, warn};
use std::cmp::min;
use std::io::{Read, Result as IoResult, Seek, SeekFrom};

/// Result of a chunks read operation.
#[derive(Debug)]
pub enum BlocksPiece {
    /// Successfully read chunks data.
    Chunks(Bytes),
    /// End of file reached - no more chunks to read.
    EOF,
}

/// A wrapper around any `Read + Seek` source that tracks the current position.
///
/// This tracks the current position automatically as reads and seeks occur,
/// eliminating the need to call `stream_position()` frequently or manually
/// maintain a position counter.
pub struct ReadPositionTracker<Source: Read + Seek> {
    /// The underlying source to read from
    source: Source,

    /// The current position in the source
    position: u64,
}

impl<Source: Read + Seek> ReadPositionTracker<Source> {
    /// Create a new ReadPositionTracker wrapping the given source.
    pub fn new(mut source: Source) -> IoResult<Self> {
        // Get the initial position from the source
        let position = source.stream_position()?;

        Ok(Self { source, position })
    }

    /// Returns the current position in the source.
    pub fn position(&self) -> u64 {
        self.position
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

impl<Source: Read + Seek> Read for ReadPositionTracker<Source> {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        let bytes_read = self.source.read(buf)?;
        self.position += bytes_read as u64;
        Ok(bytes_read)
    }
}

impl<Source: Read + Seek> Seek for ReadPositionTracker<Source> {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        // Forward seek call to the source
        let new_pos = self.source.seek(pos)?;

        // Update our tracked position
        self.position = new_pos;

        Ok(new_pos)
    }

    fn stream_position(&mut self) -> IoResult<u64> {
        // We can return our tracked position directly without a system call
        Ok(self.position)
    }
}

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
#[derive(Clone, Debug)]
struct BlockHeader {
    /// Distance from the beginning of the chunk to the beginning of the block.
    previous_chunk: u64,

    /// Distance from the beginning of the block to the end of the chunk.
    next_chunk: u64,
}

/// State for block reader
#[derive(Clone)]
enum BlockReaderState {
    /// Reader is reading fresh chunks.
    Fresh,
    /// We just read a [BlockHeader]
    ReadHeader(BlockHeader),
    /// We just read a header with inconsistent header hash.
    /// This likely implies a corruped header and possibly corrupted
    /// block, therefore possibly corrupted chunks, so safeway forward is
    /// to skip current chunks.
    ReadCorruptedHeader(BlockHeader),

    /// Read a block header which disagrees about when previous chunk
    /// begins. This is likely due to a corruption of the current chunk and
    /// a suprise begin of a new chunk. We should trust the new block header
    /// and assume that the new chunk indeed begins where it indicates.
    BlockHeaderInconsistency(BlockHeader),

    ReadInvalidHeader,

    /// We have reached EOF.
    EOF,
    /// Actively reading a section of chunks.
    Active,
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
    /// The underlying source to read from, wrapped in a position tracker
    source: ReadPositionTracker<Source>,

    /// Reader configuration.
    config: BlockReaderConfig,

    /// Current chunk start position (used for calculating chunk boundaries)
    chunk_begin: u64,

    /// Buffer for storing chunk data that has been read.
    buffer: BytesMut,

    /// State of the reader
    state: BlockReaderState,
}

impl<Source: Read + Seek> BlockReader<Source> {
    /// Creates a new BlockReader with default configuration.
    pub fn new(source: Source) -> Result<Self> {
        Self::with_config(source, BlockReaderConfig::default())
    }

    /// Creates a new BlockReader with custom configuration.
    pub fn with_config(source: Source, config: BlockReaderConfig) -> Result<Self> {
        let source = ReadPositionTracker::new(source).map_err(|e| DiskyError::Io(e))?;

        let position = source.position();

        // Create a BytesMut with some initial capacity
        let buffer = BytesMut::with_capacity(config.block_size as usize);

        Ok(Self {
            source,
            config,
            chunk_begin: position,
            buffer,
            state: BlockReaderState::Fresh,
        })
    }

    /// Checks if a position falls on a block boundary.
    fn at_block_boundary(&self) -> bool {
        utils::is_block_boundary(self.source.position, self.config.block_size)
    }

    /// Reads a block header from the current position.
    ///
    /// This function reads 24 bytes from the current position, interprets it as a block header,
    /// and validates the header hash. The position is automatically updated by the ReadPositionTracker.
    ///
    /// # Returns
    ///
    /// * `Result<BlockHeader>` - The parsed header if valid, or an error
    fn read_block_header(&mut self) -> Result<BlockHeader> {
        // Read the header bytes
        let mut header_bytes = [0u8; BLOCK_HEADER_SIZE as usize];
        let bytes_read = self.source.read(&mut header_bytes)?;

        if bytes_read < header_bytes.len() {
            self.state = BlockReaderState::EOF;
            return Err(DiskyError::UnexpectedEof);
        }

        // Extract header components
        let header_hash = u64::from_le_bytes([
            header_bytes[0],
            header_bytes[1],
            header_bytes[2],
            header_bytes[3],
            header_bytes[4],
            header_bytes[5],
            header_bytes[6],
            header_bytes[7],
        ]);

        let previous_chunk = u64::from_le_bytes([
            header_bytes[8],
            header_bytes[9],
            header_bytes[10],
            header_bytes[11],
            header_bytes[12],
            header_bytes[13],
            header_bytes[14],
            header_bytes[15],
        ]);

        let next_chunk = u64::from_le_bytes([
            header_bytes[16],
            header_bytes[17],
            header_bytes[18],
            header_bytes[19],
            header_bytes[20],
            header_bytes[21],
            header_bytes[22],
            header_bytes[23],
        ]);

        // Verify header hash
        let content = &header_bytes[8..24];
        let calculated_hash = highway_hash(content);

        let block_header = BlockHeader {
            previous_chunk,
            next_chunk,
        };

        if calculated_hash != header_hash {
            self.state = BlockReaderState::ReadCorruptedHeader(block_header);
            return Err(DiskyError::BlockHeaderHashMismatch);
        }

        match utils::validate_header_values(
            block_header.previous_chunk,
            block_header.next_chunk,
            self.config.block_size,
        ) {
            Ok(_) => Ok(block_header),
            Err(err) => {
                self.state = BlockReaderState::ReadInvalidHeader;
                Err(err)
            }
        }
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
        let to_next_boundary =
            utils::remaining_in_block(self.source.position(), self.config.block_size);
        let to_read = min(max_bytes, to_next_boundary) as usize;

        if to_read == 0 {
            return Ok(0);
        }

        // Get the current length before reading
        let old_len = self.buffer.len();
        
        // Check if we need more capacity
        let remaining = self.buffer.capacity() - old_len;
        if remaining < to_read {
            // Only reserve additional space if needed
            self.buffer.reserve(to_read - remaining);
        }
        
        // Resize the buffer to have enough initialized space
        self.buffer.resize(old_len + to_read, 0);
        
        // Create a mutable slice of the newly allocated portion
        let mut_slice = &mut self.buffer.as_mut()[old_len..old_len + to_read];
        
        // Read directly into the allocated slice of BytesMut
        let bytes_read = self.source.read(mut_slice)?;
        
        if bytes_read < to_read {
            // If we read less than expected, truncate the buffer back to actual size
            self.buffer.truncate(old_len + bytes_read);
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
    ///
    /// # Returns
    ///
    /// * `Result<BlocksPiece>` - Either `BlocksPiece::Chunks` with data or
    ///   `BlocksPiece::EOF` if end of file is reached, or an error.
    pub fn read_chunks(&mut self) -> Result<BlocksPiece> {
        // Get previous state.
        let state = self.state.clone();

        match &state {
            BlockReaderState::Fresh | BlockReaderState::ReadHeader(_) => {
                // Set state that we are now actively reading.
                self.state = BlockReaderState::Active;
                // Clear buffer to make space for new chunks.
                self.buffer.clear();
                // Position is automatically tracked by ReadPositionTracker
            }
            _ => (),
        }

        match &state {
            BlockReaderState::Fresh => {
                self.chunk_begin = self.source.position();
                self.read_new_chunks()
            }
            BlockReaderState::ReadHeader(block_header) => {
                // We previously read a block header delimiting a new chunk. 
                // Now we read that new chunk which began BLOCK_HEADER_SIZE ago.
                self.chunk_begin = self.source.position() - BLOCK_HEADER_SIZE;
                self.read_from_header_with_check(block_header)
            }
            // Need to call `recover` before trying to read again.
            BlockReaderState::ReadCorruptedHeader(_) | BlockReaderState::BlockHeaderInconsistency(_) => Err(
                DiskyError::ReadCorruptedBlock(
                    "Attemping to `read_chunks` on a corrupted reader. Try to call `recover` before reading again.".to_string()
                    )
                ),

            BlockReaderState::EOF => Ok(BlocksPiece::EOF),
            BlockReaderState::ReadInvalidHeader => Err(DiskyError::UnrecoverableCorruption(
                    "Cannot `read_chunks` after having read an invalid header.".to_string())),

            // We do not know how to recover from this. Please look in logs to see how we ended up
            // trying to read again while we're in an active reading stage.
            // Maybe a race condition?
            BlockReaderState::Active => Err(DiskyError::Other(
                "Unrecoverable, how did you get here? This could be a race-condition, note that the base disky reader is not thread-safe on its own.".to_string(),
            )),
        }
    }

    /// Read as if we're at the start of a new chunk.
    fn read_new_chunks(&mut self) -> Result<BlocksPiece> {
        if self.at_block_boundary() {
            self.read_from_boundary()
        } else {
            self.read_from_within()
        }
    }

    fn read_from_header_with_check(&mut self, header: &BlockHeader) -> Result<BlocksPiece> {
        self.verify_expected_previous_chunk(header)?;
        self.read_from_header(header)
    }

    fn verify_expected_previous_chunk(&mut self, header: &BlockHeader) -> Result<()> {
        let expected_previous_chunk = self.source.position() - BLOCK_HEADER_SIZE - self.chunk_begin;
        if header.previous_chunk != expected_previous_chunk {
            self.state = BlockReaderState::BlockHeaderInconsistency(header.clone());
            return Err(DiskyError::BlockHeaderInconsistency(format!(
                "Block header previous_chunk value mismatch: expected {}, got {}",
                expected_previous_chunk, header.previous_chunk
            )));
        }
        Ok(())
    }

    /// Read from right after a block header
    fn read_from_header(&mut self, header: &BlockHeader) -> Result<BlocksPiece> {
        // We can't reliably check `expected_previous_chunk` here, since there
        // may have been > 1 chunk read up until this point.

        // Check if this is a continuation of an existing chunk or the start of a new chunk
        // If previous_chunk is 0, this is the start of a new chunk
        // otherwise, it's a continuation of the current chunk
        let is_continuation = header.previous_chunk > 0;

        // If we already have data in the buffer and this header indicates a new chunk,
        // return the data we have so far without reading further
        if !self.buffer.is_empty() && !is_continuation {
            // Set current state to being [ReadHeader] so that we resume
            // from that next time.
            self.state = BlockReaderState::ReadHeader(header.clone());
            // We've completed a chunk that ended exactly at a block boundary,
            // and the next block contains a new chunk
            return Ok(BlocksPiece::Chunks(self.buffer.split().freeze()));
        }

        // Determine the size remaining of the chunk.
        let chunk_size = header.next_chunk - BLOCK_HEADER_SIZE;

        // Read the chunk data, handling any additional headers
        self.read_complete_chunk_data(chunk_size)?;

        // Return the completed chunk
        Ok(BlocksPiece::Chunks(self.buffer.split().freeze()))
    }

    /// Reads chunks starting at a block boundary.
    ///
    /// This is called when we're at a block boundary and we need to read new chunks.
    /// The block header tells us how far to read for the complete data section.
    ///
    /// # Returns
    ///
    /// * `Result<BlocksPiece>` - The complete chunk data, or an error
    fn read_from_boundary(&mut self) -> Result<BlocksPiece> {
        // Read the block header
        let header = self.read_block_header()?;

        self.read_from_header(&header)
    }

    /// Reads chunks when we're within a block.
    ///
    /// This is called when our current position is not at a block boundary,
    /// so we need to read until the next block boundary and then use the header
    /// to determine how to proceed.
    ///
    /// # Returns
    ///
    /// * `Result<BlocksPiece>` - The complete chunk data, or an error
    fn read_from_within(&mut self) -> Result<BlocksPiece> {
        // Read until the next block boundary
        let remaining_in_block =
            utils::remaining_in_block(self.source.position(), self.config.block_size);
        let bytes_read = self.read_data_block(remaining_in_block)?;

        if bytes_read == 0 {
            if self.buffer.is_empty() {
                self.state = BlockReaderState::EOF;
                return Ok(BlocksPiece::EOF);
            }

            return Ok(BlocksPiece::Chunks(self.buffer.split().freeze()));
        }

        // If we're at a block boundary, we read from the boundary.
        if self.at_block_boundary() {
            return self.read_from_boundary();
        }

        // Indicate successful read of complete chunks.
        self.state = BlockReaderState::Fresh;

        // Return the completed chunk
        Ok(BlocksPiece::Chunks(self.buffer.split().freeze()))
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
            if self.at_block_boundary() {
                // Read the block header
                let header = self.read_block_header()?;

                // Verify the previous_chunk value is consistent
                self.verify_expected_previous_chunk(&header)?;

                // Update remaining size based on next_chunk if needed
                remaining_size = header.next_chunk - BLOCK_HEADER_SIZE;

                continue;
            }

            // Read data until the next boundary or until we've read all remaining data
            let bytes_read = self.read_data_block(remaining_size)?;

            if bytes_read == 0 {
                // End of file reached unexpectedly
                self.state = BlockReaderState::EOF;
                return Err(DiskyError::UnexpectedEof);
            }

            // Update remaining size
            remaining_size -= bytes_read as u64;
        }

        self.state = BlockReaderState::Fresh;
        Ok(())
    }

    /// Attempt to recover from a corrupted state. We will attempt to transition
    /// to a non-corruped reader state.
    pub fn recover(&mut self) -> Result<()> {
        let state = self.state.clone();

        match &state {
            // We are not corrupted, recover is invalid call here. We consider
            // this an error to make it easy to catch logic errors causing this call.
            BlockReaderState::Fresh => Err(DiskyError::Other(
                "Attempted to recover from a `Fresh` state.".to_string(),
            )),
            // We are not corrupted, recover is invalid call here. We consider
            // this an error to make it easy to catch logic errors causing this call.
            BlockReaderState::ReadHeader(_block_header) => Err(DiskyError::Other(
                "Attempted to recover from `ReadHeader` state".to_string(),
            )),
            BlockReaderState::ReadCorruptedHeader(block_header) => {
                warn!("Attempting to recover from corrupted block header: {:?}", block_header);
                
                // Find the next valid chunk by seeking to the next block boundary
                // and reading its header
                info!("Trying to find next valid chunk...");
                self.find_next_valid_chunk()?;

                // Reset state to Fresh to start reading from the next valid chunk
                info!("Recovery successful, resetting to Fresh state");
                self.state = BlockReaderState::Fresh;

                Ok(())
            }
            BlockReaderState::BlockHeaderInconsistency(block_header) => {
                warn!("Attempting to recover from block header inconsistency: {:?}", block_header);
                
                // We must seek back to the start of previous chunk.
                let previous_chunk = block_header.previous_chunk + BLOCK_HEADER_SIZE;
                
                // Seek back to start of previous chunk assuming the current one
                // was corrupted and that the new block is valid.
                info!("Seeking back by {} bytes to beginning of previous chunk", previous_chunk);
                
                let seek_offset: i64 = previous_chunk.try_into().map_err(
                    |e| {
                        error!("Seek offset conversion failed: {:?}", e);
                        DiskyError::Other(
                            format!(
                                "Overflow when seeking during recovery of `previous chunk inconsistency` {:?}", 
                                e
                            )
                        )
                    }
                )?;

                self.source.seek(SeekFrom::Current(-seek_offset))?;
                info!("Successfully sought back to position: {}", self.source.position());

                // Reset state to Fresh to start reading from the next valid chunk
                info!("Recovery successful, resetting to Fresh state");
                self.state = BlockReaderState::Fresh;

                Ok(())
            }
            BlockReaderState::EOF => {
                Err(DiskyError::Other("Cannot recover from `EOF`.".to_string()))
            }

            BlockReaderState::ReadInvalidHeader => Err(DiskyError::UnrecoverableCorruption(
                "Cannot `recover` from an invalid header.".to_string(),
            )),

            // We do not know how to recover from this. Please look in logs to see how we ended up
            // trying to recover during active reading, maybe a race condition?
            BlockReaderState::Active => Err(DiskyError::Other(
                "Cannot recover from `Active` reading state.".to_string(),
            )),
        }
    }

    /// Returns the current position in the file.
    pub fn file_position(&self) -> u64 {
        self.source.position()
    }

    /// Returns the current chunk beginning position.
    pub fn chunk_begin(&self) -> u64 {
        self.chunk_begin
    }

    /// Returns a reference to the underlying source.
    pub fn get_ref(&self) -> &Source {
        self.source.get_ref()
    }

    /// Returns a mutable reference to the underlying source.
    pub fn get_mut(&mut self) -> &mut Source {
        self.source.get_mut()
    }

    /// Returns the underlying source, consuming self.
    pub fn into_inner(self) -> Source {
        self.source.into_inner()
    }

    /// Seeks to the next block boundary and tries to find a valid chunk.
    /// If the header at the next block boundary is also corrupted, continues
    /// looking at subsequent block boundaries until a valid header is found.
    fn find_next_valid_chunk(&mut self) -> Result<()> {
        info!("Trying to find next valid chunk at position: {}", self.source.position());
        
        // Calculate offset to next block boundary
        let offset_to_next_boundary =
            utils::remaining_in_block(self.source.position(), self.config.block_size);
            
        debug!("Seeking {} bytes to next block boundary", offset_to_next_boundary);

        // Seek to the next block boundary
        self.source
            .seek(SeekFrom::Current(offset_to_next_boundary as i64))?;
        // Position is automatically tracked by ReadPositionTracker
        
        info!("Reached next block boundary at position: {}", self.source.position());

        // Try reading headers at block boundaries until we find a valid one or reach EOF
        loop {
            // Save position at start of potential header
            let header_start_pos = self.source.position();

            // Try to read and validate a block header using existing method
            match self.read_block_header() {
                Ok(header) => {
                    // We found a valid header! Seek to the end of this chunk
                    // using the next_chunk offset from header
                    info!("Found valid header at position {}: {:?}", header_start_pos, header);
                    let start_of_next_chunk = header_start_pos + header.next_chunk;
                    
                    debug!("Seeking to end of chunk at position: {}", start_of_next_chunk);
                    self.source.seek(SeekFrom::Start(start_of_next_chunk))?;
                    // Position is automatically updated by the seek operation
                    
                    info!("Successfully found valid chunk, recovery complete");
                    return Ok(());
                }
                Err(DiskyError::UnexpectedEof) => {
                    // Reached EOF, no more valid chunks
                    warn!("Reached EOF while looking for valid chunk");
                    return Err(DiskyError::UnexpectedEof);
                }
                Err(e) => {
                    // This header was also corrupted
                    warn!("Found another corrupted header at position {}: {}", header_start_pos, e);
                    
                    // Since read_block_header already updated file_position, just need to seek to next block
                    let to_next_boundary =
                        utils::remaining_in_block(self.source.position(), self.config.block_size);
                        
                    debug!("Trying next block boundary, seeking {} bytes", to_next_boundary);
                    self.source
                        .seek(SeekFrom::Current(to_next_boundary as i64))?;
                    // Position is automatically tracked by ReadPositionTracker
                    
                    info!("Now at position: {}", self.source.position());
                }
            }
        }
    }
}
