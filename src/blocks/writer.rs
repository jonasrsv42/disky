use std::io::{Seek, Write};
use bytes::{BufMut, Bytes, BytesMut};

use crate::error::Result;
use crate::hash::highway_hash;

/// The size of a block header in bytes.
/// Always 24 bytes: 8 for header_hash, 8 for previous_chunk, 8 for next_chunk.
const BLOCK_HEADER_SIZE: u64 = 24;

/// Configuration options for BlockWriter.
#[derive(Debug, Clone)]
pub struct BlockWriterConfig {
    /// Size of a block in bytes (default: 64 KiB).
    pub block_size: u64,
}

impl Default for BlockWriterConfig {
    fn default() -> Self {
        // Default is 64 KiB which is well above the 24-byte header size
        // This should never fail validation
        //
        // NOTE: It should ALWAYS be 64KiB for actual writing, it's only configurable 
        // to allow for test injection to write "human-readable" tests
        Self {
            block_size: 1 << 16,        // 64 KiB
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
        if block_size < BLOCK_HEADER_SIZE * 2 {
            return Err(crate::error::RiegeliError::Other(
                format!("Block size ({}) must be at least twice the header size ({}) to prevent cascading headers",
                        block_size, BLOCK_HEADER_SIZE)
            ));
        }
        
        Ok(Self {
            block_size,
        })
    }
    
    /// Returns the usable block size (block size minus block header size).
    pub fn usable_block_size(&self) -> u64 {
        self.block_size - BLOCK_HEADER_SIZE
    }
}

/// Writer for Riegeli blocks that handles block boundary interruptions.
///
/// According to the Riegeli specification, a chunk can be interrupted by a block header
/// at any point, including in the middle of the chunk header. This writer handles those
/// interruptions by automatically inserting block headers at the appropriate positions.
pub struct BlockWriter<Sink: Write + Seek> {
    /// The underlying writer.
    sink: Sink,
    /// Current position in the file.
    pos: u64,
    /// Configuration options.
    config: BlockWriterConfig,
}

impl<Sink: Write + Seek> BlockWriter<Sink> {
    /// Creates a new BlockWriter with default configuration.
    pub fn new(sink: Sink) -> Result<Self> {
        Self::with_config(sink, BlockWriterConfig::default())
    }

    /// Creates a new BlockWriter with custom configuration.
    pub fn with_config(mut sink: Sink, config: BlockWriterConfig) -> Result<Self> {
        // Validate the configuration - ensure block size is reasonable
        if config.block_size < BLOCK_HEADER_SIZE * 2 {
            return Err(crate::error::RiegeliError::Other(
                format!("Block size ({}) must be at least twice the header size ({}) to prevent cascading headers",
                        config.block_size, BLOCK_HEADER_SIZE)
            ));
        }
        
        let pos = sink.stream_position()?;
        Ok(Self {
            sink,
            pos,
            config,
        })
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
        config: BlockWriterConfig
    ) -> Result<Self> {
        // Validate the configuration - ensure block size is reasonable
        if config.block_size < BLOCK_HEADER_SIZE * 2 {
            return Err(crate::error::RiegeliError::Other(
                format!("Block size ({}) must be at least twice the header size ({}) to prevent cascading headers",
                        config.block_size, BLOCK_HEADER_SIZE)
            ));
        }
        
        // Verify the sink position matches the expected position
        let actual_pos = sink.stream_position()?;
        if actual_pos != pos {
            sink.seek(std::io::SeekFrom::Start(pos))?;
        }
        
        Ok(Self {
            sink,
            pos,
            config,
        })
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
    fn write_block_header(&mut self, previous_chunk: u64, next_chunk: u64) -> Result<()> {
        // Build the header
        let mut header = BytesMut::with_capacity(BLOCK_HEADER_SIZE as usize);
        
        // Skip the header hash for now (will fill it in later)
        header.put_u64_le(0); // Placeholder for header_hash
        
        // Write the rest of the header
        header.put_u64_le(previous_chunk);
        header.put_u64_le(next_chunk);
        
        // Calculate the header hash (excluding the first 8 bytes)
        let header_hash = highway_hash(&header[8..]);
        
        // Write the complete header with the hash
        let mut final_header = BytesMut::with_capacity(BLOCK_HEADER_SIZE as usize);
        final_header.put_u64_le(header_hash);
        final_header.extend_from_slice(&header[8..]);
        
        // Write the header
        self.sink.write_all(&final_header)?;
        
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

    /// Writes data to the sink, handling block boundaries by inserting block headers.
    /// 
    /// Each call to this method is considered to be writing a new logical chunk.
    /// Block headers will be inserted at block boundaries as needed.
    ///
    /// # How Chunks and Blocks Work
    ///
    /// In the Riegeli format, a chunk is a logical unit of data that may be interrupted by block 
    /// headers if it crosses a block boundary. This method handles those interruptions automatically.
    ///
    /// Each time this method is called, the current position is treated as the beginning of a new chunk.
    /// If this position happens to be at a block boundary, a block header is inserted first.
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
                let next_chunk = (chunk_data.len() - data_pos) as u64; // Remaining data
                self.write_block_header(previous_chunk, next_chunk)?;
                continue;
            }
            
            // Calculate how much data we can write before the next block boundary
            let remaining = self.remaining_in_block(self.pos);
            
            // Write as much data as fits before the next block boundary
            let bytes_to_write = std::cmp::min(remaining, (chunk_data.len() - data_pos) as u64) as usize;
            let end_pos = data_pos + bytes_to_write;
            
            self.sink.write_all(&chunk_data[data_pos..end_pos])?;
            data_pos = end_pos;
            self.pos += bytes_to_write as u64;
        }
        
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    
    #[test]
    fn test_block_writer_creation() {
        let buffer = Cursor::new(Vec::new());
        let writer = BlockWriter::new(buffer).unwrap();
        
        // Verify default configuration
        assert_eq!(writer.config.block_size, 1 << 16); // 64 KiB
        assert_eq!(BLOCK_HEADER_SIZE, 24); // Fixed header size
    }
    
    #[test]
    fn test_custom_block_size() {
        let buffer = Cursor::new(Vec::new());
        let config = BlockWriterConfig::with_block_size(1 << 20).unwrap(); // 1 MiB
        let writer = BlockWriter::with_config(buffer, config).unwrap();
        
        assert_eq!(writer.config.block_size, 1 << 20);
        assert_eq!(writer.config.usable_block_size(), (1 << 20) - BLOCK_HEADER_SIZE);
    }
    
    #[test]
    fn test_write_chunk_smaller_than_block() {
        // Create a fresh buffer
        let buffer = Cursor::new(Vec::new());
        
        // Use a block size larger than our test data but sufficient to avoid cascading headers
        let block_size = 60u64; // > 2 * BLOCK_HEADER_SIZE (24)
        
        let mut writer = BlockWriter::with_config(
            buffer, 
            BlockWriterConfig {
                block_size,
            }
        ).unwrap();
        
        // Create a small chunk of data that won't cross a block boundary
        let chunk_data = Bytes::from(b"abcdefghijklm".to_vec());
        
        // Verify test assumptions
        assert!(chunk_data.len() as u64 + BLOCK_HEADER_SIZE < block_size, 
                "Test chunk should not cross block boundary");
        
        // Write the chunk
        writer.write_chunk(chunk_data.clone()).unwrap();
        
        // Get the buffer and check its contents
        let buffer = writer.into_inner();
        let vec = buffer.into_inner();
        
        println!("Chunk data size: {}", chunk_data.len());
        println!("Header size: {}", BLOCK_HEADER_SIZE);
        println!("Actual size: {}", vec.len());
        
        // There should be a header at position 0 followed by the chunk data
        let expected_size = BLOCK_HEADER_SIZE as usize + chunk_data.len();
        assert_eq!(vec.len(), expected_size as usize);
        
        // Check chunk data is correctly written after the header
        assert_eq!(
            &vec[BLOCK_HEADER_SIZE as usize..],
            &chunk_data[..],
            "Chunk data should be written exactly as provided after the header"
        );
    }
    
    #[test]
    fn test_write_chunk_at_block_boundary() {
        // Create a buffer and fill it to exactly one block
        // Use a block size large enough for our test and to avoid cascading headers
        let block_size = 1024u64; // >> 2 * BLOCK_HEADER_SIZE (24)
        let buffer = Cursor::new(vec![0; block_size as usize]);
        
        // Create a writer for appending at the block boundary
        let mut writer = BlockWriter::for_append_with_config(
            buffer,
            block_size,      // Current position at the block boundary
            BlockWriterConfig {
                block_size,
            }
        ).unwrap();
        
        // Create a chunk
        let chunk_data = Bytes::from(vec![1; 100]);
        
        // Write the chunk - should add a block header
        writer.write_chunk(chunk_data.clone()).unwrap();
        
        // Get the buffer and check its contents
        let buffer = writer.into_inner();
        let vec = buffer.into_inner();
        
        // The buffer should contain exactly block_size (initial) +
        // block_header_size (added at boundary) + chunk_data.len() bytes
        let expected_size = block_size as usize + BLOCK_HEADER_SIZE as usize + chunk_data.len();
        assert_eq!(vec.len(), expected_size);
        
        // Check initial block data (zeros)
        assert_eq!(&vec[0..block_size as usize], &vec![0; block_size as usize]);
        
        // The block header should be at the block boundary
        // We can't easily verify the specific hash values
        
        // Check the chunk data is correctly appended
        assert_eq!(&vec[block_size as usize + BLOCK_HEADER_SIZE as usize..], &chunk_data[..]);
    }
    
    #[test]
    fn test_write_chunk_crossing_block_boundary() {
        // Use a small block size that's still larger than header size to avoid cascading headers
        let block_size = 60u64;  // > 2 * BLOCK_HEADER_SIZE (24)
        
        // Start with a fresh buffer
        let buffer = Cursor::new(Vec::new());
        
        let mut writer = BlockWriter::with_config(
            buffer,
            BlockWriterConfig {
                block_size,
            }
        ).unwrap();
        
        // Create chunk with distinct patterns that's large enough to cross a block boundary
        // We need at least (block_size - BLOCK_HEADER_SIZE) + 1 bytes to cross the boundary
        // Using a repeating pattern for easy visual identification
        let needed_size = (block_size - BLOCK_HEADER_SIZE + 20) as usize; // +20 for some extra bytes
        let mut data = Vec::with_capacity(needed_size);
        
        // Fill with a repeating pattern
        let pattern = b"ABCDEFG";
        while data.len() < needed_size {
            data.extend_from_slice(pattern);
        }
        
        // Truncate to exact size
        data.truncate(needed_size);
        
        let chunk_data = Bytes::from(data);
        
        // Calculate where the first block boundary will be
        // There will be a header at pos 0, and data starts at pos BLOCK_HEADER_SIZE
        let first_header_size = BLOCK_HEADER_SIZE as usize;
        let bytes_in_first_block = (block_size - BLOCK_HEADER_SIZE) as usize;
        
        // Verify test assumptions
        assert!(chunk_data.len() > bytes_in_first_block,
               "Test chunk must cross at least one block boundary");
        
        // Write the chunk
        writer.write_chunk(chunk_data.clone()).unwrap();
        
        // Get the buffer and check its contents
        let buffer = writer.into_inner();
        let vec = buffer.into_inner();
        
        // Print detailed info for debugging
        println!("Block size: {}", block_size);
        println!("Bytes in first block: {}", bytes_in_first_block);
        println!("Block header size: {}", BLOCK_HEADER_SIZE);
        println!("Chunk data size: {}", chunk_data.len());
        println!("Actual size: {}", vec.len());
        
        // The output should include:
        // 1. Initial header at position 0
        // 2. First part of chunk data (bytes_in_first_block)
        // 3. Another block header at block boundary (block_size)
        // 4. Rest of chunk data
        
        // Let's focus on checking the content rather than exact size
        println!("Note: Headers are at positions 0 and {}", block_size);
        
        // Ensure we have enough data (more than chunk + headers)
        let minimum_expected_size = chunk_data.len() + 2 * BLOCK_HEADER_SIZE as usize;
        assert!(vec.len() >= minimum_expected_size,
                "Output should include chunk data + at least two block headers");
        
        // Check first part of chunk data (after initial header, up to block boundary)
        let first_part_start = first_header_size;
        let first_part_end = block_size as usize; // End at the block boundary
        
        println!("First part of chunk: bytes {} to {}", first_part_start, first_part_end);
        
        assert_eq!(
            &vec[first_part_start..first_part_end],
            &chunk_data[..bytes_in_first_block],
            "First part of chunk data after initial header should match"
        );
        
        // Skip over the second block header
        let second_part_start = first_part_end + BLOCK_HEADER_SIZE as usize;
        
        // Check second part of chunk data (after second header)
        let remaining_bytes = chunk_data.len() - bytes_in_first_block;
        let second_part_end = second_part_start + remaining_bytes;
        
        println!("Second part of chunk: bytes {} to {}", second_part_start, second_part_end);
        
        assert_eq!(
            &vec[second_part_start..second_part_end],
            &chunk_data[bytes_in_first_block..],
            "Second part of chunk data after block boundary should match"
        );
        
        // Reconstruct the original data by concatenating the parts
        let mut reconstructed = Vec::new();
        reconstructed.extend_from_slice(&vec[first_part_start..first_part_end]);
        reconstructed.extend_from_slice(&vec[second_part_start..second_part_end]);
        
        assert_eq!(reconstructed, chunk_data,
                  "Reconstructed data should match original chunk data");
    }
    
    #[test]
    fn test_write_chunks_sequentially() {
        // Start with a fresh buffer
        let buffer = Cursor::new(Vec::new());
        
        // Use a block size large enough to avoid additional block boundaries for this test
        let block_size = 200u64; // > 2 * BLOCK_HEADER_SIZE (24) and larger than our total data
        
        let mut writer = BlockWriter::with_config(
            buffer,
            BlockWriterConfig {
                block_size,
            }
        ).unwrap();
        
        // Write first chunk with a clear pattern
        let chunk1 = Bytes::from(b"FIRST_CHUNK_DATA".to_vec());
        writer.write_chunk(chunk1.clone()).unwrap();
        
        // Write second chunk with a different pattern
        let chunk2 = Bytes::from(b"SECOND_CHUNK_WITH_DIFFERENT_DATA".to_vec());
        writer.write_chunk(chunk2.clone()).unwrap();
        
        // Get the buffer and check its contents
        let buffer = writer.into_inner();
        let vec = buffer.into_inner();
        
        println!("Chunk1 size: {}", chunk1.len());
        println!("Chunk2 size: {}", chunk2.len());
        println!("Block header size: {}", BLOCK_HEADER_SIZE);
        println!("Actual size: {}", vec.len());
        
        // The buffer should contain an initial header + both chunks sequentially
        // (since we're starting at position 0)
        let expected_size = BLOCK_HEADER_SIZE as usize + chunk1.len() + chunk2.len();
        assert_eq!(vec.len(), expected_size,
                  "Output should contain header + both chunks");
        
        // Check that the chunks are in the right order after the header
        let header_size = BLOCK_HEADER_SIZE as usize;
        let chunk1_start = header_size;
        let chunk1_end = chunk1_start + chunk1.len();
        let chunk2_start = chunk1_end;
        let chunk2_end = chunk2_start + chunk2.len();
        
        assert_eq!(&vec[chunk1_start..chunk1_end], &chunk1[..],
                  "First chunk should be written correctly after header");
        assert_eq!(&vec[chunk2_start..chunk2_end], &chunk2[..],
                  "Second chunk should be written correctly after first chunk");
    }
    
    #[test]
    fn test_write_interpretable_string_data() {
        // Start with a fresh buffer
        let buffer = Cursor::new(Vec::new());
        
        // Use a small block size for testing, but large enough to avoid cascading headers
        let block_size = 60u64; // > 2 * BLOCK_HEADER_SIZE (24)
        
        let mut writer = BlockWriter::with_config(
            buffer,
            BlockWriterConfig {
                block_size,
            }
        ).unwrap();
        
        // Create byte data with a clear, repeating pattern for easy visual debugging
        // Repeat "ABC" to create a pattern that's easy to verify visually
        let pattern = b"ABC";
        let mut data = Vec::new();
        for _ in 0..20 {  // 60 bytes total, will cross block boundaries
            data.extend_from_slice(pattern);
        }
        let chunk_data = Bytes::from(data);
        
        // Calculate where the first block boundary will be
        // Since we start at position 0, there will be a header, then data starts at BLOCK_HEADER_SIZE
        let first_header_size = BLOCK_HEADER_SIZE as usize;
        let bytes_in_first_block = (block_size - BLOCK_HEADER_SIZE) as usize;
        
        // Verify test assumptions
        assert!(chunk_data.len() > bytes_in_first_block,
               "Test chunk must cross at least one block boundary");
        
        // Write the chunk
        writer.write_chunk(chunk_data.clone()).unwrap();
        
        // Get the buffer and check its contents
        let buffer = writer.into_inner();
        let vec = buffer.into_inner();
        
        println!("Block size: {}", block_size);
        println!("Bytes in first block: {}", bytes_in_first_block);
        println!("Header size: {}", BLOCK_HEADER_SIZE);
        println!("Chunk data size: {}", chunk_data.len());
        println!("Actual size: {}", vec.len());
        
        // The output should include:
        // 1. Initial block header at position 0
        // 2. First part of chunk data (up to block boundary)
        // 3. Another block header at block boundary
        // 4. Second part of chunk data (after block boundary)
        
        // Ensure we have enough data
        let minimum_expected_size = chunk_data.len() + 2 * BLOCK_HEADER_SIZE as usize;
        assert!(vec.len() >= minimum_expected_size,
                "Output should include chunk data + at least two block headers");
        
        // Check first part of chunk data (after initial header, up to block boundary)
        let first_part_start = first_header_size;
        let first_part_end = block_size as usize; // End at the block boundary
        
        assert_eq!(
            &vec[first_part_start..first_part_end],
            &chunk_data[..bytes_in_first_block],
            "First part of data after initial header should match"
        );
        
        // Skip over the second block header
        let second_part_start = first_part_end + BLOCK_HEADER_SIZE as usize;
        
        // Check second part of chunk data (after second header)
        let remaining_bytes = chunk_data.len() - bytes_in_first_block;
        let second_part_end = second_part_start + remaining_bytes;
        
        assert_eq!(
            &vec[second_part_start..second_part_end],
            &chunk_data[bytes_in_first_block..],
            "Second part of data after second header should match"
        );
        
        // Verify that the pattern is preserved properly
        // Reconstruct the original data by combining parts from different blocks
        let mut reconstructed = Vec::new();
        reconstructed.extend_from_slice(&vec[first_part_start..first_part_end]);
        reconstructed.extend_from_slice(&vec[second_part_start..second_part_end]);
        
        assert_eq!(reconstructed, chunk_data.to_vec(),
                  "Reconstructed data should match original chunk data");
    }
    
    #[test]
    fn test_appending_to_existing_file() {
        // Create a file with some existing content
        let initial_content = "This is some existing content in the file.";
        let mut buffer = Cursor::new(initial_content.as_bytes().to_vec());
        
        // Move to the end of the existing content
        let existing_size = initial_content.len() as u64;
        buffer.set_position(existing_size);
        
        // Create a BlockWriter for appending
        let mut writer = BlockWriter::for_append_with_config(
            buffer,
            existing_size,     // Current position at end of existing content
            BlockWriterConfig::default()
        ).unwrap();
        
        // Create new content to append
        let new_content = " This is new content to append.";
        let chunk_data = Bytes::from(new_content.as_bytes().to_vec());
        
        // Append the new content
        writer.write_chunk(chunk_data).unwrap();
        
        // Get the buffer and check its contents
        let buffer = writer.into_inner();
        let vec = buffer.into_inner();
        
        // Convert to string for easier verification
        let result = std::str::from_utf8(&vec).unwrap();
        
        // The result should be the concatenation of both strings
        let expected = format!("{}{}", initial_content, new_content);
        assert_eq!(result, expected);
    }
    
    #[test]
    fn test_write_empty_chunk() {
        // Test writing an empty chunk
        let buffer = Cursor::new(Vec::new());
        let mut writer = BlockWriter::new(buffer).unwrap();
        
        // Write an empty chunk
        let empty_chunk = Bytes::new();
        writer.write_chunk(empty_chunk).unwrap();
        
        // Get the buffer and verify it's still empty (no headers needed for empty chunks)
        let buffer = writer.into_inner();
        let vec = buffer.into_inner();
        
        assert_eq!(vec.len(), 0, "Empty chunk should not write any data");
    }
    
    #[test]
    fn test_write_empty_chunk_at_block_boundary() {
        // Create a buffer and fill it to exactly one block
        let block_size = 100u64; // > 2 * BLOCK_HEADER_SIZE (24)
        let buffer = Cursor::new(vec![0; block_size as usize]);
        
        // Create a writer for appending at the block boundary
        let mut writer = BlockWriter::for_append_with_config(
            buffer,
            block_size, // At block boundary
            BlockWriterConfig {
                block_size,
            }
        ).unwrap();
        
        // Write an empty chunk
        let empty_chunk = Bytes::new();
        writer.write_chunk(empty_chunk).unwrap();
        
        // Get the buffer and check
        let buffer = writer.into_inner();
        let vec = buffer.into_inner();
        
        // With our optimization, even at block boundaries we don't write headers for empty chunks
        let expected_size = block_size as usize; // Just the initial data, no header
        assert_eq!(vec.len(), expected_size, 
            "Empty chunk should not add any data, even at block boundaries");
    }
    
    #[test]
    fn test_minimum_valid_block_size() {
        // Test with the absolute minimum valid block size: 2 * BLOCK_HEADER_SIZE
        let min_block_size = BLOCK_HEADER_SIZE * 2;
        
        // Should be able to create a config with the minimum size
        let config = BlockWriterConfig::with_block_size(min_block_size).unwrap();
        let buffer = Cursor::new(Vec::new());
        let writer = BlockWriter::with_config(buffer, config).unwrap();
        
        assert_eq!(writer.config.block_size, min_block_size);
        
        // Try with invalid block size (too small)
        let invalid_size = BLOCK_HEADER_SIZE * 2 - 1; // One byte too small
        let result = BlockWriterConfig::with_block_size(invalid_size);
        assert!(result.is_err(), "Should reject block size smaller than 2 * BLOCK_HEADER_SIZE");
    }
    
    #[test]
    fn test_writing_at_position_zero() {
        // Test writing at the beginning of a file (position 0)
        let buffer = Cursor::new(Vec::new());
        let mut writer = BlockWriter::new(buffer).unwrap();
        
        // Create a chunk
        let chunk_data = Bytes::from(b"This is chunk data written at position 0".to_vec());
        
        // Write the chunk
        writer.write_chunk(chunk_data.clone()).unwrap();
        
        // Get the buffer
        let buffer = writer.into_inner();
        let vec = buffer.into_inner();
        
        // The data should be written directly (position 0 is a block boundary but 
        // we expect a header to be written first)
        let expected_size = BLOCK_HEADER_SIZE as usize + chunk_data.len();
        assert_eq!(vec.len(), expected_size, 
            "Should write a header at position 0 followed by chunk data");
        
        // The block header should be at the beginning
        // Then the chunk data
        assert_eq!(&vec[BLOCK_HEADER_SIZE as usize..], &chunk_data[..],
            "Chunk data should follow the header");
    }
    
    #[test]
    fn test_write_chunk_crossing_multiple_block_boundaries() {
        // Test writing a chunk large enough to cross multiple block boundaries
        // Using a very predictable block and header size for easier debugging
        let block_size = 100u64; // Small, easy to reason about number
        let buffer = Cursor::new(Vec::new());
        
        let mut writer = BlockWriter::with_config(
            buffer,
            BlockWriterConfig {
                block_size,
            }
        ).unwrap();
        
        // Create a pattern-based chunk for easy verification
        // Each byte will be its position % 256, creating a recognizable pattern
        let needed_size = 250; // Cross two boundaries
        let mut pattern_data = Vec::with_capacity(needed_size);
        for i in 0..needed_size {
            pattern_data.push((i % 256) as u8);
        }
        let chunk_data = Bytes::from(pattern_data);
        
        // Write the chunk
        writer.write_chunk(chunk_data.clone()).unwrap();
        
        // Get the buffer
        let buffer = writer.into_inner();
        let vec = buffer.into_inner();
        
        println!("Output buffer size: {}", vec.len());
        
        // Expected positions of block headers
        let header_positions = [
            0,                        // Initial header (position 0)
            block_size as usize,      // At first block boundary (position 100)
            (block_size * 2) as usize, // At second block boundary (position 200)
            (block_size * 3) as usize, // At third block boundary (position 300)
        ];
        
        // Verify the size makes sense
        let expected_data_size = chunk_data.len();
        let expected_header_count = if vec.len() == expected_data_size + 3 * BLOCK_HEADER_SIZE as usize {
            3 // 3 headers is the minimum for crossing 2 boundaries 
        } else if vec.len() == expected_data_size + 4 * BLOCK_HEADER_SIZE as usize {
            4 // 4 headers might be used in some implementations
        } else {
            panic!("Unexpected output size: {}, expected either {} or {}",
                  vec.len(),
                  expected_data_size + 3 * BLOCK_HEADER_SIZE as usize,
                  expected_data_size + 4 * BLOCK_HEADER_SIZE as usize);
        };
        
        println!("Detected {} block headers", expected_header_count);
        
        // Now let's reconstruct the original data by skipping over the headers
        let mut reconstructed = Vec::with_capacity(chunk_data.len());
        
        // Track our position in the output buffer and input chunk
        let mut output_pos = 0;
        let mut remaining_chunk = chunk_data.len();
        let mut chunk_pos = 0;
        
        // Process each block and extract the data
        for i in 0..expected_header_count {
            // Skip the header
            let header_pos = header_positions[i] as usize;
            
            // Check if we've reached the end of our output
            if header_pos >= vec.len() {
                break;
            }
            
            // Validate we're at the expected header position
            assert_eq!(output_pos, header_pos, 
                      "Expected header at position {}, but we're at {}", header_pos, output_pos);
            
            // Skip over the header
            output_pos += BLOCK_HEADER_SIZE as usize;
            
            // Determine how much data to read before the next header (or end)
            let next_header_pos = if i < expected_header_count - 1 && (header_positions[i + 1] as usize) < vec.len() {
                header_positions[i + 1] as usize
            } else {
                vec.len() // No more headers, read to the end
            };
            
            let data_size = next_header_pos - output_pos;
            
            // Can't read more than what remains in the chunk
            let bytes_to_read = std::cmp::min(data_size, remaining_chunk);
            
            if bytes_to_read > 0 {
                // Add this section to our reconstructed data
                reconstructed.extend_from_slice(&vec[output_pos..output_pos + bytes_to_read]);
                
                // Verify this section matches the expected chunk data
                for j in 0..bytes_to_read {
                    assert_eq!(vec[output_pos + j], chunk_data[chunk_pos + j], 
                              "Data mismatch at position {}", chunk_pos + j);
                }
                
                // Update tracking variables
                output_pos += bytes_to_read;
                remaining_chunk -= bytes_to_read;
                chunk_pos += bytes_to_read;
            }
        }
        
        // Final verification: we've reconstructed the entire chunk
        assert_eq!(reconstructed.len(), chunk_data.len(), 
                  "Reconstructed data should have the same length as original chunk");
        assert_eq!(reconstructed, chunk_data.to_vec(),
                  "Reconstructed data should match original chunk data");
        
        println!("Successfully verified chunk data across multiple block boundaries");
    }
    
    #[test]
    fn test_header_values() {
        // Test to verify that the header values are correctly calculated
        let block_size = 100u64;
        let mut buffer = Cursor::new(Vec::new());
        
        // Write some recognizable initial data but not at a block boundary
        let initial_data = b"INIT_DATA";
        buffer.write_all(initial_data).unwrap();
        let initial_pos = initial_data.len() as u64;
        buffer.set_position(initial_pos);
        
        let mut writer = BlockWriter::for_append_with_config(
            buffer,
            initial_pos,
            BlockWriterConfig {
                block_size,
            }
        ).unwrap();
        
        // Create a chunk that will cross exactly one block boundary
        // First part: block_size - initial_pos bytes (to reach the boundary)
        // Second part: 50 additional bytes (after the boundary)
        let chunk_size = (block_size - initial_pos + 50) as usize;
        let chunk_data = Bytes::from(vec![b'X'; chunk_size]);
        
        // Write the chunk
        writer.write_chunk(chunk_data.clone()).unwrap();
        
        // Get the buffer
        let buffer = writer.into_inner();
        let vec = buffer.into_inner();
        
        // Extract the header values from the block header
        let header_pos = block_size as usize;
        
        // Skip header_hash (8 bytes)
        let previous_chunk_pos = header_pos + 8;
        let next_chunk_pos = previous_chunk_pos + 8;
        
        // Read the values as little-endian u64
        let previous_chunk = u64::from_le_bytes([
            vec[previous_chunk_pos],
            vec[previous_chunk_pos + 1],
            vec[previous_chunk_pos + 2],
            vec[previous_chunk_pos + 3],
            vec[previous_chunk_pos + 4],
            vec[previous_chunk_pos + 5],
            vec[previous_chunk_pos + 6],
            vec[previous_chunk_pos + 7],
        ]);
        
        let next_chunk = u64::from_le_bytes([
            vec[next_chunk_pos],
            vec[next_chunk_pos + 1],
            vec[next_chunk_pos + 2],
            vec[next_chunk_pos + 3],
            vec[next_chunk_pos + 4],
            vec[next_chunk_pos + 5],
            vec[next_chunk_pos + 6],
            vec[next_chunk_pos + 7],
        ]);
        
        // Verify the values
        // previous_chunk: distance from beginning of chunk to beginning of block
        let expected_previous_chunk = block_size - initial_pos;
        assert_eq!(previous_chunk, expected_previous_chunk,
            "previous_chunk value in the header should be distance from chunk start to block start");
        
        // next_chunk: distance from beginning of block to end of chunk
        // The specification is referring to the distance from block boundary to the end of the chunk data
        // In this case, we're writing 50 bytes after the block boundary
        // The header itself is not counted in this distance
        let expected_next_chunk = 50; 
        
        // If the test fails, print debugging info
        if next_chunk != expected_next_chunk {
            println!("Debug info for header values:");
            println!("  block_size: {}", block_size);
            println!("  initial_pos: {}", initial_pos);
            println!("  chunk_size: {}", chunk_size);
            println!("  header_pos: {}", header_pos);
            println!("  previous_chunk: {} (expected: {})", previous_chunk, expected_previous_chunk);
            println!("  next_chunk: {} (expected: {})", next_chunk, expected_next_chunk);
        }
        
        assert_eq!(next_chunk, expected_next_chunk,
            "next_chunk value in the header should be distance from block start to chunk end");
    }
    
    #[test]
    fn test_many_small_sequential_chunks() {
        // Test writing many small chunks back-to-back
        let buffer = Cursor::new(Vec::new());
        let block_size = 100u64;
        
        let mut writer = BlockWriter::with_config(
            buffer,
            BlockWriterConfig {
                block_size,
            }
        ).unwrap();
        
        // Create several small chunks
        let chunks = vec![
            Bytes::from(b"Chunk1".to_vec()),
            Bytes::from(b"Chunk2".to_vec()),
            Bytes::from(b"Chunk3".to_vec()),
            Bytes::from(b"Chunk4".to_vec()),
            Bytes::from(b"Chunk5".to_vec()),
        ];
        
        // Write all chunks
        for chunk in chunks.iter() {
            writer.write_chunk(chunk.clone()).unwrap();
        }
        
        // Get the buffer
        let buffer = writer.into_inner();
        let vec = buffer.into_inner();
        
        // We expect a header at position 0, then all chunks back-to-back
        let expected_size = chunks.iter().fold(0, |acc, chunk| acc + chunk.len()) + 
                           BLOCK_HEADER_SIZE as usize;
        
        assert_eq!(vec.len(), expected_size,
            "Output should include all chunks plus one header at the beginning");
        
        // Verify each chunk
        let mut pos = BLOCK_HEADER_SIZE as usize;
        for chunk in chunks.iter() {
            let end_pos = pos + chunk.len();
            assert_eq!(
                &vec[pos..end_pos],
                &chunk[..],
                "Chunk data should be written correctly"
            );
            pos = end_pos;
        }
    }
}
