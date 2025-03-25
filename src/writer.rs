// Copyright 2024
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Writer for Riegeli format files.

use std::io::{Write, Seek, SeekFrom};
use byteorder::{LittleEndian, WriteBytesExt};
use bytes::{BytesMut, BufMut};

use crate::constants::{
    BLOCK_SIZE, BLOCK_HEADER_SIZE, CHUNK_HEADER_SIZE,
    CHUNK_TYPE_SIMPLE_RECORDS, COMPRESSION_TYPE_NONE, FILE_SIGNATURE,
};
use crate::error::{Result, RiegeliError};
use crate::hash::highway_hash;
use crate::record_position::RecordPosition;

/// Options for record writer.
#[derive(Debug, Clone)]
pub struct RecordWriterOptions {
    /// Whether to transpose records for better compression.
    pub transpose: bool,
    
    /// Compression type to use.
    pub compression_type: u8,
    
    /// Chunk size in bytes.
    pub chunk_size: u64,
    
    /// Whether to pad to block boundary.
    pub pad_to_block_boundary: bool,
}

impl Default for RecordWriterOptions {
    fn default() -> Self {
        Self {
            transpose: false,
            compression_type: COMPRESSION_TYPE_NONE, // No compression by default for simplicity
            chunk_size: 1 << 20, // 1 MB
            pad_to_block_boundary: false,
        }
    }
}

/// A writer for Riegeli format files.
pub struct RecordWriter<W: Write + Seek> {
    /// The underlying writer.
    pub(crate) writer: W,
    
    /// Options for the writer.
    options: RecordWriterOptions,
    
    /// Current chunk size accumulated so far.
    pub(crate) chunk_size_so_far: u64,
    
    /// Buffer for records in the current chunk.
    records_buffer: BytesMut,
    
    /// Buffer for record sizes in the current chunk.
    sizes_buffer: BytesMut,
    
    /// Number of records in the current chunk.
    num_records: u64,
    
    /// Position of the beginning of the current chunk.
    chunk_begin: u64,
    
    /// Whether the file signature has been written.
    signature_written: bool,
    
    /// Whether the writer has been closed.
    closed: bool,
}

impl<W: Write + Seek> RecordWriter<W> {
    /// Creates a new RecordWriter with default options.
    pub fn new(writer: W) -> Result<Self> {
        Self::with_options(writer, RecordWriterOptions::default())
    }

    /// Creates a new RecordWriter with the specified options.
    pub fn with_options(writer: W, options: RecordWriterOptions) -> Result<Self> {
        let mut record_writer = Self {
            writer,
            options,
            chunk_size_so_far: 0,
            records_buffer: BytesMut::new(),
            sizes_buffer: BytesMut::new(),
            num_records: 0,
            chunk_begin: 0,
            signature_written: false,
            closed: false,
        };
        
        // Write the file signature
        record_writer.write_file_signature()?;
        
        Ok(record_writer)
    }

    /// Writes the file signature (first chunk of a Riegeli file).
    fn write_file_signature(&mut self) -> Result<()> {
        if self.signature_written {
            return Ok(());
        }
        
        self.writer.write_all(&FILE_SIGNATURE)?;
        self.signature_written = true;
        self.chunk_begin = self.writer.stream_position()?;
        
        Ok(())
    }

    /// Writes a record to the file.
    pub fn write_record(&mut self, record: &[u8]) -> Result<RecordPosition> {
        if self.closed {
            return Err(RiegeliError::Other("Writer is closed".to_string()));
        }
        
        // Store the record's size
        let record_size = record.len() as u64;
        
        // Add to size buffer (using simple varint encoding for now)
        Self::write_varint_to_buf(record_size, &mut self.sizes_buffer);
        
        // Add to records buffer
        self.records_buffer.extend_from_slice(record);
        
        // Update chunk state
        self.chunk_size_so_far += record_size;
        self.num_records += 1;
        
        // If the chunk is full, write it
        if self.chunk_size_so_far >= self.options.chunk_size {
            self.flush_chunk()?;
        }
        
        // Return the record position
        if self.num_records > 0 {
            Ok(RecordPosition::new(self.chunk_begin, self.num_records - 1))
        } else {
            Ok(RecordPosition::new(self.chunk_begin, 0))
        }
    }

    /// Writes a simple varint encoding to the buffer.
    fn write_varint_to_buf(value: u64, buffer: &mut BytesMut) {
        let mut val = value;
        while val >= 0x80 {
            buffer.put_u8((val & 0x7F) as u8 | 0x80);
            val >>= 7;
        }
        buffer.put_u8(val as u8);
    }

    /// Flushes the current chunk to the writer.
    fn flush_chunk(&mut self) -> Result<()> {
        if self.num_records == 0 {
            return Ok(());
        }
        
        // Calculate total size of record values
        let decoded_data_size = self.records_buffer.len() as u64;
        
        // Prepare data buffer with compression type and sizes
        let mut data = BytesMut::new();
        
        // Compression type (none for now)
        data.put_u8(COMPRESSION_TYPE_NONE);
        
        // Sizes buffer size as varint
        Self::write_varint_to_buf(self.sizes_buffer.len() as u64, &mut data);
        
        // Append sizes and records
        data.extend_from_slice(&self.sizes_buffer);
        data.extend_from_slice(&self.records_buffer);
        
        // Write the chunk
        self.write_chunk(CHUNK_TYPE_SIMPLE_RECORDS, &data, self.num_records, decoded_data_size)?;
        
        // Reset chunk state
        self.chunk_size_so_far = 0;
        self.records_buffer.clear();
        self.sizes_buffer.clear();
        self.num_records = 0;
        self.chunk_begin = self.writer.stream_position()?;
        
        Ok(())
    }

    /// Writes a chunk with the specified type, data, number of records, and decoded data size.
    fn write_chunk(&mut self, chunk_type: u8, data: &[u8], num_records: u64, decoded_data_size: u64) -> Result<()> {
        let data_size = data.len() as u64;
        
        // Calculate the position in the file
        let current_pos = self.writer.stream_position()?;
        
        // Check if we need to write a block header
        let block_index = current_pos / BLOCK_SIZE as u64;
        let next_block_pos = (block_index + 1) * BLOCK_SIZE as u64;
        
        // If the chunk header would cross a block boundary, write a block header
        if current_pos < next_block_pos && current_pos + CHUNK_HEADER_SIZE as u64 > next_block_pos {
            self.write_block_header(current_pos, next_block_pos)?;
        }
        
        // Calculate the data hash
        let data_hash = highway_hash(data);
        
        // Build the chunk header
        let mut header = BytesMut::with_capacity(CHUNK_HEADER_SIZE);
        
        // Skip the header hash for now
        header.put_u64_le(0); // Placeholder for header_hash
        
        // Write the rest of the header
        header.put_u64_le(data_size);
        header.put_u64_le(data_hash);
        header.put_u8(chunk_type);
        
        // Write num_records (7 bytes)
        let mut num_records_bytes = [0u8; 8];
        WriteBytesExt::write_u64::<LittleEndian>(&mut num_records_bytes.as_mut(), num_records)?;
        header.extend_from_slice(&num_records_bytes[0..7]);
        
        header.put_u64_le(decoded_data_size);
        
        // Calculate the header hash (excluding the first 8 bytes)
        let header_hash = highway_hash(&header[8..]);
        
        // Write the header hash
        let mut final_header = BytesMut::with_capacity(CHUNK_HEADER_SIZE);
        final_header.put_u64_le(header_hash);
        final_header.extend_from_slice(&header[8..]);
        
        // Write the header and data
        self.writer.write_all(&final_header)?;
        self.writer.write_all(data)?;
        
        // Add padding to satisfy chunk constraints
        if self.options.pad_to_block_boundary {
            let current_pos = self.writer.stream_position()?;
            let block_index = current_pos / BLOCK_SIZE as u64;
            let next_block_pos = (block_index + 1) * BLOCK_SIZE as u64;
            
            if current_pos < next_block_pos {
                let padding_size = (next_block_pos - current_pos) as usize;
                let padding = vec![0u8; padding_size];
                self.writer.write_all(&padding)?;
            }
        }
        
        Ok(())
    }

    /// Writes a block header at the current position.
    fn write_block_header(&mut self, block_begin: u64, next_block_pos: u64) -> Result<()> {
        // Calculate the header fields
        let previous_chunk = block_begin - self.chunk_begin;
        
        // Estimate the chunk end (simple estimate for now)
        let chunk_size_estimate = self.chunk_size_so_far + CHUNK_HEADER_SIZE as u64;
        let chunk_end = self.chunk_begin + chunk_size_estimate;
        let next_chunk = chunk_end - block_begin;
        
        // Build the header
        let mut header = BytesMut::with_capacity(BLOCK_HEADER_SIZE);
        
        // Skip the header hash for now
        header.put_u64_le(0); // Placeholder for header_hash
        
        // Write the rest of the header
        header.put_u64_le(previous_chunk);
        header.put_u64_le(next_chunk);
        
        // Calculate the header hash (excluding the first 8 bytes)
        let header_hash = highway_hash(&header[8..]);
        
        // Write the header hash
        let mut final_header = BytesMut::with_capacity(BLOCK_HEADER_SIZE);
        final_header.put_u64_le(header_hash);
        final_header.extend_from_slice(&header[8..]);
        
        // Seek to the block boundary
        self.writer.seek(SeekFrom::Start(next_block_pos - BLOCK_HEADER_SIZE as u64))?;
        
        // Write the header
        self.writer.write_all(&final_header)?;
        
        Ok(())
    }

    /// Returns the position of the next record.
    pub fn pos(&mut self) -> Result<RecordPosition> {
        Ok(RecordPosition::new(self.chunk_begin, self.num_records))
    }

    /// Flushes any buffered records.
    pub fn flush(&mut self) -> Result<()> {
        self.flush_chunk()?;
        self.writer.flush()?;
        Ok(())
    }

    /// Closes the writer.
    pub fn close(&mut self) -> Result<()> {
        if !self.closed {
            self.flush()?;
            self.closed = true;
        }
        Ok(())
    }
    
    /// Gets the inner writer. This is primarily used for testing.
    #[cfg(test)]
    pub fn get_data(&mut self) -> Result<Vec<u8>> 
    where
        W: std::io::Read,
    {
        self.flush()?;
        let pos = self.writer.stream_position()?;
        self.writer.seek(SeekFrom::Start(0))?;
        let mut data = Vec::new();
        self.writer.read_to_end(&mut data)?;
        self.writer.seek(SeekFrom::Start(pos))?;
        Ok(data)
    }
}

impl<W: Write + Seek> Drop for RecordWriter<W> {
    fn drop(&mut self) {
        let _ = self.close();
    }
}