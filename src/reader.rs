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

//! Reader for Riegeli format files.

use std::io::{self, Cursor, Read, Seek, SeekFrom};
use byteorder::{LittleEndian, ReadBytesExt};
use bytes::{Bytes, Buf};

use crate::constants::{
    BLOCK_SIZE, BLOCK_HEADER_SIZE, CHUNK_HEADER_SIZE, CHUNK_TYPE_FILE_SIGNATURE,
    CHUNK_TYPE_SIMPLE_RECORDS, COMPRESSION_TYPE_NONE, FILE_SIGNATURE,
};
use crate::error::{Result, RiegeliError};
use crate::hash::highway_hash;
use crate::record_position::RecordPosition;

/// Options for record reader.
#[derive(Debug, Clone)]
pub struct RecordReaderOptions {
    /// Whether to recover from corruption.
    pub recover_corruption: bool,
}

impl Default for RecordReaderOptions {
    fn default() -> Self {
        Self {
            recover_corruption: false,
        }
    }
}

/// A reader for Riegeli format files.
#[derive(Debug)]
pub struct RecordReader<R: Read + Seek> {
    /// The underlying reader.
    reader: R,
    
    /// Options for the reader.
    options: RecordReaderOptions,
    
    /// Position of the beginning of the current chunk.
    chunk_begin: u64,
    
    /// Current position within the chunk (record index).
    record_index: u64,
    
    /// Total number of records in the current chunk.
    num_records: u64,
    
    /// Decoded record sizes for the current chunk.
    record_sizes: Vec<u64>,
    
    /// Decoded record data for the current chunk.
    record_data: Bytes,
    
    /// Current offset in the record data.
    data_offset: usize,
    
    /// Whether the file signature has been verified.
    signature_verified: bool,
    
    /// Whether the reader has been closed.
    closed: bool,
}

impl<R: Read + Seek> RecordReader<R> {
    /// Creates a new RecordReader with default options.
    pub fn new(reader: R) -> Result<Self> {
        Self::with_options(reader, RecordReaderOptions::default())
    }

    /// Creates a new RecordReader with the specified options.
    pub fn with_options(reader: R, options: RecordReaderOptions) -> Result<Self> {
        let mut record_reader = Self {
            reader,
            options,
            chunk_begin: 0,
            record_index: 0,
            num_records: 0,
            record_sizes: Vec::new(),
            record_data: Bytes::new(),
            data_offset: 0,
            signature_verified: false,
            closed: false,
        };
        
        // Verify the file signature
        record_reader.verify_file_signature()?;
        
        Ok(record_reader)
    }

    /// Verifies the file signature.
    fn verify_file_signature(&mut self) -> Result<()> {
        if self.signature_verified {
            return Ok(());
        }
        
        let mut signature = [0u8; 64];
        
        match self.reader.read_exact(&mut signature) {
            Ok(_) => {
                if signature != FILE_SIGNATURE {
                    return Err(RiegeliError::InvalidFileSignature);
                }
            },
            Err(e) => {
                // If recovery is enabled, we'll try to continue anyway
                if self.options.recover_corruption {
                    // Just pretend we read the signature
                    self.reader.seek(SeekFrom::Start(64))?;
                } else {
                    return Err(RiegeliError::Io(e));
                }
            }
        }
        
        self.signature_verified = true;
        self.chunk_begin = self.reader.stream_position()?;
        
        Ok(())
    }

    /// Reads the next record from the file.
    pub fn read_record(&mut self) -> Result<Bytes> {
        if self.closed {
            return Err(RiegeliError::Other("Reader is closed".to_string()));
        }
        
        // If we've read all records in the current chunk (or no chunk is loaded),
        // read the next chunk
        if self.record_index >= self.num_records {
            self.read_chunk()?;
        }
        
        // If we still don't have records, we've reached the end of the file
        if self.record_index >= self.num_records {
            return Err(RiegeliError::UnexpectedEof);
        }
        
        // Get the size of the current record
        let record_size = self.record_sizes[self.record_index as usize];
        
        // Extract the record data
        let record_end = self.data_offset + record_size as usize;
        if record_end > self.record_data.len() {
            return Err(RiegeliError::Corruption("Record extends beyond chunk data".to_string()));
        }
        
        let record = self.record_data.slice(self.data_offset..record_end);
        self.data_offset = record_end;
        
        // Move to the next record
        self.record_index += 1;
        
        Ok(record)
    }

    /// Reads a chunk from the file.
    fn read_chunk(&mut self) -> Result<()> {
        // Reset chunk state
        self.record_sizes.clear();
        self.record_data = Bytes::new();
        self.data_offset = 0;
        self.record_index = 0;
        self.num_records = 0;
        
        // Remember the chunk begin position
        self.chunk_begin = self.reader.stream_position()?;
        
        // Read until we find a valid chunk
        let mut retry_count = 0;
        let max_retries = 10; // Limit retries to avoid infinite loops
        
        loop {
            // Try to read the chunk header
            let header_result = self.read_chunk_header();
            
            if let Err(e) = header_result {
                // If we're at EOF, return Ok with no records
                if let RiegeliError::UnexpectedEof = e {
                    return Ok(());
                }
                
                // If we should recover from corruption, skip ahead and try again
                if self.options.recover_corruption {
                    retry_count += 1;
                    if retry_count > max_retries {
                        // Too many retries, give up
                        return Ok(());
                    }
                    
                    // Get the current position and try to skip ahead
                    let current_pos = self.reader.stream_position()?;
                    
                    // First, try to skip to the next block boundary
                    let next_block_pos = (current_pos / BLOCK_SIZE as u64 + 1) * BLOCK_SIZE as u64;
                    
                    // Check if we can seek that far
                    match self.reader.seek(SeekFrom::End(0)) {
                        Ok(end_pos) => {
                            if next_block_pos >= end_pos {
                                // We've reached the end of the file, stop reading
                                return Ok(());
                            }
                            
                            // We can seek to the next block
                            self.reader.seek(SeekFrom::Start(next_block_pos))?;
                            self.chunk_begin = next_block_pos;
                            continue;
                        }
                        Err(_) => {
                            // Just skip some bytes and try again
                            match self.reader.seek(SeekFrom::Current(64)) {
                                Ok(new_pos) => {
                                    self.chunk_begin = new_pos;
                                    continue;
                                }
                                Err(_) => {
                                    // Can't seek forward, we're likely at the end
                                    return Ok(());
                                }
                            }
                        }
                    }
                }
                
                return Err(e);
            }
            
            // We have a valid chunk header
            let (chunk_type, data_size, data_hash, num_records, decoded_data_size) = header_result.unwrap();
            
            // Skip file signature chunks
            if chunk_type == CHUNK_TYPE_FILE_SIGNATURE {
                self.reader.seek(SeekFrom::Current(data_size as i64))?;
                self.chunk_begin = self.reader.stream_position()?;
                continue;
            }
            
            // We only support simple records for now
            if chunk_type != CHUNK_TYPE_SIMPLE_RECORDS {
                // Skip this chunk
                self.reader.seek(SeekFrom::Current(data_size as i64))?;
                self.chunk_begin = self.reader.stream_position()?;
                continue;
            }
            
            // Try to read the chunk data
            match self.read_chunk_data(data_size, data_hash) {
                Ok(chunk_data) => {
                    // Try to parse simple record format
                    match self.parse_simple_records(chunk_data, num_records, decoded_data_size) {
                        Ok(_) => {
                            // Successfully read a chunk with records
                            break;
                        }
                        Err(_) if self.options.recover_corruption => {
                            // Corrupted data, try to skip ahead
                            retry_count += 1;
                            if retry_count > max_retries {
                                return Ok(());
                            }
                            
                            // Skip to the next block boundary
                            let current_pos = self.reader.stream_position()?;
                            let next_block_pos = (current_pos / BLOCK_SIZE as u64 + 1) * BLOCK_SIZE as u64;
                            
                            if let Ok(end_pos) = self.reader.seek(SeekFrom::End(0)) {
                                if next_block_pos >= end_pos {
                                    return Ok(());
                                }
                                self.reader.seek(SeekFrom::Start(next_block_pos))?;
                                self.chunk_begin = next_block_pos;
                                continue;
                            } else {
                                // Just try to skip ahead a bit
                                self.reader.seek(SeekFrom::Current(64))?;
                                self.chunk_begin = self.reader.stream_position()?;
                                continue;
                            }
                        }
                        Err(e) => return Err(e),
                    }
                }
                Err(_) if self.options.recover_corruption => {
                    // Corrupted data, try to skip ahead
                    retry_count += 1;
                    if retry_count > max_retries {
                        return Ok(());
                    }
                    
                    // Skip to after this chunk
                    self.reader.seek(SeekFrom::Current(data_size as i64))?;
                    self.chunk_begin = self.reader.stream_position()?;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        
        Ok(())
    }

    /// Reads a chunk header.
    fn read_chunk_header(&mut self) -> Result<(u8, u64, u64, u64, u64)> {
        // Check if we need to skip a block header
        let current_pos = self.reader.stream_position()?;
        let block_index = current_pos / BLOCK_SIZE as u64;
        let block_pos = block_index * BLOCK_SIZE as u64;
        
        if current_pos == block_pos + BLOCK_SIZE as u64 - BLOCK_HEADER_SIZE as u64 {
            // Skip the block header
            self.reader.seek(SeekFrom::Current(BLOCK_HEADER_SIZE as i64))?;
        }
        
        // Read the chunk header
        let mut header = [0u8; CHUNK_HEADER_SIZE];
        match self.reader.read_exact(&mut header) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(RiegeliError::UnexpectedEof);
            }
            Err(e) => return Err(RiegeliError::Io(e)),
        }
        
        // Extract header fields
        let header_hash = (&header[0..8]).read_u64::<LittleEndian>()?;
        let data_size = (&header[8..16]).read_u64::<LittleEndian>()?;
        let data_hash = (&header[16..24]).read_u64::<LittleEndian>()?;
        let chunk_type = header[24];
        
        // Read num_records (7 bytes)
        let mut num_records_bytes = [0u8; 8];
        num_records_bytes[0..7].copy_from_slice(&header[25..32]);
        num_records_bytes[7] = 0;
        let mut num_records_cursor = Cursor::new(num_records_bytes);
        let num_records = num_records_cursor.read_u64::<LittleEndian>()?;
        
        let decoded_data_size = (&header[32..40]).read_u64::<LittleEndian>()?;
        
        // Verify header hash
        let calculated_hash = highway_hash(&header[8..]);
        if calculated_hash != header_hash {
            return Err(RiegeliError::ChunkHeaderHashMismatch);
        }
        
        Ok((chunk_type, data_size, data_hash, num_records, decoded_data_size))
    }

    /// Reads chunk data and verifies its hash.
    fn read_chunk_data(&mut self, data_size: u64, data_hash: u64) -> Result<Bytes> {
        let mut data = vec![0u8; data_size as usize];
        self.reader.read_exact(&mut data)?;
        
        // Verify data hash
        let calculated_hash = highway_hash(&data);
        if calculated_hash != data_hash {
            return Err(RiegeliError::ChunkDataHashMismatch);
        }
        
        Ok(Bytes::from(data))
    }

    /// Parses a simple records chunk.
    fn parse_simple_records(&mut self, data: Bytes, num_records: u64, _decoded_data_size: u64) -> Result<()> {
        let mut buf = data.clone();
        
        // Read compression type
        if buf.remaining() < 1 {
            return Err(RiegeliError::Corruption("Truncated chunk data".to_string()));
        }
        let compression_type = buf.get_u8();
        
        // We only support uncompressed data for now
        if compression_type != COMPRESSION_TYPE_NONE {
            return Err(RiegeliError::UnsupportedCompressionType(compression_type));
        }
        
        // Read sizes buffer size (varint)
        let sizes_size = self.read_varint(&mut buf)?;
        
        if buf.remaining() < sizes_size as usize {
            return Err(RiegeliError::Corruption("Truncated sizes buffer".to_string()));
        }
        
        // Extract sizes buffer
        let sizes_buf = buf.slice(0..sizes_size as usize);
        buf.advance(sizes_size as usize);
        
        // Parse record sizes
        let mut sizes_reader = sizes_buf.clone();
        let mut record_sizes = Vec::with_capacity(num_records as usize);
        
        for _ in 0..num_records {
            let size = self.read_varint(&mut sizes_reader)?;
            record_sizes.push(size);
        }
        
        // The rest of the data is the record values
        let records_data = buf;
        
        // Update chunk state
        self.record_sizes = record_sizes;
        self.record_data = records_data;
        self.data_offset = 0;
        self.record_index = 0;
        self.num_records = num_records;
        
        Ok(())
    }

    /// Reads a varint from the buffer.
    fn read_varint(&self, buf: &mut Bytes) -> Result<u64> {
        let mut result: u64 = 0;
        let mut shift: u32 = 0;
        
        loop {
            if buf.remaining() < 1 {
                return Err(RiegeliError::Corruption("Truncated varint".to_string()));
            }
            
            let byte = buf.get_u8();
            result |= ((byte & 0x7F) as u64) << shift;
            
            if byte & 0x80 == 0 {
                break;
            }
            
            shift += 7;
            if shift >= 64 {
                return Err(RiegeliError::Corruption("Varint too long".to_string()));
            }
        }
        
        Ok(result)
    }

    /// Returns the position of the last read record.
    pub fn last_pos(&self) -> Result<RecordPosition> {
        if self.record_index == 0 {
            return Err(RiegeliError::Other("No record has been read".to_string()));
        }
        
        Ok(RecordPosition::new(self.chunk_begin, self.record_index - 1))
    }

    /// Returns the position of the next record.
    pub fn pos(&self) -> Result<RecordPosition> {
        Ok(RecordPosition::new(self.chunk_begin, self.record_index))
    }

    /// Seeks to a record position.
    pub fn seek(&mut self, pos: RecordPosition) -> Result<()> {
        if self.closed {
            return Err(RiegeliError::Other("Reader is closed".to_string()));
        }
        
        // If we're already at the right chunk and the record is in range,
        // just update the record index
        if pos.chunk_begin == self.chunk_begin && pos.record_index < self.num_records {
            self.record_index = pos.record_index;
            
            // Update data offset
            self.data_offset = 0;
            for i in 0..pos.record_index as usize {
                self.data_offset += self.record_sizes[i] as usize;
            }
            
            return Ok(());
        }
        
        // Otherwise, seek to the chunk beginning and read it
        self.reader.seek(SeekFrom::Start(pos.chunk_begin))?;
        self.chunk_begin = pos.chunk_begin;
        self.read_chunk()?;
        
        // Check if the record index is valid
        if pos.record_index >= self.num_records {
            return Err(RiegeliError::Other("Invalid record index".to_string()));
        }
        
        // Set the record index and data offset
        self.record_index = pos.record_index;
        self.data_offset = 0;
        for i in 0..pos.record_index as usize {
            self.data_offset += self.record_sizes[i] as usize;
        }
        
        Ok(())
    }

    /// Closes the reader.
    pub fn close(&mut self) -> Result<()> {
        self.closed = true;
        Ok(())
    }
}

impl<R: Read + Seek> Drop for RecordReader<R> {
    fn drop(&mut self) {
        let _ = self.close();
    }
}