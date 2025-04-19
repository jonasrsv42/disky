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

//! Reader for Riegeli simple chunk format.
//!
//! This module provides functionality for reading records from simple chunks
//! according to the Riegeli file format specification.

use crate::chunks::header::{ChunkHeader, ChunkType};
use crate::compression::core::CompressionType;
use crate::error::{DiskyError, Result};
use crate::varint;
use bytes::{Buf, Bytes};

/// The possible states of record iteration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordResult {
    /// A record was found
    Record(Bytes),
    /// No more records in this chunk, buffer has been advanced to after the chunk
    EndOfChunk,
}

/// Represents the state of a SimpleChunkReader during processing.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ReaderState {
    /// The reader is ready to read records
    Ready,
    /// The reader is actively reading records
    Reading,
    /// The reader has reached the end of the current chunk and returned Done
    EndOfChunk,
    /// The reader has encountered an error and cannot continue
    Error,
}

/// Parser for Riegeli simple chunks with records.
///
/// This parser processes the data of a simple chunk (after the 40-byte header)
/// and allows reading records one by one. When there are no more records,
/// it returns EndOfChunk and advances the input buffer past this chunk.
pub struct SimpleChunkParser<'a> {
    /// The current state of the reader
    state: ReaderState,
    
    /// The chunk header that was provided at creation
    header: ChunkHeader,
    
    /// The compression type used in this chunk
    compression_type: CompressionType,
    
    /// The raw serialized record sizes (decompressed if necessary)
    /// Uses Buf trait to maintain position as we read sizes
    sizes_data: Bytes,
    
    /// The raw record values (decompressed if necessary)
    /// Uses Buf trait to maintain position as we read records
    records_data: Bytes,
    
    /// Number of records read so far
    records_read: u64,
    
    /// The original input buffer that will be advanced when done
    input_buffer: &'a mut Bytes,
    
    /// The total size of the chunk data
    chunk_size: usize,
}

impl<'a> SimpleChunkParser<'a> {
    /// Creates a new SimpleChunkParser from a chunk header and mutable data reference.
    ///
    /// # Arguments
    ///
    /// * `header` - The parsed chunk header
    /// * `chunk_data` - Mutable reference to the buffer containing chunk data (after the 40-byte header)
    ///
    /// # Returns
    ///
    /// A Result containing the parser or an error if the data could not be parsed.
    /// When the parser is done reading records, it will advance the buffer past this chunk.
    pub fn new(header: ChunkHeader, chunk_data: &'a mut Bytes) -> Result<Self> {
        // Verify this is a simple records chunk
        if header.chunk_type != ChunkType::SimpleRecords {
            return Err(DiskyError::Other(format!(
                "Expected simple records chunk (r), got: {:?}",
                header.chunk_type
            )));
        }
        
        // Calculate how many bytes are part of this chunk
        let expected_chunk_size = header.data_size as usize;
        
        // Check if we have enough data for this chunk
        if chunk_data.remaining() < expected_chunk_size {
            return Err(DiskyError::Other(format!(
                "Chunk data incomplete: expected {} bytes, got {}",
                expected_chunk_size, 
                chunk_data.remaining()
            )));
        }
        
        // Take a slice of the current chunk data without advancing yet
        let this_chunk_data = chunk_data.slice(0..expected_chunk_size);
        
        // Prepare to parse the chunk's internal format
        let mut data = if this_chunk_data.len() > 0 {
            this_chunk_data
        } else {
            return Err(DiskyError::Other("Chunk data is empty".to_string()));
        };
        
        // Read compression type
        let compression_type_byte = data.get_u8();
        let compression_type = match compression_type_byte {
            0 => CompressionType::None,
            b'b' => CompressionType::Brotli,
            b'z' => CompressionType::Zstd,
            b's' => CompressionType::Snappy,
            _ => {
                return Err(DiskyError::UnsupportedCompressionType(compression_type_byte));
            }
        };
        
        // Read size of record sizes (will be a varint)
        let sizes_len = match varint::read_vu64(&mut data) {
            Ok(len) => len as usize,
            Err(_) => {
                return Err(DiskyError::Corruption("Failed to read sizes length".to_string()));
            }
        };
        
        // Ensure sizes data is valid
        if sizes_len > data.remaining() {
            return Err(DiskyError::Corruption(format!(
                "Sizes data length ({}) exceeds available data ({})",
                sizes_len, data.remaining()
            )));
        }
        
        // Split the data into sizes and records portions (creating new Bytes objects)
        let compressed_sizes_data = data.slice(0..sizes_len);
        
        // Advance the data buffer past the sizes data
        data.advance(sizes_len);
        
        // The remaining data is the records data
        let compressed_records_data = data;
        
        // Decompress the data if needed
        let (sizes_data, records_data) = match compression_type {
            CompressionType::None => {
                // No decompression needed
                (compressed_sizes_data, compressed_records_data)
            },
            CompressionType::Brotli => {
                // Brotli decompression not yet implemented
                return Err(DiskyError::Other("Brotli decompression not yet implemented".to_string()));
            },
            CompressionType::Zstd => {
                // Zstd decompression not yet implemented
                return Err(DiskyError::Other("Zstd decompression not yet implemented".to_string()));
            },
            CompressionType::Snappy => {
                // Snappy decompression not yet implemented
                return Err(DiskyError::Other("Snappy decompression not yet implemented".to_string()));
            },
        };
        
        Ok(SimpleChunkParser {
            state: ReaderState::Ready,
            header,
            compression_type,
            sizes_data,
            records_data,
            records_read: 0,
            input_buffer: chunk_data,
            chunk_size: expected_chunk_size,
        })
    }
    
    /// Reads the next record size from the sizes data
    ///
    /// # Returns
    ///
    /// The size of the next record or an error if the size could not be read
    fn read_record_size(&mut self) -> Result<usize> {
        // Read the size of the next record directly using varint::read_vu64
        // which will advance the internal position of the Bytes buffer
        let record_size = match varint::read_vu64(&mut self.sizes_data) {
            Ok(size) => size as usize,
            Err(e) => {
                self.state = ReaderState::Error;
                return Err(e);
            }
        };
        
        Ok(record_size)
    }
    
    /// Extracts a record of the given size from the records data
    ///
    /// # Arguments
    ///
    /// * `record_size` - The size of the record to extract
    ///
    /// # Returns
    ///
    /// The record data or an error if the record could not be extracted
    fn read_record(&mut self, record_size: usize) -> Result<Bytes> {
        // Ensure the record is within bounds
        if record_size > self.records_data.remaining() {
            self.state = ReaderState::Error;
            return Err(DiskyError::Corruption(format!(
                "Record extends beyond data boundary: size={}, remaining={}",
                record_size, self.records_data.remaining()
            )));
        }
        
        // Get a slice of the record data without copying
        let record_data = self.records_data.slice(0..record_size);
        
        // Advance the internal position of the Bytes buffer
        self.records_data.advance(record_size);
        
        // Update record counter
        self.records_read += 1;
        
        Ok(record_data)
    }
    
    /// Returns the next record or indicates that there are no more records.
    ///
    /// # Returns
    ///
    /// - RecordResult::Record(Bytes) if a record was read
    /// - RecordResult::EndOfChunk if there are no more records, the input buffer has been advanced
    /// - Err if an error occurred or if next() is called after EndOfChunk was returned
    pub fn next(&mut self) -> Result<RecordResult> {
        match self.state {
            ReaderState::Ready => {
                // Transition to Reading state
                self.state = ReaderState::Reading;
                self.next()
            }
            ReaderState::Reading => {
                // Check if we've reached the end of the records
                if self.records_read >= self.header.num_records {
                    // Advance the original buffer past this chunk
                    self.input_buffer.advance(self.chunk_size);
                    
                    // Update state
                    self.state = ReaderState::EndOfChunk;
                    
                    return Ok(RecordResult::EndOfChunk);
                }
                
                // Get the size of the next record
                let record_size = self.read_record_size()?;
                
                // Read the actual record
                let record = self.read_record(record_size)?;
                
                Ok(RecordResult::Record(record))
            }
            ReaderState::EndOfChunk => {
                Err(DiskyError::Other(
                    "Cannot invoke next() after SimpleChunkParser has returned EndOfChunk".to_string(),
                ))
            }
            ReaderState::Error => Err(DiskyError::Other(
                "Cannot read from an errored chunk".to_string(),
            )),
        }
    }
    
    /// Returns the number of records read so far.
    pub fn records_read(&self) -> u64 {
        self.records_read
    }
    
    /// Returns the total number of records in this chunk.
    pub fn total_records(&self) -> u64 {
        self.header.num_records
    }
    
    /// Returns the compression type used in this chunk.
    pub fn compression_type(&self) -> CompressionType {
        self.compression_type
    }
    
    /// Returns the chunk header.
    pub fn header(&self) -> &ChunkHeader {
        &self.header
    }
}


#[cfg(test)]
mod tests {
    // Tests are in src/chunks/tests/simple_chunk_parser_tests.rs
}
