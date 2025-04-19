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
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// The possible states of record iteration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordResult {
    /// A record was found
    Record(Bytes),
    /// No more records in this chunk, contains remaining bytes
    Done(Bytes),
}

/// Represents the state of a SimpleChunkReader during processing.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ReaderState {
    /// The reader is ready to read records
    Ready,
    /// The reader is actively reading records
    Reading,
    /// The reader has reached the end of the current chunk
    EndOfChunk,
    /// The reader has encountered an error and cannot continue
    Error,
}

/// Reader for Riegeli simple chunks with records.
///
/// This reader processes the data of a simple chunk (after the 40-byte header)
/// and allows reading records one by one. When there are no more records,
/// it returns any remaining bytes that might contain additional chunks.
pub struct SimpleChunkReader {
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
    
    /// The remaining bytes after this chunk (may contain additional chunks)
    remaining_bytes: Bytes,
}

impl SimpleChunkReader {
    /// Creates a new SimpleChunkReader from a chunk header and data.
    ///
    /// # Arguments
    ///
    /// * `header` - The parsed chunk header
    /// * `chunk_data` - The chunk data (after the 40-byte header)
    ///
    /// # Returns
    ///
    /// A Result containing the reader or an error if the data could not be parsed
    pub fn new(header: ChunkHeader, chunk_data: Bytes) -> Result<Self> {
        // Verify this is a simple records chunk
        if header.chunk_type != ChunkType::SimpleRecords {
            return Err(DiskyError::Other(format!(
                "Expected simple records chunk (r), got: {:?}",
                header.chunk_type
            )));
        }
        
        // Calculate how many bytes are part of this chunk vs remaining
        let expected_chunk_size = header.data_size as usize;
        
        // Check if we have enough data for this chunk
        if chunk_data.len() < expected_chunk_size {
            return Err(DiskyError::Other(format!(
                "Chunk data incomplete: expected {} bytes, got {}",
                expected_chunk_size, 
                chunk_data.len()
            )));
        }
        
        // Extract this chunk's data and any remaining bytes
        let (this_chunk_data, remaining) = if chunk_data.len() > expected_chunk_size {
            // Take the expected chunk size and leave the rest as remaining bytes
            (chunk_data.slice(0..expected_chunk_size), chunk_data.slice(expected_chunk_size..))
        } else {
            // Use all the data and no remaining bytes
            (chunk_data, Bytes::new())
        };
        
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
        
        Ok(SimpleChunkReader {
            state: ReaderState::Ready,
            header,
            compression_type,
            sizes_data,
            records_data,
            records_read: 0,
            remaining_bytes: remaining,
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
        
        // Copy the record data into a new BytesMut buffer
        let mut record_data = BytesMut::with_capacity(record_size);
        record_data.put(&self.records_data[..record_size]);
        
        // Advance the internal position of the Bytes buffer
        self.records_data.advance(record_size);
        
        // Update record counter
        self.records_read += 1;
        
        // Check if we've reached the end
        if self.records_read >= self.header.num_records {
            self.state = ReaderState::EndOfChunk;
        }
        
        Ok(record_data.freeze())
    }
    
    /// Returns the next record or indicates that there are no more records.
    ///
    /// # Returns
    ///
    /// - RecordResult::Record(Bytes) if a record was read
    /// - RecordResult::Done(Bytes) if there are no more records, with any remaining bytes
    /// - Err if an error occurred
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
                    self.state = ReaderState::EndOfChunk;
                    return Ok(RecordResult::Done(self.remaining_bytes.clone()));
                }
                
                // Get the size of the next record
                let record_size = self.read_record_size()?;
                
                // Read the actual record
                let record = self.read_record(record_size)?;
                
                Ok(RecordResult::Record(record))
            }
            ReaderState::EndOfChunk => {
                Ok(RecordResult::Done(self.remaining_bytes.clone()))
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
    use super::*;
    use crate::chunks::SimpleChunkWriter;
    use crate::chunks::writer::ChunkWriter;
    use crate::chunks::header_parser::parse_chunk_header;
    use bytes::BytesMut;
    
    #[test]
    fn test_basic_read_write() {
        // Create a chunk with test records
        let mut writer = SimpleChunkWriter::new(CompressionType::None);
        writer.write_record(b"Record 1").unwrap();
        writer.write_record(b"Record 2").unwrap();
        writer.write_record(b"Record 3").unwrap();
        
        let serialized_chunk = writer.serialize_chunk().unwrap();
        
        // Parse the header and get the chunk data
        let mut chunk_data = serialized_chunk.clone();
        let header = parse_chunk_header(&mut chunk_data).unwrap();
        
        // Create a reader
        let mut reader = SimpleChunkReader::new(header, chunk_data).unwrap();
        
        // Check initial state
        assert_eq!(reader.records_read(), 0);
        assert_eq!(reader.total_records(), 3);
        assert_eq!(reader.compression_type(), CompressionType::None);
        
        // Read records
        let record1 = match reader.next().unwrap() {
            RecordResult::Record(record) => record,
            _ => panic!("Expected Record"),
        };
        assert_eq!(record1, Bytes::from_static(b"Record 1"));
        
        let record2 = match reader.next().unwrap() {
            RecordResult::Record(record) => record,
            _ => panic!("Expected Record"),
        };
        assert_eq!(record2, Bytes::from_static(b"Record 2"));
        
        let record3 = match reader.next().unwrap() {
            RecordResult::Record(record) => record,
            _ => panic!("Expected Record"),
        };
        assert_eq!(record3, Bytes::from_static(b"Record 3"));
        
        // No more records, should return Done
        let remaining = match reader.next().unwrap() {
            RecordResult::Done(remaining) => remaining,
            _ => panic!("Expected Done"),
        };
        assert_eq!(remaining.len(), 0);
    }
    
    #[test]
    fn test_multiple_chunks() {
        // Create two chunks
        let mut writer1 = SimpleChunkWriter::new(CompressionType::None);
        writer1.write_record(b"Chunk1 Record1").unwrap();
        writer1.write_record(b"Chunk1 Record2").unwrap();
        let chunk1 = writer1.serialize_chunk().unwrap();
        
        let mut writer2 = SimpleChunkWriter::new(CompressionType::None);
        writer2.write_record(b"Chunk2 Record1").unwrap();
        writer2.write_record(b"Chunk2 Record2").unwrap();
        writer2.write_record(b"Chunk2 Record3").unwrap();
        let chunk2 = writer2.serialize_chunk().unwrap();
        
        // Concatenate the chunks
        let mut concatenated = BytesMut::with_capacity(chunk1.len() + chunk2.len());
        concatenated.extend_from_slice(&chunk1);
        concatenated.extend_from_slice(&chunk2);
        let mut combined_chunks = concatenated.freeze();
        
        // Parse the first chunk header and data
        let header1 = parse_chunk_header(&mut combined_chunks).unwrap();
        
        // Create reader for the first chunk
        let mut reader1 = SimpleChunkReader::new(header1, combined_chunks).unwrap();
        
        // Read records from first chunk
        let record1 = match reader1.next().unwrap() {
            RecordResult::Record(record) => record,
            _ => panic!("Expected Record"),
        };
        assert_eq!(record1, Bytes::from_static(b"Chunk1 Record1"));
        
        let record2 = match reader1.next().unwrap() {
            RecordResult::Record(record) => record,
            _ => panic!("Expected Record"),
        };
        assert_eq!(record2, Bytes::from_static(b"Chunk1 Record2"));
        
        // Done with first chunk, should return remaining bytes (second chunk)
        let mut remaining_bytes = match reader1.next().unwrap() {
            RecordResult::Done(remaining) => remaining,
            _ => panic!("Expected Done with remaining bytes"),
        };
        
        // The remaining bytes should be exactly the second chunk
        assert_eq!(remaining_bytes.len(), chunk2.len());
        
        // Parse the second chunk header and data
        let header2 = parse_chunk_header(&mut remaining_bytes).unwrap();
        
        // Create reader for the second chunk
        let mut reader2 = SimpleChunkReader::new(header2, remaining_bytes).unwrap();
        
        // Read records from second chunk
        let record1 = match reader2.next().unwrap() {
            RecordResult::Record(record) => record,
            _ => panic!("Expected Record"),
        };
        assert_eq!(record1, Bytes::from_static(b"Chunk2 Record1"));
        
        let record2 = match reader2.next().unwrap() {
            RecordResult::Record(record) => record,
            _ => panic!("Expected Record"),
        };
        assert_eq!(record2, Bytes::from_static(b"Chunk2 Record2"));
        
        let record3 = match reader2.next().unwrap() {
            RecordResult::Record(record) => record,
            _ => panic!("Expected Record"),
        };
        assert_eq!(record3, Bytes::from_static(b"Chunk2 Record3"));
        
        // Done with second chunk, no more remaining bytes
        let remaining = match reader2.next().unwrap() {
            RecordResult::Done(remaining) => remaining,
            _ => panic!("Expected Done with no remaining bytes"),
        };
        assert_eq!(remaining.len(), 0);
    }
}
