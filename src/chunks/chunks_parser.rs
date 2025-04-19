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

//! Parser for Riegeli chunks.
//!
//! This module provides functionality for parsing Riegeli chunks from the
//! output of BlockReader::read_chunks. The parser handles different chunk types
//! and allows for skipping chunks or lazily parsing records.

use bytes::{Buf, Bytes};

use crate::chunks::header::{ChunkHeader, ChunkType};
use crate::chunks::header_parser::parse_chunk_header;
use crate::chunks::simple_chunk_parser::{RecordResult, SimpleChunkParser};
use crate::error::{DiskyError, Result};

/// Represents a parsed chunk from a Riegeli file
#[derive(Debug)]
pub enum Chunk<'a> {
    /// File signature chunk
    Signature,

    /// Simple chunk with records
    SimpleRecords(SimpleChunk<'a>),

    /// Padding chunk
    Padding,
}

/// A simple records chunk that can be iterated to get individual records
#[derive(Debug)]
pub struct SimpleChunk<'a> {
    /// The header information for this chunk
    pub header: ChunkHeader,

    /// The raw data for this chunk
    data: &'a mut Bytes,
}

impl<'a> SimpleChunk<'a> {
    /// Creates a new SimpleChunk taking ownership of data
    fn new(header: ChunkHeader, data: &'a mut Bytes) -> SimpleChunk<'a> {
        Self { header, data }
    }

    /// Consumes this chunk and returns an iterator over the records
    pub fn into_records(self) -> Result<SimpleChunkIterator<'a>> {
        let parser = SimpleChunkParser::new(self.header, self.data)?;
        Ok(SimpleChunkIterator { parser })
    }

    /// Returns the number of records in this chunk
    pub fn record_count(&self) -> u64 {
        self.header.num_records
    }

    /// Returns the decoded data size for this chunk
    pub fn decoded_size(&self) -> u64 {
        self.header.decoded_data_size
    }
}

/// Iterator for records in a SimpleChunk
pub struct SimpleChunkIterator<'a> {
    parser: SimpleChunkParser<'a>,
}

impl<'a> Iterator for SimpleChunkIterator<'a> {
    type Item = Result<Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.parser.next() {
            Ok(RecordResult::Record(record)) => Some(Ok(record)),
            Ok(RecordResult::EndOfChunk) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

/// Parser for Riegeli chunks that consumes the output of BlockReader::read_chunks
pub struct ChunksParser {
    /// The buffer of chunk data
    buffer: Bytes,

    /// Backup slice pointing to the next chunk's position, used for recovery
    next_chunk_backup: Option<Bytes>,
}

impl ChunksParser {
    /// Creates a new ChunksParser with the given chunk data
    pub fn new(chunk_data: Bytes) -> Self {
        Self {
            buffer: chunk_data,
            next_chunk_backup: None,
        }
    }

    /// Resets the buffer to the next chunk if we have a backup
    ///
    /// This method is useful when a SimpleChunk iterator encounters an error
    /// and doesn't fully consume all records, leaving the buffer in a potentially
    /// inconsistent state. Calling this method resets to the next chunk position.
    ///
    /// # Returns
    ///
    /// - Ok(()) if the reset was successful
    /// - Err if there's no backup to restore
    pub fn recover_to_next_chunk(&mut self) -> Result<()> {
        if let Some(backup) = self.next_chunk_backup.take() {
            self.buffer = backup;
            Ok(())
        } else {
            Err(DiskyError::Other(
                "No backup position available to recover to next chunk".to_string(),
            ))
        }
    }

    /// Parses the next chunk from the buffer
    ///
    /// # Returns
    ///
    /// - Ok(Chunk) if a chunk was successfully parsed
    /// - Err if an error occurred during parsing or there's not enough data
    pub fn next_chunk(&mut self) -> Result<Chunk> {
        // Clear any previous backup
        self.next_chunk_backup = None;

        // Parse the chunk header
        let header = parse_chunk_header(&mut self.buffer)?;

        // Make sure we have the full chunk data
        if self.buffer.len() < header.data_size as usize {
            return Err(DiskyError::Corruption(format!(
                "Chunk data is smaller than expected: expected {} bytes, got {} bytes",
                header.data_size,
                self.buffer.remaining()
            )));
        }

        // Store a backup slice pointing to the next chunk
        if self.buffer.len() > header.data_size as usize {
            self.next_chunk_backup = Some(self.buffer.slice(header.data_size as usize..));
        }

        // Parse the chunk based on its type
        match header.chunk_type {
            ChunkType::Signature => {
                // For signature chunks, advance the buffer past the chunk and return the type
                self.buffer.advance(header.data_size as usize);
                Ok(Chunk::Signature)
            }
            ChunkType::SimpleRecords => {
                // For simple records, return a chunk that can be iterated
                let simple_chunk = SimpleChunk::new(header, &mut self.buffer);
                Ok(Chunk::SimpleRecords(simple_chunk))
            }
            ChunkType::Padding => {
                // Currently we don't have a proper parser for padding chunks
                // So we throw an error to indicate this isn't supported yet
                Err(DiskyError::Other(format!(
                    "Padding chunk parsing not yet implemented. Use recover_to_next_chunk() to skip."
                )))
            }
        }
    }
}
