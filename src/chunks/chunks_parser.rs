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

use crate::chunks::header::ChunkType;
use crate::chunks::header_parser::parse_chunk_header;
use crate::chunks::simple_chunk_parser::{SimpleChunkParser, SimpleChunkPiece};
use crate::error::{DiskyError, Result};

/// Represents a parsed chunk piece from a Riegeli file. It it as piece of a chunk
/// which may be the size of the chunk itself or a partial piece like in the case
/// of records being emitted during parsing of a SimpleChunk.
/// or a piece
#[derive(Debug)]
pub enum ChunkPiece {
    /// File signature chunk
    Signature,

    SimpleChunkStart,
    Record(Bytes),
    SimpleChunkEnd,

    /// Padding chunk
    Padding,

    /// Done parsing all chunks in current buffer
    ChunksEnd,
}

enum State {
    /// Ready to parse a new chunk
    Fresh,
    SimpleChunk(SimpleChunkParser),
    /// Done parsing chunks.
    Finish,
}

/// Parser for Riegeli chunks that consumes the output of BlockReader::read_chunks
pub struct ChunksParser {
    /// The buffer of chunk data
    buffer: Bytes,

    state: State,
}

impl ChunksParser {
    /// Creates a new ChunksParser with the given chunk data
    pub fn new(chunk_data: Bytes) -> Self {
        Self {
            buffer: chunk_data,
            state: State::Fresh,
        }
    }

    /// refresh parser state, for-example to recover
    /// from an error during SimpleChunk parsing by going to the next chunk.
    pub fn refresh(&mut self) {
        self.state = State::Fresh;
    }

    fn next_chunk(&mut self) -> Result<ChunkPiece> {
        if self.buffer.is_empty() {
            self.state = State::Finish;
            return Ok(ChunkPiece::ChunksEnd);
        }

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

        // Split out the current chunk and advance buffer to the next chunk.
        let buffer_view = self.buffer.split_to(header.data_size as usize);

        // Parse the chunk based on its type
        match header.chunk_type {
            ChunkType::Signature => {
                // For signature chunks, we do nothing, they should only be
                // validated at the start of the file.
                Ok(ChunkPiece::Signature)
            }
            ChunkType::SimpleRecords => {
                // For simple records, return a chunk that can be iterated
                let parser = SimpleChunkParser::new(header, buffer_view)?;
                self.state = State::SimpleChunk(parser);
                Ok(ChunkPiece::SimpleChunkStart)
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

    pub fn next(&mut self) -> Result<ChunkPiece> {
        match &mut self.state {
            State::Fresh => self.next_chunk(),
            State::SimpleChunk(simple_chunk_parser) => match simple_chunk_parser.next()? {
                SimpleChunkPiece::Record(bytes) => Ok(ChunkPiece::Record(bytes)),
                SimpleChunkPiece::EndOfChunk => {
                    self.state = State::Fresh;
                    Ok(ChunkPiece::SimpleChunkEnd)
                }
            },
            State::Finish => Err(DiskyError::Other(
                "Cannot advance on a finished ChunksParser.".to_string(),
            )),
        }
    }
}
