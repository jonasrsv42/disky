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
//!
//! # Example
//!
//! ```
//! use disky::chunks::chunks_parser::{ChunksParser, ChunkPiece};
//! use disky::chunks::signature_parser::validate_signature;
//! use disky::blocks::reader::BlockReader;
//! # use bytes::Bytes;
//!
//! # fn example() -> disky::error::Result<()> {
//! # let chunk_data = Bytes::new(); // In a real example, this would come from BlockReader::read_chunks
//! // Create a parser
//! let mut parser = ChunksParser::new(chunk_data);
//!
//! // Parse chunks and process each piece
//! loop {
//!     match parser.next()? {
//!         ChunkPiece::Signature(header) => {
//!             // Found a file signature chunk with header
//!             validate_signature(&header)?;
//!             println!("Found and validated signature chunk");
//!         },
//!         ChunkPiece::SimpleChunkStart => {
//!             println!("Starting to parse a simple chunk");
//!         },
//!         ChunkPiece::Record(data) => {
//!             // Process record data
//!             println!("Found record of {} bytes", data.len());
//!         },
//!         ChunkPiece::SimpleChunkEnd => {
//!             println!("Finished parsing simple chunk");
//!         },
//!         ChunkPiece::ChunksEnd => {
//!             // No more chunks in buffer
//!             println!("End of chunks");
//!             break;
//!         },
//!         ChunkPiece::Padding => {
//!             // Found a padding chunk (though this is not fully implemented yet)
//!             println!("Found padding chunk");
//!         },
//!     }
//! }
//!
//! // In case of errors during parsing, you can recover and continue with the next chunk
//! # let result = parser.next();
//! # if result.is_err() {
//!     // Error occurred, reset parser state to continue with next chunk
//!     parser.skip_chunk();
//!     // Continue parsing from the next chunk
//! # }
//! # Ok(())
//! # }
//! ```

use bytes::{Buf, Bytes};

use crate::chunks::header::{ChunkHeader, ChunkType};
use crate::chunks::header_parser::parse_chunk_header;
use crate::chunks::simple_chunk_parser::{SimpleChunkParser, SimpleChunkPiece};
use crate::error::{DiskyError, Result};

/// Represents a parsed piece from a Riegeli chunk.
///
/// A ChunkPiece can be a complete chunk (like a Signature or Padding chunk),
/// or it can represent a part of the parsing process for a complex chunk type
/// (like SimpleChunkStart, Record, and SimpleChunkEnd for a simple chunk with records).
#[derive(Debug)]
pub enum ChunkPiece {
    /// File signature chunk - indicates the start of a Riegeli file
    /// Contains the chunk header for validation
    Signature(ChunkHeader),

    /// Start of a simple chunk with records - indicates that records will follow
    SimpleChunkStart,

    /// A single record from a simple chunk - contains the actual record data
    Record(Bytes),

    /// End of a simple chunk - indicates that all records in the current chunk have been read
    SimpleChunkEnd,

    /// Padding chunk - used for alignment or other purposes in the file
    Padding,

    /// Done parsing all chunks in current buffer - no more chunks available
    ChunksEnd,
}

/// Internal parser state to track parsing progress
enum State {
    /// Ready to parse a new chunk - initial state or after completing a chunk
    Fresh,

    /// In the process of parsing a simple chunk - contains the SimpleChunkParser
    SimpleChunk(SimpleChunkParser),

    /// Done parsing chunks - reached the end of the buffer or an unrecoverable error
    Finish,
}

/// Parser for Riegeli chunks that processes data retrieved from a BlockReader
///
/// The ChunksParser takes a buffer of chunk data (typically from BlockReader::read_chunks)
/// and allows iterative parsing of chunks and their contents. It supports:
///
/// - Parsing different chunk types (signature, simple records, etc.)
/// - Iterating through records in chunks
/// - Recovering from errors to continue parsing subsequent chunks
pub struct ChunksParser {
    /// The buffer of chunk data being parsed
    buffer: Bytes,

    /// Current state of the parser that tracks parsing progress
    state: State,
}

impl ChunksParser {
    /// Creates a new ChunksParser with the given chunk data
    ///
    /// # Arguments
    ///
    /// * `chunk_data` - The buffer of chunk data to parse, typically from BlockReader::read_chunks
    ///
    /// # Returns
    ///
    /// A new ChunksParser instance ready to start parsing chunks
    ///
    /// # Example
    ///
    /// ```
    /// use disky::chunks::chunks_parser::ChunksParser;
    /// use bytes::Bytes;
    ///
    /// let chunk_data = Bytes::from_static(&[/* chunk data would go here */]);
    /// let parser = ChunksParser::new(chunk_data);
    /// ```
    pub fn new(chunk_data: Bytes) -> Self {
        Self {
            buffer: chunk_data,
            state: State::Fresh,
        }
    }

    /// Refreshes the parser state to recover from errors during chunk parsing
    ///
    /// This method resets the internal state to allow parsing the next chunk
    /// after an error occurs. It's particularly useful for recovering from:
    ///
    /// - Errors during SimpleChunk parsing
    /// - Unsupported chunk types
    /// - Corrupted chunks
    ///
    /// After calling skip_chunk(), the next call to next() will attempt to parse
    /// the next chunk in the buffer, skipping any problematic chunk.
    ///
    /// # Example
    ///
    /// ```
    /// use disky::chunks::chunks_parser::ChunksParser;
    /// # use bytes::Bytes;
    ///
    /// # fn example() {
    /// # let chunk_data = Bytes::new();
    /// let mut parser = ChunksParser::new(chunk_data);
    ///
    /// // If an error occurs during parsing
    /// if let Err(_) = parser.next() {
    ///     // Refresh state to skip the problematic chunk
    ///     parser.skip_chunk();
    ///     // Continue parsing with the next chunk
    /// }
    /// # }
    /// ```
    pub fn skip_chunk(&mut self) {
        self.state = State::Fresh;
    }

    /// Parses the next chunk in the buffer
    ///
    /// This internal method handles the initial parsing of a chunk header and sets up
    /// the appropriate parser for the specific chunk type.
    ///
    /// # Returns
    ///
    /// * `Ok(ChunkPiece)` - The next chunk piece parsed from the buffer
    /// * `Err` - If an error occurs during parsing
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The chunk header cannot be parsed
    /// - The chunk data is incomplete
    /// - The chunk type is unsupported
    fn next_chunk(&mut self) -> Result<ChunkPiece> {
        if self.buffer.is_empty() {
            self.state = State::Finish;
            return Ok(ChunkPiece::ChunksEnd);
        }

        // Parse the chunk header
        let header = parse_chunk_header(&mut self.buffer)?;

        // Make sure we have the full chunk data
        if self.buffer.len() < header.data_size as usize {
            return Err(DiskyError::UnexpectedEndOfChunk(format!(
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
                // For signature chunks, we return the ChunkHeader for validation
                Ok(ChunkPiece::Signature(header))
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
                Err(DiskyError::UnsupportedChunkType(
                    header.chunk_type.as_byte(),
                ))
            }
        }
    }

    /// Returns the next piece from the current chunk being parsed
    ///
    /// This method advances the parser state and returns the next available piece.
    /// For simple chunks with records, it will first return SimpleChunkStart,
    /// then multiple Record pieces (one for each record), and finally SimpleChunkEnd.
    ///
    /// # Returns
    ///
    /// * `Ok(ChunkPiece)` - The next chunk piece
    /// * `Err` - If an error occurs during parsing
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The chunk header cannot be parsed
    /// - The chunk data is incomplete or corrupted
    /// - The chunk type is unsupported
    /// - The parser is already in the Finish state
    ///
    /// # Example
    ///
    /// ```
    /// use disky::chunks::chunks_parser::{ChunksParser, ChunkPiece};
    /// # use bytes::Bytes;
    /// # use disky::error::Result;
    ///
    /// # fn example() -> Result<()> {
    /// # let chunk_data = Bytes::new();
    /// let mut parser = ChunksParser::new(chunk_data);
    ///
    /// // Parse and process chunks
    /// loop {
    ///     match parser.next() {
    ///         Ok(ChunkPiece::ChunksEnd) => break, // No more chunks
    ///         Ok(piece) => {
    ///             // Process the chunk piece
    ///             // ...
    ///         },
    ///         Err(e) => {
    ///             // Handle error or skip_chunk and continue
    ///             parser.skip_chunk();
    ///         }
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
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

#[cfg(test)]
mod tests {
    // Tests are in src/chunks/tests/chunks_parser_tests.rs
}

