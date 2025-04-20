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

//! High-level record reader for Riegeli files.
//!
//! # Overview
//!
//! The `reader` module provides a performant, streaming API for extracting records from
//! Riegeli formatted files. It handles all the complexities of the format including:
//!
//! - Block boundaries and headers
//! - Chunk parsing and validation
//! - Signature verification
//! - Corruption detection and optional recovery
//!
//! # Usage
//!
//! ```no_run
//! use std::fs::File;
//! use disky::reader::RecordReader;
//! use disky::reader::DiskyPiece;
//! use disky::error::Result;
//!
//! fn read_records(path: &str) -> Result<()> {
//!     let file = File::open(path)?;
//!     let mut reader = RecordReader::new(file)?;
//!
//!     // Read records until EOF
//!     loop {
//!         match reader.next_record()? {
//!             DiskyPiece::Record(bytes) => {
//!                 // Process record bytes
//!                 println!("Record size: {}", bytes.len());
//!             }
//!             DiskyPiece::EOF => break,
//!         }
//!     }
//!     Ok(())
//! }
//! ```

use std::io::{Read, Seek};

use bytes::Bytes;
use log::{error, info, warn};

use crate::blocks::reader::{BlockReader, BlockReaderConfig, BlocksPiece};
use crate::chunks::chunks_parser::{ChunkPiece, ChunksParser};
use crate::chunks::signature_parser::validate_signature;
use crate::error::{DiskyError, Result};

/// Strategy for handling data corruption during file reading.
///
/// Determines whether `RecordReader` should attempt to recover from corrupted blocks
/// and continue reading subsequent records, or fail immediately.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorruptionStrategy {
    /// Return errors immediately on any corruption (default)
    Error,

    /// Attempt recovery by skipping corrupted blocks/chunks
    ///
    /// When enabled, the reader will attempt to find the next valid record after
    /// encountering corruption, potentially skipping damaged portions of the file.
    Recover,
}

/// Result returned by `next_record()`, representing either record data or EOF.
///
/// This enum avoids using `Option<Bytes>` for better semantics and future extensibility.
#[derive(Debug)]
pub enum DiskyPiece {
    /// A complete record extracted from the file
    Record(Bytes),

    /// End of file reached, no more records available
    EOF,
}

impl Default for CorruptionStrategy {
    fn default() -> Self {
        CorruptionStrategy::Error
    }
}

/// Configuration for the [`RecordReader`].
///
/// Controls the behavior of record reading, including block size and corruption handling.
#[derive(Debug, Clone)]
pub struct RecordReaderConfig {
    /// Underlying block reader configuration (block size, etc.)
    pub block_config: BlockReaderConfig,

    /// How to handle corrupted data encountered during reading
    pub corruption_strategy: CorruptionStrategy,
}

impl Default for RecordReaderConfig {
    fn default() -> Self {
        Self {
            block_config: BlockReaderConfig::default(),
            corruption_strategy: CorruptionStrategy::default(),
        }
    }
}

impl RecordReaderConfig {
    /// Creates a config with a custom block size.
    ///
    /// # Arguments
    ///
    /// * `block_size` - Size of blocks in bytes (must be at least 48 bytes)
    ///
    /// # Errors
    ///
    /// Returns an error if block_size is too small to prevent cascading headers.
    pub fn with_block_size(block_size: u64) -> Result<Self> {
        let block_config = BlockReaderConfig::with_block_size(block_size)?;
        Ok(Self {
            block_config,
            corruption_strategy: CorruptionStrategy::default(),
        })
    }

    /// Sets the corruption handling strategy.
    ///
    /// Returns self for method chaining.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use disky::reader::{RecordReaderConfig, CorruptionStrategy};
    ///
    /// let config = RecordReaderConfig::default()
    ///     .with_corruption_strategy(CorruptionStrategy::Recover);
    /// ```
    pub fn with_corruption_strategy(mut self, strategy: CorruptionStrategy) -> Self {
        self.corruption_strategy = strategy;
        self
    }
}

/// Internal state machine states for the RecordReader.
///
/// Each state represents a specific phase in the record reading process.
/// The reader transitions between these states based on file contents
/// and processing requirements.
enum ReaderState {
    /// Initial state before any reading has begun
    Ready,

    /// Reading blocks at the start of a file to find signature
    ReadingInitialBlocks,

    /// Reading blocks after signature verification
    ReadingSubsequentBlocks,

    /// Processing the initial signature chunk
    ExpectingSignature(ChunksParser),

    /// Processing chunks to extract records
    ParsingChunks(ChunksParser),

    /// Handling corrupted block header
    BlockCorruption(DiskyError),

    /// Handling corrupted chunk data with parser for potential recovery
    ChunkCorruption(DiskyError, ChunksParser),

    /// Invalid internal state, indicates implementation error
    InvalidState(DiskyError),

    /// End of file reached, no more records available
    EOF,

    /// Unrecoverable corruption encountered
    Corrupted,
}

/// High-level reader for extracting records from Riegeli files.
///
/// `RecordReader` provides a streaming API that reads Riegeli-formatted files and
/// extracts individual records one at a time while handling block headers,
/// chunk boundaries, validation, and optional corruption recovery.
///
/// The reader works as a state machine, transparently handling all the low-level
/// format details including:
///
/// - Reading and validating block headers
/// - Processing chunk formats and boundaries
/// - Verifying signatures and checksums
/// - Recovering from certain types of data corruption when configured
///
/// # Generic Parameters
///
/// * `Source` - Any type that implements both `Read` and `Seek` (e.g., `File`, `Cursor<Vec<u8>>`)
pub struct RecordReader<Source: Read + Seek> {
    /// Underlying block-level reader
    block_reader: BlockReader<Source>,

    /// Current state in the reading state machine
    state: ReaderState,

    /// Reader configuration parameters
    config: RecordReaderConfig,
}

impl<Source: Read + Seek> RecordReader<Source> {
    /// Creates a new reader with default configuration.
    ///
    /// This constructor uses sensible defaults:
    /// - 64 KiB block size
    /// - Error handling for corruption (no recovery attempts)
    ///
    /// # Errors
    ///
    /// Returns an error if the source cannot be positioned or read.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::fs::File;
    /// use disky::reader::RecordReader;
    ///
    /// # fn example() -> disky::error::Result<()> {
    /// let file = File::open("example.riegeli")?;
    /// let reader = RecordReader::new(file)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(source: Source) -> Result<Self> {
        Self::with_config(source, RecordReaderConfig::default())
    }

    /// Creates a new reader with custom configuration.
    ///
    /// Use this constructor when you need to customize behavior such as:
    /// - Setting a non-standard block size
    /// - Enabling corruption recovery
    ///
    /// # Errors
    ///
    /// Returns an error if the source cannot be positioned or read.
    pub fn with_config(source: Source, config: RecordReaderConfig) -> Result<Self> {
        Ok(Self {
            block_reader: BlockReader::with_config(source, config.block_config.clone())?,
            state: ReaderState::Ready,
            config,
        })
    }

    /// Reads the next record from the file or signals EOF.
    ///
    /// This is the primary method for extracting records from a Riegeli file.
    /// When it returns `DiskyPiece::EOF`, no more records are available.
    ///
    /// # Returns
    ///
    /// Returns either:
    /// - `DiskyPiece::Record(bytes)` containing the next record data
    /// - `DiskyPiece::EOF` when the end of file is reached
    ///
    /// # Errors
    ///
    /// Returns an error when:
    /// - File format is invalid or corrupted (in non-recovery mode)
    /// - I/O errors occur during reading
    /// - Signature validation fails
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::fs::File;
    /// use disky::reader::{RecordReader, DiskyPiece};
    ///
    /// # fn example() -> disky::error::Result<()> {
    /// let file = File::open("data.riegeli")?;
    /// let mut reader = RecordReader::new(file)?;
    ///
    /// while let DiskyPiece::Record(record) = reader.next_record()? {
    ///     println!("Record size: {}", record.len());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn next_record(&mut self) -> Result<DiskyPiece> {
        loop {
            // Replace the current state with a temporary invalid state
            // so we can process the state value and set a new valid state
            let current_state = std::mem::replace(
                &mut self.state,
                ReaderState::InvalidState(DiskyError::Other(
                    "Temporary state during transition".to_string(),
                )),
            );

            match current_state {
                ReaderState::Ready => {
                    // Initial state, start reading initial blocks
                    self.state = ReaderState::ReadingInitialBlocks;
                }

                ReaderState::ReadingInitialBlocks => {
                    // Read chunks from the block reader, expecting a signature in the first set
                    match self.block_reader.read_chunks() {
                        Ok(block_piece) => match block_piece {
                            BlocksPiece::Chunks(chunk_data) => {
                                // Create a new chunk parser with the read data
                                let parser = ChunksParser::new(chunk_data);
                                self.state = ReaderState::ExpectingSignature(parser);
                            }
                            BlocksPiece::EOF => {
                                // We reached EOF while reading initial blocks - this is an error
                                self.state = ReaderState::Corrupted;
                                return Err(DiskyError::SignatureReadingError(
                                    "Reached EOF while reading initial signature".to_string(),
                                ));
                            }
                        },
                        Err(e) => {
                            // We do not try to recover if reading initial chunks for signature
                            // fails.
                            warn!("Error reading initial chunks for signature: {}", e);
                            self.state = ReaderState::Corrupted;
                            return Err(DiskyError::SignatureReadingError(e.to_string()));
                        }
                    }
                }

                ReaderState::ReadingSubsequentBlocks => {
                    // Read chunks from the block reader after signature validation
                    match self.block_reader.read_chunks() {
                        Ok(block_piece) => match block_piece {
                            BlocksPiece::Chunks(chunk_data) => {
                                // Create a new chunk parser with the read data
                                let parser = ChunksParser::new(chunk_data);
                                self.state = ReaderState::ParsingChunks(parser);
                            }
                            BlocksPiece::EOF => {
                                // Reached end of file
                                self.state = ReaderState::EOF;
                            }
                        },
                        Err(e) => match e {
                            // Recoverable errors
                            DiskyError::BlockHeaderHashMismatch
                            | DiskyError::InvalidBlockHeader(_)
                            | DiskyError::BlockHeaderInconsistency(_) => {
                                // Transition to block corruption state.
                                warn!("Potentially recoverable block corruption detected: {}", e);
                                self.state = ReaderState::BlockCorruption(e);
                            }
                            error => {
                                // Unrecoverable error: transition to reader being corrupted and return error.
                                error!("Unrecoverable error during reading: {}", error);
                                self.state = ReaderState::Corrupted;
                                return Err(error);
                            }
                        },
                    }
                }

                ReaderState::ExpectingSignature(mut parser) => {
                    // We expect the first chunk to be a signature
                    match parser.next() {
                        Ok(ChunkPiece::Signature(header)) => {
                            // Verify the signature
                            if let Err(e) = validate_signature(&header) {
                                error!("Signature validation failed: {}", e);
                                self.state = ReaderState::Corrupted;
                                return Err(e);
                            }

                            // Transition to regular chunk parsing
                            self.state = ReaderState::ParsingChunks(parser);
                        }
                        Ok(other) => {
                            // First chunk wasn't a signature - this is a corrupted file
                            self.state = ReaderState::Corrupted;
                            return Err(DiskyError::NotDiskyFile(format!(
                                "Expected signature chunk at file start, got {:?}",
                                other
                            )));
                        }
                        Err(e) => {
                            // Error parsing the signature, we don't try to recover from this.
                            self.state = ReaderState::Corrupted;
                            return Err(e);
                        }
                    }
                }

                ReaderState::ParsingChunks(mut parser) => {
                    match parser.next() {
                        Ok(ChunkPiece::Signature(_))
                        | Ok(ChunkPiece::SimpleChunkStart)
                        | Ok(ChunkPiece::SimpleChunkEnd)
                        | Ok(ChunkPiece::Padding) => {
                            // Just continue parsing with the same parser
                            self.state = ReaderState::ParsingChunks(parser);
                        }

                        Ok(ChunkPiece::Record(record)) => {
                            // Found a record, put the parser back and return the record
                            self.state = ReaderState::ParsingChunks(parser);
                            return Ok(DiskyPiece::Record(record));
                        }

                        Ok(ChunkPiece::ChunksEnd) => {
                            // End of current chunks, read more blocks
                            self.state = ReaderState::ReadingSubsequentBlocks;
                        }

                        Err(e) => {
                            self.state = ReaderState::ChunkCorruption(e, parser);
                        }
                    }
                }

                ReaderState::EOF => {
                    // End of file reached, no more records
                    self.state = ReaderState::EOF;
                    return Ok(DiskyPiece::EOF);
                }

                ReaderState::BlockCorruption(error) => {
                    // Error during disk reading.
                    if self.config.corruption_strategy == CorruptionStrategy::Recover {
                        info!("Attempting to recover from block corruption: {}", error);
                        if let Err(recovery_err) = self.block_reader.recover() {
                            // Unrecoverable corruption
                            error!("Block recovery failed: {}", recovery_err);
                            self.state = ReaderState::Corrupted;
                            return Err(recovery_err);
                        }

                        info!("Block corruption recovery successful");
                        self.state = ReaderState::ReadingSubsequentBlocks;
                    } else {
                        warn!(
                            "Block corruption detected but recovery not enabled: {}",
                            error
                        );
                        let ret_val = Err(DiskyError::ReadCorruptedBlock(error.to_string()));

                        // Need to set state to not leave us in transitionary state.
                        self.state = ReaderState::BlockCorruption(error);
                        return ret_val;
                    }
                }

                ReaderState::ChunkCorruption(error, mut parser) => {
                    // Error during parsing
                    if self.config.corruption_strategy == CorruptionStrategy::Recover {
                        match error {
                            chunk_err @ (DiskyError::ChunkDataHashMismatch
                            | DiskyError::UnsupportedChunkType(_)
                            | DiskyError::UnsupportedCompressionType(_)
                            | DiskyError::UnexpectedEndOfChunk(_)) => {
                                warn!("Attempting to recover from chunk corruption: {}", chunk_err);
                                // Try to recover by skipping the chunk.
                                parser.skip_chunk();
                                info!("Skipped corrupted chunk, continuing with next chunk");
                                self.state = ReaderState::ParsingChunks(parser);
                            }
                            chunks_err @ (DiskyError::MissingChunkData(_)
                            | DiskyError::ChunkHeaderHashMismatch
                            | DiskyError::UnknownChunkType(_)
                            | DiskyError::UnexpectedEndOfChunkHeader(_)) => {
                                info!("Skipped entire in memory blocks, continuing to read new blocks: {}", chunks_err);
                                self.state = ReaderState::ReadingSubsequentBlocks;
                            }
                            others => {
                                error!("Unrecoverable: {}", others);
                                self.state = ReaderState::Corrupted;
                            }
                        }
                    } else {
                        // No recovery, return the error
                        warn!(
                            "Chunk corruption detected but recovery not enabled: {}",
                            error
                        );
                        self.state = ReaderState::Corrupted;
                        return Err(DiskyError::ReadCorruptedChunk(error.to_string()));
                    }
                }
                ReaderState::Corrupted => {
                    // Already corrupted, can't read more records
                    error!("Attempting to read from already corrupted reader state");
                    self.state = ReaderState::Corrupted;
                    return Err(DiskyError::UnrecoverableCorruption(
                        "Reader in corrupted state".to_string(),
                    ));
                }

                ReaderState::InvalidState(err) => {
                    error!("Invalid reader state encountered: {}", err);
                    let ret_val = Err(DiskyError::InvalidReaderState(err.to_string()));
                    self.state = ReaderState::InvalidState(err);
                    return ret_val;
                }
            }
        }
    }
}

/// Allows the reader to be used as an iterator that yields records.
///
/// This implementation enables using `RecordReader` directly in for loops, iterator chains,
/// and other iterator-based APIs. Each iteration will either yield the next record
/// or an error if one occurs during reading.
///
/// # Examples
///
/// ```no_run
/// use std::fs::File;
/// use disky::reader::RecordReader;
///
/// # fn example() -> disky::error::Result<()> {
/// let file = File::open("data.riegeli")?;
/// let reader = RecordReader::new(file)?;
///
/// // Process records with a for loop
/// for record_result in reader {
///     let record = record_result?;
///     println!("Record size: {}", record.len());
/// }
/// # Ok(())
/// # }
/// ```
impl<Source: Read + Seek> Iterator for RecordReader<Source> {
    type Item = Result<Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_record() {
            Ok(DiskyPiece::Record(bytes)) => Some(Ok(bytes)),
            Ok(DiskyPiece::EOF) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    // Tests will go here - we'll implement them separately
}
