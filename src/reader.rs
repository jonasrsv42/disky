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

//! Record reader for Riegeli files.
//!
//! This module provides a high-level API for reading records from Riegeli files.
//! It combines the block-level reading of `BlockReader` with the parsing logic
//! of `ChunksParser` into a simple state machine that yields records one at a time.

use std::io::{Read, Seek};

use bytes::Bytes;

use crate::blocks::reader::{BlockReader, BlockReaderConfig, BlocksPiece};
use crate::chunks::chunks_parser::{ChunkPiece, ChunksParser};
use crate::chunks::signature_parser::validate_signature;
use crate::error::{DiskyError, Result};

/// Configures how the RecordReader should handle corruption
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorruptionStrategy {
    /// Errors out on any corruption (default)
    Error,

    /// Attempts to recover from corruption by skipping corrupted blocks and chunks
    Recover,
}

/// Returned by reads
pub enum DiskyPiece {
    /// A bytes record from disky file.
    Record(Bytes),

    /// End of disky file.
    EOF,
}

impl Default for CorruptionStrategy {
    fn default() -> Self {
        CorruptionStrategy::Error
    }
}

/// Configuration for the RecordReader
#[derive(Debug, Clone)]
pub struct RecordReaderConfig {
    /// Block reader configuration
    pub block_config: BlockReaderConfig,

    /// Strategy for handling corruption
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
    /// Creates a new RecordReaderConfig with the specified block size
    pub fn with_block_size(block_size: u64) -> Result<Self> {
        let block_config = BlockReaderConfig::with_block_size(block_size)?;
        Ok(Self {
            block_config,
            corruption_strategy: CorruptionStrategy::default(),
        })
    }

    /// Sets the corruption strategy
    pub fn with_corruption_strategy(mut self, strategy: CorruptionStrategy) -> Self {
        self.corruption_strategy = strategy;
        self
    }
}

/// State for the RecordReader state machine
enum ReaderState {
    /// Initial state, reader is ready to start reading
    Ready,

    /// Reading initial blocks, which should start with a signature
    ReadingInitialBlocks,

    /// Reading subsequent blocks after signature has been verified
    ReadingSubsequentBlocks,

    /// Expecting the initial signature chunk - directly owns the parser
    ExpectingSignature(ChunksParser),

    /// Actively parsing chunks from previously read block data
    ParsingChunks(ChunksParser),

    /// We encountered a block corruption when trying to read chunks.
    BlockCorruption(DiskyError),

    /// We encountered a chunks corruption when trying to parse chunks.
    ChunkCorruption(DiskyError, ChunksParser),

    /// We somehow reached an invalid reader state. There must be an implementation
    /// bug.
    InvalidState(DiskyError),

    /// End of file reached, no more records
    EOF,

    /// A corrupted section was encountered that cannot be recovered
    Corrupted,
}

pub struct RecordReader<Source: Read + Seek> {
    /// The block-level reader for handling Riegeli blocks and headers
    block_reader: BlockReader<Source>,

    /// Current state of the reader state machine
    state: ReaderState,

    /// Configuration for the reader
    config: RecordReaderConfig,
}

impl<Source: Read + Seek> RecordReader<Source> {
    /// Creates a new RecordReader with default configuration.
    ///
    /// # Arguments
    ///
    /// * `source` - Any type that implements `Read + Seek`
    ///
    /// # Returns
    ///
    /// A Result containing the RecordReader or an error
    pub fn new(source: Source) -> Result<Self> {
        Self::with_config(source, RecordReaderConfig::default())
    }

    /// Creates a new RecordReader with custom configuration.
    ///
    /// # Arguments
    ///
    /// * `source` - Any type that implements `Read + Seek`
    /// * `config` - Reader configuration
    ///
    /// # Returns
    ///
    /// A Result containing the RecordReader or an error
    pub fn with_config(source: Source, config: RecordReaderConfig) -> Result<Self> {
        Ok(Self {
            block_reader: BlockReader::with_config(source, config.block_config.clone())?,
            state: ReaderState::Ready,
            config,
        })
    }

    /// Reads the next record from the file.
    ///
    /// This method advances the reader state machine through its various states
    /// as needed to read the next record. It handles block reading, chunk parsing,
    /// and extracting records from chunks.
    ///
    /// # Returns
    ///
    /// * `Result<Option<Bytes>>` - The next record as Bytes if available, None if end of file,
    ///   or an error if reading fails and recovery is disabled or fails
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
                                self.state = ReaderState::EOF;
                                return Err(DiskyError::SignatureReadingError(
                                    "Reached EOF while reading initial signature".to_string(),
                                ));
                            }
                        },
                        Err(e) => {
                            // We do not try to recover if reading initial chunks for signature
                            // fails.
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
                                self.state = ReaderState::BlockCorruption(e);
                            }
                            error => {
                                // Unrecoverable error: transition to reader being corrupted and return error.
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
                    // Handle errors based on corruption strategy
                    if self.config.corruption_strategy == CorruptionStrategy::Recover {
                        if let Err(recovery_err) = self.block_reader.recover() {
                            // Unrecoverable corruption
                            self.state = ReaderState::Corrupted;
                            return Err(recovery_err);
                        }
                    } else {
                        let ret_val = Err(DiskyError::ReadCorruptedBlock(error.to_string()));

                        // Need to set state to not leave us in transitionary state.
                        self.state = ReaderState::BlockCorruption(error);
                        return ret_val;
                    }
                }

                ReaderState::ChunkCorruption(error, mut parser) => {
                    // Error during parsing
                    if self.config.corruption_strategy == CorruptionStrategy::Recover {
                        // Try to recover by refreshing the parser state
                        parser.refresh();
                        self.state = ReaderState::ParsingChunks(parser);
                    } else {
                        // No recovery, return the error
                        self.state = ReaderState::Corrupted;
                        return Err(DiskyError::ReadCorruptedChunk(error.to_string()));
                    }
                }
                ReaderState::Corrupted => {
                    // Already corrupted, can't read more records
                    self.state = ReaderState::Corrupted;
                    return Err(DiskyError::UnrecoverableCorruption(
                        "Reader in corrupted state".to_string(),
                    ));
                }

                ReaderState::InvalidState(err) => {
                    let ret_val = Err(DiskyError::InvalidReaderState(err.to_string()));
                    self.state = ReaderState::InvalidState(err);
                    return ret_val;
                }
            }
        }
    }

    /// Returns the current position in the file.
    pub fn file_position(&self) -> u64 {
        self.block_reader.file_position()
    }

    /// Returns a reference to the underlying source.
    pub fn get_ref(&self) -> &Source {
        self.block_reader.get_ref()
    }

    /// Returns a mutable reference to the underlying source.
    pub fn get_mut(&mut self) -> &mut Source {
        self.block_reader.get_mut()
    }

    /// Returns the underlying source, consuming self.
    pub fn into_inner(self) -> Source {
        self.block_reader.into_inner()
    }

    /// Attempt to recover from a corrupted state.
    ///
    /// This method tries to recover from corruption by resetting the state machine
    /// and asking the block reader to recover.
    ///
    /// # Returns
    ///
    /// A Result indicating if recovery was successful
    pub fn recover(&mut self) -> Result<()> {
        // Only attempt recovery if we're in a corrupted state
        if matches!(self.state, ReaderState::Corrupted) {
            // Ask the block reader to recover
            self.block_reader.recover()?;

            // Reset state
            self.state = ReaderState::Ready;

            Ok(())
        } else {
            // Not in a corrupted state, no need to recover
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    // Tests will go here - we'll implement them separately
}
