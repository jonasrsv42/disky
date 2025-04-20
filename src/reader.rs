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

use crate::blocks::reader::{BlockReader, BlockReaderConfig};
use crate::chunks::chunks_parser::{ChunkPiece, ChunksParser};
use crate::error::{DiskyError, Result};

/// Configures how the RecordReader should handle corruption
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorruptionStrategy {
    /// Errors out on any corruption (default)
    Error,
    
    /// Attempts to recover from corruption by skipping corrupted blocks and chunks
    Recover,
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
    
    /// Currently reading from block reader, no active chunk parser
    ReadingBlock,
    
    /// Actively parsing chunks from previously read block data
    ParsingChunks(ChunksParser),
    
    /// End of file reached, no more records
    EndOfFile,
    
    /// A corrupted section was encountered that cannot be recovered
    Corrupted,
}

/// High-level reader for Riegeli records.
///
/// `RecordReader` combines a `BlockReader` and `ChunksParser` into a state machine
/// that reads and parses records from a Riegeli file. It handles the necessary
/// transitions between reading blocks, handling block headers, parsing chunks,
/// and extracting records.
///
/// # Example
///
/// ```
/// use disky::reader::{RecordReader, RecordReaderConfig, CorruptionStrategy};
/// use std::fs::File;
/// 
/// # fn example() -> disky::error::Result<()> {
/// # let file = File::open("example.riegeli")?;
/// 
/// // Create a reader with default configuration
/// let mut reader = RecordReader::new(file)?;
/// 
/// // Or with custom configuration
/// # let file = File::open("example.riegeli")?;
/// let config = RecordReaderConfig::default()
///     .with_corruption_strategy(CorruptionStrategy::Recover);
/// let mut reader = RecordReader::with_config(file, config)?;
/// 
/// // Read all records in the file
/// loop {
///     match reader.next_record() {
///         Ok(Some(record)) => {
///             // Process the record data (as Bytes)
///             println!("Record size: {}", record.len());
///         },
///         Ok(None) => {
///             // End of file reached
///             break;
///         },
///         Err(e) => {
///             // Handle error (depending on corruption_strategy, some errors may be skipped)
///             println!("Error: {:?}", e);
///             break;
///         }
///     }
/// }
/// # Ok(())
/// # }
/// ```
pub struct RecordReader<Source: Read + Seek> {
    /// The block-level reader for handling Riegeli blocks and headers
    block_reader: BlockReader<Source>,
    
    /// Current state of the reader state machine
    state: ReaderState,
    
    /// Flag indicating if the file signature has been verified
    signature_verified: bool,
    
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
            signature_verified: false,
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
    pub fn next_record(&mut self) -> Result<Option<Bytes>> {
        loop {
            match &mut self.state {
                ReaderState::Ready => {
                    // Initial state, start reading blocks
                    self.state = ReaderState::ReadingBlock;
                }
                
                ReaderState::ReadingBlock => {
                    // Read chunks from the block reader
                    match self.block_reader.read_chunks() {
                        Ok(chunk_data) => {
                            // Create a new chunk parser with the read data
                            let parser = ChunksParser::new(chunk_data);
                            self.state = ReaderState::ParsingChunks(parser);
                        }
                        Err(e) => {
                            // Handle errors based on corruption strategy
                            if self.config.corruption_strategy == CorruptionStrategy::Recover {
                                // Try to recover, but only for certain errors
                                match &e {
                                    DiskyError::Corruption(_) | DiskyError::BlockHeaderHashMismatch => {
                                        // Try to recover from block-level corruption
                                        if let Err(recovery_err) = self.block_reader.recover() {
                                            // Unrecoverable corruption
                                            self.state = ReaderState::Corrupted;
                                            return Err(recovery_err);
                                        }
                                        // Successfully recovered, try reading again
                                        // Stay in ReadingBlock state
                                    }
                                    DiskyError::UnexpectedEof => {
                                        // End of file reached
                                        self.state = ReaderState::EndOfFile;
                                        return Ok(None);
                                    }
                                    _ => {
                                        // Other errors are not recoverable
                                        self.state = ReaderState::Corrupted;
                                        return Err(e);
                                    }
                                }
                            } else {
                                // No recovery, just return the error
                                if let DiskyError::UnexpectedEof = e {
                                    // End of file is a special case
                                    self.state = ReaderState::EndOfFile;
                                    return Ok(None);
                                } else {
                                    // Any other error
                                    self.state = ReaderState::Corrupted;
                                    return Err(e);
                                }
                            }
                        }
                    }
                }
                
                ReaderState::ParsingChunks(parser) => {
                    // Parse the next chunk piece
                    match parser.next() {
                        Ok(ChunkPiece::Signature) => {
                            // File signature chunk - verify it only on first read
                            if !self.signature_verified {
                                self.signature_verified = true;
                            }
                            // Continue parsing
                        }
                        
                        Ok(ChunkPiece::SimpleChunkStart) => {
                            // Start of a simple chunk - continue parsing
                            // Records will come next
                        }
                        
                        Ok(ChunkPiece::Record(record)) => {
                            // Found a record, return it
                            return Ok(Some(record));
                        }
                        
                        Ok(ChunkPiece::SimpleChunkEnd) => {
                            // End of simple chunk, continue parsing
                        }
                        
                        Ok(ChunkPiece::ChunksEnd) => {
                            // End of current chunks, read more blocks
                            self.state = ReaderState::ReadingBlock;
                        }
                        
                        Ok(ChunkPiece::Padding) => {
                            // Padding chunk (not fully implemented yet)
                            // If we encounter it, try to recover based on strategy
                            if self.config.corruption_strategy == CorruptionStrategy::Recover {
                                // Refresh parser to skip this chunk
                                parser.refresh();
                            } else {
                                // Return an error since padding chunks aren't fully supported
                                return Err(DiskyError::Other(
                                    "Padding chunks are not fully supported yet".to_string()
                                ));
                            }
                        }
                        
                        Err(e) => {
                            // Error during parsing
                            if self.config.corruption_strategy == CorruptionStrategy::Recover {
                                // Try to recover by refreshing the parser state
                                parser.refresh();
                                
                                // Check if we have chunks left to parse
                                match parser.next() {
                                    Ok(ChunkPiece::ChunksEnd) => {
                                        // No more chunks to parse, read more blocks
                                        self.state = ReaderState::ReadingBlock;
                                    }
                                    Err(_) => {
                                        // Still getting errors, try reading next block
                                        self.state = ReaderState::ReadingBlock;
                                    }
                                    _ => {
                                        // Found another valid chunk piece, continue parsing
                                        // Stay in ParsingChunks state
                                    }
                                }
                            } else {
                                // No recovery, return the error
                                self.state = ReaderState::Corrupted;
                                return Err(e);
                            }
                        }
                    }
                }
                
                ReaderState::EndOfFile => {
                    // End of file reached, no more records
                    return Ok(None);
                }
                
                ReaderState::Corrupted => {
                    // Already corrupted, can't read more records
                    return Err(DiskyError::UnrecoverableCorruption(
                        "Reader in corrupted state".to_string()
                    ));
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