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

//! Riegeli record writer implementation.
//!
//! This module provides functionality for writing records to Riegeli files,
//! following the Riegeli specification.

use std::io::{Seek, Write};

use crate::blocks::writer::{BlockWriter, BlockWriterConfig};
use crate::chunks::signature_writer::SignatureWriter;
use crate::chunks::{ChunkWriter, SimpleChunkWriter};
use crate::compression::CompressionType;
use crate::error::{DiskyError, Result};

/// Configuration options for a RecordWriter.
#[derive(Debug, Clone)]
pub struct RecordWriterConfig {
    /// Compression type to use for records.
    pub compression_type: CompressionType,

    /// Maximum size of records in a chunk in bytes (uncompressed).
    ///
    /// When this size is reached, the chunk will be written and a new chunk started.
    pub chunk_size_bytes: u64,

    /// Block configuration.
    pub block_config: BlockWriterConfig,
}

impl Default for RecordWriterConfig {
    fn default() -> Self {
        Self {
            compression_type: CompressionType::None,
            chunk_size_bytes: 1024 * 1024, // Default to 1 MB chunks
            block_config: BlockWriterConfig::default(),
        }
    }
}

impl RecordWriterConfig {
    // Set compression type for writer.
    pub fn with_compression(mut self, compression: CompressionType) -> Self {
        self.compression_type = compression;

        return self;
    }
}

/// Enum to represent the state of a RecordWriter.
#[derive(Debug, PartialEq)]
pub(crate) enum WriterState {
    /// The writer is new, no data has been written yet.
    New,

    /// The writer has written the file signature.
    SignatureWritten,

    /// The writer has written at least one record since the last flush.
    RecordsWritten,

    /// The writer has flushed all records and has nothing pending.
    Flushed,

    /// The writer has been closed.
    Closed,
}

/// Writer for Riegeli records.
///
/// This implements the high-level writer for Riegeli files, handling:
/// - File signature writing
/// - Record chunking
/// - Block boundary handling
///
/// The writer follows the Riegeli file format specification:
/// 1. Every Riegeli file starts with a 64-byte signature (24-byte block header + 40-byte chunk header)
/// 2. Records are grouped into chunks
/// 3. Block headers are inserted at block boundaries (typically every 64 KiB)
///
/// # Example
///
/// ```no_run
/// use std::fs::File;
/// use disky::writer::{RecordWriter, RecordWriterConfig};
/// use disky::compression::CompressionType;
///
/// // Create a new writer with default settings
/// let file = File::create("example.riegeli").unwrap();
/// let mut writer = RecordWriter::new(file).unwrap();
///
/// // Write some records
/// writer.write_record(b"Record 1").unwrap();
/// writer.write_record(b"Record 2").unwrap();
/// writer.write_record(b"Record 3").unwrap();
///
/// // Ensure all data is written
/// writer.close().unwrap();
/// ```
pub struct RecordWriter<Sink: Write + Seek> {
    /// The block writer.
    block_writer: BlockWriter<Sink>,

    /// The chunk writer for records.
    chunk_writer: SimpleChunkWriter,

    /// Configuration for the writer.
    config: RecordWriterConfig,

    /// Current state of the writer.
    state: WriterState,
}

impl<Sink: Write + Seek> RecordWriter<Sink> {
    /// Creates a new RecordWriter with default configuration.
    pub fn new(sink: Sink) -> Result<Self> {
        Self::with_config(sink, RecordWriterConfig::default())
    }

    /// Creates a new RecordWriter with custom configuration.
    pub fn with_config(sink: Sink, config: RecordWriterConfig) -> Result<Self> {
        let block_writer = BlockWriter::with_config(sink, config.block_config.clone())?;

        // Create a chunk writer with the appropriate chunk size pre-allocation
        let chunk_writer = SimpleChunkWriter::with_chunk_size(
            config.compression_type,
            config.chunk_size_bytes as usize,
        )?;

        let mut writer = Self {
            block_writer,
            chunk_writer,
            config,
            state: WriterState::New,
        };

        // Write the file signature immediately
        writer.write_file_signature()?;

        Ok(writer)
    }

    /// Creates a new RecordWriter for appending to an existing file.
    pub fn for_append(sink: Sink, position: u64) -> Result<Self> {
        Self::for_append_with_config(sink, position, RecordWriterConfig::default())
    }

    /// Creates a new RecordWriter for appending to an existing file with custom configuration.
    ///
    /// The position should be the size of the existing file.
    pub fn for_append_with_config(
        sink: Sink,
        position: u64,
        config: RecordWriterConfig,
    ) -> Result<Self> {
        let block_writer =
            BlockWriter::for_append_with_config(sink, position, config.block_config.clone())?;

        // Create a chunk writer with the appropriate chunk size pre-allocation
        let chunk_writer = SimpleChunkWriter::with_chunk_size(
            config.compression_type,
            config.chunk_size_bytes as usize,
        )?;

        Ok(Self {
            block_writer,
            chunk_writer,
            config,
            state: WriterState::SignatureWritten,
        })
    }

    /// Writes the Riegeli file signature.
    fn write_file_signature(&mut self) -> Result<()> {
        match self.state {
            WriterState::New => {
                // Create a signature writer
                let mut signature_writer = SignatureWriter::new();

                // Serialize the signature header
                let signature = signature_writer.serialize_chunk()?;

                // Write the signature through the block writer
                self.block_writer.write_chunk(signature)?;

                // Update state
                self.state = WriterState::SignatureWritten;

                Ok(())
            }
            WriterState::SignatureWritten
            | WriterState::RecordsWritten
            | WriterState::Flushed
            | WriterState::Closed => {
                return Err(DiskyError::Other(
                    "File signature has already been written".to_string(),
                ));
            }
        }
    }

    /// Writes a record to the Riegeli file.
    ///
    /// The record will be added to the current chunk. If the chunk becomes too large
    /// (either by number of records or size), it will be written out and a new chunk started.
    ///
    /// # Arguments
    ///
    /// * `record` - The record data to write
    ///
    /// # Returns
    ///
    /// * `Result<()>` - Success or an error if the write failed
    pub fn write_record(&mut self, record: &[u8]) -> Result<()> {
        if self.state == WriterState::Closed {
            return Err(DiskyError::WritingClosedFile);
        }

        match self.state {
            WriterState::New
            | WriterState::SignatureWritten
            | WriterState::RecordsWritten
            | WriterState::Flushed => {
                // Add the record to the chunk and get current records size
                let record_size = self.chunk_writer.write_record(record)?;

                self.state = WriterState::RecordsWritten;

                // Check if we need to write out the chunk based on size only
                if record_size.0 >= self.config.chunk_size_bytes {
                    self.flush_chunk()?;
                }

                Ok(())
            }
            WriterState::Closed => return Err(DiskyError::WritingClosedFile),
        }
    }

    /// Flushes the current chunk to disk.
    ///
    /// This serializes the current chunk and writes it through the block writer.
    /// After this, a new chunk will be started.
    pub fn flush_chunk(&mut self) -> Result<()> {
        match self.state {
            WriterState::RecordsWritten => {
                // Serialize the chunk - this will reset the chunk writer's internal state
                let chunk = self.chunk_writer.serialize_chunk()?;

                // Write the chunk through the block writer
                self.block_writer.write_chunk(chunk)?;

                // Update state to Flushed
                self.state = WriterState::Flushed;

                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Flushes any remaining records to disk.
    ///
    /// This is equivalent to flush_chunk followed by a flush of the underlying writer.
    pub fn flush(&mut self) -> Result<()> {
        match self.state {
            WriterState::New | WriterState::Closed => (),
            WriterState::RecordsWritten | WriterState::SignatureWritten => {
                self.flush_chunk()?;
                self.block_writer.flush()?;
            }
            WriterState::Flushed => {
                self.block_writer.flush()?;
            }
        };

        self.state = WriterState::Flushed;

        Ok(())
    }

    /// Closes the writer, flushing any remaining data.
    ///
    /// After closing, no more records can be written.
    pub fn close(&mut self) -> Result<()> {
        if self.state == WriterState::Closed {
            return Ok(());
        }
        match self.state {
            WriterState::New
            | WriterState::SignatureWritten
            | WriterState::RecordsWritten
            | WriterState::Flushed => {
                // Flush any remaining data
                self.flush()?;

                // Update state
                self.state = WriterState::Closed;
                Ok(())
            }
            WriterState::Closed => return Ok(()),
        }
    }
}

impl<Sink: Write + Seek> Drop for RecordWriter<Sink> {
    fn drop(&mut self) {
        // Try to flush any remaining data on drop only if we're not in Closed or Flushed state
        // We ignore errors since there's nothing we can do about them in drop
        if self.state != WriterState::Closed && self.state != WriterState::Flushed {
            let _ = self.flush();
        }
    }
}

// Methods for testing only
#[cfg(test)]
impl<Sink: Write + Seek> RecordWriter<Sink> {
    /// Get the current writer state (testing only)
    pub(crate) fn get_state(&self) -> &WriterState {
        &self.state
    }

    /// Get a reference to the block writer (testing only)
    pub(crate) fn get_block_writer(&self) -> &BlockWriter<Sink> {
        &self.block_writer
    }

    /// Set the writer state (testing only)
    pub(crate) fn set_state(&mut self, state: WriterState) {
        self.state = state;
    }
}
