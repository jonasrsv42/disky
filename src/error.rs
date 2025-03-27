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

//! Error types for the Riegeli format.

use std::io;
use thiserror::Error;

/// The main error type for Riegeli operations.
#[derive(Debug, Error)]
pub enum RiegeliError {
    /// An I/O error occurred.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// The file does not have a valid Riegeli file signature.
    #[error("Invalid file signature")]
    InvalidFileSignature,

    /// A block header hash does not match.
    #[error("Block header hash mismatch")]
    BlockHeaderHashMismatch,

    /// A chunk header hash does not match.
    #[error("Chunk header hash mismatch")]
    ChunkHeaderHashMismatch,

    /// The chunk data hash does not match.
    #[error("Chunk data hash mismatch")]
    ChunkDataHashMismatch,

    /// The file is corrupt and cannot be read.
    #[error("File corruption: {0}")]
    Corruption(String),

    /// Unsupported compression type.
    #[error("Unsupported compression type: {0}")]
    UnsupportedCompressionType(u8),

    /// The chunk type is not recognized.
    #[error("Unknown chunk type: {0}")]
    UnknownChunkType(u8),

    /// End of file was reached unexpectedly.
    #[error("Unexpected end of file")]
    UnexpectedEof,

    /// The file is not a Riegeli file.
    #[error("Not a Riegeli file")]
    NotRiegeliFile,

    /// The file is not a Riegeli file.
    #[error("Writing a closed file")]
    WritingClosedFile,

    /// A general error occurred.
    #[error("{0}")]
    Other(String),
}

/// A specialized Result type for Riegeli operations.
pub type Result<T> = std::result::Result<T, RiegeliError>;
