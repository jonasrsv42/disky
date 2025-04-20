//! Common utilities and constants for Riegeli block operations.

use crate::error::{Result, DiskyError};

/// The size of a block header in bytes.
/// Always 24 bytes: 8 for header_hash, 8 for previous_chunk, 8 for next_chunk.
pub const BLOCK_HEADER_SIZE: u64 = 24;

/// Default riegeli block size is 64 kiB
pub const DEFAULT_BLOCK_SIZE: u64 = 1 << 16;

/// Validates that the block size is large enough to prevent cascading headers.
///
/// The block size must be at least twice the header size to prevent cascading headers.
/// This is because if a header spans across a block boundary, another header would be needed,
/// which could cause an infinite cascade of headers.
///
/// # Arguments
///
/// * `block_size` - The block size to validate
///
/// # Returns
///
/// * `Result<()>` - Ok if valid, or an error explaining why it's invalid
pub fn validate_block_size(block_size: u64) -> Result<()> {
    if block_size < BLOCK_HEADER_SIZE * 2 {
        return Err(DiskyError::Other(
            format!("Block size ({}) must be at least twice the header size ({}) to prevent cascading headers",
                    block_size, BLOCK_HEADER_SIZE)
        ));
    }
    Ok(())
}

/// Returns the usable block size (block size minus block header size).
///
/// # Arguments
///
/// * `block_size` - The block size to calculate from
///
/// # Returns
///
/// * `u64` - The usable size of a block (excluding the header)
pub fn usable_block_size(block_size: u64) -> u64 {
    block_size - BLOCK_HEADER_SIZE
}

/// Checks if a position falls on a block boundary.
///
/// # Arguments
///
/// * `pos` - The position to check
/// * `block_size` - The block size to use for boundary checking
///
/// # Returns
///
/// * `bool` - True if the position is at a block boundary, false otherwise
pub fn is_block_boundary(pos: u64, block_size: u64) -> bool {
    pos % block_size == 0
}

/// Calculate how many bytes remain until the next block boundary.
///
/// # Arguments
///
/// * `pos` - The current position
/// * `block_size` - The block size to use for boundary calculation
///
/// # Returns
///
/// * `u64` - The number of bytes until the next block boundary
pub fn remaining_in_block(pos: u64, block_size: u64) -> u64 {
    block_size - pos % block_size
}

/// Validates block header values.
///
/// According to the Riegeli spec, the header values must satisfy:
/// - previous_chunk % block_size < usable_block_size
/// - next_chunk > 0
/// - (next_chunk - 1) % block_size >= BLOCK_HEADER_SIZE
///
/// # Arguments
///
/// * `previous_chunk` - The previous_chunk value from the header
/// * `next_chunk` - The next_chunk value from the header
/// * `block_size` - The block size to use for validation
///
/// # Returns
///
/// * `Result<()>` - Ok if valid, or an error describing why it's invalid
pub fn validate_header_values(
    previous_chunk: u64,
    next_chunk: u64,
    block_size: u64,
) -> Result<()> {
    let usable_size = usable_block_size(block_size);
    
    if previous_chunk % block_size >= usable_size {
        return Err(DiskyError::InvalidBlockHeader(format!(
            "Invalid previous_chunk value: {} % {} >= {}", 
            previous_chunk, block_size, usable_size
        )));
    }
    
    if next_chunk == 0 {
        return Err(DiskyError::InvalidBlockHeader(
            "Invalid next_chunk value: cannot be zero".to_string()
        ));
    }
    
    if (next_chunk - 1) % block_size < BLOCK_HEADER_SIZE {
        return Err(DiskyError::InvalidBlockHeader(format!(
            "Invalid next_chunk value: ({} - 1) % {} < {}", 
            next_chunk, block_size, BLOCK_HEADER_SIZE
        )));
    }
    
    Ok(())
}
