pub(crate) mod core;

#[cfg(feature = "zstd")]
pub(crate) mod zstd;

#[cfg(test)]
mod tests;

pub use core::{CompressionType, create_decompressors_map};
pub(crate) use core::{Compressor, Decompressor, create_compressor};
