//! Parallel reader and writer implementations for Disky.
//!
//! This module contains parallel implementations of the reader and writer
//! that leverage multiple threads for improved performance.
//!
//! These parallel implementations are only available when the `parallel` feature
//! is enabled in your Cargo.toml:
//!
//! ```toml
//! [dependencies]
//! disky = { version = "0.1.0", features = ["parallel"] }
//! ```

pub mod reader;
pub mod writer;
pub mod promise;

pub(crate) mod task_queue;
pub mod resource_pool;

