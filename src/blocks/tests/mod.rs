mod config_tests;
mod basic_writing_tests;
mod block_boundary_tests;
mod edge_cases_tests;
mod real_world_tests;
mod edge_case_boundary_tests;
mod edge_case_chunk_boundary_bug;
mod reader_tests;
mod corruption_recovery_tests;

// Helper re-exports for tests
#[doc(hidden)]
pub(crate) mod helpers {
    use super::super::*;
    use std::io::Cursor;
    
    // Helper function to safely get the inner buffer from a writer
    pub fn get_buffer<S: std::io::Write + std::io::Seek>(writer: writer::BlockWriter<S>) -> Vec<u8> 
    where S: IntoInner<Output = Vec<u8>> {
        writer.into_inner().into_inner()
    }
    
    // Helper trait to make the above function work with different types
    pub trait IntoInner {
        type Output;
        fn into_inner(self) -> Self::Output;
    }
    
    // Implementation for Cursor<Vec<u8>>
    impl IntoInner for Cursor<Vec<u8>> {
        type Output = Vec<u8>;
        fn into_inner(self) -> Vec<u8> {
            self.into_inner()
        }
    }
}
