use crate::chunks::{ChunkWriter, RecordsSize};
use crate::compression::core::CompressionType;
use crate::error::Result;
use crate::varint;
use bytes::{BufMut, Bytes, BytesMut};

pub(crate) struct SimpleChunkWriter {
    // Compression to use for records and sizes.
    compression_type: CompressionType,

    // Sequence of records.
    records: BytesMut,
    // Sequence of varints of the size of the records.
    sizes: BytesMut,

    // Number of records inside this chunk.
    num_records: u64,
}

impl ChunkWriter for SimpleChunkWriter {
    fn write_record(&mut self, record: &[u8]) -> Result<RecordsSize> {
        varint::write_vu64(record.len() as u64, &mut self.sizes);
        self.records.extend_from_slice(record);

        self.num_records += 1;

        Ok(RecordsSize(self.records.len() as u64))
    }

    /// Serialize our chunk in-memory to be written to disk.
    /// 
    /// Per the Riegeli file format spec for "Simple chunk with records":
    /// - compression_type (byte) - compression type for sizes and values
    /// - compressed_sizes_size (varint64) - size of compressed_sizes
    /// - compressed_sizes (compressed_sizes_size bytes) - compressed buffer with record sizes
    /// - compressed_values (the rest of data) - compressed buffer with record values
    fn serialize_chunk(&mut self) -> Bytes {
        let mut chunk = BytesMut::new();
        
        // Reserve space for:
        // 1. one byte for `compression_type`.
        // 2. varint for size of record sizes.
        // 3. number of bytes needed for record sizes.
        // 4. Number of bytes needed for records.
        chunk.reserve(1 + 9 + self.sizes.len() + self.records.len());

        // Write compression type
        chunk.put_u8(self.compression_type.as_byte());
        
        // Write size of the sizes array as varint
        // For CompressionType::None we don't need a decompressed size prefix
        varint::write_vu64(self.sizes.len() as u64, &mut chunk);
        
        // Write the record sizes
        chunk.extend_from_slice(&self.sizes);
        
        // Write the record values
        // For CompressionType::None we don't need a decompressed size prefix
        chunk.extend_from_slice(&self.records);
        
        // Reset state for next chunk
        let result = chunk.freeze();
        self.records.clear();
        self.sizes.clear();
        self.num_records = 0;
        
        result
    }
}

impl SimpleChunkWriter {
    pub(crate) fn new(compression_type: CompressionType) -> Self {
        SimpleChunkWriter {
            compression_type,
            records: BytesMut::new(),
            sizes: BytesMut::new(),
            num_records: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::varint;
    use bytes::Buf;
    
    #[test]
    fn test_simple_chunk_writer_construction() {
        let writer = SimpleChunkWriter::new(CompressionType::None);
        assert_eq!(writer.num_records, 0);
        assert_eq!(writer.records.len(), 0);
        assert_eq!(writer.sizes.len(), 0);
    }
    
    #[test]
    fn test_write_record() {
        let mut writer = SimpleChunkWriter::new(CompressionType::None);
        
        let record1 = b"Hello, world!";
        let record_size1 = writer.write_record(record1).unwrap();
        assert_eq!(record_size1.0, record1.len() as u64);
        assert_eq!(writer.num_records, 1);
        
        let record2 = b"Another record";
        let record_size2 = writer.write_record(record2).unwrap();
        assert_eq!(record_size2.0, (record1.len() + record2.len()) as u64);
        assert_eq!(writer.num_records, 2);
        
        // Check that sizes contains varints for record lengths
        let mut sizes_copy = writer.sizes.clone().freeze();
        assert_eq!(varint::read_vu64(&mut sizes_copy).unwrap(), record1.len() as u64);
        assert_eq!(varint::read_vu64(&mut sizes_copy).unwrap(), record2.len() as u64);
        
        // Check that records contains concatenated records
        assert_eq!(writer.records.len(), record1.len() + record2.len());
        assert_eq!(&writer.records[..record1.len()], record1);
        assert_eq!(&writer.records[record1.len()..], record2);
    }
    
    #[test]
    fn test_serialize_chunk() {
        let mut writer = SimpleChunkWriter::new(CompressionType::None);
        
        // Add some records
        writer.write_record(b"Record 1").unwrap();
        writer.write_record(b"Record 2").unwrap();
        writer.write_record(b"Record 3").unwrap();
        
        // Serialize the chunk
        let serialized = writer.serialize_chunk();
        
        // Verify the writer state is reset
        assert_eq!(writer.num_records, 0);
        assert_eq!(writer.records.len(), 0);
        assert_eq!(writer.sizes.len(), 0);
        
        // Verify the serialized chunk structure
        let mut data = serialized.clone();
        
        // Check compression type
        assert_eq!(data.get_u8(), CompressionType::None.as_byte());
        
        // Read size of sizes array
        let sizes_len = varint::read_vu64(&mut data).unwrap();
        
        // Extract and verify the sizes part
        let sizes_data = data.slice(0..sizes_len as usize);
        let mut sizes_reader = sizes_data.clone();
        
        assert_eq!(varint::read_vu64(&mut sizes_reader).unwrap(), 8); // "Record 1" length
        assert_eq!(varint::read_vu64(&mut sizes_reader).unwrap(), 8); // "Record 2" length
        assert_eq!(varint::read_vu64(&mut sizes_reader).unwrap(), 8); // "Record 3" length
        
        // Advance to records data
        data.advance(sizes_len as usize);
        
        // Verify records data follows the sizes
        assert_eq!(&data[0..8], b"Record 1");
        assert_eq!(&data[8..16], b"Record 2");
        assert_eq!(&data[16..24], b"Record 3");
    }
    
    #[test]
    fn test_serialize_empty_chunk() {
        let mut writer = SimpleChunkWriter::new(CompressionType::None);
        
        // Serialize an empty chunk
        let serialized = writer.serialize_chunk();
        
        // Parse the chunk
        let mut data = serialized.clone();
        
        // Check compression type
        assert_eq!(data.get_u8(), CompressionType::None.as_byte());
        
        // Size of sizes array should be 0
        let sizes_len = varint::read_vu64(&mut data).unwrap();
        assert_eq!(sizes_len, 0);
        
        // No more data should remain as there are no records
        assert_eq!(data.len(), 0);
    }
}
