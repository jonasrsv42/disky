use crate::chunks::{ChunkWriter, RecordsSize};
use crate::chunks::header_writer::{ChunkType, write_chunk_header};
use crate::compression::core::CompressionType;
use crate::error::Result;
use crate::hash::highway_hash;
use crate::varint;
use bytes::{BufMut, Bytes, BytesMut};

pub struct SimpleChunkWriter {
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
    /// Serialize our chunk in-memory to be written to disk.
    ///
    /// According to the Riegeli file format, each chunk consists of:
    /// 1. A 40-byte chunk header
    /// 2. The chunk data
    ///
    /// For a "Simple chunk with records", the chunk data consists of:
    /// - compression_type (byte) - compression type for sizes and values
    /// - compressed_sizes_size (varint64) - size of compressed_sizes
    /// - compressed_sizes (compressed_sizes_size bytes) - compressed buffer with record sizes
    /// - compressed_values (the rest of data) - compressed buffer with record values
    fn serialize_chunk(&mut self) -> Result<Bytes> {
        // First, prepare the chunk data (without the header)
        let mut chunk_data = BytesMut::new();

        // Reserve space for:
        // 1. one byte for `compression_type`.
        // 2. varint for size of record sizes.
        // 3. number of bytes needed for record sizes.
        // 4. Number of bytes needed for records.
        chunk_data.reserve(1 + 9 + self.sizes.len() + self.records.len());

        // Write compression type
        chunk_data.put_u8(self.compression_type.as_byte());

        // Write size of the sizes array as varint
        // For CompressionType::None we don't need a decompressed size prefix
        varint::write_vu64(self.sizes.len() as u64, &mut chunk_data);

        // Write the record sizes
        chunk_data.extend_from_slice(&self.sizes);

        // Write the record values
        // For CompressionType::None we don't need a decompressed size prefix
        chunk_data.extend_from_slice(&self.records);

        // Freeze the chunk data
        let chunk_data = chunk_data.freeze();
        
        // Calculate the hash of the chunk data
        let data_hash = highway_hash(&chunk_data);
        
        // Create the chunk header
        let header = write_chunk_header(
            chunk_data.len() as u64,        // data_size - size of the entire chunk data
            data_hash,                      // data_hash - hash of chunk data
            ChunkType::SimpleRecords,       // chunk_type - 'r' for simple records
            self.num_records,               // num_records - number of records in the chunk
            self.records.len() as u64,      // decoded_data_size - sum of all record sizes (uncompressed)
        )?;
        
        // Combine the header and data into the final chunk
        let mut final_chunk = BytesMut::with_capacity(header.len() + chunk_data.len());
        final_chunk.extend_from_slice(&header);
        final_chunk.extend_from_slice(&chunk_data);
        
        // Reset state for next chunk
        self.records.clear();
        self.sizes.clear();
        self.num_records = 0;

        // Return the complete chunk
        Ok(final_chunk.freeze())
    }
}

impl SimpleChunkWriter {
    pub fn write_record(&mut self, record: &[u8]) -> Result<RecordsSize> {
        varint::write_vu64(record.len() as u64, &mut self.sizes);
        self.records.extend_from_slice(record);

        self.num_records += 1;

        Ok(RecordsSize(self.records.len() as u64))
    }
    pub fn new(compression_type: CompressionType) -> Self {
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
    use crate::chunks::header_writer::CHUNK_HEADER_SIZE;

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
        assert_eq!(
            varint::read_vu64(&mut sizes_copy).unwrap(),
            record1.len() as u64
        );
        assert_eq!(
            varint::read_vu64(&mut sizes_copy).unwrap(),
            record2.len() as u64
        );

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
        let serialized = writer.serialize_chunk().unwrap();

        // Verify the writer state is reset
        assert_eq!(writer.num_records, 0);
        assert_eq!(writer.records.len(), 0);
        assert_eq!(writer.sizes.len(), 0);

        // The serialized chunk should start with a 40-byte header
        assert!(serialized.len() > CHUNK_HEADER_SIZE);
        
        // Extract header and data
        let (header, data) = serialized.split_at(CHUNK_HEADER_SIZE);
        
        // Verify header
        // Chunk type should be 'r' for simple records
        assert_eq!(header[24], ChunkType::SimpleRecords as u8);
        
        // Num records should be 3
        let num_records = 
            (header[25] as u64) |
            ((header[26] as u64) << 8) |
            ((header[27] as u64) << 16) |
            ((header[28] as u64) << 24) |
            ((header[29] as u64) << 32) |
            ((header[30] as u64) << 40) |
            ((header[31] as u64) << 48);
        assert_eq!(num_records, 3);
        
        // Now verify the chunk data (after the header)
        let mut data = Bytes::copy_from_slice(data);

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
        
        // Verify data hash in header matches hash of actual data
        let data_hash = u64::from_le_bytes([
            header[16], header[17], header[18], header[19],
            header[20], header[21], header[22], header[23]
        ]);
        
        // Verify the hash matches
        assert_eq!(data_hash, highway_hash(&serialized.slice(CHUNK_HEADER_SIZE..)));
    }

    #[test]
    fn test_serialize_empty_chunk() {
        let mut writer = SimpleChunkWriter::new(CompressionType::None);

        // Serialize an empty chunk
        let serialized = writer.serialize_chunk().unwrap();

        // The serialized chunk should start with a 40-byte header
        assert!(serialized.len() >= CHUNK_HEADER_SIZE);
        
        // Extract header and data
        let (header, data) = serialized.split_at(CHUNK_HEADER_SIZE);
        
        // Verify header
        // Chunk type should be 'r' for simple records
        assert_eq!(header[24], ChunkType::SimpleRecords as u8);
        
        // Num records should be 0
        let num_records = 
            (header[25] as u64) |
            ((header[26] as u64) << 8) |
            ((header[27] as u64) << 16) |
            ((header[28] as u64) << 24) |
            ((header[29] as u64) << 32) |
            ((header[30] as u64) << 40) |
            ((header[31] as u64) << 48);
        assert_eq!(num_records, 0);
        
        // Parse the chunk data (after the header)
        let mut data = Bytes::copy_from_slice(data);

        // Check compression type
        assert_eq!(data.get_u8(), CompressionType::None.as_byte());

        // Size of sizes array should be 0
        let sizes_len = varint::read_vu64(&mut data).unwrap();
        assert_eq!(sizes_len, 0);

        // No more data should remain as there are no records
        assert_eq!(data.len(), 0);
        
        // Verify data hash in header matches hash of actual data
        let data_hash = u64::from_le_bytes([
            header[16], header[17], header[18], header[19],
            header[20], header[21], header[22], header[23]
        ]);
        
        // Verify the hash matches
        assert_eq!(data_hash, highway_hash(&serialized.slice(CHUNK_HEADER_SIZE..)));
    }

    #[test]
    fn test_multiple_chunk_serialization() {
        let mut writer = SimpleChunkWriter::new(CompressionType::None);

        // Create and serialize first chunk with records of known length
        let record1_1 = b"First chunk special record 1";
        let record1_2 = b"First chunk record 2";

        writer.write_record(record1_1).unwrap();
        writer.write_record(record1_2).unwrap();
        let first_chunk = writer.serialize_chunk().unwrap();

        // Verify writer state was reset
        assert_eq!(writer.num_records, 0);
        assert_eq!(writer.records.len(), 0);
        assert_eq!(writer.sizes.len(), 0);

        // Create and serialize second chunk with records of known length
        let record2_1 = b"Second chunk record 1";
        let record2_2 = b"Second chunk record 2";
        let record2_3 = b"Second chunk record 3";

        writer.write_record(record2_1).unwrap();
        writer.write_record(record2_2).unwrap();
        writer.write_record(record2_3).unwrap();
        let second_chunk = writer.serialize_chunk().unwrap();

        // Verify writer state was reset again
        assert_eq!(writer.num_records, 0);
        assert_eq!(writer.records.len(), 0);
        assert_eq!(writer.sizes.len(), 0);

        // Each chunk should start with a 40-byte header
        assert!(first_chunk.len() > CHUNK_HEADER_SIZE);
        assert!(second_chunk.len() > CHUNK_HEADER_SIZE);
        
        // Extract headers and data
        let (header1, data1_bytes) = first_chunk.split_at(CHUNK_HEADER_SIZE);
        let (header2, data2_bytes) = second_chunk.split_at(CHUNK_HEADER_SIZE);
        
        // Verify headers
        // Chunk type should be 'r' for simple records
        assert_eq!(header1[24], ChunkType::SimpleRecords as u8);
        assert_eq!(header2[24], ChunkType::SimpleRecords as u8);
        
        // Num records should match what we wrote
        let num_records1 = 
            (header1[25] as u64) |
            ((header1[26] as u64) << 8) |
            ((header1[27] as u64) << 16) |
            ((header1[28] as u64) << 24) |
            ((header1[29] as u64) << 32) |
            ((header1[30] as u64) << 40) |
            ((header1[31] as u64) << 48);
        assert_eq!(num_records1, 2);  // First chunk had 2 records
        
        let num_records2 = 
            (header2[25] as u64) |
            ((header2[26] as u64) << 8) |
            ((header2[27] as u64) << 16) |
            ((header2[28] as u64) << 24) |
            ((header2[29] as u64) << 32) |
            ((header2[30] as u64) << 40) |
            ((header2[31] as u64) << 48);
        assert_eq!(num_records2, 3);  // Second chunk had 3 records
        
        // Verify data hash in headers match hash of actual data
        let data_hash1 = u64::from_le_bytes([
            header1[16], header1[17], header1[18], header1[19],
            header1[20], header1[21], header1[22], header1[23]
        ]);
        
        let data_hash2 = u64::from_le_bytes([
            header2[16], header2[17], header2[18], header2[19],
            header2[20], header2[21], header2[22], header2[23]
        ]);
        
        // Verify the hashes match
        assert_eq!(data_hash1, highway_hash(&Bytes::copy_from_slice(data1_bytes)));
        assert_eq!(data_hash2, highway_hash(&Bytes::copy_from_slice(data2_bytes)));

        // Now verify chunk data (after headers)
        // Verify first chunk data
        let mut data1 = Bytes::copy_from_slice(data1_bytes);
        assert_eq!(data1.get_u8(), CompressionType::None.as_byte());
        let sizes_len1 = varint::read_vu64(&mut data1).unwrap();

        // Extract and verify the sizes part of first chunk
        let sizes_data1 = data1.slice(0..sizes_len1 as usize);
        let mut sizes_reader1 = sizes_data1.clone();

        assert_eq!(
            varint::read_vu64(&mut sizes_reader1).unwrap(),
            record1_1.len() as u64
        );
        assert_eq!(
            varint::read_vu64(&mut sizes_reader1).unwrap(),
            record1_2.len() as u64
        );

        // Advance to records data of first chunk
        data1.advance(sizes_len1 as usize);

        // Verify records data follows the sizes in first chunk
        let offset1 = 0;
        let offset2 = offset1 + record1_1.len();
        let end = offset2 + record1_2.len();

        assert_eq!(&data1[offset1..offset2], record1_1);
        assert_eq!(&data1[offset2..end], record1_2);

        // Verify second chunk data
        let mut data2 = Bytes::copy_from_slice(data2_bytes);
        assert_eq!(data2.get_u8(), CompressionType::None.as_byte());
        let sizes_len2 = varint::read_vu64(&mut data2).unwrap();

        // Extract and verify the sizes part of second chunk
        let sizes_data2 = data2.slice(0..sizes_len2 as usize);
        let mut sizes_reader2 = sizes_data2.clone();

        assert_eq!(
            varint::read_vu64(&mut sizes_reader2).unwrap(),
            record2_1.len() as u64
        );
        assert_eq!(
            varint::read_vu64(&mut sizes_reader2).unwrap(),
            record2_2.len() as u64
        );
        assert_eq!(
            varint::read_vu64(&mut sizes_reader2).unwrap(),
            record2_3.len() as u64
        );

        // Advance to records data of second chunk
        data2.advance(sizes_len2 as usize);

        // Verify records data follows the sizes in second chunk
        let offset1 = 0;
        let offset2 = offset1 + record2_1.len();
        let offset3 = offset2 + record2_2.len();
        let end = offset3 + record2_3.len();

        assert_eq!(&data2[offset1..offset2], record2_1);
        assert_eq!(&data2[offset2..offset3], record2_2);
        assert_eq!(&data2[offset3..end], record2_3);
    }

    #[test]
    fn test_state_resets_after_serialization() {
        // This test specifically checks that the writer's state is properly reset after serialization
        let mut writer = SimpleChunkWriter::new(CompressionType::None);

        // Add records to the first chunk
        writer.write_record(b"Record 1").unwrap();
        writer.write_record(b"Record 2").unwrap();

        // Verify state before serialization
        assert_eq!(writer.num_records, 2);
        assert!(writer.records.len() > 0);
        assert!(writer.sizes.len() > 0);

        // Serialize the chunk
        let _ = writer.serialize_chunk().unwrap();

        // Verify state was completely reset
        assert_eq!(writer.num_records, 0);
        assert_eq!(writer.records.len(), 0);
        assert_eq!(writer.sizes.len(), 0);

        // Add a new record to the second chunk
        writer.write_record(b"Record 3").unwrap();

        // Verify new state is as expected
        assert_eq!(writer.num_records, 1);
        assert_eq!(&writer.records[..], b"Record 3");

        // Make sure size was correctly stored
        let mut sizes_copy = writer.sizes.clone().freeze();
        assert_eq!(varint::read_vu64(&mut sizes_copy).unwrap(), 8); // "Record 3" length
    }

    #[test]
    fn test_isolation_between_chunks() {
        // This test verifies that data from one chunk doesn't leak into another
        let mut writer = SimpleChunkWriter::new(CompressionType::None);

        // Write and serialize first chunk
        writer.write_record(b"First chunk record").unwrap();
        let first_chunk = writer.serialize_chunk().unwrap();

        // Write and serialize second chunk with different data
        writer.write_record(b"Second chunk record").unwrap();
        let second_chunk = writer.serialize_chunk().unwrap();

        // Extract headers and data
        let (header1, data1_bytes) = first_chunk.split_at(CHUNK_HEADER_SIZE);
        let (header2, data2_bytes) = second_chunk.split_at(CHUNK_HEADER_SIZE);
        
        // Verify headers
        assert_eq!(header1[24], ChunkType::SimpleRecords as u8);
        assert_eq!(header2[24], ChunkType::SimpleRecords as u8);
        
        // Num records should match
        let num_records1 = 
            (header1[25] as u64) |
            ((header1[26] as u64) << 8) |
            ((header1[27] as u64) << 16) |
            ((header1[28] as u64) << 24) |
            ((header1[29] as u64) << 32) |
            ((header1[30] as u64) << 40) |
            ((header1[31] as u64) << 48);
        assert_eq!(num_records1, 1);  // First chunk had 1 record
        
        let num_records2 = 
            (header2[25] as u64) |
            ((header2[26] as u64) << 8) |
            ((header2[27] as u64) << 16) |
            ((header2[28] as u64) << 24) |
            ((header2[29] as u64) << 32) |
            ((header2[30] as u64) << 40) |
            ((header2[31] as u64) << 48);
        assert_eq!(num_records2, 1);  // Second chunk had 1 record

        // Parse first chunk data (after header)
        let mut data1 = Bytes::copy_from_slice(data1_bytes);
        data1.get_u8(); // Skip compression type
        let sizes_len1 = varint::read_vu64(&mut data1).unwrap();
        data1.advance(sizes_len1 as usize); // Skip sizes

        // Parse second chunk data (after header)
        let mut data2 = Bytes::copy_from_slice(data2_bytes);
        data2.get_u8(); // Skip compression type
        let sizes_len2 = varint::read_vu64(&mut data2).unwrap();
        data2.advance(sizes_len2 as usize); // Skip sizes

        // The chunks should contain only their own data
        assert_eq!(&data1[..], b"First chunk record");
        assert_eq!(&data2[..], b"Second chunk record");

        // Ensure no cross-contamination
        assert!(!data1.starts_with(b"Second"));
        assert!(!data2.starts_with(b"First"));
        
        // Verify data hash in headers match hash of actual data
        let data_hash1 = u64::from_le_bytes([
            header1[16], header1[17], header1[18], header1[19],
            header1[20], header1[21], header1[22], header1[23]
        ]);
        
        let data_hash2 = u64::from_le_bytes([
            header2[16], header2[17], header2[18], header2[19],
            header2[20], header2[21], header2[22], header2[23]
        ]);
        
        // Verify the hashes match
        assert_eq!(data_hash1, highway_hash(&Bytes::copy_from_slice(data1_bytes)));
        assert_eq!(data_hash2, highway_hash(&Bytes::copy_from_slice(data2_bytes)));
    }
}