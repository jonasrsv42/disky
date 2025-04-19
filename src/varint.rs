use crate::error::{Result, DiskyError};
use bytes::{Buf, Bytes};
use bytes::{BufMut, BytesMut};
///
/// Writes a simple varint encoding to the buffer.
#[inline]
pub fn write_vu64(value: u64, buffer: &mut BytesMut) {
    let mut val = value;
    while val >= 0x7F {
        buffer.put_u8(((val as u8) & 0x7F) | 0x80);
        val >>= 7;
    }
    buffer.put_u8(val as u8);
}

/// Reads the next varint encoded u64
#[inline]
pub fn read_vu64(buf: &mut Bytes) -> Result<u64> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;

    loop {
        if buf.remaining() < 1 {
            return Err(DiskyError::Corruption("Truncated varint".to_string()));
        }

        let byte = buf.get_u8();
        result |= ((byte & 0x7F) as u64) << shift;

        if byte & 0x80 == 0 {
            break;
        }

        shift += 7;
        if shift >= 64 {
            return Err(DiskyError::Corruption("Varint too long".to_string()));
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_varint_works() {
        let mut writable = BytesMut::new();

        write_vu64(42, &mut writable);
        write_vu64(1787569, &mut writable);
        write_vu64(7, &mut writable);

        let mut frozen = writable.freeze();

        assert_eq!(read_vu64(&mut frozen).unwrap(), 42);
        assert_eq!(read_vu64(&mut frozen).unwrap(), 1787569);
        assert_eq!(read_vu64(&mut frozen).unwrap(), 7);
    }

    #[test]
    pub fn test_read_corrupted() {
        let mut writable = BytesMut::new();

        for _ in 0..9 {
            writable.put_u8(0x50 | 0x80);
        }

        let mut frozen = writable.freeze();

        assert_eq!(
            read_vu64(&mut frozen).err().unwrap().to_string(),
            "File corruption: Truncated varint".to_string()
        );
    }

    #[test]
    pub fn test_read_too_long() {
        let mut writable = BytesMut::new();

        for _ in 0..20 {
            writable.put_u8(0x50 | 0x80);
        }

        let mut frozen = writable.freeze();

        println!("{:?}", read_vu64(&mut frozen));
        assert_eq!(
            read_vu64(&mut frozen).err().unwrap().to_string(),
            "File corruption: Varint too long".to_string()
        );
    }
}
