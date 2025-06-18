//! Hashing functionality for Disky files.

use highway::{HighwayHasher, HighwayHash, Key};
use crate::constants::HIGHWAY_HASH_KEY;

/// Calculate the HighwayHash for a chunk of bytes.
///
/// Riegeli uses HighwayHash with a specific key.
pub fn highway_hash(data: &[u8]) -> u64 {
    let mut hasher = HighwayHasher::new(Key(HIGHWAY_HASH_KEY));
    hasher.append(data);
    hasher.finalize64()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::HIGHWAY_HASH_KEY;
    use highway::{HighwayHasher, HighwayHash, Key};

    #[test]
    fn test_hash_consistency() {
        // Test data should always produce the same hash value
        let test_data = b"Riegeli test data for hash verification";
        let hash1 = highway_hash(test_data);
        let hash2 = highway_hash(test_data);
        
        assert_eq!(hash1, hash2, "Hash calculation is not consistent");
    }

    #[test]
    fn test_hash_implementation_matches_highway() {
        // Our wrapper function should produce the same results as the direct HighwayHash implementation
        let test_data = b"Test data for implementation consistency";
        
        // Calculate using our wrapper function
        let our_hash = highway_hash(test_data);
        
        // Calculate using the raw HighwayHash implementation
        let mut hasher = HighwayHasher::new(Key(HIGHWAY_HASH_KEY));
        hasher.append(test_data);
        let raw_hash = hasher.finalize64();
        
        assert_eq!(our_hash, raw_hash, "Our hash function doesn't match the raw implementation");
    }

    #[test]
    fn test_hash_uniqueness() {
        // Different data should produce different hash values
        let data1 = b"Riegeli test data 1";
        let data2 = b"Riegeli test data 2";
        
        let hash1 = highway_hash(data1);
        let hash2 = highway_hash(data2);
        
        assert_ne!(hash1, hash2, "Different data produced the same hash value");
    }

    #[test]
    fn test_hash_avalanche_effect() {
        // A small change in the input should cause a significant change in the output hash
        let data1 = vec![0u8; 100]; // 100 zeros
        let mut data2 = vec![0u8; 100]; // 100 zeros with one byte changed
        data2[50] = 1; // Change one byte in the middle
        
        let hash1 = highway_hash(&data1);
        let hash2 = highway_hash(&data2);
        
        // The hashes should be different
        assert_ne!(hash1, hash2, "Small input change didn't affect hash value");
        
        // The difference should be significant (many bits changed)
        let diff = hash1 ^ hash2;
        let bit_count = diff.count_ones();
        
        // A good hash function should change approximately half the bits
        // For a 64-bit hash, that's around 32 bits
        assert!(bit_count > 10, "Weak avalanche effect: only {} bits changed", bit_count);
    }
}
