/// The HighwayHash key used for hashing in Riegeli.
/// This is 'Riegeli/', 'records\n', 'Riegeli/', 'records\n' in 64-bit chunks.
pub const HIGHWAY_HASH_KEY: [u64; 4] = [
    0x2f696c6567656952,
    0x0a7364726f636572,
    0x2f696c6567656952,
    0x0a7364726f636572,
];
