//! CRC-64/NVME, written from its definition
//!
//! The polynomial `0xAD93D23594C93659`, reflected, with an initial value and a final xor of all
//! ones: the six parameters of the RevEng catalogue that X5 chose for the object store
//! (`docs/src/object-storage/checksums.md`). The product computes it through `crc-fast` from M13;
//! this is a table of its own, a byte at a time, so the ledger shares no code with what it judges
//! while a figure it prints is the one a slice would store for the same bytes.

/// The polynomial, bit-reversed for a reflected register
const REFLECTED_POLY: u64 = 0x9A6C_9329_AC4B_C9B5;

/// The register's value before the first byte, and the xor applied after the last
const ALL_ONES: u64 = u64::MAX;

/// The remainder of every byte value, built once at compile time
const TABLE: [u64; 256] = table();

/// Build the table of remainders for every byte value
const fn table() -> [u64; 256] {
    let mut table = [0u64; 256];
    let mut byte = 0;
    while byte < 256 {
        // shift the byte through the register one bit at a time
        let mut crc = byte as u64;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ REFLECTED_POLY
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[byte] = crc;
        byte += 1;
    }
    table
}

/// The CRC-64/NVME of some bytes
///
/// # Arguments
///
/// * `bytes` - The bytes to checksum
#[must_use]
pub fn crc64_nvme(bytes: &[u8]) -> u64 {
    // start from all ones and fold each byte in through the table
    let mut crc = ALL_ONES;
    for byte in bytes {
        let index = ((crc ^ u64::from(*byte)) & 0xff) as usize;
        crc = TABLE[index] ^ (crc >> 8);
    }
    // the final xor
    crc ^ ALL_ONES
}

#[cfg(test)]
mod tests {
    use super::crc64_nvme;

    /// The two check values X5 held both crates to
    ///
    /// `"123456789"` is the catalogue's check value and 4,096 zero bytes is `crc64fast-nvme`'s
    /// test, both as `docs/src/object-storage/checksums.md` records them.
    #[test]
    fn crc64_nvme_meets_its_check_values() {
        // the catalogue's check value
        assert_eq!(crc64_nvme(b"123456789"), 0xae8b_1486_0a79_9888);
        // a page of zeros
        assert_eq!(crc64_nvme(&[0u8; 4096]), 0x6482_d367_eb22_b64e);
        // and nothing at all is the initial value xored away
        assert_eq!(crc64_nvme(&[]), 0);
    }
}
