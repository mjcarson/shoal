//! The checksum of an object's bytes: CRC-64/NVME, and Shoal's own combine
//!
//! [X5](../../../docs/src/object-storage/checksums.md) chose the checksum every object byte is held
//! to from the caller to the disk, recorded on
//! [S18](../../../docs/src/object-storage/contract.md#q21-in-part-the-checksum-2026-10-03):
//!
//! - **the format is the definition, not the crate**: CRC-64/NVME is six parameters and a check
//!   value - the polynomial `0xAD93D23594C93659`, reflected, with an initial value and a final xor
//!   of all ones, and the check value `0xAE8B14860A799888`. Any implementation that gives the
//!   vectors frozen below reads every checksum Shoal writes;
//! - **the bytes are checksummed by `crc-fast` `=1.10.0`**, which picks PCLMULQDQ, AVX-512VL or
//!   VPCLMULQDQ at run time, taken with its default features off;
//! - **two checksums are combined by Shoal's own code**, zlib's method made generic over the width:
//!   `crc(a ‖ b) = crc(a) · x^(8·|b|) mod P ⊕ crc(b)`. The multiplier depends on `|b|` alone, so for
//!   a fixed length it is made once ([`Multiplier`]) and a combine is one multiplication modulo
//!   `P`: 78 ns on titan, where `crc-fast`'s own combine takes 124 to 236 µs.
//!
//! The combine is what lets a caller checksum a frame as it fills it and still name the whole:
//! each frame's checksum folded into the running one, without a second pass over any byte. It is
//! also what lets a slice bind a client's checksum of a unit to the unit's place at M14 without
//! reading the bytes ([S6](../../../docs/src/object-storage/device-store.md)).
//!
//! # Invariants
//!
//! **A checksum here is a persistence format.** `checksum_is_stable_across_builds` freezes X5's
//! digests as literals, computed by two crates on three hosts and five builds; a change that moves
//! one is a new definition, never a new release of this one.

use crc_fast::{CrcAlgorithm, Digest};

/// CRC-64/NVME's check value: the checksum of the nine bytes `123456789`
pub const CHECK: u64 = 0xae8b_1486_0a79_9888;

/// CRC-64/NVME's polynomial in the reflected form the combine multiplies in
///
/// `0xAD93D23594C93659` with its bits reversed.
pub const POLY_REFLECTED: u64 = 0x9a6c_9329_ac4b_c9b5;

/// The checksum of the empty string
///
/// The initial value and the final xor are both all ones, so they cancel.
pub const EMPTY: u64 = 0;

/// The checksum of some bytes, in one call
///
/// # Arguments
///
/// * `bytes` - The bytes to checksum
#[must_use]
pub fn crc64(bytes: &[u8]) -> u64 {
    // crc-fast picks its kernel at run time and allocates nothing
    crc_fast::checksum(CrcAlgorithm::Crc64Nvme, bytes)
}

/// A checksum fed in pieces, equal to [`crc64`] over the pieces joined however they were cut
pub struct Crc64 {
    /// crc-fast's running state
    digest: Digest,
    /// How many bytes it has been fed
    len: u64,
}

impl Crc64 {
    /// A checksum of nothing yet
    #[must_use]
    pub fn new() -> Self {
        Crc64 {
            digest: Digest::new(CrcAlgorithm::Crc64Nvme),
            len: 0,
        }
    }

    /// Feed the next piece
    ///
    /// # Arguments
    ///
    /// * `bytes` - The piece
    pub fn update(&mut self, bytes: &[u8]) {
        // the running state, and how much of the whole it has seen
        self.digest.update(bytes);
        self.len += bytes.len() as u64;
    }

    /// The checksum of every byte fed so far
    #[must_use]
    pub fn finish(&self) -> u64 {
        // finalize reads the state and leaves it running
        self.digest.finalize()
    }

    /// How many bytes it has been fed
    #[must_use]
    pub const fn len(&self) -> u64 {
        self.len
    }

    /// Whether it has been fed nothing
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Default for Crc64 {
    /// A checksum of nothing yet
    fn default() -> Self {
        Crc64::new()
    }
}

/// `a · b` modulo `P`, in the reflected form
///
/// # Arguments
///
/// * `a` - The first factor
/// * `b` - The second factor
const fn multmodp(a: u64, mut b: u64) -> u64 {
    // zero times anything is zero, and the loop below would never end on it
    if a == 0 {
        return 0;
    }
    // walk a's bits from the top, adding b for each one set and multiplying b by x each step
    let mut m = 1u64 << 63;
    let mut p = 0;
    loop {
        if a & m != 0 {
            p ^= b;
            if a & (m - 1) == 0 {
                break;
            }
        }
        m >>= 1;
        b = if b & 1 != 0 {
            (b >> 1) ^ POLY_REFLECTED
        } else {
            b >> 1
        };
    }
    p
}

/// `x^(2^k)` modulo `P` for `k` from 0 to 63, made at compile time
const X2N: [u64; 64] = {
    // x^1 is the second highest bit in the reflected form, and each entry squares the last
    let mut table = [0u64; 64];
    let mut p = 1u64 << 62;
    table[0] = p;
    let mut k = 1;
    while k < 64 {
        p = multmodp(p, p);
        table[k] = p;
        k += 1;
    }
    table
};

/// What a checksum is multiplied by to have a second part of a given length appended to it
///
/// `x^(8·len)` modulo `P`. It depends on the second part's length alone, so a caller combining
/// parts of one length - a frame, a chunk unit - makes it once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Multiplier {
    /// `x^(8·len)` modulo `P`
    op: u64,
    /// The length it appends
    len: u64,
}

impl Multiplier {
    /// The multiplier for a second part of `len` bytes
    ///
    /// # Arguments
    ///
    /// * `len` - The second part's length in bytes
    #[must_use]
    pub const fn for_len(len: u64) -> Self {
        // x^(8·len) is x^(len · 2^3): start at x^0, the top bit, and fold in each set bit's square
        let mut p = 1u64 << 63;
        let mut n = len;
        let mut k = 3;
        while n != 0 {
            if n & 1 != 0 {
                p = multmodp(X2N[k & 63], p);
            }
            n >>= 1;
            k += 1;
        }
        Multiplier { op: p, len }
    }

    /// The length of the second part this multiplier appends
    #[must_use]
    pub const fn len(&self) -> u64 {
        self.len
    }

    /// Whether it appends nothing
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The checksum of `a ‖ b`, from the checksum of each
    ///
    /// # Arguments
    ///
    /// * `a` - The first part's checksum
    /// * `b` - The second part's checksum, of exactly [`Multiplier::len`] bytes
    #[must_use]
    pub const fn combine(&self, a: u64, b: u64) -> u64 {
        // a shifted past b's bytes, then b laid over it
        multmodp(self.op, a) ^ b
    }
}

/// The checksum of `a ‖ b`, from the checksum of each and `b`'s length
///
/// Makes the multiplier for this call; a caller combining many parts of one length makes it once
/// with [`Multiplier::for_len`].
///
/// # Arguments
///
/// * `a` - The first part's checksum
/// * `b` - The second part's checksum
/// * `len_b` - The second part's length in bytes
#[must_use]
pub const fn combine(a: u64, b: u64, len_b: u64) -> u64 {
    Multiplier::for_len(len_b).combine(a, b)
}

/// The checksum of a run of parts, each checksummed on its own and folded in as it comes
///
/// What a sender does with the frames it fills, and what a receiver does with the frames it
/// reads: no byte is read twice. Parts of the length its multiplier was made for cost one
/// multiplication; a part of another length, the last frame of a stream, makes its own.
#[derive(Debug, Clone, Copy)]
pub struct Chain {
    /// The checksum of everything folded in so far
    crc: u64,
    /// How many bytes that is
    len: u64,
    /// The multiplier for the usual part's length
    usual: Multiplier,
}

impl Chain {
    /// A chain of nothing, expecting parts of one length
    ///
    /// # Arguments
    ///
    /// * `part_len` - The length most parts will have
    #[must_use]
    pub const fn new(part_len: u64) -> Self {
        Chain {
            crc: EMPTY,
            len: 0,
            usual: Multiplier::for_len(part_len),
        }
    }

    /// Fold in the checksum of the next part
    ///
    /// # Arguments
    ///
    /// * `crc` - The part's checksum
    /// * `len` - The part's length in bytes
    pub fn push(&mut self, crc: u64, len: u64) {
        // the usual length reuses its multiplier, any other makes one
        self.crc = if len == self.usual.len() {
            self.usual.combine(self.crc, crc)
        } else {
            combine(self.crc, crc, len)
        };
        self.len += len;
    }

    /// Checksum the next part's bytes and fold them in
    ///
    /// # Arguments
    ///
    /// * `bytes` - The part
    pub fn push_bytes(&mut self, bytes: &[u8]) {
        // the part's own checksum, then the fold
        let crc = crc64(bytes);
        self.push(crc, bytes.len() as u64);
    }

    /// The checksum of every part folded in
    #[must_use]
    pub const fn crc(&self) -> u64 {
        self.crc
    }

    /// How many bytes have been folded in
    #[must_use]
    pub const fn len(&self) -> u64 {
        self.len
    }

    /// Whether nothing has been folded in
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }
}

#[cfg(test)]
mod tests {
    use super::{combine, crc64, Chain, Crc64, Multiplier, CHECK, EMPTY};

    /// SplitMix64 as X5's harness wrote it out, so every input here is the bytes X5 checksummed
    struct SplitMix64 {
        /// The generator's state
        state: u64,
    }

    impl SplitMix64 {
        /// The next 64 bits: advance by the golden gamma and mix
        fn next_u64(&mut self) -> u64 {
            self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = self.state;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^ (z >> 31)
        }
    }

    /// `len` bytes from SplitMix64 at a seed, eight at a time and the tail from one more word
    ///
    /// # Arguments
    ///
    /// * `len` - How many bytes
    /// * `seed` - The seed
    fn seeded(len: usize, seed: u64) -> Vec<u8> {
        // whole words first, then the head of one more for whatever is left
        let mut generator = SplitMix64 { state: seed };
        let mut out = vec![0u8; len];
        let (words, tail) = out.as_chunks_mut::<8>();
        for word in words {
            *word = generator.next_u64().to_le_bytes();
        }
        let last = generator.next_u64().to_le_bytes();
        tail.copy_from_slice(&last[..tail.len()]);
        out
    }

    /// FNV-1a over a sequence of slices, as X5's harness folded its short lengths
    ///
    /// # Arguments
    ///
    /// * `parts` - The slices, in order
    fn fnv1a<'a>(parts: impl IntoIterator<Item = &'a [u8]>) -> u64 {
        // the 64-bit offset basis, then each byte xored in and multiplied by the prime
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for part in parts {
            for &byte in part {
                hash ^= u64::from(byte);
                hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
            }
        }
        hash
    }

    /// CRC-64/NVME gives X5's digests on this build, whatever kernel crc-fast chose
    ///
    /// The values are `shoal-spike-checksum/results/summary.md`'s, which `crc-fast` and
    /// `crc64fast-nvme` gave on titan, hyperion and europa from five builds. A checksum is a
    /// persistence format: if this fails, the definition moved, and nothing written before reads.
    #[test]
    fn checksum_is_stable_across_builds() {
        // the published check value, and crc64fast-nvme's 4 KiB of zeros
        assert_eq!(crc64(b"123456789"), CHECK, "the check value");
        assert_eq!(crc64(&[0u8; 4096]), 0x6482_d367_eb22_b64e, "4 KiB of zeros");
        assert_eq!(crc64(&[]), EMPTY, "the empty string");
        // every length from 0 to 257, each from its own seed, folded as X5 folded them
        let shorts: Vec<[u8; 8]> = (0..=257u64)
            .map(|len| crc64(&seeded(len as usize, 0x5100 + len)).to_le_bytes())
            .collect();
        assert_eq!(
            fnv1a(shorts.iter().map(|digest| &digest[..])),
            0x386c_f13d_45d3_100f,
            "lengths 0-257"
        );
        // each unit size X5 measured, from its own seed
        let units = [
            (4096, 0x6169_5664_2011_3d17u64),
            (16 << 10, 0x7574_3428_c915_baef),
            (64 << 10, 0x08ec_8ea0_c8cf_5c22),
            (256 << 10, 0xaa59_8083_a907_ea06),
            (1 << 20, 0xa0c9_c9f9_0458_eafc),
        ];
        for (unit, frozen) in units {
            assert_eq!(
                crc64(&seeded(unit, 0x0417 + unit as u64)),
                frozen,
                "unit of {unit} bytes"
            );
        }
        // and an odd length past every unit, which ends in a partial block
        assert_eq!(
            crc64(&seeded((1 << 20) + 13, 0x0dd)),
            0xb218_7940_c63b_ae90,
            "1 MiB + 13"
        );
    }

    /// A checksum fed in pieces equals one call over the whole, however it was cut
    #[test]
    fn a_checksum_fed_in_pieces_equals_one_call() {
        // a whole that ends in a partial block, cut at many sizes
        let whole = seeded((1 << 20) + 13, 0xc5);
        let once = crc64(&whole);
        for piece in [1, 7, 8, 13, 4096, 65_537, 1 << 20] {
            let mut running = Crc64::new();
            for part in whole.chunks(piece) {
                running.update(part);
            }
            assert_eq!(running.finish(), once, "pieces of {piece}");
            assert_eq!(running.len(), whole.len() as u64);
        }
    }

    /// Shoal's combine of two parts' checksums equals one call over the two joined
    #[test]
    fn the_combine_equals_one_call_over_the_whole() {
        // seeded cuts of 1 MiB + 13 into two parts at any byte, the empty parts included
        let whole = seeded((1 << 20) + 13, 0xc7);
        let once = crc64(&whole);
        let mut draw = SplitMix64 { state: 0xc9 };
        let mut cuts: Vec<usize> = (0..200)
            .map(|_| (draw.next_u64() % (whole.len() as u64 + 1)) as usize)
            .collect();
        cuts.extend([0, 1, whole.len() - 1, whole.len()]);
        for cut in cuts {
            let (a, b) = whole.split_at(cut);
            assert_eq!(
                combine(crc64(a), crc64(b), b.len() as u64),
                once,
                "a cut at {cut}"
            );
        }
        // and crc-fast's own combine agrees, so two implementations of one definition meet
        let (a, b) = whole.split_at(777_777);
        assert_eq!(
            crc_fast::checksum_combine(
                crc_fast::CrcAlgorithm::Crc64Nvme,
                crc64(a),
                crc64(b),
                b.len() as u64
            ),
            once
        );
    }

    /// Sixteen units chained with one multiplier, and a short last part, give the whole's checksum
    #[test]
    fn a_chain_of_units_equals_one_call() {
        for unit in [4096usize, 64 << 10, 1 << 20] {
            // sixteen whole units and a tail of thirteen bytes
            let whole = seeded(16 * unit + 13, 0xcb + unit as u64);
            let mut chain = Chain::new(unit as u64);
            for part in whole.chunks(unit) {
                chain.push_bytes(part);
            }
            assert_eq!(chain.crc(), crc64(&whole), "units of {unit}");
            assert_eq!(chain.len(), whole.len() as u64);
        }
        // a multiplier for nothing leaves a checksum as it was
        assert_eq!(Multiplier::for_len(0).combine(CHECK, EMPTY), CHECK);
    }
}
