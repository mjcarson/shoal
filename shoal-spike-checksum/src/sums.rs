//! Every candidate behind one trait: one call over some bytes, the crate's incremental interface
//! fed in pieces, a combine of two parts' checksums, and a finished checksum continued over more
//! bytes, each where the crate offers it

use std::hash::Hasher as _;

/// A checksum's output, up to 256 bits, as bytes in the order the definition writes them
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Digest {
    /// The output's bytes; only the first `len` are used
    bytes: [u8; 32],
    /// How many bytes the output has
    len: u8,
}

impl Digest {
    /// A digest of the given bytes
    ///
    /// # Arguments
    ///
    /// * `bytes` - The output, at most 32 bytes
    pub fn from_bytes(bytes: &[u8]) -> Self {
        // copy into a fixed array so a digest is Copy and never allocates
        let mut out = [0; 32];
        out[..bytes.len()].copy_from_slice(bytes);
        Digest {
            bytes: out,
            len: bytes.len() as u8,
        }
    }

    /// A 32-bit output, little endian
    ///
    /// # Arguments
    ///
    /// * `value` - The output
    pub fn from_u32(value: u32) -> Self {
        Digest::from_bytes(&value.to_le_bytes())
    }

    /// A 64-bit output, little endian
    ///
    /// # Arguments
    ///
    /// * `value` - The output
    pub fn from_u64(value: u64) -> Self {
        Digest::from_bytes(&value.to_le_bytes())
    }

    /// A 128-bit output, little endian
    ///
    /// # Arguments
    ///
    /// * `value` - The output
    pub fn from_u128(value: u128) -> Self {
        Digest::from_bytes(&value.to_le_bytes())
    }

    /// The output's bytes
    pub fn bytes(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }

    /// The output as a 64-bit integer, for an output of at most eight bytes
    pub fn as_u64(&self) -> u64 {
        // widen the little endian bytes into a u64
        let mut word = [0; 8];
        word[..self.len as usize].copy_from_slice(self.bytes());
        u64::from_le_bytes(word)
    }

    /// The output as hex, most significant byte first for an integer output
    ///
    /// # Arguments
    ///
    /// * `integer` - Whether the output is an integer stored little endian, as every CRC, xxh3 and
    ///   gxhash output here is; BLAKE3's is a byte string and is printed in order
    pub fn hex(&self, integer: bool) -> String {
        // an integer prints most significant first, as its definition's check values are written
        let bytes: Vec<u8> = if integer {
            self.bytes().iter().rev().copied().collect()
        } else {
            self.bytes().to_vec()
        };
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

/// How bytes are cut into pieces for an incremental interface
pub enum Splits {
    /// Pieces of this many bytes, the last one shorter
    Every(usize),
    /// Pieces of these lengths in turn, the last one whatever is left
    At(Vec<usize>),
}

impl Splits {
    /// The pieces of some bytes
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes to cut
    pub fn pieces<'a>(&'a self, bytes: &'a [u8]) -> Pieces<'a> {
        // listed lengths are taken in turn; fixed ones repeat
        let lens = match self {
            Splits::Every(_) => [].iter(),
            Splits::At(lens) => lens.iter(),
        };
        let every = match self {
            Splits::Every(len) => Some((*len).max(1)),
            Splits::At(_) => None,
        };
        Pieces {
            rest: bytes,
            every,
            lens,
        }
    }
}

/// The pieces of some bytes, cut without allocating, so a call's allocations are the crate's
pub struct Pieces<'a> {
    /// What is left to cut
    rest: &'a [u8],
    /// The fixed length of a piece, when the lengths are fixed
    every: Option<usize>,
    /// The listed lengths still to take, when they are listed
    lens: std::slice::Iter<'a, usize>,
}

impl<'a> Iterator for Pieces<'a> {
    type Item = &'a [u8];

    /// The next piece, or none once every byte has been handed out
    fn next(&mut self) -> Option<&'a [u8]> {
        // nothing left is the end, even for an empty input
        if self.rest.is_empty() {
            return None;
        }
        // a fixed length, or the next listed one, or whatever is left
        let take = match self.every {
            Some(len) => len,
            None => self.lens.next().copied().unwrap_or(self.rest.len()),
        }
        .min(self.rest.len());
        let (piece, after) = self.rest.split_at(take);
        self.rest = after;
        Some(piece)
    }
}

/// One candidate
pub trait Sum {
    /// The name every table uses
    fn name(&self) -> &'static str;

    /// The output's width in bits
    fn bits(&self) -> u32;

    /// Whether the output is an integer, printed most significant byte first
    fn integer(&self) -> bool {
        true
    }

    /// One call over all of the bytes
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes to checksum
    fn one_shot(&self, bytes: &[u8]) -> Digest;

    /// The bytes fed in pieces through the crate's incremental interface, or `None` when it has
    /// none
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes to checksum
    /// * `splits` - How to cut them
    fn stream(&self, bytes: &[u8], splits: &Splits) -> Option<Digest>;

    /// The name of the incremental interface, or why there is none
    fn stream_api(&self) -> &'static str;

    /// The checksum of `a ‖ b` from the checksums of `a` and `b` and the length of `b`, or `None`
    /// when the crate offers no combine
    ///
    /// # Arguments
    ///
    /// * `a` - The checksum of the first part
    /// * `b` - The checksum of the second part
    /// * `len_b` - The second part's length in bytes
    fn combine(&self, a: Digest, b: Digest, len_b: u64) -> Option<Digest> {
        let _ = (a, b, len_b);
        None
    }

    /// The name of the combine, or why there is none
    fn combine_api(&self) -> &'static str {
        "none"
    }

    /// A finished checksum continued over more bytes without the bytes it covers, or `None` when
    /// the crate cannot resume from a finished value
    ///
    /// # Arguments
    ///
    /// * `done` - The checksum of the bytes so far
    /// * `more` - The bytes that follow them
    fn append(&self, done: Digest, more: &[u8]) -> Option<Digest> {
        let _ = (done, more);
        None
    }

    /// The name of the resume, or why there is none
    fn append_api(&self) -> &'static str {
        "none"
    }

    /// The kernel the crate says it chose, where it says
    fn kernel(&self) -> Option<String> {
        None
    }
}

/// CRC-32C through the `crc32c` crate
pub struct Crc32c;

impl Sum for Crc32c {
    /// The crate and the definition
    fn name(&self) -> &'static str {
        "crc32c"
    }

    /// Thirty-two bits
    fn bits(&self) -> u32 {
        32
    }

    /// `crc32c::crc32c`
    fn one_shot(&self, bytes: &[u8]) -> Digest {
        Digest::from_u32(crc32c::crc32c(bytes))
    }

    /// `crc32c::crc32c_append` from zero, a piece at a time
    fn stream(&self, bytes: &[u8], splits: &Splits) -> Option<Digest> {
        // the crate's incremental form is the append, started from the empty checksum
        let mut crc = 0;
        for piece in splits.pieces(bytes) {
            crc = crc32c::crc32c_append(crc, piece);
        }
        Some(Digest::from_u32(crc))
    }

    /// The append is the incremental interface
    fn stream_api(&self) -> &'static str {
        "crc32c_append"
    }

    /// `crc32c::crc32c_combine`
    fn combine(&self, a: Digest, b: Digest, len_b: u64) -> Option<Digest> {
        Some(Digest::from_u32(crc32c::crc32c_combine(
            a.as_u64() as u32,
            b.as_u64() as u32,
            len_b as usize,
        )))
    }

    /// The crate's combine
    fn combine_api(&self) -> &'static str {
        "crc32c_combine"
    }

    /// `crc32c::crc32c_append` from a finished checksum
    fn append(&self, done: Digest, more: &[u8]) -> Option<Digest> {
        Some(Digest::from_u32(crc32c::crc32c_append(
            done.as_u64() as u32,
            more,
        )))
    }

    /// The append resumes from a finished value
    fn append_api(&self) -> &'static str {
        "crc32c_append"
    }
}

/// A CRC through the `crc-fast` crate, one of the two algorithms it is a candidate for
pub struct CrcFast {
    /// The algorithm
    algorithm: crc_fast::CrcAlgorithm,
    /// The name every table uses
    name: &'static str,
    /// The output's width
    bits: u32,
}

impl CrcFast {
    /// CRC-32C, which the crate calls CRC-32/ISCSI
    pub fn crc32c() -> Self {
        CrcFast {
            algorithm: crc_fast::CrcAlgorithm::Crc32Iscsi,
            name: "crc-fast crc32c",
            bits: 32,
        }
    }

    /// CRC-64/NVME
    pub fn crc64nvme() -> Self {
        CrcFast {
            algorithm: crc_fast::CrcAlgorithm::Crc64Nvme,
            name: "crc-fast crc64nvme",
            bits: 64,
        }
    }

    /// An output of this algorithm's width
    ///
    /// # Arguments
    ///
    /// * `value` - The crate's output, in a u64 whatever the width
    fn digest(&self, value: u64) -> Digest {
        // a 32-bit CRC is four bytes, not eight
        if self.bits == 32 {
            Digest::from_u32(value as u32)
        } else {
            Digest::from_u64(value)
        }
    }
}

impl Sum for CrcFast {
    /// The crate and the definition
    fn name(&self) -> &'static str {
        self.name
    }

    /// The algorithm's width
    fn bits(&self) -> u32 {
        self.bits
    }

    /// `crc_fast::checksum`
    fn one_shot(&self, bytes: &[u8]) -> Digest {
        self.digest(crc_fast::checksum(self.algorithm, bytes))
    }

    /// `crc_fast::Digest`, a piece at a time
    fn stream(&self, bytes: &[u8], splits: &Splits) -> Option<Digest> {
        // the crate's own incremental state
        let mut digest = crc_fast::Digest::new(self.algorithm);
        for piece in splits.pieces(bytes) {
            digest.update(piece);
        }
        Some(self.digest(digest.finalize()))
    }

    /// The crate's digest
    fn stream_api(&self) -> &'static str {
        "Digest::update"
    }

    /// `crc_fast::checksum_combine`
    fn combine(&self, a: Digest, b: Digest, len_b: u64) -> Option<Digest> {
        Some(self.digest(crc_fast::checksum_combine(
            self.algorithm,
            a.as_u64(),
            b.as_u64(),
            len_b,
        )))
    }

    /// The crate's combine
    fn combine_api(&self) -> &'static str {
        "checksum_combine"
    }

    /// A finished checksum continued: its checksum combined with the suffix's
    fn append(&self, done: Digest, more: &[u8]) -> Option<Digest> {
        // the crate resumes through its combine, which needs the suffix's own checksum
        let more_sum = crc_fast::checksum(self.algorithm, more);
        Some(self.digest(crc_fast::checksum_combine(
            self.algorithm,
            done.as_u64(),
            more_sum,
            more.len() as u64,
        )))
    }

    /// Through the combine
    fn append_api(&self) -> &'static str {
        "checksum_combine with the suffix's checksum"
    }

    /// `crc_fast::get_calculator_target`
    fn kernel(&self) -> Option<String> {
        Some(crc_fast::get_calculator_target(self.algorithm))
    }
}

/// CRC-64/NVME through the `crc64fast-nvme` crate
pub struct Crc64FastNvme;

impl Sum for Crc64FastNvme {
    /// The crate
    fn name(&self) -> &'static str {
        "crc64fast-nvme"
    }

    /// Sixty-four bits
    fn bits(&self) -> u32 {
        64
    }

    /// One `Digest::write` of the whole
    fn one_shot(&self, bytes: &[u8]) -> Digest {
        // the crate has no free function; one write of the whole is its one-shot
        let mut digest = crc64fast_nvme::Digest::new();
        digest.write(bytes);
        Digest::from_u64(digest.sum64())
    }

    /// `Digest::write`, a piece at a time
    fn stream(&self, bytes: &[u8], splits: &Splits) -> Option<Digest> {
        // the crate's own incremental state
        let mut digest = crc64fast_nvme::Digest::new();
        for piece in splits.pieces(bytes) {
            digest.write(piece);
        }
        Some(Digest::from_u64(digest.sum64()))
    }

    /// The crate's digest
    fn stream_api(&self) -> &'static str {
        "Digest::write"
    }
}

/// XXH3 through `xxhash-rust`, at 64 or 128 bits, seed zero
pub struct Xxh3 {
    /// Whether this is the 128-bit output
    wide: bool,
}

impl Xxh3 {
    /// The 64-bit output
    pub fn bits64() -> Self {
        Xxh3 { wide: false }
    }

    /// The 128-bit output
    pub fn bits128() -> Self {
        Xxh3 { wide: true }
    }
}

impl Sum for Xxh3 {
    /// The crate and the width
    fn name(&self) -> &'static str {
        if self.wide { "xxh3-128" } else { "xxh3-64" }
    }

    /// Sixty-four or a hundred and twenty-eight bits
    fn bits(&self) -> u32 {
        if self.wide { 128 } else { 64 }
    }

    /// `xxh3_64` or `xxh3_128`
    fn one_shot(&self, bytes: &[u8]) -> Digest {
        if self.wide {
            Digest::from_u128(xxhash_rust::xxh3::xxh3_128(bytes))
        } else {
            Digest::from_u64(xxhash_rust::xxh3::xxh3_64(bytes))
        }
    }

    /// `Xxh3::update`, a piece at a time
    fn stream(&self, bytes: &[u8], splits: &Splits) -> Option<Digest> {
        // the crate's streaming state, default secret and seed zero, as the one-shot uses
        let mut state = xxhash_rust::xxh3::Xxh3::new();
        for piece in splits.pieces(bytes) {
            state.update(piece);
        }
        if self.wide {
            Some(Digest::from_u128(state.digest128()))
        } else {
            Some(Digest::from_u64(state.digest()))
        }
    }

    /// The crate's streaming state
    fn stream_api(&self) -> &'static str {
        "Xxh3::update"
    }
}

/// BLAKE3 through the `blake3` crate
pub struct Blake3;

impl Sum for Blake3 {
    /// The crate
    fn name(&self) -> &'static str {
        "blake3"
    }

    /// Two hundred and fifty-six bits
    fn bits(&self) -> u32 {
        256
    }

    /// A byte string, printed in order
    fn integer(&self) -> bool {
        false
    }

    /// `blake3::hash`
    fn one_shot(&self, bytes: &[u8]) -> Digest {
        Digest::from_bytes(blake3::hash(bytes).as_bytes())
    }

    /// `Hasher::update`, a piece at a time
    fn stream(&self, bytes: &[u8], splits: &Splits) -> Option<Digest> {
        // the crate's own incremental state
        let mut hasher = blake3::Hasher::new();
        for piece in splits.pieces(bytes) {
            hasher.update(piece);
        }
        Some(Digest::from_bytes(hasher.finalize().as_bytes()))
    }

    /// The crate's hasher
    fn stream_api(&self) -> &'static str {
        "Hasher::update"
    }
}

/// gxhash 2.3.1, the workspace's, at 64 bits and seed zero
pub struct Gxhash2;

impl Sum for Gxhash2 {
    /// The crate and its major
    fn name(&self) -> &'static str {
        "gxhash 2"
    }

    /// Sixty-four bits
    fn bits(&self) -> u32 {
        64
    }

    /// `gxhash::gxhash64`, which is what the workspace's archive records use
    fn one_shot(&self, bytes: &[u8]) -> Digest {
        Digest::from_u64(gxhash::gxhash64(bytes, 0))
    }

    /// `GxHasher::write`, a piece at a time, from the same seed
    fn stream(&self, bytes: &[u8], splits: &Splits) -> Option<Digest> {
        // the crate's only incremental form is its `Hasher`
        let mut hasher = gxhash::GxHasher::with_seed(0);
        for piece in splits.pieces(bytes) {
            hasher.write(piece);
        }
        Some(Digest::from_u64(hasher.finish()))
    }

    /// The `Hasher` impl
    fn stream_api(&self) -> &'static str {
        "GxHasher::write"
    }
}

/// gxhash 3.5.0, the next major, at 64 bits and seed zero
pub struct Gxhash3;

impl Sum for Gxhash3 {
    /// The crate and its major
    fn name(&self) -> &'static str {
        "gxhash 3"
    }

    /// Sixty-four bits
    fn bits(&self) -> u32 {
        64
    }

    /// `gxhash::gxhash64`
    fn one_shot(&self, bytes: &[u8]) -> Digest {
        Digest::from_u64(gxhash3::gxhash64(bytes, 0))
    }

    /// `GxHasher::write`, a piece at a time, from the same seed
    fn stream(&self, bytes: &[u8], splits: &Splits) -> Option<Digest> {
        // the crate's only incremental form is its `Hasher`
        let mut hasher = gxhash3::GxHasher::with_seed(0);
        for piece in splits.pieces(bytes) {
            hasher.write(piece);
        }
        Some(Digest::from_u64(hasher.finish()))
    }

    /// The `Hasher` impl
    fn stream_api(&self) -> &'static str {
        "GxHasher::write"
    }
}

/// CRC-32 (IEEE) through `crc32fast`, already in the workspace's lockfile
pub struct Crc32Fast;

impl Sum for Crc32Fast {
    /// The crate
    fn name(&self) -> &'static str {
        "crc32fast"
    }

    /// Thirty-two bits
    fn bits(&self) -> u32 {
        32
    }

    /// `crc32fast::hash`
    fn one_shot(&self, bytes: &[u8]) -> Digest {
        Digest::from_u32(crc32fast::hash(bytes))
    }

    /// `Hasher::update`, a piece at a time
    fn stream(&self, bytes: &[u8], splits: &Splits) -> Option<Digest> {
        // the crate's own incremental state
        let mut hasher = crc32fast::Hasher::new();
        for piece in splits.pieces(bytes) {
            hasher.update(piece);
        }
        Some(Digest::from_u32(hasher.finalize()))
    }

    /// The crate's hasher
    fn stream_api(&self) -> &'static str {
        "Hasher::update"
    }

    /// `Hasher::combine` over two hashers resumed from the two checksums
    fn combine(&self, a: Digest, b: Digest, len_b: u64) -> Option<Digest> {
        // the crate combines hashers, so each checksum is made a hasher of its length again
        let mut left = crc32fast::Hasher::new_with_initial(a.as_u64() as u32);
        let right = crc32fast::Hasher::new_with_initial_len(b.as_u64() as u32, len_b);
        left.combine(&right);
        Some(Digest::from_u32(left.finalize()))
    }

    /// The crate's combine
    fn combine_api(&self) -> &'static str {
        "Hasher::combine"
    }

    /// `Hasher::new_with_initial` from a finished checksum
    fn append(&self, done: Digest, more: &[u8]) -> Option<Digest> {
        // the crate resumes from a finished value
        let mut hasher = crc32fast::Hasher::new_with_initial(done.as_u64() as u32);
        hasher.update(more);
        Some(Digest::from_u32(hasher.finalize()))
    }

    /// The resume
    fn append_api(&self) -> &'static str {
        "Hasher::new_with_initial"
    }
}

/// Every candidate, in the order every table lists them
pub fn all() -> Vec<Box<dyn Sum>> {
    vec![
        Box::new(Crc32c),
        Box::new(CrcFast::crc32c()),
        Box::new(CrcFast::crc64nvme()),
        Box::new(Crc64FastNvme),
        Box::new(Xxh3::bits64()),
        Box::new(Xxh3::bits128()),
        Box::new(Blake3),
        Box::new(Gxhash2),
        Box::new(Gxhash3),
        Box::new(Crc32Fast),
    ]
}

/// A CRC's combine written out from its definition, zlib's method (`crc32.c`, `multmodp` and
/// `x2nmodp`) made generic over the width, so that what a combine costs can be read apart from
/// what one crate's implementation of it costs
///
/// For a CRC whose initial value equals its final xor, as CRC-32C's, CRC-64/NVME's and CRC-32's
/// do, `crc(a ‖ b) = crc(a) · x^(8·|b|) mod P ⊕ crc(b)`. The multiplier depends on `|b|` alone,
/// so for a fixed unit it is computed once and a combine is one multiplication modulo P.
pub struct CrcMath {
    /// The reflected polynomial
    poly: u64,
    /// The width in bits
    width: u32,
    /// x^(2^k) mod P, for k from 0 to 63
    x2n: [u64; 64],
}

impl CrcMath {
    /// The combine of a reflected CRC
    ///
    /// # Arguments
    ///
    /// * `poly` - The reflected polynomial
    /// * `width` - The width in bits, 32 or 64
    pub fn new(poly: u64, width: u32) -> Self {
        let mut math = CrcMath {
            poly,
            width,
            x2n: [0; 64],
        };
        // x^1 is the second highest bit in the reflected form, and each entry squares the last
        let mut p = 1u64 << (width - 2);
        math.x2n[0] = p;
        for k in 1..64 {
            p = math.multmodp(p, p);
            math.x2n[k] = p;
        }
        math
    }

    /// CRC-32C's combine
    pub fn crc32c() -> Self {
        CrcMath::new(0x82f6_3b78, 32)
    }

    /// CRC-64/NVME's combine
    pub fn crc64nvme() -> Self {
        CrcMath::new(0x9a6c_9329_ac4b_c9b5, 64)
    }

    /// a · b modulo P, in the reflected form
    ///
    /// # Arguments
    ///
    /// * `a` - The first factor
    /// * `b` - The second factor
    pub fn multmodp(&self, a: u64, mut b: u64) -> u64 {
        // zero times anything is zero, and the loop below would never end on it
        if a == 0 {
            return 0;
        }
        // walk a's bits from the top, adding b for each one set, and multiplying b by x each step
        let mut m = 1u64 << (self.width - 1);
        let mut p = 0;
        loop {
            if a & m != 0 {
                p ^= b;
                if a & (m - 1) == 0 {
                    break;
                }
            }
            m >>= 1;
            b = if b & 1 != 0 { (b >> 1) ^ self.poly } else { b >> 1 };
        }
        p
    }

    /// x^(n · 2^k) modulo P
    ///
    /// # Arguments
    ///
    /// * `n` - The multiple
    /// * `k` - The power of two it is a multiple of
    fn x2nmodp(&self, mut n: u64, mut k: usize) -> u64 {
        // start at x^0, which is the top bit in the reflected form
        let mut p = 1u64 << (self.width - 1);
        while n != 0 {
            if n & 1 != 0 {
                p = self.multmodp(self.x2n[k & 63], p);
            }
            n >>= 1;
            k += 1;
        }
        p
    }

    /// The multiplier for a second part of `len` bytes: x^(8·len) modulo P
    ///
    /// # Arguments
    ///
    /// * `len` - The second part's length in bytes
    pub fn operator(&self, len: u64) -> u64 {
        self.x2nmodp(len, 3)
    }

    /// The checksum of `a ‖ b` with the multiplier for `b`'s length already made
    ///
    /// # Arguments
    ///
    /// * `a` - The first part's checksum
    /// * `b` - The second part's checksum
    /// * `op` - The multiplier from [`CrcMath::operator`]
    pub fn combine_op(&self, a: u64, b: u64, op: u64) -> u64 {
        self.multmodp(op, a) ^ b
    }

    /// The checksum of `a ‖ b`, the multiplier made for this call
    ///
    /// # Arguments
    ///
    /// * `a` - The first part's checksum
    /// * `b` - The second part's checksum
    /// * `len_b` - The second part's length in bytes
    pub fn combine(&self, a: u64, b: u64, len_b: u64) -> u64 {
        self.combine_op(a, b, self.operator(len_b))
    }
}
