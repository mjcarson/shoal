//! X13's generators: seeded bytes at any object and offset
//!
//! A described dataset is repeatable to the byte only if its bytes come from a definition no
//! crate's release can move, the property X5 asked of a checksum. Every generator here is
//! seekable: it makes the bytes at any offset of any object without making the ones before them,
//! which is what checking a ranged read and a write in place both need. Four follow published
//! definitions. The fifth, `stamped`, copies one seeded buffer and stamps every 4 KiB with its
//! object and offset: it is the floor the others are measured against, and its bytes repeat.

use std::sync::Arc;

use aws_lc_rs::cipher::{EncryptingKey, EncryptionContext, UnboundCipherKey, AES_128};
use aws_lc_rs::iv::FixedLength;
use rand_chacha::rand_core::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;

/// SplitMix64's increment, the golden ratio's fraction
pub const GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;

/// The bytes `stamped` copies from, the same length as X11's pattern
pub const STAMPED_LEN: usize = 64 << 20;

/// The block `stamped` stamps and `xoshiro` seeds anew
pub const BLOCK: usize = 4096;

/// A maker of an object's bytes at any offset
pub trait Generator: Send + Sync {
    /// The generator's name in a table
    fn name(&self) -> &'static str;

    /// Whether its bytes follow a published definition
    fn published(&self) -> bool;

    /// Fill a buffer with an object's bytes, starting at an offset
    ///
    /// # Arguments
    ///
    /// * `object` - The object
    /// * `offset` - Where in it the buffer starts
    /// * `buf` - The buffer
    fn fill(&self, object: u64, offset: u64, buf: &mut [u8]);
}

/// SplitMix64's output function
///
/// # Arguments
///
/// * `state` - The state to mix
#[must_use]
pub fn mix(state: u64) -> u64 {
    // Vigna's finalizer, the one `shoal-loadgen`'s `Seeded` and every spike's `Rng` use
    let mut z = state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// The word an object's SplitMix64 stream holds at an index
///
/// Word `i` of object `o` is `mix(seed ^ o·γ + (i + 1)·γ)`: SplitMix64 started from the state
/// `shoal-loadgen`'s `Seeded::at(seed, o)` starts from, read at its `i`th step.
///
/// # Arguments
///
/// * `seed` - The dataset's seed
/// * `object` - The object
/// * `index` - The word
#[must_use]
pub fn splitmix_word(seed: u64, object: u64, index: u64) -> u64 {
    // the object's starting state, then the step count to the word
    let key = seed ^ object.wrapping_mul(GAMMA);
    mix(key.wrapping_add(index.wrapping_add(1).wrapping_mul(GAMMA)))
}

/// Every generator X13 measures, in the order its tables list them
///
/// # Arguments
///
/// * `seed` - The dataset's seed
#[must_use]
pub fn all(seed: u64) -> Vec<Arc<dyn Generator>> {
    vec![
        Arc::new(Stamped::new(seed)),
        Arc::new(SplitMix::new(seed)),
        Arc::new(Xoshiro::new(seed)),
        Arc::new(Chacha8::new(seed)),
        Arc::new(AesCtr::new(seed)),
    ]
}

/// SplitMix64 in counter mode: each 8 bytes one word of the object's stream
pub struct SplitMix {
    /// The dataset's seed
    seed: u64,
}

impl SplitMix {
    /// The generator for a seed
    ///
    /// # Arguments
    ///
    /// * `seed` - The dataset's seed
    #[must_use]
    pub fn new(seed: u64) -> Self {
        SplitMix { seed }
    }
}

impl Generator for SplitMix {
    /// Its name in a table
    fn name(&self) -> &'static str {
        "splitmix"
    }

    /// SplitMix64 is Vigna's published generator
    fn published(&self) -> bool {
        true
    }

    /// Fill a buffer with an object's words from an offset, little endian
    ///
    /// # Arguments
    ///
    /// * `object` - The object
    /// * `offset` - Where in it the buffer starts
    /// * `buf` - The buffer
    fn fill(&self, object: u64, offset: u64, buf: &mut [u8]) {
        // a head that starts inside a word takes that word's tail
        let mut index = offset / 8;
        let skip = (offset % 8) as usize;
        let mut at = 0;
        if skip != 0 {
            let word = splitmix_word(self.seed, object, index).to_le_bytes();
            let take = (8 - skip).min(buf.len());
            buf[..take].copy_from_slice(&word[skip..skip + take]);
            at = take;
            index += 1;
        }
        // whole words, the state stepped by γ for each
        let key = self.seed ^ object.wrapping_mul(GAMMA);
        let mut state = key.wrapping_add(index.wrapping_add(1).wrapping_mul(GAMMA));
        let mut chunks = buf[at..].chunks_exact_mut(8);
        for chunk in &mut chunks {
            chunk.copy_from_slice(&mix(state).to_le_bytes());
            state = state.wrapping_add(GAMMA);
        }
        // a tail shorter than a word takes that word's head
        let tail = chunks.into_remainder();
        if !tail.is_empty() {
            let len = tail.len();
            tail.copy_from_slice(&mix(state).to_le_bytes()[..len]);
        }
    }
}

/// xoshiro256++ in four lanes, each block of 4 KiB seeded anew from the object's SplitMix64 stream
///
/// Word `j` of a block is lane `j % 4`'s output number `j / 4`. Lane `l` of block `b` starts from
/// the object's SplitMix64 words `16b + 4l` to `16b + 4l + 3`, so a block is made without the ones
/// before it. Each lane is Vigna's published xoshiro256++; the lanes and their seeding are X13's.
pub struct Xoshiro {
    /// The dataset's seed
    seed: u64,
}

impl Xoshiro {
    /// The generator for a seed
    ///
    /// # Arguments
    ///
    /// * `seed` - The dataset's seed
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Xoshiro { seed }
    }

    /// Make one block of an object into a buffer of its length
    ///
    /// # Arguments
    ///
    /// * `object` - The object
    /// * `block` - The block's number
    /// * `out` - Exactly one block
    fn block(&self, object: u64, block: u64, out: &mut [u8]) {
        // each lane's state from the object's SplitMix64 stream
        let mut state = [[0u64; 4]; 4];
        for (word, slot) in state.iter_mut().enumerate() {
            for (lane, value) in slot.iter_mut().enumerate() {
                *value = splitmix_word(self.seed, object, block * 16 + lane as u64 * 4 + word as u64);
            }
        }
        // the lanes stepped together, their outputs interleaved
        let mut words = [0u64; BLOCK / 8];
        xoshiro_lanes(&mut state, &mut words);
        for (chunk, word) in out.chunks_exact_mut(8).zip(words.iter()) {
            chunk.copy_from_slice(&word.to_le_bytes());
        }
    }
}

/// Step four xoshiro256++ lanes together, writing their outputs interleaved
///
/// `state[k][l]` is word `k` of lane `l`'s state, laid out so that the compiler steps the four
/// lanes in one vector.
///
/// # Arguments
///
/// * `state` - The lanes' states, by word then lane
/// * `out` - Where the outputs go, four a step
pub fn xoshiro_lanes(state: &mut [[u64; 4]; 4], out: &mut [u64]) {
    // the lanes' words apart, so each operation is one across the four
    let [mut s0, mut s1, mut s2, mut s3] = *state;
    for step in out.chunks_exact_mut(4) {
        for lane in 0..4 {
            // xoshiro256++'s output, then its step
            step[lane] = s0[lane]
                .wrapping_add(s3[lane])
                .rotate_left(23)
                .wrapping_add(s0[lane]);
            let shifted = s1[lane] << 17;
            s2[lane] ^= s0[lane];
            s3[lane] ^= s1[lane];
            s1[lane] ^= s2[lane];
            s0[lane] ^= s3[lane];
            s2[lane] ^= shifted;
            s3[lane] = s3[lane].rotate_left(45);
        }
    }
    *state = [s0, s1, s2, s3];
}

impl Generator for Xoshiro {
    /// Its name in a table
    fn name(&self) -> &'static str {
        "xoshiro"
    }

    /// xoshiro256++ is Vigna's published generator
    fn published(&self) -> bool {
        true
    }

    /// Fill a buffer with an object's blocks from an offset
    ///
    /// # Arguments
    ///
    /// * `object` - The object
    /// * `offset` - Where in it the buffer starts
    /// * `buf` - The buffer
    fn fill(&self, object: u64, offset: u64, buf: &mut [u8]) {
        let mut at = 0;
        let mut pos = offset;
        let mut scratch = [0u8; BLOCK];
        while at < buf.len() {
            let block = pos / BLOCK as u64;
            let inside = (pos % BLOCK as u64) as usize;
            let take = (BLOCK - inside).min(buf.len() - at);
            if inside == 0 && take == BLOCK {
                // a whole block, made where it goes
                self.block(object, block, &mut buf[at..at + BLOCK]);
            } else {
                // part of a block, made aside and cut
                self.block(object, block, &mut scratch);
                buf[at..at + take].copy_from_slice(&scratch[inside..inside + take]);
            }
            at += take;
            pos += take as u64;
        }
    }
}

/// ChaCha with eight rounds, through `rand_chacha`: the object is the stream, the offset the
/// position in it
///
/// The key is the seed's first four SplitMix64 words. `rand_chacha` lays its state out as
/// Bernstein's ChaCha does, a 64-bit block counter and a 64-bit stream, not RFC 8439's.
pub struct Chacha8 {
    /// The generator at the start of stream zero, cloned for every fill
    start: ChaCha8Rng,
}

impl Chacha8 {
    /// The generator for a seed
    ///
    /// # Arguments
    ///
    /// * `seed` - The dataset's seed
    #[must_use]
    pub fn new(seed: u64) -> Self {
        // the key from the seed's own SplitMix64 stream
        let mut key = [0u8; 32];
        for (index, chunk) in key.chunks_exact_mut(8).enumerate() {
            chunk.copy_from_slice(&splitmix_word(seed, 0, index as u64).to_le_bytes());
        }
        Chacha8::with_key(key)
    }

    /// The generator for a key, which a published vector names
    ///
    /// # Arguments
    ///
    /// * `key` - The key
    #[must_use]
    pub fn with_key(key: [u8; 32]) -> Self {
        Chacha8 {
            start: ChaCha8Rng::from_seed(key),
        }
    }
}

impl Generator for Chacha8 {
    /// Its name in a table
    fn name(&self) -> &'static str {
        "chacha8"
    }

    /// ChaCha8 is Bernstein's published cipher at eight rounds
    fn published(&self) -> bool {
        true
    }

    /// Fill a buffer with an object's keystream from an offset
    ///
    /// # Arguments
    ///
    /// * `object` - The object
    /// * `offset` - Where in it the buffer starts
    /// * `buf` - The buffer
    fn fill(&self, object: u64, offset: u64, buf: &mut [u8]) {
        // the object's stream, at the word holding the offset
        let mut rng = self.start.clone();
        rng.set_stream(object);
        rng.set_word_pos(u128::from(offset / 4));
        // a head that starts inside a word takes that word's tail
        let skip = (offset % 4) as usize;
        let mut at = 0;
        if skip != 0 {
            let word = rng.next_u32().to_le_bytes();
            let take = (4 - skip).min(buf.len());
            buf[..take].copy_from_slice(&word[skip..skip + take]);
            at = take;
        }
        rng.fill_bytes(&mut buf[at..]);
    }
}

/// AES-128 in counter mode, through aws-lc-rs: the counter block is the object, big endian, then
/// the offset's block number
///
/// The keystream is the encryption of zeros, so a fill zeroes the buffer first. The key is the
/// seed's first two SplitMix64 words.
pub struct AesCtr {
    /// The key, scheduled once
    key: EncryptingKey,
}

impl AesCtr {
    /// The generator for a seed
    ///
    /// # Arguments
    ///
    /// * `seed` - The dataset's seed
    #[must_use]
    pub fn new(seed: u64) -> Self {
        // the key from the seed's own SplitMix64 stream
        let mut key = [0u8; 16];
        for (index, chunk) in key.chunks_exact_mut(8).enumerate() {
            chunk.copy_from_slice(&splitmix_word(seed, 0, index as u64).to_le_bytes());
        }
        AesCtr::with_key(key)
    }

    /// The generator for a key, which a published vector names
    ///
    /// # Arguments
    ///
    /// * `key` - The key
    #[must_use]
    pub fn with_key(key: [u8; 16]) -> Self {
        let unbound = UnboundCipherKey::new(&AES_128, &key).expect("a 16 byte key");
        AesCtr {
            key: EncryptingKey::ctr(unbound).expect("AES-128-CTR is supported"),
        }
    }

    /// Encrypt a buffer in place from a counter block
    ///
    /// # Arguments
    ///
    /// * `counter` - The first counter block
    /// * `buf` - What to encrypt, in place
    pub fn encrypt(&self, counter: [u8; 16], buf: &mut [u8]) {
        let context = EncryptionContext::Iv128(FixedLength::from(counter));
        self.key
            .less_safe_encrypt(buf, context)
            .expect("counter mode encrypts any length");
    }
}

/// The counter block of an object's block of 16 bytes
///
/// # Arguments
///
/// * `object` - The object
/// * `block` - The block of 16 bytes
#[must_use]
fn counter_block(object: u64, block: u64) -> [u8; 16] {
    // the object, then the block, both big endian, as counter mode counts
    let mut counter = [0u8; 16];
    counter[..8].copy_from_slice(&object.to_be_bytes());
    counter[8..].copy_from_slice(&block.to_be_bytes());
    counter
}

impl Generator for AesCtr {
    /// Its name in a table
    fn name(&self) -> &'static str {
        "aes-ctr"
    }

    /// AES is FIPS 197, and counter mode SP 800-38A
    fn published(&self) -> bool {
        true
    }

    /// Fill a buffer with an object's keystream from an offset
    ///
    /// # Arguments
    ///
    /// * `object` - The object
    /// * `offset` - Where in it the buffer starts
    /// * `buf` - The buffer
    fn fill(&self, object: u64, offset: u64, buf: &mut [u8]) {
        // a head that starts inside a block takes that block's tail
        let mut block = offset / 16;
        let skip = (offset % 16) as usize;
        let mut at = 0;
        if skip != 0 {
            let mut head = [0u8; 16];
            self.encrypt(counter_block(object, block), &mut head);
            let take = (16 - skip).min(buf.len());
            buf[..take].copy_from_slice(&head[skip..skip + take]);
            at = take;
            block += 1;
        }
        // the rest from its first whole block: zeros, encrypted
        let rest = &mut buf[at..];
        if !rest.is_empty() {
            rest.fill(0);
            self.encrypt(counter_block(object, block), rest);
        }
    }
}

/// One seeded buffer copied, each 4 KiB stamped with its object and offset
///
/// Object `o` starts at a block of the buffer chosen by `mix(seed ^ o)`, and wraps. The first
/// sixteen bytes of every block are the object and the block's offset, little endian, so a read
/// of the wrong object or offset is caught. Everything else repeats from object to object.
pub struct Stamped {
    /// The buffer copied from
    pattern: Vec<u8>,
    /// The dataset's seed
    seed: u64,
}

impl Stamped {
    /// The generator for a seed
    ///
    /// # Arguments
    ///
    /// * `seed` - The dataset's seed
    #[must_use]
    pub fn new(seed: u64) -> Self {
        // the buffer, the seed's own SplitMix64 stream
        let mut pattern = vec![0u8; STAMPED_LEN];
        SplitMix::new(seed).fill(u64::MAX, 0, &mut pattern);
        Stamped { pattern, seed }
    }
}

impl Generator for Stamped {
    /// Its name in a table
    fn name(&self) -> &'static str {
        "stamped"
    }

    /// A copied buffer follows no definition but X13's
    fn published(&self) -> bool {
        false
    }

    /// Fill a buffer with an object's bytes from an offset
    ///
    /// # Arguments
    ///
    /// * `object` - The object
    /// * `offset` - Where in it the buffer starts
    /// * `buf` - The buffer
    fn fill(&self, object: u64, offset: u64, buf: &mut [u8]) {
        // where the object starts in the buffer, a whole block in
        let blocks = (STAMPED_LEN / BLOCK) as u64;
        let base = (mix(self.seed ^ object) % blocks) * BLOCK as u64;
        // the copy, in pieces where it wraps
        let mut at = 0;
        while at < buf.len() {
            let from = ((base + offset + at as u64) % STAMPED_LEN as u64) as usize;
            let take = (STAMPED_LEN - from).min(buf.len() - at);
            buf[at..at + take].copy_from_slice(&self.pattern[from..from + take]);
            at += take;
        }
        // the stamps of every block the buffer touches, as much of each as it holds
        let end = offset + buf.len() as u64;
        let mut block = offset / BLOCK as u64 * BLOCK as u64;
        while block < end {
            let mut stamp = [0u8; 16];
            stamp[..8].copy_from_slice(&object.to_le_bytes());
            stamp[8..].copy_from_slice(&block.to_le_bytes());
            for (index, byte) in stamp.iter().enumerate() {
                let pos = block + index as u64;
                if pos >= offset && pos < end {
                    buf[(pos - offset) as usize] = *byte;
                }
            }
            block += BLOCK as u64;
        }
    }
}

/// Whether a generator meets its published reference, or `None` for one with no definition to
/// meet
///
/// Each generator is built from the key or the state its reference names, and made as a driver
/// makes it, so the check covers the generator's own code and not just the crate under it.
///
/// # Arguments
///
/// * `name` - The generator's name
#[must_use]
pub fn meets_reference(name: &str) -> Option<bool> {
    match name {
        // Vigna's `splitmix64.c` at seed 1234567, which is object zero's stream
        "splitmix" => {
            let expected: [u64; 5] = [
                6_457_827_717_110_365_317,
                3_203_168_211_198_807_973,
                9_817_491_932_198_370_423,
                4_593_380_528_125_082_431,
                16_408_922_859_458_223_821,
            ];
            let mut bytes = [0u8; 40];
            SplitMix::new(1_234_567).fill(0, 0, &mut bytes);
            Some(
                bytes
                    .chunks_exact(8)
                    .zip(expected)
                    .all(|(chunk, want)| u64::from_le_bytes(chunk.try_into().expect("eight bytes")) == want),
            )
        }
        // xoshiro256++ from the state 1, 2, 3, 4, in every lane
        "xoshiro" => {
            let expected: [u64; 10] = [
                41_943_041,
                58_720_359,
                3_588_806_011_781_223,
                3_591_011_842_654_386,
                9_228_616_714_210_784_205,
                9_973_669_472_204_895_162,
                14_011_001_112_246_962_877,
                12_406_186_145_184_390_807,
                15_849_039_046_786_891_736,
                10_450_023_813_501_588_000,
            ];
            let mut state = [[1u64; 4], [2; 4], [3; 4], [4; 4]];
            let mut out = [0u64; 40];
            xoshiro_lanes(&mut state, &mut out);
            Some(
                expected
                    .iter()
                    .enumerate()
                    .all(|(step, want)| out[step * 4..step * 4 + 4] == [*want; 4]),
            )
        }
        // draft-strombergson-chacha-test-vectors' TC1: eight rounds, a zero key and nonce
        "chacha8" => {
            let expected = "3e00ef2f895f40d67f5bb8e81f09a5a12c840ec3ce9a7f3b181be188ef711a1e\
                            984ce172b9216f419f445367456d5619314a42a3da86b001387bfdb80e0cfe42";
            let mut bytes = [0u8; 64];
            Chacha8::with_key([0u8; 32]).fill(0, 0, &mut bytes);
            Some(hex(&bytes) == expected)
        }
        // SP 800-38A F.5.1: four blocks from one counter, so the counter's step is checked too
        "aes-ctr" => {
            let key = unhex::<16>("2b7e151628aed2a6abf7158809cf4f3c");
            let counter = unhex::<16>("f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff");
            let plain = "6bc1bee22e409f96e93d7e117393172aae2d8a571e03ac9c9eb76fac45af8e51\
                         30c81c46a35ce411e5fbc1191a0a52eff69f2445df4f9b17ad2b417be66c3710";
            let cipher = "874d6191b620e3261bef6864990db6ce9806f66b7970fdff8617187bb9fffdff\
                          5ae4df3edbd5d35e5b4f09020db03eab1e031dda2fbe03d1792170a0f3009cee";
            let mut bytes = unhex::<64>(plain);
            AesCtr::with_key(key).encrypt(counter, &mut bytes);
            Some(hex(&bytes) == cipher)
        }
        _ => None,
    }
}

/// Whether a generator's fill at any offset is that slice of one fill from zero, and a far fill
/// split anywhere is the same as one, past AES's 2^32 blocks where its counter carries
///
/// # Arguments
///
/// * `generator` - The generator
#[must_use]
pub fn seeks(generator: &dyn Generator) -> bool {
    // near offsets: inside words, blocks and counters, short and long
    let mut whole = vec![0u8; 2 << 20];
    generator.fill(7, 0, &mut whole);
    let near = [(0, 1), (1, 7), (3, 4096), (15, 17), (4095, 2), (4097, 65_536), (777_777, 300_000), (12, 1 << 20)];
    let near_ok = near.iter().all(|&(offset, len)| {
        let mut part = vec![0u8; len];
        generator.fill(7, offset, &mut part);
        part == whole[offset as usize..offset as usize + len]
    });
    // far objects and offsets, split at every kind of boundary
    let far = [(u64::MAX - 3, (1u64 << 36) - 16), (1 << 40, (1 << 40) - 7), (0, (1u64 << 36) - 4096)];
    let far_ok = far.iter().all(|&(object, offset)| {
        let mut one = vec![0u8; 8192];
        generator.fill(object, offset, &mut one);
        [1usize, 16, 17, 4096, 5000].iter().all(|&split| {
            let mut two = vec![0u8; 8192];
            let (left, right) = two.split_at_mut(split);
            generator.fill(object, offset, left);
            generator.fill(object, offset + split as u64, right);
            one == two
        })
    });
    near_ok && far_ok
}

/// The cells whose CRC-64/NVME every host and build has to agree on, as object, offset and length
pub const DIGEST_CELLS: &[(u64, u64, usize)] = &[
    (0, 0, 1 << 20),
    (12_345, (3 << 30) + 5, 100 << 10),
    (u64::MAX, (1 << 40) - 7, 4096),
];

/// The CRC-64/NVME of each of [`DIGEST_CELLS`] for a generator
///
/// # Arguments
///
/// * `generator` - The generator
#[must_use]
pub fn digests(generator: &dyn Generator) -> Vec<u64> {
    DIGEST_CELLS
        .iter()
        .map(|&(object, offset, len)| {
            let mut bytes = vec![0u8; len];
            generator.fill(object, offset, &mut bytes);
            crc_fast::checksum(crc_fast::CrcAlgorithm::Crc64Nvme, &bytes)
        })
        .collect()
}

/// Bytes written as lowercase hex
///
/// # Arguments
///
/// * `bytes` - The bytes
#[must_use]
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Hex read back into a fixed number of bytes
///
/// # Arguments
///
/// * `text` - The hex
#[must_use]
fn unhex<const N: usize>(text: &str) -> [u8; N] {
    let mut out = [0u8; N];
    for (index, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[index * 2..index * 2 + 2], 16).expect("hex");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fill at any offset is the same bytes as that slice of a fill from zero, near and far
    #[test]
    fn every_generator_seeks() {
        for generator in all(0x5831_3133) {
            assert!(seeks(generator.as_ref()), "{}", generator.name());
        }
    }

    /// Two objects, or two seeds, never give the same bytes, and a stamped block carries its
    /// object and offset
    #[test]
    fn objects_and_seeds_differ() {
        for generator in all(1) {
            let (mut a, mut b) = (vec![0u8; 4096], vec![0u8; 4096]);
            generator.fill(1, 0, &mut a);
            generator.fill(2, 0, &mut b);
            assert_ne!(a, b, "{}", generator.name());
        }
        let (one, two) = (all(1), all(2));
        for (left, right) in one.iter().zip(two.iter()) {
            let (mut a, mut b) = (vec![0u8; 4096], vec![0u8; 4096]);
            left.fill(5, 4096, &mut a);
            right.fill(5, 4096, &mut b);
            assert_ne!(a, b, "{}", left.name());
        }
        let mut stamped = vec![0u8; 16];
        Stamped::new(1).fill(9, 8192, &mut stamped);
        assert_eq!(stamped[..8], 9u64.to_le_bytes());
        assert_eq!(stamped[8..], 8192u64.to_le_bytes());
    }

    /// Every published generator meets its reference, and the copy has none to meet
    #[test]
    fn published_generators_meet_their_references() {
        for generator in all(0x5831_3133) {
            let expected = generator.published().then_some(true);
            assert_eq!(meets_reference(generator.name()), expected, "{}", generator.name());
        }
    }

    /// The checksum the cells are digested with is CRC-64/NVME, by its check value
    #[test]
    fn the_digest_is_crc64_nvme() {
        assert_eq!(crc_fast::checksum(crc_fast::CrcAlgorithm::Crc64Nvme, b"123456789"), 0xae8b_1486_0a79_9888);
    }
}
