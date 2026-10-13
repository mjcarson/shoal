//! A stripe row's bytes, made where they are sent and never stored by the driver
//!
//! X13 settled how a benchmark's object bytes are made: a stream makes its own, inline, as
//! SplitMix64 in counter mode, faster than any lab device takes them on one core
//! ([X13](../../docs/src/object-storage/benchmark-shape.md)). This is that generator, copied from
//! `shoal-spike/src/driver/generate.rs`, which has no library target: word `i` of a stream is
//! `mix(seed ^ object·γ + (i + 1)·γ)`, so any part of it can be made without the rest.
//!
//! A row's stream is named by its key and by the write that made it, so an overwrite sends bytes
//! that differ from the row's old ones and nothing upstream can have deduplicated them.

/// SplitMix64's increment, the golden ratio's fraction in 64 bits
pub const GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;

/// SplitMix64's finalizer
///
/// # Arguments
///
/// * `state` - The generator's state after a step
#[must_use]
pub fn mix(state: u64) -> u64 {
    // Vigna's finalizer, the one `shoal-loadgen`'s `Seeded` and every spike's `Rng` use
    let mut z = state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// The word a stream holds at an index
///
/// # Arguments
///
/// * `seed` - The write that made the bytes
/// * `object` - The row's key
/// * `index` - Which word
#[must_use]
pub fn word(seed: u64, object: u64, index: u64) -> u64 {
    // the stream's starting state, then the step count to the word
    let key = seed ^ object.wrapping_mul(GAMMA);
    mix(key.wrapping_add(index.wrapping_add(1).wrapping_mul(GAMMA)))
}

/// Fill a buffer with a stream's bytes from an offset, little endian
///
/// # Arguments
///
/// * `seed` - The write that made the bytes
/// * `object` - The row's key
/// * `offset` - Where in the stream the buffer starts
/// * `buf` - The buffer
pub fn fill(seed: u64, object: u64, offset: u64, buf: &mut [u8]) {
    // a head that starts inside a word takes that word's tail
    let mut index = offset / 8;
    let skip = (offset % 8) as usize;
    let mut at = 0;
    if skip != 0 {
        let head = word(seed, object, index).to_le_bytes();
        let take = (8 - skip).min(buf.len());
        buf[..take].copy_from_slice(&head[skip..skip + take]);
        at = take;
        index += 1;
    }
    // whole words, the state stepped by γ for each
    let key = seed ^ object.wrapping_mul(GAMMA);
    let mut state = key.wrapping_add(index.wrapping_add(1).wrapping_mul(GAMMA));
    let (words, tail) = buf[at..].as_chunks_mut::<8>();
    for word in words {
        *word = mix(state).to_le_bytes();
        state = state.wrapping_add(GAMMA);
    }
    // a tail shorter than a word takes that word's head
    if !tail.is_empty() {
        let len = tail.len();
        tail.copy_from_slice(&mix(state).to_le_bytes()[..len]);
    }
}

/// A row's whole bytes
///
/// # Arguments
///
/// * `seed` - The write that made the bytes
/// * `object` - The row's key
/// * `len` - How many bytes
#[must_use]
pub fn make(seed: u64, object: u64, len: usize) -> Vec<u8> {
    // zeroed, then filled from the stream's start
    let mut bytes = vec![0; len];
    fill(seed, object, 0, &mut bytes);
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stream filled in pieces at any offsets is the stream filled whole
    #[test]
    fn pieces_make_the_whole() {
        let whole = make(7, 42, 1000);
        // pieces that start and end inside words
        let mut pieces = vec![0u8; 1000];
        let mut at = 0;
        for len in [3, 13, 8, 1, 200, 775] {
            fill(7, 42, at as u64, &mut pieces[at..at + len]);
            at += len;
        }
        assert_eq!(at, 1000);
        assert_eq!(whole, pieces);
    }

    /// The bytes differ by key and by the write that made them, and repeat for the same pair
    #[test]
    fn bytes_are_named_by_key_and_write() {
        assert_eq!(make(1, 2, 64), make(1, 2, 64));
        assert_ne!(make(1, 2, 64), make(1, 3, 64));
        assert_ne!(make(1, 2, 64), make(2, 2, 64));
    }

    /// Word zero is the published SplitMix64's first output from the stream's state
    #[test]
    fn the_first_word_is_splitmix64s() {
        // SplitMix64 from state s returns mix(s + γ) first
        let state = 5u64 ^ 9u64.wrapping_mul(GAMMA);
        assert_eq!(word(5, 9, 0), mix(state.wrapping_add(GAMMA)));
        let mut buf = [0u8; 8];
        fill(5, 9, 0, &mut buf);
        assert_eq!(u64::from_le_bytes(buf), word(5, 9, 0));
    }
}
