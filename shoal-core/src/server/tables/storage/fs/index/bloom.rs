//! The filter a run of the paged archive map keeps in memory
//!
//! A run is a sorted file of pages on disk, and finding that it does not hold a key would cost a
//! page read without this. A blocked Bloom filter answers "not here" for about ninety nine keys in
//! a hundred at ten bits a key, so a lookup of a partition that was never archived - a new key a
//! merge builds from its intents alone, a conditional insert that expects no row - reads nothing
//! ([F76](../../../../../../../docs/src/features/paged-archive-map.md)).

use rkyv::{Archive, Deserialize, Serialize};

/// The bits in one block of the filter, one cache line
const BLOCK_BITS: u64 = 512;

/// The words in one block of the filter
const BLOCK_WORDS: usize = 8;

/// The most probes a key sets in its block
///
/// Seven probes of nine bits each use 63 of a mixed key's 64 bits.
const MAX_PROBES: u32 = 7;

/// Mix a partition key so every bit of the result depends on every bit of the key
///
/// A partition key is already a hash, but its top twelve bits are its tablet and a run holds
/// whole tablets, so the filter is fed a second mix rather than the key's own bits
/// (SplitMix64's finalizer).
///
/// # Arguments
///
/// * `key` - The partition key
#[must_use]
pub fn mix(key: u64) -> u64 {
    // SplitMix64's finalizer, over the key offset by its increment
    let mut mixed = key.wrapping_add(0x9E37_79B9_7F4A_7C15);
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    mixed ^ (mixed >> 31)
}

/// A blocked Bloom filter of partition keys
///
/// Every key sets its probes inside one 512-bit block chosen by its mixed bits, so a lookup
/// touches one cache line. An empty filter, one built at zero bits a key, rules nothing out.
#[derive(Debug, Clone, Default, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct Bloom {
    /// The filter's bits, in blocks of eight words
    words: Vec<u64>,
    /// How many bits each key sets in its block
    probes: u32,
}

impl Bloom {
    /// A filter sized for some keys at some bits a key
    ///
    /// # Arguments
    ///
    /// * `keys` - How many keys it will hold at most
    /// * `bits_per_key` - The bits a key, zero for a filter that rules nothing out
    #[must_use]
    pub fn new(keys: usize, bits_per_key: u32) -> Self {
        // no bits a key, or no keys, is a filter that rules nothing out
        if bits_per_key == 0 || keys == 0 {
            return Bloom::default();
        }
        // whole blocks covering the bits asked for
        let bits = (keys as u64).saturating_mul(u64::from(bits_per_key));
        let blocks = bits.div_ceil(BLOCK_BITS).max(1);
        // the probes that minimise false positives at this many bits a key: ln 2 of them
        let probes = ((f64::from(bits_per_key) * std::f64::consts::LN_2).round() as u32)
            .clamp(1, MAX_PROBES);
        // truncation cannot happen: a filter is bounded far below usize by the keys of one run
        #[allow(clippy::cast_possible_truncation)]
        let words = vec![0u64; blocks as usize * BLOCK_WORDS];
        Bloom { words, probes }
    }

    /// Whether this filter rules nothing out
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.words.is_empty()
    }

    /// The block a mixed key falls in
    ///
    /// # Arguments
    ///
    /// * `mixed` - The mixed key
    fn block(&self, mixed: u64) -> usize {
        // the blocks this filter has, never zero for a filter with words
        let blocks = (self.words.len() / BLOCK_WORDS) as u64;
        // the high half of the product spreads the key over the blocks without a division
        // truncation cannot happen: the result is below the block count
        #[allow(clippy::cast_possible_truncation)]
        let block = ((u128::from(mixed) * u128::from(blocks)) >> 64) as usize;
        block * BLOCK_WORDS
    }

    /// Add a key to this filter
    ///
    /// # Arguments
    ///
    /// * `key` - The partition key
    pub fn insert(&mut self, key: u64) {
        // an empty filter holds nothing and rules nothing out
        if self.words.is_empty() {
            return;
        }
        // the block comes from the key's first mix and the probes from its second
        let mixed = mix(key);
        let block = self.block(mixed);
        let probes = mix(mixed);
        // each probe is nine bits of the second mix, a bit of the block's 512
        for probe in 0..self.probes {
            let bit = (probes >> (9 * probe)) & (BLOCK_BITS - 1);
            // truncation cannot happen: a bit of a block is below 512
            #[allow(clippy::cast_possible_truncation)]
            let word = block + (bit / 64) as usize;
            self.words[word] |= 1 << (bit % 64);
        }
    }

    /// Whether this filter may hold a key: false means it certainly does not
    ///
    /// # Arguments
    ///
    /// * `key` - The partition key
    #[must_use]
    pub fn may_contain(&self, key: u64) -> bool {
        // an empty filter rules nothing out
        if self.words.is_empty() {
            return true;
        }
        // the same block and probes the key was inserted with
        let mixed = mix(key);
        let block = self.block(mixed);
        let probes = mix(mixed);
        // every probe's bit has to be set
        (0..self.probes).all(|probe| {
            let bit = (probes >> (9 * probe)) & (BLOCK_BITS - 1);
            // truncation cannot happen: a bit of a block is below 512
            #[allow(clippy::cast_possible_truncation)]
            let word = block + (bit / 64) as usize;
            self.words[word] & (1 << (bit % 64)) != 0
        })
    }

    /// The bytes this filter holds in memory
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.words.capacity() * std::mem::size_of::<u64>()
    }
}

#[cfg(test)]
mod tests {
    use super::Bloom;

    /// A filter never rules out a key it holds, and rules out most it does not
    ///
    /// A false negative would make a lookup answer that an archived partition does not exist,
    /// which a conditional insert would then commit over. A false positive costs a page read.
    #[test]
    fn the_filter_has_no_false_negatives() {
        // a hundred thousand keys spread like partition keys, at ten bits a key
        let keys: Vec<u64> = (0..100_000u64)
            .map(|key| super::mix(key ^ 0xDEAD_BEEF))
            .collect();
        let mut filter = Bloom::new(keys.len(), 10);
        for key in &keys {
            filter.insert(*key);
        }
        // every key held is found
        assert!(keys.iter().all(|key| filter.may_contain(*key)));
        // and keys never inserted are mostly ruled out: about one percent at ten bits
        let positives = (0..100_000u64)
            .map(|key| super::mix(key ^ 0x0123_4567_89AB))
            .filter(|key| filter.may_contain(*key))
            .count();
        assert!(
            positives < 2_500,
            "{positives} of 100,000 absent keys passed the filter"
        );
        // about a byte and a quarter a key
        assert!(filter.bytes() <= 100_000 * 10 / 8 + 64);
    }

    /// A filter of zero bits a key, or of no keys, rules nothing out
    #[test]
    fn an_empty_filter_rules_nothing_out() {
        let filter = Bloom::new(1000, 0);
        assert!(filter.is_empty());
        assert!(filter.may_contain(7));
        assert_eq!(filter.bytes(), 0);
        let filter = Bloom::new(0, 10);
        assert!(filter.may_contain(7));
    }
}
