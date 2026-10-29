//! The code's own tests: every loss pattern, the bytes it writes frozen against X4's, folds against
//! a fresh encode, and the parity check's summaries

use super::{
    decode_now, encode_now, fold_now, matrix_of, parity_disagrees, summarise, SUMMARY_BYTES,
};

/// SplitMix64, as X4's harness seeds its buffers
struct SplitMix64 {
    /// The state
    state: u64,
}

impl SplitMix64 {
    /// A generator from a seed
    ///
    /// # Arguments
    ///
    /// * `seed` - The seed
    fn new(seed: u64) -> Self {
        SplitMix64 { state: seed }
    }

    /// The next word
    fn next_u64(&mut self) -> u64 {
        // advance by the golden gamma and mix, as the reference implementation does
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Bytes from the generator, eight at a time and the tail from one more word
    ///
    /// # Arguments
    ///
    /// * `len` - How many
    fn bytes(&mut self, len: usize) -> Vec<u8> {
        let mut out = vec![0u8; len];
        let mut words = out.chunks_exact_mut(8);
        for word in &mut words {
            word.copy_from_slice(&self.next_u64().to_le_bytes());
        }
        let tail = words.into_remainder();
        let last = self.next_u64().to_le_bytes();
        tail.copy_from_slice(&last[..tail.len()]);
        out
    }
}

/// FNV-1a over the parity chunks, X4's digest
///
/// # Arguments
///
/// * `parts` - The chunks
fn fnv1a(parts: &[Vec<u8>]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for part in parts {
        for &byte in part {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    hash
}

/// A stripe's data chunks, seeded
///
/// # Arguments
///
/// * `k` - The data chunks
/// * `len` - Their length
/// * `seed` - The seed, one more for each chunk
fn seeded(k: u8, len: usize, seed: u64) -> Vec<Vec<u8>> {
    (0..u64::from(k))
        .map(|i| SplitMix64::new(seed.wrapping_add(i)).bytes(len))
        .collect()
}

/// A stripe's parity, encoded at once
///
/// # Arguments
///
/// * `k` - The data chunks
/// * `m` - The parity chunks
/// * `data` - The data chunks
fn parity_of(k: u8, m: u8, data: &[Vec<u8>]) -> Vec<Vec<u8>> {
    let len = data.first().map_or(0, Vec::len);
    let mut parity = vec![vec![0u8; len]; usize::from(m)];
    let sources: Vec<&[u8]> = data.iter().map(Vec::as_slice).collect();
    let mut outs: Vec<&mut [u8]> = parity.iter_mut().map(Vec::as_mut_slice).collect();
    encode_now(k, m, &sources, &mut outs).expect("an encode");
    parity
}

/// Every subset of `size` of `0..width`, in order
///
/// # Arguments
///
/// * `width` - The positions
/// * `size` - The subset's size
fn subsets(width: u8, size: u8) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut current = Vec::new();
    /// Extend the current subset from a position on
    fn walk(from: u8, width: u8, size: u8, current: &mut Vec<u8>, out: &mut Vec<Vec<u8>>) {
        if current.len() == usize::from(size) {
            out.push(current.clone());
            return;
        }
        for position in from..width {
            current.push(position);
            walk(position + 1, width, size, current, out);
            current.pop();
        }
    }
    walk(0, width, size, &mut current, &mut out);
    out
}

/// For each supported k+m up to a bound, every set of `m` lost chunks is recovered (S8, M18)
///
/// Every layout from 2 to 12 data chunks and 1 to 4 parity chunks, XOR's included, at a length
/// that runs every kernel's widest loop and a tail: each `m` positions lost, data or parity, and
/// made again from the `k` that are left through the product's decode.
#[test]
fn every_loss_pattern_decodes() {
    let len = 64 * 3 + 37;
    let mut patterns = 0usize;
    for k in 2..=12u8 {
        for m in 1..=4u8 {
            let data = seeded(k, len, u64::from(k) * 100 + u64::from(m));
            let parity = parity_of(k, m, &data);
            let chunks: Vec<&Vec<u8>> = data.iter().chain(parity.iter()).collect();
            for lost in subsets(k + m, m) {
                // the k left, as sources, and the m lost made again
                let sources: Vec<(u8, &[u8])> = (0..k + m)
                    .filter(|position| !lost.contains(position))
                    .map(|position| (position, chunks[usize::from(position)].as_slice()))
                    .collect();
                let mut made = vec![vec![0u8; len]; lost.len()];
                let mut outs: Vec<&mut [u8]> = made.iter_mut().map(Vec::as_mut_slice).collect();
                decode_now(k, m, &sources, &lost, &mut outs).expect("a decode");
                for (target, bytes) in lost.iter().zip(&made) {
                    assert_eq!(bytes, chunks[usize::from(*target)], "{k}+{m} lost {lost:?}");
                }
                patterns += 1;
            }
        }
    }
    // every pattern of every layout was tried
    assert_eq!(patterns, 8536);
}

/// Fixed inputs encode to fixed bytes, pinned by literal, on Zen1 and Zen4 builds alike (S8, M18)
///
/// X4's buffers: data unit `i` of a 4 KiB stripe row is SplitMix64 seeded at `0xd16e57 + i`, and the
/// digest is FNV-1a over the parity. X4 recorded each digest's first eight hex digits on five runs
/// across three hosts, two microarchitectures and three builds, the same for ISA-L's C library and
/// for this crate ([X4](../../../../../docs/src/object-storage/erasure-coding-crates.md)); at one
/// parity chunk the format is XOR, whose digest is X4's XOR. The whole digests are frozen here, and
/// every kernel set this cpu has gives them.
#[test]
fn encoding_is_stable_across_builds() {
    // each layout, X4's prefix, and the whole digest frozen when M18 took the crate
    let frozen: [(u8, u8, &str, u64); 5] = [
        (2, 1, "9e4fbbc1", 0x9e4f_bbc1_c039_f723),
        (4, 2, "785f7e13", 0x785f_7e13_8047_126b),
        (6, 3, "e2ac4410", 0xe2ac_4410_b25a_af67),
        (8, 3, "14840a39", 0x1484_0a39_18cd_349e),
        (10, 4, "33ccf46e", 0x33cc_f46e_9012_e88d),
    ];
    for (k, m, prefix, whole) in frozen {
        let data = seeded(k, 4096, 0xd16e57);
        let parity = parity_of(k, m, &data);
        let digest = fnv1a(&parity);
        let shown = format!("{digest:016x}");
        assert!(shown.starts_with(prefix), "{k}+{m}: {shown} is not X4's {prefix}");
        assert_eq!(digest, whole, "{k}+{m}: {shown}");
        // and every kernel set this cpu runs gives the same bytes
        for set in ["scalar", "ssse3", "avx2", "gfni"] {
            let Some(kernels) = rusty_erasure::kernels_named(set) else {
                continue;
            };
            let coder = rusty_erasure::Coder::with_kernels(matrix_of(k, m).expect("a matrix"), kernels)
                .expect("a coder");
            let mut by_set = vec![vec![0u8; 4096]; usize::from(m)];
            let sources: Vec<&[u8]> = data.iter().map(Vec::as_slice).collect();
            let mut outs: Vec<&mut [u8]> = by_set.iter_mut().map(Vec::as_mut_slice).collect();
            coder.encode(&sources, &mut outs).expect("an encode");
            assert_eq!(by_set, parity, "{k}+{m} under {set}");
        }
    }
}

/// One parity chunk is the data's XOR, by the fast kernel and by the coder's matrix alike
#[test]
fn one_parity_chunk_is_the_xor_of_the_data() {
    for k in 2..=10u8 {
        let data = seeded(k, 1000, u64::from(k));
        let parity = parity_of(k, 1, &data);
        // the XOR by hand
        let mut xor = vec![0u8; 1000];
        for chunk in &data {
            for (out, byte) in xor.iter_mut().zip(chunk) {
                *out ^= byte;
            }
        }
        assert_eq!(parity[0], xor, "{k}+1");
        // and the coder made from the matrix gives the same
        let coder = rusty_erasure::coder(matrix_of(k, 1).expect("a matrix")).expect("a coder");
        let mut by_coder = vec![0u8; 1000];
        let sources: Vec<&[u8]> = data.iter().map(Vec::as_slice).collect();
        coder.encode(&sources, &mut [&mut by_coder]).expect("an encode");
        assert_eq!(by_coder, xor, "{k}+1 by the coder");
    }
}

/// A sequence of changes folded into each parity chunk leaves what encoding the data afresh gives
///
/// A parity holder folds `c · (old XOR new)` into its own parity, row by row; after any sequence of
/// changes to any data chunks, over any ranges, each parity chunk equals a fresh encode.
#[test]
fn a_fold_sequence_equals_a_fresh_encode() {
    let len = 2048;
    let mut picks = SplitMix64::new(7);
    for (k, m) in [(2u8, 1u8), (3, 1), (2, 2), (4, 2), (6, 3), (10, 4)] {
        let mut data = seeded(k, len, 99);
        let mut parity = parity_of(k, m, &data);
        for _ in 0..40 {
            // a change to one data chunk, over a range of it
            let chunk = (picks.next_u64() % u64::from(k)) as usize;
            let start = (picks.next_u64() % len as u64) as usize;
            let end = start + 1 + (picks.next_u64() % (len - start) as u64) as usize;
            let new = picks.bytes(end - start);
            let change: Vec<u8> = data[chunk][start..end]
                .iter()
                .zip(&new)
                .map(|(old, new)| old ^ new)
                .collect();
            data[chunk][start..end].copy_from_slice(&new);
            // each parity holder folds it into its own range
            for (row, held) in parity.iter_mut().enumerate() {
                fold_now(k, m, row as u8, chunk as u8, &change, &mut held[start..end]).expect("a fold");
            }
            assert_eq!(parity, parity_of(k, m, &data), "{k}+{m}");
        }
    }
}

/// The data chunks' summaries encode to the parity chunks', and a parity byte wrong with every
/// unit valid is found
#[test]
fn summaries_check_parity_against_data() {
    let len = 3 * SUMMARY_BYTES + 100;
    for (k, m) in [(2u8, 1u8), (4, 2), (6, 3)] {
        let data = seeded(k, len, 5);
        let mut parity = parity_of(k, m, &data);
        // a summary built in pieces is the summary built whole
        let summary_of = |chunk: &[u8]| {
            let mut whole = [0u8; SUMMARY_BYTES];
            summarise(0, chunk, &mut whole);
            let mut pieces = [0u8; SUMMARY_BYTES];
            for (index, piece) in chunk.chunks(1000).enumerate() {
                summarise(index as u64 * 1000, piece, &mut pieces);
            }
            assert_eq!(whole, pieces);
            whole
        };
        let data_summaries: Vec<[u8; SUMMARY_BYTES]> = data.iter().map(|chunk| summary_of(chunk)).collect();
        let check = |parity: &[Vec<u8>]| {
            let held: Vec<[u8; SUMMARY_BYTES]> = parity.iter().map(|chunk| summary_of(chunk)).collect();
            let data_refs: Vec<&[u8]> = data_summaries.iter().map(|s| s.as_slice()).collect();
            let held_refs: Vec<&[u8]> = held.iter().map(|s| s.as_slice()).collect();
            parity_disagrees(k, m, &data_refs, &held_refs).expect("a check")
        };
        assert!(check(&parity).is_empty(), "{k}+{m}");
        // one byte of the last parity chunk wrong
        parity[usize::from(m) - 1][len - 7] ^= 0x5a;
        assert_eq!(check(&parity), vec![m - 1], "{k}+{m}");
    }
}

/// A layout of one data chunk, or of no parity, is no layout the code takes
#[test]
fn copies_are_not_a_code() {
    assert!(matrix_of(1, 2).is_err());
    assert!(matrix_of(4, 0).is_err());
    let data = [vec![1u8; 64]];
    let sources: Vec<&[u8]> = data.iter().map(Vec::as_slice).collect();
    let mut parity = [vec![0u8; 64]];
    let mut outs: Vec<&mut [u8]> = parity.iter_mut().map(Vec::as_mut_slice).collect();
    assert!(encode_now(1, 1, &sources, &mut outs).is_err());
}
