//! One adapter for each candidate, all behind the same trait so they are fed the same buffers
//!
//! A row has k data units of `unit` bytes, the caller's, and the chunks the code stores. A
//! systematic code stores m parity chunks and its chunks are numbered data first: chunk `i < k` is
//! data unit `i` and chunk `k + j` is stored chunk `j`. A code that is not systematic stores all
//! k + m chunks and every chunk is a stored one. A lost chunk is named by that number.

use std::ops::Range;

use serde::{Deserialize, Serialize};

pub mod isal;
pub mod raptor;
pub mod rlnc;
pub mod rs_simd;
pub mod rse;
pub mod rusty;
pub mod xor;

/// A k+m layout
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Layout {
    /// The data chunks
    pub k: usize,
    /// The parity chunks, or the chunks beyond k a code that is not systematic stores
    pub m: usize,
}

impl Layout {
    /// A layout of k data chunks and m more
    ///
    /// # Arguments
    ///
    /// * `k` - The data chunks
    /// * `m` - The chunks beyond them
    pub const fn new(k: usize, m: usize) -> Self {
        Layout { k, m }
    }

    /// The total chunks of a stripe
    pub fn n(&self) -> usize {
        self.k + self.m
    }
}

impl std::fmt::Display for Layout {
    /// `k+m`
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}+{}", self.k, self.m)
    }
}

/// What a code stores for a row: how many chunks and how long each is
#[derive(Clone, Copy, Debug)]
pub struct StoredShape {
    /// The chunks stored
    pub count: usize,
    /// The bytes in each
    pub len: usize,
}

/// One candidate, adapted to the harness
pub trait Code {
    /// The candidate's name in every table
    fn name(&self) -> &'static str;

    /// Whether the data chunks are the data as written
    fn systematic(&self) -> bool;

    /// Whether a rebuilt chunk is the lost chunk's bytes again. A recoded chunk is a new
    /// combination, so a rebuild of one is checked by decoding with it instead
    ///
    /// # Arguments
    ///
    /// * `target` - The chunk rebuilt
    fn rebuild_restores(&self, _target: usize) -> bool {
        true
    }

    /// Start any random coefficients again from their seed, so an encode can be repeated
    fn reseed(&mut self) {}

    /// Whether this code can run this layout at this unit, or why it cannot
    ///
    /// # Arguments
    ///
    /// * `layout` - The layout
    /// * `unit` - The size of a data unit
    fn supports(&self, layout: Layout, unit: usize) -> Result<(), String>;

    /// Get ready for a layout and unit, and say what a row stores
    ///
    /// # Arguments
    ///
    /// * `layout` - The layout
    /// * `unit` - The size of a data unit
    fn prepare(&mut self, layout: Layout, unit: usize) -> Result<StoredShape, String>;

    /// Encode a row's data units into its stored chunks
    ///
    /// # Arguments
    ///
    /// * `data` - The k data units
    /// * `stored` - The stored chunks, every one written
    fn encode(&mut self, data: &[&[u8]], stored: &mut [&mut [u8]]) -> Result<(), String>;

    /// Restore the data a reader needs with the chunks in `lost` unreadable
    ///
    /// A systematic code writes each lost data unit back into `data`; one that is not writes every
    /// data unit. A lost chunk's buffer is never read. `Ok(false)` is a set of chunks this code
    /// could not decode from, which is counted and is not an error.
    ///
    /// # Arguments
    ///
    /// * `data` - The k data units
    /// * `stored` - The stored chunks, read only by contract
    /// * `lost` - The chunk numbers that cannot be read
    fn decode(
        &mut self,
        data: &mut [&mut [u8]],
        stored: &mut [&mut [u8]],
        lost: &[usize],
    ) -> Result<bool, String>;

    /// Rebuild one lost chunk in place from the others
    ///
    /// # Arguments
    ///
    /// * `data` - The k data units
    /// * `stored` - The stored chunks
    /// * `target` - The chunk number to rebuild; its buffer is never read
    fn rebuild(
        &mut self,
        data: &mut [&mut [u8]],
        stored: &mut [&mut [u8]],
        target: usize,
    ) -> Result<bool, String>;

    /// Fold a write of part of one data unit into the stored chunks, without the other units
    ///
    /// # Arguments
    ///
    /// * `index` - The data unit written
    /// * `old` - The bytes of `range` before the write
    /// * `new` - The bytes of `range` after it
    /// * `stored` - The stored chunks, of which `range` is updated
    /// * `range` - Where in the unit the write fell
    fn update(
        &mut self,
        index: usize,
        old: &[u8],
        new: &[u8],
        stored: &mut [&mut [u8]],
        range: Range<usize>,
    ) -> Result<(), String>;
}

/// Every candidate, in the order the tables list them
pub fn all() -> Vec<Box<dyn Code>> {
    vec![
        Box::new(xor::Xor::default()),
        Box::new(rusty::Rusty::default()),
        Box::new(isal::Isal::default()),
        Box::new(rse::Rse::default()),
        Box::new(rs_simd::RsSimd::default()),
        Box::new(raptor::Raptor::default()),
        Box::new(rlnc::Rlnc::default()),
        Box::new(rlnc::RlncSystematic::default()),
    ]
}

/// The lost data units of a loss pattern, in order
///
/// # Arguments
///
/// * `k` - The data units
/// * `lost` - The lost chunk numbers
pub fn lost_data(k: usize, lost: &[usize]) -> Vec<usize> {
    let mut out: Vec<usize> = lost.iter().copied().filter(|&i| i < k).collect();
    out.sort_unstable();
    out
}

/// The readable chunks of a row by number, and the lost data units to write
pub type Split<'a> = (Vec<(usize, &'a [u8])>, Vec<&'a mut [u8]>);

/// Split a row into the chunks that can be read and the lost data units to write
///
/// The returned survivors are every readable chunk in chunk order, as `(number, bytes)`, and the
/// outputs are the lost data units in order.
///
/// # Arguments
///
/// * `data` - The k data units
/// * `stored` - The stored chunks
/// * `lost` - The chunk numbers that cannot be read
/// * `systematic` - Whether data units are chunks
pub fn split_row<'a>(
    data: &'a mut [&mut [u8]],
    stored: &'a [&mut [u8]],
    lost: &[usize],
    systematic: bool,
) -> Split<'a> {
    let k = data.len();
    let mut survivors = Vec::with_capacity(k + stored.len());
    let mut outputs = Vec::with_capacity(lost.len());
    if systematic {
        // a data unit is either read or written, never both
        for (i, unit) in data.iter_mut().enumerate() {
            if lost.contains(&i) {
                outputs.push(&mut **unit);
            } else {
                survivors.push((i, &**unit));
            }
        }
        // a stored chunk is chunk k + j
        for (j, chunk) in stored.iter().enumerate() {
            if !lost.contains(&(k + j)) {
                survivors.push((k + j, &**chunk));
            }
        }
    } else {
        // every data unit is written and every stored chunk is a chunk
        outputs.extend(data.iter_mut().map(|unit| &mut **unit));
        for (j, chunk) in stored.iter().enumerate() {
            if !lost.contains(&j) {
                survivors.push((j, &**chunk));
            }
        }
    }
    (survivors, outputs)
}

/// XOR `a` and `b` into `out`
///
/// # Arguments
///
/// * `out` - Where the result goes
/// * `a` - One input
/// * `b` - The other
// the word loop is kept as it was measured, rather than moved to `as_chunks`
#[allow(clippy::chunks_exact_to_as_chunks)]
pub fn xor_into(out: &mut [u8], a: &[u8], b: &[u8]) {
    // whole words first, which the compiler vectorizes
    let mut out_words = out.chunks_exact_mut(8);
    let mut a_words = a.chunks_exact(8);
    let mut b_words = b.chunks_exact(8);
    for ((o, x), y) in (&mut out_words).zip(&mut a_words).zip(&mut b_words) {
        let value =
            u64::from_ne_bytes(x.try_into().unwrap()) ^ u64::from_ne_bytes(y.try_into().unwrap());
        o.copy_from_slice(&value.to_ne_bytes());
    }
    // then whatever is left
    for ((o, x), y) in out_words
        .into_remainder()
        .iter_mut()
        .zip(a_words.remainder())
        .zip(b_words.remainder())
    {
        *o = x ^ y;
    }
}

/// XOR `src` into `out`
///
/// # Arguments
///
/// * `out` - What is xored into
/// * `src` - What is xored in
// the word loop is kept as it was measured, rather than moved to `as_chunks`
#[allow(clippy::chunks_exact_to_as_chunks)]
pub fn xor_in(out: &mut [u8], src: &[u8]) {
    // whole words first, which the compiler vectorizes
    let mut out_words = out.chunks_exact_mut(8);
    let mut src_words = src.chunks_exact(8);
    for (o, x) in (&mut out_words).zip(&mut src_words) {
        let value = u64::from_ne_bytes(o[..].try_into().unwrap())
            ^ u64::from_ne_bytes(x.try_into().unwrap());
        o.copy_from_slice(&value.to_ne_bytes());
    }
    // then whatever is left
    for (o, x) in out_words
        .into_remainder()
        .iter_mut()
        .zip(src_words.remainder())
    {
        *o ^= x;
    }
}
