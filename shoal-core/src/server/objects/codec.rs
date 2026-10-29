//! The erasure code: what a pool's parity is, and every computation an object path makes with it
//!
//! A k+m pool's parity is Reed-Solomon over GF(2^8) on ISA-L's Cauchy matrix, and plain XOR at one
//! parity chunk, through `rusty_erasure` pinned at `=0.4.1`
//! ([Q20](../../../../docs/src/object-storage/contract.md), [X4](../../../../docs/src/object-storage/erasure-coding-crates.md)).
//! [`matrix_of`] is the one place that says which: what a pool writes with it is on its devices
//! for as long as the pool lives, so it never changes
//! ([F90](../../../../docs/src/features/erasure-coded-pools.md)).
//!
//! Every computation here runs on an object executor, never on a table shard (X9), and takes at
//! most [`STEP`] bytes of input a call of the code before its task may yield, so an executor's
//! latency goal can cut a stripe's encode as it cuts a stage. The coders and decode plans are made
//! once a thread and kept; each executor's calls are counted into its node's [`Counters`].
//!
//! # Invariants
//!
//! **The code a pool is written with is the code it is read with, for ever.** A change to
//! [`matrix_of`], to the crate's version or to its kernels' output fails
//! `encoding_is_stable_across_builds`, whose digests are X4's.
//!
//! **A decode is given exactly `k` sources of one state.** Which chunks those are, and that their
//! labels are the ones a row names together, is the caller's to choose (P10); this module trusts
//! the bytes it is handed.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use rusty_erasure::{Coder, DecodePlan, Matrix};

use crate::shared::protocol::stats::CodecFigures;

/// The most bytes of input one call of the code takes before its task may yield: X9's step
pub const STEP: usize = 64 << 10;

/// The length a chunk is folded to for the parity check by summaries: X12's 4 KiB block
///
/// The code acts on every byte position alike, so the XOR of a chunk's 4 KiB blocks is encoded by
/// the same matrix as the chunk's bytes are, and the data chunks' summaries encode to the parity
/// chunks' ([S11](../../../../docs/src/object-storage/scrub.md#the-parity-check)).
pub const SUMMARY_BYTES: usize = 4096;

/// Why the code could not do what it was asked: a shape it refused, never a byte it misread
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodecError(pub String);

impl std::fmt::Display for CodecError {
    /// Say what the code refused
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write to
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the erasure code refused: {}", self.0)
    }
}

impl std::error::Error for CodecError {}

/// What a node's object executors did with the code, counted as it happens
///
/// Each call counts the bytes it took in and the time the call took, the yields between calls
/// left out, so a rate read from two snapshots is a core's own.
#[derive(Debug, Default)]
pub struct Counters {
    /// Bytes of data encoded into parity
    encoded_bytes: AtomicU64,
    /// Nanoseconds the encodes took
    encode_ns: AtomicU64,
    /// Bytes of surviving chunks decoded
    decoded_bytes: AtomicU64,
    /// Nanoseconds the decodes took
    decode_ns: AtomicU64,
    /// Bytes of change folded into parity
    folded_bytes: AtomicU64,
    /// Nanoseconds the folds took
    fold_ns: AtomicU64,
    /// Bytes folded into summaries
    summarised_bytes: AtomicU64,
}

impl Counters {
    /// What the counters say now, with the kernel set this cpu runs the code with
    #[must_use]
    pub fn figures(&self) -> CodecFigures {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        CodecFigures {
            encoded_bytes: load(&self.encoded_bytes),
            encode_ns: load(&self.encode_ns),
            decoded_bytes: load(&self.decoded_bytes),
            decode_ns: load(&self.decode_ns),
            folded_bytes: load(&self.folded_bytes),
            fold_ns: load(&self.fold_ns),
            summarised_bytes: load(&self.summarised_bytes),
            kernel: rusty_erasure::best_kernels().name.to_string(),
        }
    }
}

/// What one call of the code was, for its counters
#[derive(Debug, Clone, Copy)]
enum Work {
    /// An encode
    Encode,
    /// A decode
    Decode,
    /// A fold of a change into parity
    Fold,
}

thread_local! {
    /// The counters of the node whose executor this thread is, once it installs them
    static COUNTERS: RefCell<Option<Arc<Counters>>> = const { RefCell::new(None) };
    /// Each layout's coder, made once a thread
    static CODERS: RefCell<HashMap<(u8, u8), Rc<Coder>>> = RefCell::new(HashMap::new());
    /// Each layout's single parity rows, a coder each, for a parity holder's fold
    static ROWS: RefCell<HashMap<(u8, u8, u8), Rc<Coder>>> = RefCell::new(HashMap::new());
    /// Each loss pattern's decode plan: the layout, the sources present and the targets
    static PLANS: RefCell<HashMap<(u8, u8, u32, Vec<u8>), Rc<DecodePlan>>> = RefCell::new(HashMap::new());
}

/// Count this thread's calls of the code into a node's counters
///
/// Each object executor installs its node's at its start, so a process running several nodes, as
/// a test does, keeps each node's apart.
///
/// # Arguments
///
/// * `counters` - The node's counters
pub fn install(counters: Arc<Counters>) {
    COUNTERS.with(|slot| *slot.borrow_mut() = Some(counters));
}

/// Count one call of the code
///
/// # Arguments
///
/// * `work` - What it was
/// * `bytes` - The bytes it took in
/// * `started` - When it started
fn count(work: Work, bytes: usize, started: Instant) {
    let ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    COUNTERS.with(|slot| {
        if let Some(counters) = slot.borrow().as_ref() {
            let (total, time) = match work {
                Work::Encode => (&counters.encoded_bytes, &counters.encode_ns),
                Work::Decode => (&counters.decoded_bytes, &counters.decode_ns),
                Work::Fold => (&counters.folded_bytes, &counters.fold_ns),
            };
            total.fetch_add(bytes as u64, Ordering::Relaxed);
            time.fetch_add(ns, Ordering::Relaxed);
        }
    });
}

/// The encode matrix of a layout: `k` rows of identity, then its `m` parity rows
///
/// At one parity chunk the row is all ones, so the parity is the XOR of the data; at more it is
/// ISA-L's Cauchy matrix (`gf_gen_cauchy1_matrix`), which inverts at every `k` and `m`. This is
/// the format a pool's parity is in on its devices.
///
/// # Arguments
///
/// * `k` - The data chunks
/// * `m` - The parity chunks
///
/// # Errors
///
/// A layout the code has no matrix for.
pub fn matrix_of(k: u8, m: u8) -> Result<Matrix, CodecError> {
    let (k, m) = (usize::from(k), usize::from(m));
    if k < 2 || m == 0 {
        return Err(CodecError(format!("{k}+{m} is no erasure coded layout")));
    }
    // ISA-L's Cauchy rows past one parity chunk
    if m > 1 {
        return Matrix::cauchy(k, m).map_err(|error| CodecError(format!("{k}+{m}: {error:?}")));
    }
    // and at one, identity above a row of ones: Cauchy's single row is not XOR's
    let mut rows = vec![0u8; (k + 1) * k];
    for i in 0..k {
        rows[i * k + i] = 1;
    }
    rows[k * k..].fill(1);
    Matrix::from_bytes(k + 1, k, rows).map_err(|error| CodecError(format!("{k}+1: {error:?}")))
}

/// A layout's coder, made once a thread on the best kernels this cpu has
///
/// # Arguments
///
/// * `k` - The data chunks
/// * `m` - The parity chunks
fn coder(k: u8, m: u8) -> Result<Rc<Coder>, CodecError> {
    if let Some(coder) = CODERS.with(|coders| coders.borrow().get(&(k, m)).cloned()) {
        return Ok(coder);
    }
    let made = Rc::new(
        rusty_erasure::coder(matrix_of(k, m)?).map_err(|error| CodecError(format!("{k}+{m}: {error:?}")))?,
    );
    CODERS.with(|coders| coders.borrow_mut().insert((k, m), made.clone()));
    Ok(made)
}

/// A coder for one parity row of a layout alone, made once a thread
///
/// A parity holder folds a change into its own parity, and the crate's update folds into every
/// parity row it was made with, so each row has a coder of its own: identity above that one row.
///
/// # Arguments
///
/// * `k` - The data chunks
/// * `m` - The parity chunks
/// * `row` - The parity row, `0..m`
fn row_coder(k: u8, m: u8, row: u8) -> Result<Rc<Coder>, CodecError> {
    if let Some(coder) = ROWS.with(|rows| rows.borrow().get(&(k, m, row)).cloned()) {
        return Ok(coder);
    }
    if row >= m {
        return Err(CodecError(format!("{k}+{m} has no parity row {row}")));
    }
    // identity, then the layout's row
    let full = matrix_of(k, m)?;
    let width = usize::from(k);
    let mut rows = vec![0u8; (width + 1) * width];
    for i in 0..width {
        rows[i * width + i] = 1;
    }
    let start = (width + usize::from(row)) * width;
    rows[width * width..].copy_from_slice(&full.as_bytes()[start..start + width]);
    let matrix = Matrix::from_bytes(width + 1, width, rows).map_err(|error| CodecError(format!("{error:?}")))?;
    let made = Rc::new(rusty_erasure::coder(matrix).map_err(|error| CodecError(format!("{error:?}")))?);
    ROWS.with(|rows| rows.borrow_mut().insert((k, m, row), made.clone()));
    Ok(made)
}

/// How many bytes of each source one call of the code takes: [`STEP`] of input across them all
///
/// # Arguments
///
/// * `sources` - How many sources a call reads
fn column(sources: usize) -> usize {
    // a multiple of 64, so every kernel runs its widest loop, and never none
    ((STEP / sources.max(1)) & !63).max(64)
}

/// Encode a range of columns of a stripe's data into its parity
///
/// # Arguments
///
/// * `k` - The data chunks
/// * `m` - The parity chunks
/// * `data` - The data chunks, each as long as the parity
/// * `parity` - The parity chunks, overwritten
/// * `from` - The first column
/// * `to` - The column past the last
fn encode_columns(k: u8, m: u8, data: &[&[u8]], parity: &mut [&mut [u8]], from: usize, to: usize) -> Result<(), CodecError> {
    let started = Instant::now();
    let sources: Vec<&[u8]> = data.iter().map(|chunk| &chunk[from..to]).collect();
    // one parity chunk is the data's XOR, the fastest kernel there is for it
    if m == 1 {
        let out = parity
            .first_mut()
            .ok_or_else(|| CodecError("no parity chunk to encode into".to_string()))?;
        rusty_erasure::raid::xor_gen(&sources, &mut out[from..to]).map_err(|error| CodecError(format!("{error:?}")))?;
    } else {
        let mut outs: Vec<&mut [u8]> = parity.iter_mut().map(|chunk| &mut chunk[from..to]).collect();
        coder(k, m)?
            .encode(&sources, &mut outs)
            .map_err(|error| CodecError(format!("{error:?}")))?;
    }
    count(Work::Encode, (to - from) * sources.len(), started);
    Ok(())
}

/// Check a stripe's chunks are of one length and as many as its layout has
///
/// # Arguments
///
/// * `k` - The data chunks
/// * `m` - The parity chunks
/// * `data` - The data chunks
/// * `parity` - The parity chunks
fn check_shape(k: u8, m: u8, data: &[&[u8]], parity: &[&mut [u8]]) -> Result<usize, CodecError> {
    if k < 2 || m == 0 {
        return Err(CodecError(format!("{k}+{m} is no erasure coded layout")));
    }
    if data.len() != usize::from(k) || parity.len() != usize::from(m) {
        return Err(CodecError(format!(
            "{k}+{m} was handed {} data chunks and {} parity chunks",
            data.len(),
            parity.len()
        )));
    }
    let len = parity.first().map_or(0, |chunk| chunk.len());
    if data.iter().any(|chunk| chunk.len() != len) || parity.iter().any(|chunk| chunk.len() != len) {
        return Err(CodecError("a stripe's chunks were handed at different lengths".to_string()));
    }
    Ok(len)
}

/// Encode a stripe's data into its parity, yielding between steps
///
/// Every chunk is as long as the parity: a short data chunk is handed padded with zeros, which is
/// what its absent bytes are ([S8](../../../../docs/src/object-storage/erasure-coding.md#whole-stripes-and-short-ones)).
///
/// # Arguments
///
/// * `k` - The data chunks
/// * `m` - The parity chunks
/// * `data` - The data chunks
/// * `parity` - The parity chunks, overwritten
///
/// # Errors
///
/// A shape the layout does not have.
pub async fn encode(k: u8, m: u8, data: &[&[u8]], parity: &mut [&mut [u8]]) -> Result<(), CodecError> {
    let len = check_shape(k, m, data, parity)?;
    let step = column(usize::from(k));
    let mut from = 0;
    // a step of the code, then a yield the executor's latency goal may take
    while from < len {
        let to = (from + step).min(len);
        encode_columns(k, m, data, parity, from, to)?;
        glommio::yield_if_needed().await;
        from = to;
    }
    Ok(())
}

/// Encode a stripe's data into its parity at once, off any executor: for tests and checks
///
/// # Arguments
///
/// * `k` - The data chunks
/// * `m` - The parity chunks
/// * `data` - The data chunks
/// * `parity` - The parity chunks, overwritten
///
/// # Errors
///
/// A shape the layout does not have.
pub fn encode_now(k: u8, m: u8, data: &[&[u8]], parity: &mut [&mut [u8]]) -> Result<(), CodecError> {
    let len = check_shape(k, m, data, parity)?;
    encode_columns(k, m, data, parity, 0, len)
}

/// A loss pattern's decode plan, made once a thread
///
/// # Arguments
///
/// * `k` - The data chunks
/// * `m` - The parity chunks
/// * `present` - A bit for each source position
/// * `targets` - The positions to make, in order
fn plan(k: u8, m: u8, present: u32, targets: &[u8]) -> Result<Rc<DecodePlan>, CodecError> {
    let key = (k, m, present, targets.to_vec());
    if let Some(plan) = PLANS.with(|plans| plans.borrow().get(&key).cloned()) {
        return Ok(plan);
    }
    let width = usize::from(k) + usize::from(m);
    let flags: Vec<bool> = (0..width).map(|position| present & (1 << position) != 0).collect();
    let rebuild: Vec<usize> = targets.iter().map(|target| usize::from(*target)).collect();
    let made = Rc::new(
        coder(k, m)?
            .decode_plan(&flags, &rebuild)
            .map_err(|error| CodecError(format!("{k}+{m} from {present:#b}: {error:?}")))?,
    );
    PLANS.with(|plans| plans.borrow_mut().insert(key, made.clone()));
    Ok(made)
}

/// Check a decode's sources and targets, and say which sources are present
///
/// # Arguments
///
/// * `k` - The data chunks
/// * `m` - The parity chunks
/// * `sources` - Each source's position and bytes
/// * `targets` - The positions to make
/// * `out` - Where each target goes
fn check_decode(k: u8, m: u8, sources: &[(u8, &[u8])], targets: &[u8], out: &[&mut [u8]]) -> Result<(u32, usize), CodecError> {
    let width = u32::from(k) + u32::from(m);
    if sources.len() != usize::from(k) {
        return Err(CodecError(format!("a {k}+{m} decode was handed {} sources", sources.len())));
    }
    if targets.len() != out.len() || targets.is_empty() {
        return Err(CodecError("a decode's targets and its outputs differ in number".to_string()));
    }
    let len = out[0].len();
    let mut present = 0u32;
    for (position, bytes) in sources {
        if u32::from(*position) >= width || bytes.len() != len || present & (1 << position) != 0 {
            return Err(CodecError(format!("a {k}+{m} decode's source at {position} is not one it can take")));
        }
        present |= 1 << position;
    }
    if out.iter().any(|chunk| chunk.len() != len)
        || targets.iter().any(|target| u32::from(*target) >= width || present & (1 << target) != 0)
    {
        return Err(CodecError(format!("a {k}+{m} decode's targets are not ones it can make")));
    }
    Ok((present, len))
}

/// Decode a range of columns of the targets from the sources
///
/// # Arguments
///
/// * `k` - The data chunks
/// * `m` - The parity chunks
/// * `plan` - The loss pattern's plan
/// * `sources` - Each source's position and bytes
/// * `out` - Where each target goes
/// * `from` - The first column
/// * `to` - The column past the last
fn decode_columns(
    k: u8,
    m: u8,
    plan: &DecodePlan,
    sources: &[(u8, &[u8])],
    out: &mut [&mut [u8]],
    from: usize,
    to: usize,
) -> Result<(), CodecError> {
    let started = Instant::now();
    let width = usize::from(k) + usize::from(m);
    let mut shards: Vec<Option<&[u8]>> = vec![None; width];
    for (position, bytes) in sources {
        shards[usize::from(*position)] = Some(&bytes[from..to]);
    }
    let mut outs: Vec<&mut [u8]> = out.iter_mut().map(|chunk| &mut chunk[from..to]).collect();
    coder(k, m)?
        .recover_with(plan, &shards, &mut outs)
        .map_err(|error| CodecError(format!("{error:?}")))?;
    count(Work::Decode, (to - from) * sources.len(), started);
    Ok(())
}

/// Decode some positions of a stripe from exactly `k` others, yielding between steps
///
/// The sources and the targets are any positions, data or parity, of one range of the chunks:
/// a degraded read decodes the units its range covers, and a rebuild a chunk's every unit.
///
/// # Arguments
///
/// * `k` - The data chunks
/// * `m` - The parity chunks
/// * `sources` - Each source's position and bytes, `k` of them, one length
/// * `targets` - The positions to make
/// * `out` - Where each target goes, as long as the sources
///
/// # Errors
///
/// Sources or targets the layout cannot take.
pub async fn decode(k: u8, m: u8, sources: &[(u8, &[u8])], targets: &[u8], out: &mut [&mut [u8]]) -> Result<(), CodecError> {
    let (present, len) = check_decode(k, m, sources, targets, out)?;
    let plan = plan(k, m, present, targets)?;
    let step = column(usize::from(k));
    let mut from = 0;
    // a step of the code, then a yield the executor's latency goal may take
    while from < len {
        let to = (from + step).min(len);
        decode_columns(k, m, &plan, sources, out, from, to)?;
        glommio::yield_if_needed().await;
        from = to;
    }
    Ok(())
}

/// Decode some positions of a stripe at once, off any executor: for tests and checks
///
/// # Arguments
///
/// * `k` - The data chunks
/// * `m` - The parity chunks
/// * `sources` - Each source's position and bytes
/// * `targets` - The positions to make
/// * `out` - Where each target goes
///
/// # Errors
///
/// Sources or targets the layout cannot take.
pub fn decode_now(k: u8, m: u8, sources: &[(u8, &[u8])], targets: &[u8], out: &mut [&mut [u8]]) -> Result<(), CodecError> {
    let (present, len) = check_decode(k, m, sources, targets, out)?;
    let plan = plan(k, m, present, targets)?;
    decode_columns(k, m, &plan, sources, out, 0, len)
}

/// Fold a change to one data chunk into one parity chunk: `parity ^= c · change`
///
/// The change is the old bytes XOR the new, at the same offsets of the data chunk as of the
/// parity chunk, and the parity is the holder's own, verified before it is handed here (P15).
/// What the holder stages is the result, never the change (X1's `parity_staged_as_a_patch`).
///
/// # Arguments
///
/// * `k` - The data chunks
/// * `m` - The parity chunks
/// * `row` - The parity row, the parity's position less `k`
/// * `data` - The data chunk that changed, `0..k`
/// * `change` - Its old bytes XOR its new
/// * `parity` - The parity's bytes at the same offsets, folded in place
///
/// # Errors
///
/// A row, a data chunk or a length the layout does not have.
pub async fn fold(k: u8, m: u8, row: u8, data: u8, change: &[u8], parity: &mut [u8]) -> Result<(), CodecError> {
    if data >= k || change.len() != parity.len() {
        return Err(CodecError(format!("a {k}+{m} fold of data chunk {data} at its parity's length is not one")));
    }
    let coder = row_coder(k, m, row)?;
    let step = column(1);
    let mut from = 0;
    // a step of the code, then a yield the executor's latency goal may take
    while from < change.len() {
        let to = (from + step).min(change.len());
        let started = Instant::now();
        coder
            .update(usize::from(data), &change[from..to], &mut [&mut parity[from..to]])
            .map_err(|error| CodecError(format!("{error:?}")))?;
        count(Work::Fold, to - from, started);
        glommio::yield_if_needed().await;
        from = to;
    }
    Ok(())
}

/// Fold a change into a parity chunk at once, off any executor: for tests and checks
///
/// # Arguments
///
/// * `k` - The data chunks
/// * `m` - The parity chunks
/// * `row` - The parity row
/// * `data` - The data chunk that changed
/// * `change` - Its old bytes XOR its new
/// * `parity` - The parity's bytes at the same offsets, folded in place
///
/// # Errors
///
/// A row, a data chunk or a length the layout does not have.
pub fn fold_now(k: u8, m: u8, row: u8, data: u8, change: &[u8], parity: &mut [u8]) -> Result<(), CodecError> {
    if data >= k || change.len() != parity.len() {
        return Err(CodecError(format!("a {k}+{m} fold of data chunk {data} at its parity's length is not one")));
    }
    row_coder(k, m, row)?
        .update(usize::from(data), change, &mut [parity])
        .map_err(|error| CodecError(format!("{error:?}")))
}

/// Fold some of a chunk's bytes into its summary: the XOR of its 4 KiB blocks
///
/// A chunk's bytes are folded where they lie in it, so a summary built a piece at a time from any
/// offsets is the same as one built whole, and bytes a chunk does not hold are zeros that fold to
/// nothing.
///
/// # Arguments
///
/// * `offset` - Where in the chunk the bytes start
/// * `bytes` - The bytes
/// * `summary` - The summary, folded in place
pub fn summarise(offset: u64, bytes: &[u8], summary: &mut [u8; SUMMARY_BYTES]) {
    let mut at = (offset % SUMMARY_BYTES as u64) as usize;
    for byte in bytes {
        summary[at] ^= byte;
        at += 1;
        if at == SUMMARY_BYTES {
            at = 0;
        }
    }
    COUNTERS.with(|slot| {
        if let Some(counters) = slot.borrow().as_ref() {
            counters.summarised_bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        }
    });
}

/// Which parity chunks' summaries disagree with what the data chunks' summaries encode to
///
/// Empty when parity matches data. Nothing says which chunk is wrong when they do not: data and
/// parity that each verify and disagree are stopped with their evidence, and an operator's word
/// decides (M21's `ResolveStripe`).
///
/// # Arguments
///
/// * `k` - The data chunks
/// * `m` - The parity chunks
/// * `data` - Each data chunk's summary, zeros for a chunk that holds no bytes
/// * `parity` - Each parity chunk's summary
///
/// # Errors
///
/// A shape the layout does not have.
pub fn parity_disagrees(k: u8, m: u8, data: &[&[u8]], parity: &[&[u8]]) -> Result<Vec<u8>, CodecError> {
    if parity.len() != usize::from(m) {
        return Err(CodecError(format!("{k}+{m} was handed {} parity summaries", parity.len())));
    }
    let mut encoded = vec![vec![0u8; SUMMARY_BYTES]; usize::from(m)];
    {
        let mut outs: Vec<&mut [u8]> = encoded.iter_mut().map(Vec::as_mut_slice).collect();
        encode_now(k, m, data, &mut outs)?;
    }
    Ok(encoded
        .iter()
        .zip(parity)
        .enumerate()
        .filter(|(_, (made, held))| made.as_slice() != **held)
        .map(|(row, _)| row as u8)
        .collect())
}

#[cfg(test)]
mod tests;
