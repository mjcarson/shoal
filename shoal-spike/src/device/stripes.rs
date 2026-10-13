//! X12: a population of real stripes, and the work a rebuild and a deep scrub do on them
//!
//! X7's population is seeded noise nobody verifies. A rebuild and a deep scrub are judged by
//! what they compute, so X12's chunks are stripes as S6 and S8 describe them: each chunk a file
//! of its own, a header block carrying the chunk's identity and a CRC-64/NVME of every 64 KiB
//! unit (X5's checksum through `crc-fast`), then the units. A stripe's data chunks are seeded
//! noise and its parity is encoded by `rusty_erasure` on ISA-L's Cauchy matrix (X4's choice), so
//! a chunk rebuilt from `k` others can be checked against the one it replaces, and a deep scrub's
//! summary check can be checked against parity that matches its data.
//!
//! A slice holds one position of a stripe; here every position of a stripe is on the one device
//! being measured, in one object directory, since a single device stands for every device a
//! rebuild reads or writes. Two stripes of the 4+2 population carry a fault planted when they
//! are written, which a deep scrub must find:
//!
//! - a parity byte changed and its unit's checksum made to match, which only the summary check
//!   can see;
//! - a data byte changed with its checksum left as it was, which the checksum must see.
//!
//! The rebuild's walks leave those two stripes out. Like every spike's, this code is thrown away:
//! the header here is the spike's, not a format.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Instant;

use crc_fast::CrcAlgorithm;
use glommio::io::{DmaBuffer, DmaFile, ReadResult};
use rusty_erasure::{Coder, DecodePlan, Matrix};

use super::io::{self, HEADER};
use super::stats::Rng;
use super::sys;

/// A chunk unit: the granule a checksum covers and a read is verified in
pub const UNIT: u64 = 64 << 10;

/// Units in a chunk
pub const UNITS: usize = 64;

/// A chunk's units: 4 MiB, the floor X7 set under a rotational pool's chunk
pub const CHUNK: u64 = UNIT * UNITS as u64;

/// The placement groups a population is spread over, as X7's is
pub const PGS: usize = 64;

/// What a header block begins with
const MAGIC: &[u8; 8] = b"X12CHUNK";

/// Where the unit checksums start in a header block
const TABLE_AT: usize = 64;

/// The seed every population's bytes are drawn from
const SEED: u64 = 0x12_5eed;

/// A stripe's layout
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Layout {
    /// Three copies: a rebuild reads one and writes it
    Copy,
    /// Two data chunks and one parity, the widest layout the lab's three hosts place
    Rs21,
    /// Four data chunks and two parity, S5's example pool
    Rs42,
}

impl Layout {
    /// Data chunks: what a rebuild reads
    #[must_use]
    pub fn k(self) -> usize {
        match self {
            Layout::Copy => 1,
            Layout::Rs21 => 2,
            Layout::Rs42 => 4,
        }
    }

    /// Parity chunks, or the extra copies
    #[must_use]
    pub fn m(self) -> usize {
        match self {
            Layout::Copy | Layout::Rs42 => 2,
            Layout::Rs21 => 1,
        }
    }

    /// Positions in a stripe
    #[must_use]
    pub fn width(self) -> usize {
        self.k() + self.m()
    }

    /// Its name in a cell
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Layout::Copy => "copy",
            Layout::Rs21 => "2+1",
            Layout::Rs42 => "4+2",
        }
    }

    /// Its number in a header block and on the wire
    #[must_use]
    pub fn code(self) -> u8 {
        match self {
            Layout::Copy => 1,
            Layout::Rs21 => 2,
            Layout::Rs42 => 4,
        }
    }

    /// The layout a number names
    ///
    /// # Arguments
    ///
    /// * `code` - The number
    #[must_use]
    pub fn from_code(code: u8) -> Option<Layout> {
        match code {
            1 => Some(Layout::Copy),
            2 => Some(Layout::Rs21),
            4 => Some(Layout::Rs42),
            _ => None,
        }
    }

    /// The layout whose population holds this layout's chunks: a copy is read from the 4+2
    /// population's data chunks, since every copy of a chunk is the same bytes
    #[must_use]
    pub fn population(self) -> Layout {
        match self {
            Layout::Copy | Layout::Rs42 => Layout::Rs42,
            Layout::Rs21 => Layout::Rs21,
        }
    }

    /// Stripes in its population
    ///
    /// # Arguments
    ///
    /// * `quick` - Whether this is a quick run
    #[must_use]
    pub fn stripes(self, quick: bool) -> usize {
        match (self.population(), quick) {
            (Layout::Rs21, false) => 256,
            (_, false) => 192,
            (_, true) => 8,
        }
    }
}

/// A fault planted in a stripe when it is written
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// A parity byte changed and its checksum made to match: only the summary check sees it
    Parity,
    /// A data byte changed and its checksum left as it was: the checksum sees it
    Data,
}

/// Which fault, if any, a stripe of a population carries: the last two of the 4+2 population
///
/// # Arguments
///
/// * `layout` - The population's layout
/// * `stripes` - Stripes in it
/// * `stripe` - The stripe
#[must_use]
pub fn planted(layout: Layout, stripes: usize, stripe: usize) -> Option<Fault> {
    if layout != Layout::Rs42 || stripes < 4 {
        return None;
    }
    if stripe == stripes - 2 {
        Some(Fault::Parity)
    } else if stripe == stripes - 1 {
        Some(Fault::Data)
    } else {
        None
    }
}

/// A chunk's header block, as this spike writes it
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkHeader {
    /// The stripe's layout
    pub layout: Layout,
    /// The stripe
    pub stripe: u64,
    /// The chunk's position in it
    pub position: u8,
    /// A CRC-64/NVME of every unit
    pub crcs: [u64; UNITS],
}

impl ChunkHeader {
    /// Write the header into a block
    ///
    /// # Arguments
    ///
    /// * `block` - A header block, zeroed past what is written
    pub fn encode(&self, block: &mut [u8]) {
        // identity first, then the table, then zeros
        block.fill(0);
        block[..8].copy_from_slice(MAGIC);
        block[8] = self.layout.code();
        block[9] = self.position;
        block[16..24].copy_from_slice(&self.stripe.to_le_bytes());
        for (unit, crc) in self.crcs.iter().enumerate() {
            let at = TABLE_AT + unit * 8;
            block[at..at + 8].copy_from_slice(&crc.to_le_bytes());
        }
    }

    /// Read a header from a block, or `None` if it is not one
    ///
    /// # Arguments
    ///
    /// * `block` - The block
    #[must_use]
    pub fn decode(block: &[u8]) -> Option<ChunkHeader> {
        if block.len() < TABLE_AT + UNITS * 8 || &block[..8] != MAGIC {
            return None;
        }
        let layout = Layout::from_code(block[8])?;
        let word = |at: usize| u64::from_le_bytes(block[at..at + 8].try_into().expect("eight bytes"));
        let mut crcs = [0; UNITS];
        for (unit, crc) in crcs.iter_mut().enumerate() {
            *crc = word(TABLE_AT + unit * 8);
        }
        Some(ChunkHeader { layout, stripe: word(16), position: block[9], crcs })
    }
}

/// One unit's checksum
///
/// # Arguments
///
/// * `unit` - The unit's bytes
#[must_use]
pub fn crc(unit: &[u8]) -> u64 {
    crc_fast::checksum(CrcAlgorithm::Crc64Nvme, unit)
}

/// Every unit's checksum of a chunk held whole
///
/// # Arguments
///
/// * `chunk` - The chunk's units, `CHUNK` bytes
#[must_use]
pub fn crc_table(chunk: &[u8]) -> [u64; UNITS] {
    let mut crcs = [0; UNITS];
    for (unit, bytes) in chunk.chunks_exact(UNIT as usize).enumerate() {
        crcs[unit] = crc(bytes);
    }
    crcs
}

/// Fold a unit into a summary: the XOR of every unit of a chunk, which an erasure code's
/// linearity carries through, so the data chunks' summaries encode to the parity chunks'
///
/// # Arguments
///
/// * `summary` - The summary so far, a unit long
/// * `unit` - The unit
pub fn fold_into(summary: &mut [u8], unit: &[u8]) {
    // a word at a time, which the compiler widens to the vector registers
    for (into, from) in summary.chunks_exact_mut(8).zip(unit.chunks_exact(8)) {
        let folded = u64::from_ne_bytes(into.try_into().expect("eight")) ^ u64::from_ne_bytes(from.try_into().expect("eight"));
        into.copy_from_slice(&folded.to_ne_bytes());
    }
}

/// A summary one block long: the check's linearity holds at any length, since a code acts on
/// every byte position alike, so a chunk can be folded to one 4 KiB block instead of a unit
pub const BLOCK: usize = 4096;

/// Fold a unit into a block-long summary, every block of the unit in turn, so the summary stays
/// in the core's first cache while the unit streams through
///
/// # Arguments
///
/// * `summary` - The summary so far, a block long
/// * `unit` - The unit
pub fn fold_block(summary: &mut [u8], unit: &[u8]) {
    for block in unit.chunks_exact(BLOCK) {
        fold_into(summary, block);
    }
}

/// The path of one chunk of a population: a stripe is an object, its positions files in it
///
/// # Arguments
///
/// * `root` - The population's directory
/// * `stripe` - The stripe
/// * `position` - The chunk's position
#[must_use]
pub fn chunk_path(root: &Path, stripe: usize, position: usize) -> PathBuf {
    root.join(format!("pg{:02}", stripe % PGS))
        .join(format!("{stripe:016x}"))
        .join(format!("000000.{position}"))
}

/// The directory a layout's population lives in under a measurement's root
///
/// # Arguments
///
/// * `base` - The directory every X12 population is under
/// * `layout` - The layout
#[must_use]
pub fn population_dir(base: &Path, layout: Layout) -> PathBuf {
    base.join(format!("stripes-{}", layout.population().name()))
}

/// A coder for a layout, or none for copies
///
/// # Arguments
///
/// * `layout` - The layout
#[must_use]
pub fn coder(layout: Layout) -> Option<Coder> {
    match layout {
        Layout::Copy => None,
        _ => Some(
            rusty_erasure::coder(Matrix::cauchy(layout.k(), layout.m()).expect("a Cauchy matrix of a lab layout"))
                .expect("a coder of a lab layout"),
        ),
    }
}

/// Make the chunks of one stripe in memory: its data seeded, its parity encoded, its fault planted
///
/// # Arguments
///
/// * `coder` - The layout's coder
/// * `layout` - The population's layout
/// * `stripes` - Stripes in the population
/// * `stripe` - The stripe
#[must_use]
pub fn make_stripe(coder: &Coder, layout: Layout, stripes: usize, stripe: usize) -> (Vec<Vec<u8>>, Vec<[u64; UNITS]>) {
    let (k, m) = (layout.k(), layout.m());
    // the data chunks, each from a seed of its own
    let mut chunks: Vec<Vec<u8>> = (0..k)
        .map(|position| {
            let mut bytes = vec![0_u8; CHUNK as usize];
            Rng::new(SEED ^ ((layout.code() as u64) << 56) ^ ((stripe as u64) << 8) ^ position as u64).fill(&mut bytes);
            bytes
        })
        .collect();
    // the parity, encoded over the whole chunk at once, which is every unit row at once
    let mut parity: Vec<Vec<u8>> = (0..m).map(|_| vec![0_u8; CHUNK as usize]).collect();
    {
        let data: Vec<&[u8]> = chunks.iter().map(Vec::as_slice).collect();
        let mut outputs: Vec<&mut [u8]> = parity.iter_mut().map(Vec::as_mut_slice).collect();
        coder.encode(&data, &mut outputs).expect("equal chunks encode");
    }
    chunks.extend(parity);
    // a parity fault is planted before the checksums, so they match it
    let fault = planted(layout, stripes, stripe);
    if fault == Some(Fault::Parity) {
        chunks[k][5 * UNIT as usize + 1234] ^= 0x5a;
    }
    let tables: Vec<[u64; UNITS]> = chunks.iter().map(|chunk| crc_table(chunk)).collect();
    // a data fault after them, so its unit no longer matches its checksum
    if fault == Some(Fault::Data) {
        chunks[0][7 * UNIT as usize + 4321] ^= 0xa5;
    }
    (chunks, tables)
}

/// The file every population's checksum tables are kept in, beside its chunks
const TABLES: &str = "tables.bin";

/// Make a layout's population if it is not there: every stripe made, every chunk written and
/// synced, and the tables written beside them
///
/// # Arguments
///
/// * `root` - Its directory
/// * `layout` - The population's layout
/// * `stripes` - Stripes in it
pub async fn populate(root: PathBuf, layout: Layout, stripes: usize) {
    let layout = layout.population();
    let marker = root.join("complete");
    let expect = format!("{} {stripes}", layout.name());
    if std::fs::read_to_string(&marker).is_ok_and(|held| held == expect) {
        return;
    }
    io::wipe(&root);
    // every placement group first, so each takes its place before any chunk is written
    for pg in 0..PGS {
        std::fs::create_dir_all(root.join(format!("pg{pg:02}"))).expect("made");
    }
    let coder = coder(layout).expect("a population is erasure coded");
    let mut tables = Vec::with_capacity(stripes * layout.width() * UNITS * 8);
    for stripe in 0..stripes {
        let (chunks, crcs) = make_stripe(&coder, layout, stripes, stripe);
        let object = chunk_path(&root, stripe, 0);
        std::fs::create_dir_all(object.parent().expect("a parent")).expect("made");
        for (position, chunk) in chunks.iter().enumerate() {
            // the header, then the units, from buffers the device can take directly
            let mut header = glommio::allocate_dma_buffer(HEADER as usize);
            ChunkHeader { layout, stripe: stripe as u64, position: position as u8, crcs: crcs[position] }
                .encode(header.as_bytes_mut());
            let mut body = glommio::allocate_dma_buffer(CHUNK as usize);
            body.as_bytes_mut().copy_from_slice(chunk);
            let file = io::open(&chunk_path(&root, stripe, position), true).await;
            let (header, body) = (Rc::new(header), Rc::new(body));
            file.write_rc_at(header, 0).await.expect("a header lands");
            file.write_rc_at(body, HEADER).await.expect("a chunk lands");
            file.fdatasync().await.expect("synced");
            file.close().await.expect("closed");
            for crc in crcs[position] {
                tables.extend_from_slice(&crc.to_le_bytes());
            }
        }
    }
    std::fs::write(root.join(TABLES), tables).expect("the tables are written");
    let _ = sys::syncfs(&root);
    std::fs::write(&marker, expect).expect("marked");
}

/// A chunk read from a device: its header block and its units, in pieces
pub struct ReadChunk {
    /// The header block
    pub header: ReadResult,
    /// The units, in pieces of `piece` bytes
    pub pieces: Vec<ReadResult>,
    /// Bytes a piece
    pub piece: u64,
}

impl ReadChunk {
    /// One unit's bytes
    ///
    /// # Arguments
    ///
    /// * `unit` - The unit
    #[must_use]
    pub fn unit(&self, unit: usize) -> &[u8] {
        let at = unit as u64 * UNIT;
        let piece = &self.pieces[(at / self.piece) as usize];
        let offset = (at % self.piece) as usize;
        &piece[offset..offset + UNIT as usize]
    }
}

/// A chunk held in memory: a stand-in for one that arrived over the network
pub struct HeldChunk {
    /// Its header
    pub header: ChunkHeader,
    /// Its units
    pub bytes: Vec<u8>,
}

impl HeldChunk {
    /// One unit's bytes
    ///
    /// # Arguments
    ///
    /// * `unit` - The unit
    #[must_use]
    pub fn unit(&self, unit: usize) -> &[u8] {
        &self.bytes[unit * UNIT as usize..(unit + 1) * UNIT as usize]
    }
}

/// Something a chunk's units can be read from
pub trait Units {
    /// One unit's bytes
    ///
    /// # Arguments
    ///
    /// * `unit` - The unit
    fn unit_bytes(&self, unit: usize) -> &[u8];
}

impl Units for ReadChunk {
    /// One unit's bytes, from the piece holding it
    ///
    /// # Arguments
    ///
    /// * `unit` - The unit
    fn unit_bytes(&self, unit: usize) -> &[u8] {
        self.unit(unit)
    }
}

impl Units for HeldChunk {
    /// One unit's bytes, from memory
    ///
    /// # Arguments
    ///
    /// * `unit` - The unit
    fn unit_bytes(&self, unit: usize) -> &[u8] {
        self.unit(unit)
    }
}

/// Where a step's cpu went, nanoseconds
#[derive(Debug, Clone, Copy, Default)]
pub struct Steps {
    /// Checksumming units, read or rebuilt
    pub crc_ns: u64,
    /// Folding units into summaries
    pub fold_ns: u64,
    /// Decoding, or copying a chunk for a copy's rebuild
    pub decode_ns: u64,
    /// Encoding summaries and comparing them
    pub summary_ns: u64,
}

impl Steps {
    /// Add another's figures to these
    ///
    /// # Arguments
    ///
    /// * `other` - The other
    pub fn add(&mut self, other: &Steps) {
        self.crc_ns += other.crc_ns;
        self.fold_ns += other.fold_ns;
        self.decode_ns += other.decode_ns;
        self.summary_ns += other.summary_ns;
    }
}

/// Nanoseconds since an instant
///
/// # Arguments
///
/// * `since` - The instant
fn ns(since: Instant) -> u64 {
    since.elapsed().as_nanos() as u64
}

/// Verify every unit of a chunk against a table, folding each into a summary if one is given,
/// yielding the executor between units
///
/// # Arguments
///
/// * `chunk` - The chunk's units
/// * `crcs` - The checksums it should have
/// * `summary` - The summary to fold into, if the parity check wants one
/// * `steps` - Where the cpu is counted
pub async fn verify<C: Units>(chunk: &C, crcs: &[u64; UNITS], mut summary: Option<&mut [u8]>, steps: &mut Steps) -> u32 {
    let mut failed = 0;
    for (unit, expected) in crcs.iter().enumerate() {
        let bytes = chunk.unit_bytes(unit);
        // the checksum, then the fold while the unit is still in cache
        let began = Instant::now();
        if crc(bytes) != *expected {
            failed += 1;
        }
        steps.crc_ns += ns(began);
        if let Some(summary) = summary.as_deref_mut() {
            let began = Instant::now();
            fold_into(summary, bytes);
            steps.fold_ns += ns(began);
        }
        glommio::yield_if_needed().await;
    }
    failed
}

/// A layout's coder and the decode plans it has made, one for each position lost
pub struct Codec {
    /// The layout
    pub layout: Layout,
    /// Its coder, none for copies
    pub coder: Option<Coder>,
    /// A plan for each lost position, made once and kept
    plans: RefCell<HashMap<usize, Rc<DecodePlan>>>,
}

impl Codec {
    /// A codec for a layout
    ///
    /// # Arguments
    ///
    /// * `layout` - The layout
    #[must_use]
    pub fn new(layout: Layout) -> Self {
        Codec { layout, coder: coder(layout), plans: RefCell::new(HashMap::new()) }
    }

    /// The kernels the coder dispatched to
    #[must_use]
    pub fn kernels(&self) -> &'static str {
        self.coder.as_ref().map_or("copy", |coder| coder.kernels().name)
    }

    /// The positions a rebuild of a lost one reads: the first `k` others, which is the set the
    /// decode plan takes
    ///
    /// # Arguments
    ///
    /// * `lost` - The position lost
    #[must_use]
    pub fn survivors(&self, lost: usize) -> Vec<usize> {
        match self.layout {
            // a copy is read from any other copy, which is the same bytes
            Layout::Copy => vec![lost],
            _ => (0..self.layout.width()).filter(|&position| position != lost).take(self.layout.k()).collect(),
        }
    }

    /// The decode plan for a lost position, made the first time it is asked for
    ///
    /// # Arguments
    ///
    /// * `lost` - The position lost
    fn plan(&self, lost: usize) -> Rc<DecodePlan> {
        if let Some(plan) = self.plans.borrow().get(&lost) {
            return plan.clone();
        }
        let coder = self.coder.as_ref().expect("copies are not decoded");
        let present: Vec<bool> = (0..self.layout.width()).map(|position| position != lost).collect();
        let plan = Rc::new(coder.decode_plan(&present, &[lost]).expect("one position lost decodes"));
        self.plans.borrow_mut().insert(lost, plan.clone());
        plan
    }

    /// Rebuild one unit of a lost position from the survivors' same unit, on the kept plan; a
    /// copy's is the survivor's unit copied
    ///
    /// # Arguments
    ///
    /// * `units` - The survivors' units, in the order `survivors` names them
    /// * `survivors` - The survivors' positions
    /// * `lost` - The position lost
    /// * `target` - Where the rebuilt unit goes
    pub fn decode_unit(&self, units: &[&[u8]], survivors: &[usize], lost: usize, target: &mut [u8]) {
        match &self.coder {
            None => target.copy_from_slice(units[0]),
            Some(coder) => {
                // every position, the survivors present and the rest absent
                let mut shards: Vec<Option<&[u8]>> = vec![None; self.layout.width()];
                for (unit, &position) in units.iter().zip(survivors) {
                    shards[position] = Some(*unit);
                }
                coder.recover_with(&self.plan(lost), &shards, &mut [target]).expect("a plan's survivors decode");
            }
        }
    }

    /// Rebuild a lost position into output pieces, a unit at a time, yielding between units
    ///
    /// For a copy the survivor's units are copied, which is what a copy's rebuild costs in memory.
    ///
    /// # Arguments
    ///
    /// * `sources` - The survivors, in the order `survivors` names them
    /// * `lost` - The position lost
    /// * `out` - The output, in pieces of `piece` bytes
    /// * `piece` - Bytes a piece
    /// * `steps` - Where the cpu is counted
    pub async fn rebuild<C: Units>(&self, sources: &[&C], lost: usize, out: &mut [DmaBuffer], piece: u64, steps: &mut Steps) {
        let survivors = self.survivors(lost);
        for unit in 0..UNITS {
            // the unit's place in the output
            let at = unit as u64 * UNIT;
            let target = &mut out[(at / piece) as usize].as_bytes_mut()[(at % piece) as usize..][..UNIT as usize];
            let units: Vec<&[u8]> = sources.iter().map(|source| source.unit_bytes(unit)).collect();
            let began = Instant::now();
            self.decode_unit(&units, &survivors, lost, target);
            steps.decode_ns += ns(began);
            glommio::yield_if_needed().await;
        }
    }

    /// Check a stripe's summaries: the data chunks' encoded must equal the parity chunks'
    ///
    /// # Arguments
    ///
    /// * `summaries` - One summary a position, data then parity
    /// * `scratch` - The parity's re-encoding, one unit a parity chunk
    /// * `steps` - Where the cpu is counted
    pub fn summaries_match(&self, summaries: &[Vec<u8>], scratch: &mut [Vec<u8>], steps: &mut Steps) -> bool {
        let began = Instant::now();
        let k = self.layout.k();
        let matched = match &self.coder {
            // copies must simply be equal
            None => summaries.windows(2).all(|pair| pair[0] == pair[1]),
            Some(coder) => {
                let data: Vec<&[u8]> = summaries[..k].iter().map(Vec::as_slice).collect();
                let mut outputs: Vec<&mut [u8]> = scratch.iter_mut().map(Vec::as_mut_slice).collect();
                coder.encode(&data, &mut outputs).expect("equal summaries encode");
                scratch.iter().zip(&summaries[k..]).all(|(made, held)| made == held)
            }
        };
        steps.summary_ns += ns(began);
        matched
    }
}

/// A layout's population held open, with every chunk's checksums
pub struct Population {
    /// The population's layout
    pub layout: Layout,
    /// Every chunk, open for reading, by stripe then position
    pub files: Vec<Vec<DmaFile>>,
    /// Every chunk's checksums, as its header holds them, by stripe then position
    pub tables: Vec<Vec<[u64; UNITS]>>,
}

impl Population {
    /// Open every chunk of a population and load its tables
    ///
    /// # Arguments
    ///
    /// * `root` - Its directory
    /// * `layout` - Its layout
    /// * `stripes` - Stripes in it
    pub async fn open(root: &Path, layout: Layout, stripes: usize) -> Population {
        let layout = layout.population();
        let width = layout.width();
        // the tables, in the order they were written
        let raw = std::fs::read(root.join(TABLES)).expect("a population's tables");
        assert_eq!(raw.len(), stripes * width * UNITS * 8, "the tables are the population's");
        let mut words = raw.chunks_exact(8).map(|word| u64::from_le_bytes(word.try_into().expect("eight")));
        let mut tables = Vec::with_capacity(stripes);
        let mut files = Vec::with_capacity(stripes);
        for stripe in 0..stripes {
            let mut stripe_tables = Vec::with_capacity(width);
            let mut stripe_files = Vec::with_capacity(width);
            for position in 0..width {
                let mut crcs = [0; UNITS];
                for crc in &mut crcs {
                    *crc = words.next().expect("a word");
                }
                stripe_tables.push(crcs);
                stripe_files.push(io::open_read(&chunk_path(root, stripe, position)).await);
            }
            tables.push(stripe_tables);
            files.push(stripe_files);
        }
        Population { layout, files, tables }
    }

    /// Stripes in it
    #[must_use]
    pub fn stripes(&self) -> usize {
        self.files.len()
    }

    /// The stripes in the order a walk visits them, placement group by placement group from a
    /// starting group, so every side reads the device as a slice's walk would and no two sides
    /// start at the same head
    ///
    /// # Arguments
    ///
    /// * `start` - The placement group the walk starts at
    /// * `planted_too` - Whether the stripes with a planted fault are visited
    #[must_use]
    pub fn walk(&self, start: usize, planted_too: bool) -> Vec<usize> {
        let stripes = self.stripes();
        let mut order = Vec::with_capacity(stripes);
        for step in 0..PGS {
            let pg = (start + step) % PGS;
            // the group's stripes, in the order they were written
            order.extend(
                (pg..stripes)
                    .step_by(PGS)
                    .filter(|&stripe| planted_too || planted(self.layout, stripes, stripe).is_none()),
            );
        }
        order
    }

    /// Read a chunk into memory whole and decode its header, outside anything timed
    ///
    /// # Arguments
    ///
    /// * `stripe` - The stripe
    /// * `position` - The position
    pub async fn hold(&self, stripe: usize, position: usize) -> HeldChunk {
        let file = &self.files[stripe][position];
        let header = file.read_at_aligned(0, HEADER as usize).await.expect("a header");
        let header = ChunkHeader::decode(&header).expect("a header this spike wrote");
        let body = file.read_at_aligned(HEADER, CHUNK as usize).await.expect("a chunk");
        HeldChunk { header, bytes: body.to_vec() }
    }

    /// Close every chunk
    pub async fn close(self) {
        for stripe in self.files {
            for file in stripe {
                let _ = file.close().await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fold every unit of a chunk into a summary
    ///
    /// # Arguments
    ///
    /// * `chunk` - The chunk
    fn summary(chunk: &[u8]) -> Vec<u8> {
        let mut summary = vec![0_u8; UNIT as usize];
        for unit in chunk.chunks_exact(UNIT as usize) {
            fold_into(&mut summary, unit);
        }
        summary
    }

    /// The data chunks' summaries encode to the parity chunks', and a parity byte changed with
    /// its checksum made to match is seen only there
    ///
    /// # Arguments
    ///
    /// * `layout` - The layout
    fn linearity(layout: Layout) {
        let codec = Codec::new(layout);
        let coder = codec.coder.as_ref().expect("a coded layout");
        // a clean stripe: the summaries match
        let (chunks, _) = make_stripe(coder, layout, 1, 0);
        let summaries: Vec<Vec<u8>> = chunks.iter().map(|chunk| summary(chunk)).collect();
        let mut scratch = vec![vec![0_u8; UNIT as usize]; layout.m()];
        let mut steps = Steps::default();
        assert!(codec.summaries_match(&summaries, &mut scratch, &mut steps), "a clean {} stripe", layout.name());
        // one parity byte changed: every checksum would pass, the summaries do not
        let mut bad = summaries.clone();
        let mut parity = chunks[layout.k()].clone();
        parity[3 * UNIT as usize + 17] ^= 1;
        bad[layout.k()] = summary(&parity);
        assert!(!codec.summaries_match(&bad, &mut scratch, &mut steps), "a changed parity byte is seen");
        // the same bit changed in two units of one chunk cancels in the fold: Ceph's caveat
        let mut twice = chunks[layout.k()].clone();
        twice[3 * UNIT as usize + 17] ^= 1;
        twice[9 * UNIT as usize + 17] ^= 1;
        bad[layout.k()] = summary(&twice);
        assert!(codec.summaries_match(&bad, &mut scratch, &mut steps), "two flips that cancel are not seen by the summary");
        assert_ne!(crc_table(&twice), crc_table(&chunks[layout.k()]), "but every unit's checksum sees them");
    }

    /// The summary check holds for 2+1
    #[test]
    fn summary_linearity_21() {
        linearity(Layout::Rs21);
    }

    /// The summary check holds for 4+2
    #[test]
    fn summary_linearity_42() {
        linearity(Layout::Rs42);
    }

    /// A summary folded to one block holds too, and still sees a changed parity byte
    #[test]
    fn block_summary_linearity_42() {
        let codec = Codec::new(Layout::Rs42);
        let (chunks, _) = make_stripe(codec.coder.as_ref().expect("coded"), Layout::Rs42, 1, 0);
        // every chunk folded to a block
        let fold = |chunk: &[u8]| {
            let mut summary = vec![0_u8; BLOCK];
            for unit in chunk.chunks_exact(UNIT as usize) {
                fold_block(&mut summary, unit);
            }
            summary
        };
        let summaries: Vec<Vec<u8>> = chunks.iter().map(|chunk| fold(chunk)).collect();
        let mut scratch = vec![vec![0_u8; BLOCK]; 2];
        let mut steps = Steps::default();
        assert!(codec.summaries_match(&summaries, &mut scratch, &mut steps), "a clean stripe's block summaries match");
        // one parity byte changed is seen
        let mut parity = chunks[5].clone();
        parity[40 * UNIT as usize + 4095] ^= 0x10;
        let mut bad = summaries.clone();
        bad[5] = fold(&parity);
        assert!(!codec.summaries_match(&bad, &mut scratch, &mut steps), "a changed parity byte is seen in a block summary");
    }

    /// Every position of a stripe rebuilt from the first `k` others equals the one lost
    ///
    /// # Arguments
    ///
    /// * `layout` - The layout
    fn round_trip(layout: Layout) {
        let executor = glommio::LocalExecutorBuilder::default().make().expect("an executor");
        executor.run(async move {
            let codec = Codec::new(layout);
            let (chunks, tables) = make_stripe(codec.coder.as_ref().expect("coded"), layout, 1, 0);
            let held: Vec<HeldChunk> = chunks
                .iter()
                .enumerate()
                .map(|(position, bytes)| HeldChunk {
                    header: ChunkHeader { layout, stripe: 0, position: position as u8, crcs: tables[position] },
                    bytes: bytes.clone(),
                })
                .collect();
            for lost in 0..layout.width() {
                let sources: Vec<&HeldChunk> = codec.survivors(lost).iter().map(|&position| &held[position]).collect();
                let mut out: Vec<DmaBuffer> = (0..4).map(|_| glommio::allocate_dma_buffer(1 << 20)).collect();
                let mut steps = Steps::default();
                codec.rebuild(&sources, lost, &mut out, 1 << 20, &mut steps).await;
                let rebuilt: Vec<u8> = out.iter().flat_map(|piece| piece.as_bytes().to_vec()).collect();
                assert_eq!(crc_table(&rebuilt), tables[lost], "{} position {lost}", layout.name());
                assert!(rebuilt == chunks[lost], "{} position {lost} byte for byte", layout.name());
            }
        });
    }

    /// 2+1 rebuilds every position
    #[test]
    fn decode_round_trip_21() {
        round_trip(Layout::Rs21);
    }

    /// 4+2 rebuilds every position
    #[test]
    fn decode_round_trip_42() {
        round_trip(Layout::Rs42);
    }

    /// A header survives its block, and a table names exactly the unit that changed
    #[test]
    fn header_round_trip() {
        let mut chunk = vec![0_u8; CHUNK as usize];
        Rng::new(3).fill(&mut chunk);
        let header = ChunkHeader { layout: Layout::Rs42, stripe: 77, position: 5, crcs: crc_table(&chunk) };
        let mut block = vec![0_u8; HEADER as usize];
        header.encode(&mut block);
        assert_eq!(ChunkHeader::decode(&block), Some(header.clone()));
        // one byte of unit 41 changed: that unit's checksum and no other
        chunk[41 * UNIT as usize + 9] ^= 0x80;
        let changed: Vec<usize> = crc_table(&chunk)
            .iter()
            .zip(&header.crcs)
            .enumerate()
            .filter(|(_, (now, then))| now != then)
            .map(|(unit, _)| unit)
            .collect();
        assert_eq!(changed, vec![41]);
    }

    /// The planted faults are where the walks expect them, and a walk leaves them out when asked
    #[test]
    fn planted_faults_placed() {
        assert_eq!(planted(Layout::Rs42, 192, 190), Some(Fault::Parity));
        assert_eq!(planted(Layout::Rs42, 192, 191), Some(Fault::Data));
        assert_eq!(planted(Layout::Rs42, 192, 189), None);
        assert_eq!(planted(Layout::Rs21, 256, 255), None);
        // the planted data fault fails exactly one unit's checksum
        let coder = coder(Layout::Rs42).expect("coded");
        let (chunks, tables) = make_stripe(&coder, Layout::Rs42, 192, 191);
        assert_ne!(crc_table(&chunks[0]), tables[0]);
        // and the planted parity fault none
        let (chunks, tables) = make_stripe(&coder, Layout::Rs42, 192, 190);
        assert_eq!(crc_table(&chunks[4]), tables[4]);
    }
}
