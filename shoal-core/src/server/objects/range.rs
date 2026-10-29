//! A read's range of an object as the parts it was read in: each the bytes a holder answered,
//! where the answer left them, or a run of zeros
//! ([F109](../../../../docs/src/features/reads-from-stripe-buffers.md))
//!
//! A read gathers its range a stripe at a time, and an erasure coded stripe a data chunk at a time.
//! Each piece arrives as [`ChunkBytes`], in a buffer of its own from a slice on this node or in the
//! body of the frame a lane read, and a hole or bytes a floor hides are zeros nobody is asked for.
//! The range keeps those pieces as they came: its checksum is folded from theirs, a run of zeros by
//! its length alone, and the connection's writer frames its data from slices of them. Before F109
//! the coordinating shard copied an erasure coded stripe's parts into one buffer, the stripes into
//! another and that behind the answer's head into a third, all on a table shard.
//!
//! # Invariants
//!
//! **A range is its parts in order, and nothing else.** Its bytes are the concatenation of its parts
//! however they are cut; no empty part is kept and no two runs of zeros are adjacent, so a window
//! names the same bytes whichever parts it spans.
//!
//! **A part is never changed once it is in a range.** A pending write is laid over a holder's bytes
//! before they become a part, while the read still holds them alone.
//!
//! **A range is `Send`**: it rides a reply, which crosses shards for a table's answer even though an
//! object's stays on the shard that ran it.

use crate::shared::checksum::{self, Chain, Crc64};
use crate::shared::protocol::peer::chunk::ChunkBytes;

use super::store::STEP;

/// The zeros every run of zeros in a range is written from, a slice of it a time
///
/// Never written, so its pages are the kernel's zero page wherever it is read.
static ZEROS: [u8; ZERO_BLOCK] = [0; ZERO_BLOCK];

/// How many zeros one slice of a run carries: a default data frame's worth
pub const ZERO_BLOCK: usize = 1 << 20;

/// One part of a range
#[derive(Debug, Clone)]
pub enum RangePart {
    /// Bytes a holder answered, or decoded, where they were left
    Bytes(ChunkBytes),
    /// A run of zeros this long: a hole, or bytes a floor hides
    Zeros(u64),
}

impl RangePart {
    /// How many bytes the part is
    #[must_use]
    pub fn len(&self) -> u64 {
        match self {
            RangePart::Bytes(bytes) => bytes.len() as u64,
            RangePart::Zeros(len) => *len,
        }
    }

    /// Whether the part is no bytes
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A read's range as the parts it was read in
#[derive(Debug, Clone, Default)]
pub struct RangeParts {
    /// The parts, in order, none empty and no two runs of zeros adjacent
    parts: Vec<RangePart>,
    /// How many bytes they are together
    len: u64,
}

impl RangeParts {
    /// A range of nothing yet
    #[must_use]
    pub fn new() -> Self {
        RangeParts::default()
    }

    /// Add a holder's bytes at the range's end
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes, kept where they are
    pub fn push_bytes(&mut self, bytes: ChunkBytes) {
        // nothing is no part
        if bytes.is_empty() {
            return;
        }
        self.len += bytes.len() as u64;
        self.parts.push(RangePart::Bytes(bytes));
    }

    /// Add a run of zeros at the range's end
    ///
    /// # Arguments
    ///
    /// * `len` - How many zeros
    pub fn push_zeros(&mut self, len: u64) {
        // nothing is no part, and a run after a run is one run
        if len == 0 {
            return;
        }
        self.len += len;
        if let Some(RangePart::Zeros(last)) = self.parts.last_mut() {
            *last += len;
            return;
        }
        self.parts.push(RangePart::Zeros(len));
    }

    /// Add another range's parts at this one's end, moved and never copied
    ///
    /// # Arguments
    ///
    /// * `other` - The range to add
    pub fn append(&mut self, other: RangeParts) {
        // each part through the pushes, which keep runs of zeros whole
        for part in other.parts {
            match part {
                RangePart::Bytes(bytes) => self.push_bytes(bytes),
                RangePart::Zeros(len) => self.push_zeros(len),
            }
        }
    }

    /// How many bytes the range is
    #[must_use]
    pub const fn len(&self) -> u64 {
        self.len
    }

    /// Whether the range is no bytes
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The parts, in order
    #[must_use]
    pub fn parts(&self) -> &[RangePart] {
        &self.parts
    }

    /// The bytes of a window of the range, in order, as slices of its parts and of the zero block
    ///
    /// The window is cut to the range's end. What a data frame of a read's answer is written from.
    ///
    /// # Arguments
    ///
    /// * `offset` - Where in the range the window starts
    /// * `len` - How long it is
    pub fn window(&self, offset: u64, len: u64) -> impl Iterator<Item = &[u8]> + '_ {
        let end = offset.saturating_add(len).min(self.len);
        // each part with where it starts in the range
        let starts = self.parts.iter().scan(0u64, |at, part| {
            let start = *at;
            *at += part.len();
            Some((start, part))
        });
        starts
            .skip_while(move |(start, part)| start + part.len() <= offset)
            .take_while(move |(start, _)| *start < end)
            .flat_map(move |(start, part)| {
                // the window's part of this part, in the part's own offsets, and none of an empty
                // window
                let from = (offset.max(start) - start) as usize;
                let to = ((end.min(start + part.len()) - start) as usize).max(from);
                match part {
                    RangePart::Bytes(bytes) => Slices {
                        bytes: (to > from).then(|| &bytes[from..to]),
                        zeros: 0,
                    },
                    RangePart::Zeros(_) => Slices {
                        bytes: None,
                        zeros: (to - from) as u64,
                    },
                }
            })
    }

    /// Every byte of the range, in order, as slices of its parts and of the zero block
    pub fn slices(&self) -> impl Iterator<Item = &[u8]> + '_ {
        self.window(0, self.len)
    }

    /// The range's CRC-64/NVME, on the shard that holds it
    ///
    /// Each part's bytes are checksummed in steps of [`STEP`], the executor offered back between
    /// them, and folded in by its length; a run of zeros is folded in by its length alone, with no
    /// pass over it.
    pub async fn crc64(&self) -> u64 {
        let mut chain = Chain::new(0);
        for part in &self.parts {
            match part {
                RangePart::Bytes(bytes) => {
                    // a step at a time, so a mebibyte never holds a table's queries behind it
                    let mut crc = Crc64::new();
                    for step in bytes.chunks(STEP) {
                        crc.update(step);
                        glommio::yield_if_needed().await;
                    }
                    chain.push(crc.finish(), crc.len());
                }
                RangePart::Zeros(len) => chain.push(checksum::zeros(*len), *len),
            }
        }
        chain.crc()
    }

    /// The range's bytes in one buffer of their own: moved where it is one part in a buffer of its
    /// own already, copied otherwise
    #[must_use]
    pub fn into_vec(self) -> Vec<u8> {
        self.into_contiguous().into_vec()
    }

    /// The range's bytes in one piece: its one part moved out where it has one, copied together
    /// otherwise
    ///
    /// What a write that reads a chunk's units before it stages them takes a stripe's bytes as.
    #[must_use]
    pub fn into_contiguous(mut self) -> ChunkBytes {
        // one part of bytes is already contiguous wherever it is
        if let [RangePart::Bytes(_)] = self.parts.as_slice() {
            if let Some(RangePart::Bytes(bytes)) = self.parts.pop() {
                return bytes;
            }
        }
        // anything else is copied into a buffer of its own
        let mut out = Vec::with_capacity(self.len as usize);
        for slice in self.slices() {
            out.extend_from_slice(slice);
        }
        ChunkBytes::Owned(out)
    }
}

impl From<ChunkBytes> for RangeParts {
    /// A range of one holder's bytes
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes
    fn from(bytes: ChunkBytes) -> Self {
        let mut range = RangeParts::new();
        range.push_bytes(bytes);
        range
    }
}

impl From<Vec<u8>> for RangeParts {
    /// A range of bytes in a buffer of their own
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes
    fn from(bytes: Vec<u8>) -> Self {
        RangeParts::from(ChunkBytes::Owned(bytes))
    }
}

/// The slices of one part of a window: its bytes, or its zeros a block at a time
struct Slices<'a> {
    /// The part's bytes in the window, until they are taken
    bytes: Option<&'a [u8]>,
    /// The zeros left to give
    zeros: u64,
}

impl<'a> Iterator for Slices<'a> {
    type Item = &'a [u8];

    /// The next slice: the bytes once, or the next block of zeros
    fn next(&mut self) -> Option<&'a [u8]> {
        // the bytes, whole
        if let Some(bytes) = self.bytes.take() {
            return Some(bytes);
        }
        // the zeros, a block at a time
        if self.zeros == 0 {
            return None;
        }
        let take = self.zeros.min(ZERO_BLOCK as u64);
        self.zeros -= take;
        Some(&ZEROS[..take as usize])
    }
}

#[cfg(test)]
mod tests {
    use super::{RangePart, RangeParts, ZERO_BLOCK};
    use crate::shared::checksum;
    use crate::shared::protocol::peer::chunk::ChunkBytes;

    /// Bytes from a seed, each different from its neighbours
    ///
    /// # Arguments
    ///
    /// * `len` - How many
    /// * `seed` - The seed
    fn seeded(len: usize, seed: u64) -> Vec<u8> {
        (0..len as u64)
            .map(|index| (crate::shared::rendezvous::mix64(index ^ (seed << 40)) >> 24) as u8)
            .collect()
    }

    /// Bytes in a frame's body from a place in it on, as a lane hands them over
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes
    fn framed(bytes: &[u8]) -> ChunkBytes {
        let mut body = rkyv::util::AlignedVec::new();
        body.extend_from_slice(&[0xee; 40]);
        body.extend_from_slice(bytes);
        ChunkBytes::Framed { body, at: 40 }
    }

    /// A range of the shapes a read makes, and the bytes it stands for: a stripe from a slice, a
    /// hole longer than a zero block, a stripe from a lane in two data chunks' parts, an overlaid
    /// stripe, and a short last stripe's zeros
    fn shapes() -> (RangeParts, Vec<u8>) {
        let mut range = RangeParts::new();
        let mut flat = Vec::new();
        // a whole stripe from a slice on this node
        let first = seeded(300_001, 1);
        range.push_bytes(ChunkBytes::Owned(first.clone()));
        flat.extend(&first);
        // a hole, in two pushes that are one run, past a zero block
        range.push_zeros(ZERO_BLOCK as u64 + 17);
        range.push_zeros(4_000);
        flat.extend(std::iter::repeat_n(0u8, ZERO_BLOCK + 4_017));
        // an erasure coded stripe's two data chunks' parts, from lanes, and nothing between them
        let (left, right) = (seeded(70_000, 2), seeded(1 << 20, 3));
        let mut stripe = RangeParts::from(framed(&left));
        stripe.push_bytes(ChunkBytes::default());
        stripe.push_bytes(framed(&right));
        range.append(stripe);
        flat.extend(&left);
        flat.extend(&right);
        // a stripe with a pending write laid over it before it became a part
        let mut overlaid = seeded(65_536, 4);
        overlaid[100..200].copy_from_slice(&[0xab; 100]);
        range.push_bytes(ChunkBytes::Owned(overlaid.clone()));
        flat.extend(&overlaid);
        // a short last stripe: its bytes, then the zeros past them
        let short = seeded(5, 5);
        range.push_bytes(ChunkBytes::Owned(short.clone()));
        range.push_zeros(123_456);
        flat.extend(&short);
        flat.extend(std::iter::repeat_n(0u8, 123_456));
        (range, flat)
    }

    /// Every window of a range, the data frames of every frame size among them, is the bytes the
    /// range stands for at that window, as the copied buffer's frames were
    #[test]
    fn a_window_is_the_bytes_the_range_stands_for() {
        let (range, flat) = shapes();
        assert_eq!(range.len(), flat.len() as u64);
        // empty parts are dropped and runs of zeros joined
        assert!(range.parts().iter().all(|part| !part.is_empty()));
        assert!(range
            .parts()
            .windows(2)
            .all(|pair| !matches!(pair, [RangePart::Zeros(_), RangePart::Zeros(_)])));
        // the data frames a stream of each frame size writes, the last one short
        for frame in [1usize << 20, 64 << 10, 4_096, 1_000_003] {
            let mut offset = 0;
            while offset < flat.len() {
                let len = frame.min(flat.len() - offset);
                let written: Vec<u8> = range
                    .window(offset as u64, len as u64)
                    .flat_map(|slice| slice.iter().copied())
                    .collect();
                assert!(
                    written == flat[offset..offset + len],
                    "the frame at {offset} of {len} bytes, frames of {frame}"
                );
                offset += len;
            }
        }
        // a window at each part's edges, inside one part, and past the end
        for (offset, len) in [(0, 1), (300_000, 2), (300_001, 1), (1_400_000, 70_100), (0, u64::MAX), (u64::MAX, 9)] {
            let end = offset.saturating_add(len).min(flat.len() as u64);
            let start = offset.min(end);
            let written: Vec<u8> = range.window(offset, len).flat_map(|slice| slice.iter().copied()).collect();
            assert!(written == flat[start as usize..end as usize], "the window at {offset} of {len}");
        }
        // a run of zeros is given a zero block at a time, and no window gives an empty slice
        let hole: Vec<usize> = range.window(300_001, ZERO_BLOCK as u64 + 4_017).map(<[u8]>::len).collect();
        assert_eq!(hole, vec![ZERO_BLOCK, 4_017]);
        assert!(range.slices().all(|slice| !slice.is_empty()));
        assert_eq!(range.window(300_000, 0).count(), 0, "an empty window");
        // and the whole, in one buffer
        assert!(range.into_vec() == flat);
    }

    /// A range's checksum, folded from its parts and its zeros by their lengths, is the checksum of
    /// the bytes it stands for
    #[test]
    fn a_ranges_checksum_is_its_bytes_checksum() {
        glommio::LocalExecutorBuilder::default()
            .spawn(|| async move {
                let (range, flat) = shapes();
                assert_eq!(range.crc64().await, checksum::crc64(&flat), "every shape");
                // nothing, only zeros, and one part longer than many steps
                assert_eq!(RangeParts::new().crc64().await, checksum::EMPTY);
                let mut hole = RangeParts::new();
                hole.push_zeros(3 << 20);
                assert_eq!(hole.crc64().await, checksum::crc64(&vec![0u8; 3 << 20]), "a hole");
                let long = seeded((4 << 20) + 9, 6);
                assert_eq!(RangeParts::from(long.clone()).crc64().await, checksum::crc64(&long));
            })
            .expect("an executor")
            .join()
            .expect("the test ran");
    }

    /// A range of one part in a buffer of its own gives that buffer back, never a copy; any other
    /// is copied together
    #[test]
    fn one_part_is_moved_out_whole() {
        // one owned part: the same allocation
        let bytes = seeded(10_000, 7);
        let moved = bytes.clone();
        let at = moved.as_ptr();
        let out = RangeParts::from(moved).into_vec();
        assert_eq!(out.as_ptr(), at, "one buffer of its own is moved out");
        assert_eq!(out, bytes);
        // one framed part stays in its frame
        let range = RangeParts::from(framed(&bytes));
        assert!(matches!(range.into_contiguous(), ChunkBytes::Framed { at: 40, .. }));
        // bytes and zeros together are copied into one
        let mut both = RangeParts::from(bytes.clone());
        both.push_zeros(10);
        let mut flat = bytes;
        flat.extend([0u8; 10]);
        assert_eq!(both.into_contiguous(), flat);
        // and nothing is nothing
        assert!(RangeParts::new().into_contiguous().is_empty());
    }
}
