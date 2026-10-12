//! A slice's journal: segments written ahead, records appended into them and synced by group
//!
//! A journal is a few files of fixed size, written ahead with zeros when they are made and
//! overwritten after, so a stage is the cheap sync and not the dear one
//! ([S6](../../../../../docs/src/object-storage/device-store.md#staging-two-cases)). Each segment's
//! first block names its generation, and every record in it is seeded with that generation, so a
//! record of an older use of the segment never reads as a record.
//!
//! # Invariants
//!
//! **A segment is a unit of reuse, not a place in a ring.** It is written again only once every
//! record in it is dropped, so a record held for long - one a later write stands on, or one whose
//! commit has not been heard - pins only its own segment. With no segment free a stage is refused
//! [`StoreError::JournalFull`], which a caller retries.
//!
//! **A start finds every record whatever tore before it**: it scans every block of every segment
//! for a record under the segment's seed, so a record acknowledged after an earlier one that tore
//! is found, where a walk from one record to the next would stop at the tear.

use std::cell::{Cell, RefCell};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use glommio::io::DmaFile;

use super::chunk::BLOCK;
use super::record::{decode_segment, encode_segment, Record};
use super::sync::Committer;
use super::{failed, open, read_exact, sync_dir, write_all, zero_fill, StoreError};
use crate::shared::devices::{SliceId, JOURNAL_SEGMENTS};

/// Where a record is in a slice's journal
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Location {
    /// Its segment
    pub segment: usize,
    /// Its first block's offset in the segment
    pub offset: u64,
}

/// One segment of a journal
struct Segment {
    /// Its file, written ahead to its full length
    file: Rc<DmaFile>,
    /// The generation every record in it is seeded with now; zero for one never used
    generation: u64,
    /// Where its next record goes
    write_at: u64,
    /// The records in it not yet dropped
    live: usize,
    /// The committer that syncs it
    committer: Rc<Committer>,
    /// Open once its header names its generation: a record in it waits for that, or a crash could
    /// leave an acknowledged record under the segment's old generation, where no start finds it
    ready: Rc<glommio::sync::Semaphore>,
}

/// More permits than any number of appends at once: a segment's header is written
const READY: u64 = 1 << 40;

/// A slice's journal
pub struct Journal {
    /// The slice it is a journal of, which every record is seeded with
    slice: SliceId,
    /// The bytes of one segment
    segment_bytes: u64,
    /// Its segments
    segments: RefCell<Vec<Segment>>,
    /// The segment records go into now
    active: Cell<usize>,
    /// The sequence the next record takes
    next_seq: Cell<u64>,
}

impl Journal {
    /// The path of one segment's file
    ///
    /// # Arguments
    ///
    /// * `dir` - The journal's directory
    /// * `index` - The segment
    fn segment_path(dir: &Path, index: usize) -> PathBuf {
        dir.join(format!("segment-{index}"))
    }

    /// Open a slice's journal, making its segments if they are not there, and every record found
    ///
    /// A segment found short of its length is made again: it was being written ahead when a crash
    /// came, and holds no record yet.
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice
    /// * `dir` - The journal's directory, which exists
    /// * `segment_bytes` - The bytes of one segment, a whole number of blocks
    ///
    /// # Errors
    ///
    /// When a segment cannot be made, opened or read.
    pub async fn open(
        slice: SliceId,
        dir: &Path,
        segment_bytes: u64,
    ) -> Result<(Journal, Vec<(Record, Location)>), StoreError> {
        let mut segments = Vec::new();
        let mut found = Vec::new();
        for index in 0..JOURNAL_SEGMENTS as usize {
            let path = Self::segment_path(dir, index);
            // a segment written ahead whole, or one made now
            let whole = std::fs::metadata(&path).is_ok_and(|meta| meta.len() == segment_bytes);
            if !whole {
                let staged = dir.join(format!("segment-{index}.tmp"));
                let file = open(&staged, true).await?;
                zero_fill(&file, segment_bytes).await?;
                super::close(file).await;
                glommio::io::rename(&staged, &path)
                    .await
                    .map_err(|error| failed("renaming a journal segment", error))?;
                sync_dir(dir).await?;
            }
            let file = open(&path, false).await?;
            // its generation, then every record in it seeded with it
            let head = read_exact(&file, 0, BLOCK).await?;
            let generation = decode_segment(&head).unwrap_or(0);
            let mut write_at = BLOCK as u64;
            let mut live = 0;
            if generation > 0 {
                let bytes = read_exact(&file, 0, usize::try_from(segment_bytes).unwrap_or(0)).await?;
                let mut at = BLOCK;
                while at + BLOCK <= bytes.len() {
                    let block = &bytes[at..at + BLOCK];
                    let Some((record, blocks)) = Record::decode_block(slice, generation, block) else {
                        at += BLOCK;
                        continue;
                    };
                    let end = at + blocks as usize * BLOCK;
                    // a record whose units run past its segment never completed
                    if end > bytes.len() {
                        at += BLOCK;
                        continue;
                    }
                    // part of a chunk carries the header its apply writes
                    let record = if record.head_blocks() == 2 {
                        match record.with_header(&bytes[at + BLOCK..at + 2 * BLOCK]) {
                            Some(record) => record,
                            None => {
                                at += BLOCK;
                                continue;
                            }
                        }
                    } else {
                        record
                    };
                    found.push((
                        record,
                        Location {
                            segment: index,
                            offset: at as u64,
                        },
                    ));
                    live += 1;
                    write_at = write_at.max(end as u64);
                    at = end;
                    glommio::yield_if_needed().await;
                }
            }
            let committer = Committer::start(file.clone());
            segments.push(Segment {
                file,
                generation,
                write_at,
                live,
                committer,
                ready: Rc::new(glommio::sync::Semaphore::new(READY)),
            });
        }
        // records go on into the newest segment, and the next sequence is past every one found
        let active = segments
            .iter()
            .enumerate()
            .max_by_key(|(_, segment)| segment.generation)
            .map_or(0, |(index, _)| index);
        let next_seq = found.iter().map(|(record, _)| record.seq).max().map_or(1, |seq| seq + 1);
        let journal = Journal {
            slice,
            segment_bytes,
            segments: RefCell::new(segments),
            active: Cell::new(active),
            next_seq: Cell::new(next_seq),
        };
        Ok((journal, found))
    }

    /// Take room for a record, starting a free segment when the active one is full
    ///
    /// Returns the segment, the offset and the segment's generation, and whether its header block
    /// has to be written first.
    ///
    /// # Arguments
    ///
    /// * `len` - The record's bytes
    fn take(&self, len: u64) -> Result<(usize, u64, u64, bool), StoreError> {
        let mut segments = self.segments.borrow_mut();
        if len + BLOCK as u64 > self.segment_bytes {
            return Err(StoreError::Invalid(format!(
                "a record of {len} bytes is past a journal segment of {}",
                self.segment_bytes
            )));
        }
        let active = self.active.get();
        let segment = &mut segments[active];
        if segment.generation > 0 && segment.write_at + len <= self.segment_bytes {
            let at = segment.write_at;
            segment.write_at += len;
            segment.live += 1;
            return Ok((active, at, segment.generation, false));
        }
        // a segment every record of which is dropped, the active one included, starts again
        let generation = segments.iter().map(|segment| segment.generation).max().unwrap_or(0) + 1;
        let free = (0..segments.len())
            .map(|offset| (active + 1 + offset) % segments.len())
            .find(|index| segments[*index].live == 0)
            .ok_or(StoreError::JournalFull)?;
        let segment = &mut segments[free];
        segment.generation = generation;
        segment.write_at = BLOCK as u64 + len;
        segment.live = 1;
        // closed until its header is written
        segment.ready = Rc::new(glommio::sync::Semaphore::new(0));
        self.active.set(free);
        Ok((free, BLOCK as u64, generation, true))
    }

    /// Append a record and wait until it is durable
    ///
    /// The record's sequence is assigned here. Its head and its units are written, then the
    /// segment's committer syncs everything that completed before it began.
    ///
    /// # Arguments
    ///
    /// * `record` - The record, its sequence assigned here
    /// * `units` - Its units' bytes, in its order, for part of a chunk
    ///
    /// # Errors
    ///
    /// A full journal, or a write or sync the device failed.
    pub async fn append(&self, record: &mut Record, units: &[&[u8]]) -> Result<Location, StoreError> {
        record.seq = self.next_seq.get();
        self.next_seq.set(record.seq + 1);
        let (segment, offset, generation, fresh) = self.take(record.len())?;
        let (file, committer, ready) = {
            let segments = self.segments.borrow();
            (
                segments[segment].file.clone(),
                segments[segment].committer.clone(),
                segments[segment].ready.clone(),
            )
        };
        // a segment started again names its new generation before any record in it, and every
        // record taken into it waits for that
        if fresh {
            // waiters are let go even if it fails: the device is failed then, and refuses them
            let wrote = write_all(&file, &encode_segment(generation), 0).await;
            ready.signal(READY);
            if let Err(error) = wrote {
                self.release(Location { segment, offset });
                return Err(error);
            }
        } else {
            let _open = ready
                .acquire_permit(1)
                .await
                .map_err(|error| failed("waiting for a journal segment's header", error))?;
        }
        let mut bytes = record.encode_head(self.slice, generation);
        for unit in units {
            bytes.extend_from_slice(unit);
        }
        if let Err(error) = write_all(&file, &bytes, offset).await {
            self.release(Location { segment, offset });
            return Err(error);
        }
        committer
            .durable()
            .await
            .map_err(|error| StoreError::DeviceFailed { reason: error })?;
        Ok(Location { segment, offset })
    }

    /// Drop a record: its segment can be written again once every record in it is
    ///
    /// # Arguments
    ///
    /// * `location` - Where it is
    pub fn release(&self, location: Location) {
        let mut segments = self.segments.borrow_mut();
        if let Some(segment) = segments.get_mut(location.segment) {
            segment.live = segment.live.saturating_sub(1);
        }
    }

    /// Read part of a chunk's record's units back, in its order
    ///
    /// # Arguments
    ///
    /// * `location` - Where the record is
    /// * `count` - How many units it carries
    /// * `unit_bytes` - The bytes of one
    ///
    /// # Errors
    ///
    /// A read the device failed.
    pub async fn read_units(
        &self,
        location: Location,
        count: usize,
        unit_bytes: u32,
    ) -> Result<Vec<Vec<u8>>, StoreError> {
        let file = self.segments.borrow()[location.segment].file.clone();
        let start = location.offset + 2 * BLOCK as u64;
        let bytes = read_exact(&file, start, count * unit_bytes as usize).await?;
        Ok(bytes
            .chunks(unit_bytes as usize)
            .map(<[u8]>::to_vec)
            .collect())
    }

    /// Every segment's committer, for a shared flush to sync any one of them
    #[must_use]
    pub fn committers(&self) -> Vec<Rc<Committer>> {
        self.segments
            .borrow()
            .iter()
            .map(|segment| segment.committer.clone())
            .collect()
    }

    /// The records not yet dropped
    #[must_use]
    pub fn live(&self) -> usize {
        self.segments.borrow().iter().map(|segment| segment.live).sum()
    }

    /// Stop every committer once what was asked of it is durable, and close every segment
    pub async fn close(&self) {
        let committers = self.committers();
        for committer in committers {
            committer.stop().await;
        }
        // the segments out of the journal, so each file's last reference is closed here
        let segments = std::mem::take(&mut *self.segments.borrow_mut());
        for segment in segments {
            super::close(segment.file).await;
        }
    }
}
