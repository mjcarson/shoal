//! An erasure coded stripe on the coordinating shard: a range read healthy or degraded, and a write
//! of the whole stripe, or of part of it by reconstruct-write or parity delta (F90)
//!
//! A k+m stripe's layout is contiguous: data chunk `j` holds bytes `[j·C, (j+1)·C)` of the stripe
//! and parity chunk `l` is as long as data chunk zero, so unit `u` of every chunk is one unit row
//! ([S8](../../../../../docs/src/object-storage/erasure-coding.md#geometry)). A data chunk past the
//! stripe's bytes holds none: it is never stored, never asked for, and reads as zeros.
//!
//! - **A read** asks only the data chunks its range falls in, each for exactly its part, and
//!   decodes nothing while they answer ([S9](../../../../../docs/src/object-storage/read-path.md#degraded-reads)).
//!   A part whose holder is stale, down or corrupt is decoded from `k` others over the same range
//!   of their chunks, and no more; with fewer than `k`, that stripe fails by name.
//! - **A write** reads what it needs, stages every chunk it changes under its label and confirms
//!   every chunk it leaves alone, and commits once `k + f` positions hold the stripe after it
//!   ([the acknowledgement rule](../../../../../docs/src/object-storage/write-path.md#the-acknowledgement-rule)).
//!   [`plan_write`] says how: a whole stripe encoded once where the write covers it, or a floor,
//!   a hole or a gap leaves old bytes no partial write may revive; parity delta where it touches
//!   fewer data chunks than it leaves; reconstruct-write otherwise.
//!
//! # Invariants
//!
//! **Only bytes at the label the row names reach a reader or a decoder** (P10): a holder's answer
//! at any other label is never taken, and one ahead of the row reads the row again ([`take`]).
//!
//! **A decode is handed exactly `k` sources of one range**, every one at its named label or
//! holding no bytes there.
//!
//! **A partial write never makes a stale chunk current**: a chunk it changes in part is staged
//! over the label the row names, which a stale holder refuses; only a whole chunk is staged with
//! no base. A position whose chunk holds no bytes is staged whole when a write gives it some.
//!
//! **Every arithmetic step runs on an object executor**, through [`ObjectStore::compute`].

use std::collections::{BTreeMap, BTreeSet};

use futures::future::join_all;

use super::place::{Placement, StripeSet};
use super::read::StripeRead;
use super::write::{
    apply, checked_units, discard, gather as gather_replies, stage_whole, Proposed, RetryWhy, StripePart, StripeStep,
    WholePieces,
};
use super::ObjectTask;
use crate::server::database::ShoalDatabase;
use crate::server::objects::{codec, Holder, ObjectStore, RangeParts};
use crate::shared::objects::{ChangedUnits, Geometry, Label, ObjectEntry};
use crate::shared::protocol::object::ObjectOutcome;
use crate::shared::protocol::peer::chunk::{ChunkBytes, ChunkReply, ChunkRequest, ParityChange, StoreCode, UnitBytes};
use crate::shared::stripes::{commit_bundle, hidden_from, is_hole, write_tag, ChunkId, StripeRow, Touch, WriteCommit};
use crate::shared::traits::QuerySupport;

/// What one holder's answer to a read of part of its chunk is to a reader or a decoder
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Taken {
    /// The bytes, at the label the row names, where the answer that brought them left them (F106)
    Bytes(ChunkBytes),
    /// The holder holds a label the row does not name and not an older one: the row has to be
    /// read again, and is behind its chunks if it reads the same
    Ahead,
    /// A unit failed its checksum: the chunk is set aside, and its position marked
    Corrupt(String),
    /// Anything else, which is never taken: stale, another label, nothing, or no answer
    Missing(String),
}

/// What a reader or a decoder takes of a holder's answer for a position, by the row read (P10)
///
/// Only bytes at the label the row names are taken. A label ahead of it means a write committed
/// since the row was read, and a label behind it, or any other, is a chunk of another state: a
/// decode over it would mix two writes' bytes into one that nobody wrote.
///
/// # Arguments
///
/// * `row` - The stripe's row as read
/// * `position` - The position asked
/// * `reply` - Its holder's answer
#[must_use]
pub(super) fn take(row: &StripeRow, position: usize, reply: ChunkReply) -> Taken {
    match reply {
        ChunkReply::Bytes { label, bytes } if label == row.named(position) => Taken::Bytes(bytes),
        ChunkReply::Bytes { label, .. } => Taken::Missing(format!("bytes at {label:?}, not the row's")),
        ChunkReply::Held { label: Some(held) } if held.ahead_of(row.named(position)) => Taken::Ahead,
        ChunkReply::Refused {
            code: StoreCode::Corrupt,
            message,
        } => Taken::Corrupt(message),
        other => Taken::Missing(format!("{other:?}")),
    }
}

/// What a gather of one range of some positions came to
#[derive(Debug, Default)]
pub(super) struct Gathered {
    /// Each position asked for, its bytes over the range, where the answer that brought them left
    /// them (F106)
    pub bytes: BTreeMap<usize, ChunkBytes>,
    /// Positions whose holder answered a unit that failed its checksum
    pub corrupt: Vec<usize>,
    /// Bytes decoded rather than read
    pub decoded: u64,
    /// Bytes read from holders
    pub read: u64,
}

/// Read one range of some positions' chunks, decoding from `k` others what cannot be read
///
/// The range is in each chunk's own offsets, and every position asked for is answered with
/// exactly `len` bytes: past a position's share of the stripe, zeros, which nobody is asked for.
/// A position the row calls current is asked of its holder at the label the row names; one that
/// is stale, or whose holder does not answer with its bytes, is decoded from `k` sources over the
/// same range - read first from those already in hand, then those holding nothing there, then
/// the rest, a holder on this node first.
///
/// # Arguments
///
/// * `store` - The node's object store
/// * `holders` - Each position's chunk and holder
/// * `row` - The stripe's row as read
/// * `geometry` - The object's geometry
/// * `wanted` - The positions to answer
/// * `offset` - Where in each chunk the range starts
/// * `len` - How long it is
/// * `background` - Whether its requests are a rebuild's or a move's, served behind the foreground
///
/// # Errors
///
/// [`StripeRead::Ahead`] when a holder is past the row, and [`StripeRead::Failed`] when fewer than
/// `k` positions give their bytes.
#[allow(clippy::too_many_arguments)]
pub(super) async fn gather(
    store: &ObjectStore,
    holders: &[(ChunkId, Holder)],
    row: &StripeRow,
    geometry: &Geometry,
    wanted: &[usize],
    offset: u64,
    len: u64,
    background: bool,
) -> Result<Gathered, StripeRead> {
    let width = geometry.width().min(holders.len());
    let k = usize::from(geometry.data);
    let mut gathered = Gathered::default();
    let mut failed: BTreeSet<usize> = BTreeSet::new();
    let mut why: Vec<String> = Vec::new();
    // a position whose share of the stripe ends before the range holds zeros there
    let zeros = |position: usize| geometry.chunk_length(row.length, position) <= offset;
    // the positions asked for, each read from its holder if the row calls it current
    let mut asks = Vec::new();
    for &position in wanted {
        if zeros(position) {
            gathered.bytes.insert(position, vec![0u8; len as usize].into());
        } else if position < width && row.current(position) {
            asks.push(position);
        } else {
            failed.insert(position);
            why.push(format!("position {position} is stale"));
        }
    }
    read_into(store, holders, row, geometry, &asks, offset, len, background, &mut gathered, &mut failed, &mut why).await?;
    let missing: Vec<usize> = wanted
        .iter()
        .copied()
        .filter(|position| !gathered.bytes.contains_key(position))
        .collect();
    if missing.is_empty() {
        return Ok(gathered);
    }
    // k sources over the same range: those in hand, those holding nothing there, then the rest
    let mut sources: Vec<usize> = gathered.bytes.keys().copied().collect();
    let mut candidates: Vec<usize> = (0..width)
        .filter(|position| !gathered.bytes.contains_key(position) && !failed.contains(position))
        .filter(|position| zeros(*position) || row.current(*position))
        .collect();
    candidates.sort_by_key(|position| (!zeros(*position), !store.is_local(&holders[*position].1)));
    let mut candidates = candidates.into_iter();
    while sources.len() < k {
        // as many more as the decode is short of, all at once
        let mut asks = Vec::new();
        while sources.len() + asks.len() < k {
            let Some(position) = candidates.next() else {
                break;
            };
            if zeros(position) {
                gathered.bytes.insert(position, vec![0u8; len as usize].into());
                sources.push(position);
            } else {
                asks.push(position);
            }
        }
        if asks.is_empty() && sources.len() < k {
            return Err(StripeRead::Failed(format!(
                "stripe {} has {} current chunks that answered of the {k} a decode needs: {}",
                row.stripe,
                sources.len(),
                why.join("; ")
            )));
        }
        read_into(store, holders, row, geometry, &asks, offset, len, background, &mut gathered, &mut failed, &mut why).await?;
        sources.extend(asks.iter().copied().filter(|position| gathered.bytes.contains_key(position)));
    }
    // the missing positions, decoded on an object executor from exactly k sources
    sources.truncate(k);
    let inputs: Vec<(u8, Vec<u8>)> = sources
        .iter()
        .map(|position| (*position as u8, gathered.bytes[position].to_vec()))
        .collect();
    let targets: Vec<u8> = missing.iter().map(|position| *position as u8).collect();
    let (data, parity) = (geometry.data, geometry.parity);
    let decoded = store
        .compute(move || {
            Box::pin(async move {
                let mut out = vec![vec![0u8; len as usize]; targets.len()];
                let made = {
                    let refs: Vec<(u8, &[u8])> = inputs.iter().map(|(at, bytes)| (*at, bytes.as_slice())).collect();
                    let mut outs: Vec<&mut [u8]> = out.iter_mut().map(Vec::as_mut_slice).collect();
                    codec::decode(data, parity, &refs, &targets, &mut outs).await
                };
                made.map(|()| out)
            })
        })
        .await
        .map_err(|error| StripeRead::Failed(error.to_string()))?
        .map_err(|error| StripeRead::Failed(error.to_string()))?;
    for (position, bytes) in missing.into_iter().zip(decoded) {
        gathered.decoded += bytes.len() as u64;
        gathered.bytes.insert(position, bytes.into());
    }
    Ok(gathered)
}

/// Read one range of some positions from their holders, at once, into what a gather has
///
/// # Arguments
///
/// * `store` - The node's object store
/// * `holders` - Each position's chunk and holder
/// * `row` - The stripe's row as read
/// * `geometry` - The object's geometry
/// * `asks` - The positions to read
/// * `offset` - Where in each chunk the range starts
/// * `len` - How long it is
/// * `background` - Whether the reads are a rebuild's or a move's
/// * `gathered` - Where the bytes go
/// * `failed` - Where a position that gave none goes
/// * `why` - What each failure was
///
/// # Errors
///
/// [`StripeRead::Ahead`] when a holder is past the row.
#[allow(clippy::too_many_arguments)]
async fn read_into(
    store: &ObjectStore,
    holders: &[(ChunkId, Holder)],
    row: &StripeRow,
    geometry: &Geometry,
    asks: &[usize],
    offset: u64,
    len: u64,
    background: bool,
    gathered: &mut Gathered,
    failed: &mut BTreeSet<usize>,
    why: &mut Vec<String>,
) -> Result<(), StripeRead> {
    let reads = asks.iter().map(|position| {
        let (chunk, holder) = holders[*position];
        // only the position's own share: past it is zeros nobody is asked for
        let end = (offset + len).min(geometry.chunk_length(row.length, *position));
        let request = ChunkRequest::Read {
            chunk,
            offset,
            len: end.saturating_sub(offset),
            labels: vec![row.named(*position)],
        };
        let request = if background {
            super::recover::background(request)
        } else {
            request
        };
        async move { (*position, store.ask_holder(holder, request).await) }
    });
    for (position, reply) in join_all(reads).await {
        match take(row, position, reply) {
            Taken::Bytes(mut bytes) => {
                gathered.read += bytes.len() as u64;
                bytes.resize(len as usize, 0);
                gathered.bytes.insert(position, bytes);
            }
            Taken::Ahead => return Err(StripeRead::Ahead),
            Taken::Corrupt(message) => {
                gathered.corrupt.push(position);
                failed.insert(position);
                why.push(format!("position {position}: corrupt: {message}"));
            }
            Taken::Missing(message) => {
                failed.insert(position);
                why.push(format!("position {position}: {message}"));
            }
        }
    }
    Ok(())
}

/// How a write makes a stripe's parity
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WriteMode {
    /// The whole stripe encoded once and every chunk that holds bytes staged whole
    Whole,
    /// The data chunks the write touches staged, and each parity holder sent the change
    Delta,
    /// The affected unit rows encoded again from their data units, and the new parity staged
    Reconstruct,
}

/// What a write of an erasure coded stripe does, decided from the row it read
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct WritePlan {
    /// How it makes parity
    pub mode: WriteMode,
    /// How much of the stripe holds bytes after it
    pub length: u64,
    /// The positions it stages: the data chunks it changes and every parity chunk, or for a
    /// whole stripe every chunk that holds bytes
    pub staged: Vec<usize>,
    /// Of those, the ones staged whole
    pub whole: Vec<usize>,
    /// The positions it leaves alone that hold bytes, which their holders confirm
    pub untouched: Vec<usize>,
    /// The positions that hold no bytes after it, held with nothing asked
    pub empty: Vec<usize>,
}

/// Decide how a write of a range of an erasure coded stripe is made
///
/// A whole stripe where the write covers every byte it will hold, where a floor or a hole leaves
/// old bytes that must not return, where it starts past the stripe's end, or where it takes a chunk
/// past its file's class (F96); otherwise the data
/// chunks it touches are staged, and parity by delta where it touches fewer data chunks than it
/// leaves and every one it touches is current, by reconstruct-write otherwise.
///
/// # Arguments
///
/// * `geometry` - The object's geometry
/// * `base` - The row read
/// * `held` - How much of the stripe holds bytes a reader sees: the row's length under any floor
/// * `old_bytes_hidden` - Whether a floor or a hole hides bytes the stripe's chunks hold
/// * `at` - Where in the stripe the write starts
/// * `len` - How long it is
#[must_use]
pub(super) fn plan_write(
    geometry: &Geometry,
    base: &StripeRow,
    held: u64,
    old_bytes_hidden: bool,
    at: u64,
    len: u64,
) -> WritePlan {
    let width = geometry.width();
    let k = usize::from(geometry.data);
    let chunk = geometry.chunk_bytes();
    let length = held.max(at + len);
    // what holds bytes after, and what does not
    let empty: Vec<usize> = (0..width)
        .filter(|position| geometry.chunk_length(length, *position) == 0)
        .collect();
    let holding: Vec<usize> = (0..width).filter(|position| !empty.contains(position)).collect();
    // a whole stripe: nothing old is kept, or what is kept could revive bytes a floor cut, or a
    // chunk that holds bytes grows past its file's class, since a chunk is never extended in place
    // (F96)
    let covers = at == 0 && at + len >= held;
    let outgrows = geometry.outgrows(base.length, length);
    if old_bytes_hidden || at > held || covers || outgrows {
        return WritePlan {
            mode: WriteMode::Whole,
            length,
            staged: holding.clone(),
            whole: holding,
            untouched: Vec::new(),
            empty,
        };
    }
    // the data chunks the range falls in, and every parity chunk
    let touched: Vec<usize> = (0..k)
        .filter(|position| {
            let start = *position as u64 * chunk;
            at < start + chunk && at + len > start
        })
        .collect();
    let mut staged = touched.clone();
    staged.extend(k..width);
    // a data chunk staged whole: one that held nothing, or that the write covers end to end
    let whole: Vec<usize> = touched
        .iter()
        .copied()
        .filter(|position| {
            let start = *position as u64 * chunk;
            let before = geometry.chunk_length(base.length, *position);
            let after = geometry.chunk_length(length, *position);
            before == 0 || (at <= start && at + len >= start + after)
        })
        .collect();
    let untouched: Vec<usize> = (0..k)
        .filter(|position| !touched.contains(position) && !empty.contains(position))
        .collect();
    // delta reads the d old ranges, and each parity holder its own parity where it lies;
    // reconstruct reads the k - d untouched; a touched chunk not current has no old bytes to diff
    let d = touched.len();
    let current = touched
        .iter()
        .all(|position| base.current(*position) || geometry.chunk_length(base.length, *position) == 0);
    let mode = if 2 * d < k && current {
        WriteMode::Delta
    } else {
        WriteMode::Reconstruct
    };
    WritePlan {
        mode,
        length,
        staged,
        whole,
        untouched,
        empty,
    }
}

impl<D: ShoalDatabase> ObjectTask<D>
where
    <D::ClientType as QuerySupport>::QueryKinds: Clone,
{
    /// Read part of an erasure coded stripe: each data chunk the range falls in, its part alone,
    /// decoded from `k` others where it cannot be read, each part kept where its answer or its
    /// decode left it (F109)
    ///
    /// # Arguments
    ///
    /// * `placement` - Where the stripe is
    /// * `entry` - The object's entry
    /// * `row` - The stripe's row, if it has one
    /// * `stripe` - The stripe
    /// * `offset` - Where in the stripe the part starts
    /// * `len` - How long it is
    pub(super) async fn read_erasure(
        &self,
        placement: &Placement,
        entry: &ObjectEntry,
        row: Option<&StripeRow>,
        stripe: u64,
        offset: u64,
        len: u64,
    ) -> Result<RangeParts, StripeRead> {
        let key = placement.pg_of(entry.object, stripe);
        let known = self.meta.placement_for(placement, key).await;
        let set = placement
            .stripe_set(entry.object, stripe, &known)
            .map_err(StripeRead::Failed)?;
        match self.read_parts(&set, entry, row, stripe, offset, len).await {
            Err(StripeRead::Failed(why)) => {
                // the group's state as it is now, which a move may have changed under this shard
                let Ok(mut read) = self.meta.read_placements(&[key]).await else {
                    return Err(StripeRead::Failed(why));
                };
                let now = read.remove(&key).unwrap_or_default();
                if now == known {
                    return Err(StripeRead::Failed(why));
                }
                let set = placement
                    .stripe_set(entry.object, stripe, &now)
                    .map_err(StripeRead::Failed)?;
                self.read_parts(&set, entry, row, stripe, offset, len).await
            }
            other => other,
        }
    }

    /// Read part of an erasure coded stripe from the holders a placement group's state names
    ///
    /// # Arguments
    ///
    /// * `set` - The stripe's chunks and holders
    /// * `entry` - The object's entry
    /// * `row` - The stripe's row, if it has one
    /// * `stripe` - The stripe
    /// * `offset` - Where in the stripe the part starts
    /// * `len` - How long it is
    async fn read_parts(
        &self,
        set: &StripeSet,
        entry: &ObjectEntry,
        row: Option<&StripeRow>,
        stripe: u64,
        offset: u64,
        len: u64,
    ) -> Result<RangeParts, StripeRead> {
        let Some(store) = &self.store else {
            return Err(StripeRead::Failed("this node runs no object executor to read stripes with".to_string()));
        };
        let geometry = entry.geometry;
        // a stripe its put wrote and nothing changed has no row: every position at the put's label
        let start = stripe * u64::from(geometry.stripe_bytes);
        let row = row.cloned().unwrap_or_else(|| {
            let length = entry.put_length.saturating_sub(start).min(u64::from(geometry.stripe_bytes));
            StripeRow::put(self.consumer, entry.object, stripe, set.width, length, self.key())
        });
        // each data chunk's part, at once
        let parts = geometry.split(offset, len);
        let holders = set.current();
        let (row_read, geometry_read) = (&row, &geometry);
        let reads = parts.iter().map(|(position, at, part)| async move {
            let wanted = [*position];
            gather(store, holders, row_read, geometry_read, &wanted, *at, *part, false).await
        });
        let mut out = RangeParts::new();
        let mut failure = None;
        for ((position, _, _), gathered) in parts.iter().zip(join_all(reads).await) {
            match gathered {
                Ok(mut gathered) => {
                    // a holder that answered a unit that fails its checksum is reported (F89)
                    for corrupt in &gathered.corrupt {
                        self.report_corrupt(entry, &row, stripe, *corrupt, set.epoch);
                    }
                    // the part where its answer or its decode left it, never copied (F109)
                    out.push_bytes(gathered.bytes.remove(position).unwrap_or_default());
                }
                // a holder past the row wins over every other failure: the row is read again
                Err(StripeRead::Ahead) => return Err(StripeRead::Ahead),
                Err(StripeRead::Failed(why)) => failure = Some(why),
            }
        }
        match failure {
            Some(why) => Err(StripeRead::Failed(format!("{} of {}: {why}", stripe, self.op.path))),
            None => Ok(out),
        }
    }

    /// Write one stripe's part of an erasure coded object: read what the write needs, stage what it
    /// changes, confirm what it leaves, commit once `k + f` positions hold it, apply
    ///
    /// # Arguments
    ///
    /// * `placement` - The pool's placement
    /// * `set` - Where the stripe's chunks are, under the placement group's state read
    /// * `entry` - The object's entry
    /// * `row` - The stripe's row, if it has one
    /// * `part` - The stripe's part of the write
    /// * `attempt` - Which try of this stripe this is
    /// * `nonce` - This task's own
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn write_erasure(
        &self,
        placement: &Placement,
        set: &StripeSet,
        entry: &ObjectEntry,
        row: Option<&StripeRow>,
        part: &StripePart<'_>,
        attempt: u32,
        nonce: u64,
    ) -> StripeStep {
        let store = match self.store() {
            Ok(store) => store.clone(),
            Err(refused) => return StripeStep::Refused(refused),
        };
        let geometry = entry.geometry;
        let stripe_bytes = u64::from(geometry.stripe_bytes);
        let start = part.stripe * stripe_bytes;
        // a stripe with no row gets one before anything is staged, so a stage is never left on a
        // stripe whose length no one chunk can say (decide)
        let Some(row) = row else {
            let length = entry.put_length.saturating_sub(start).min(stripe_bytes);
            let made = StripeRow::put(self.consumer, entry.object, part.stripe, set.width, length, self.key()).noop();
            return match self.meta.commit_stripe(made, None, None).await {
                crate::shared::objects::StripeAnswer::Written | crate::shared::objects::StripeAnswer::Refused(_) => {
                    StripeStep::Retry(RetryWhy::RowMade)
                }
                crate::shared::objects::StripeAnswer::Failed(code, why) => StripeStep::Failed(code, why),
                other => StripeStep::Failed(
                    crate::shared::protocol::error::ErrorCode::Internal,
                    format!("a stripe's row answered {other:?}"),
                ),
            };
        };
        let base = row.clone();
        // a stripe an operator declared lost takes no write: its bytes are gone (F93)
        if base.lost {
            return StripeStep::Refused(self.refused(
                ObjectOutcome::Lost,
                format!(
                    "stripe {} of {} was declared lost by an operator's ResolveStripe",
                    part.stripe, self.op.path
                ),
            ));
        }
        // a fence past the epoch read: a truncate is committing, or died having fenced
        if base.fence > entry.epoch {
            return StripeStep::Advance;
        }
        // a stamp past it: a truncate committed since the entry was read
        if base.stamp > entry.epoch {
            return StripeStep::Reread;
        }
        let hole = is_hole(entry, Some(row), part.stripe);
        let hidden = hidden_from(entry, Some(row), part.stripe).saturating_sub(start);
        let held = if hole { 0 } else { base.length.min(hidden) };
        let label = Label {
            sequence: base.sequence + 1,
            tag: write_tag(&self.op.id, part.stripe, attempt, nonce),
        };
        let plan = plan_write(&geometry, &base, held, hole || hidden < base.length, part.at, part.data.len() as u64);
        // what each position stages, computed with what the write reads
        let staging = match plan.mode {
            WriteMode::Whole => self.stage_plan_whole(&store, set, &base, &geometry, held, part, &plan).await,
            WriteMode::Delta | WriteMode::Reconstruct => {
                self.stage_plan_partial(&store, set, &base, &geometry, part, &plan).await
            }
        };
        let staging = match staging {
            Ok(staging) => staging,
            Err(StripeRead::Ahead) => return StripeStep::Ahead,
            Err(StripeRead::Failed(why)) => {
                return StripeStep::Refused(self.refused(ObjectOutcome::NotReady, why));
            }
        };
        // every holder of every position: a stage, a confirmation, or nothing for a chunk that
        // holds no bytes; an arrival beside its old slice while the group moves; each whole
        // chunk's header naming whose it is (M20)
        let owner = self.key();
        let mut asks = Vec::with_capacity(set.targets.len());
        let mut kinds = Vec::with_capacity(set.targets.len());
        for (chunk, holder) in &set.targets {
            let position = usize::from(chunk.position);
            let kind = if plan.empty.contains(&position) {
                Ask::Empty
            } else if plan.untouched.contains(&position) {
                Ask::Confirm
            } else {
                Ask::Stage
            };
            let answer = match (kind, staging.get(&position)) {
                (Ask::Empty, _) => Err(ChunkReply::Done),
                (Ask::Confirm, _) => {
                    store
                        .send(
                            *holder,
                            ChunkRequest::Confirm {
                                chunk: *chunk,
                                labels: vec![base.named(position)],
                            },
                        )
                        .await
                }
                (Ask::Stage, Some(Staging::Whole(pieces))) => {
                    stage_whole(
                        &store,
                        *holder,
                        *chunk,
                        label,
                        geometry.chunk_bytes(),
                        geometry.unit_bytes,
                        geometry.chunk_length(plan.length, position),
                        pieces,
                        nonce,
                        plan.length,
                        owner,
                    )
                    .await
                }
                (Ask::Stage, Some(Staging::Units(units))) => {
                    store
                        .send(
                            *holder,
                            ChunkRequest::StageUnits {
                                chunk: *chunk,
                                makes: label,
                                expects: vec![base.named(position)],
                                length: geometry.chunk_length(plan.length, position),
                                unit_bytes: geometry.unit_bytes,
                                units: units.clone(),
                            },
                        )
                        .await
                }
                (Ask::Stage, Some(Staging::Delta(changes))) => {
                    store
                        .send(
                            *holder,
                            ChunkRequest::StageParity {
                                chunk: *chunk,
                                makes: label,
                                expects: vec![base.named(position)],
                                length: geometry.chunk_length(plan.length, position),
                                unit_bytes: geometry.unit_bytes,
                                data: geometry.data,
                                parity: geometry.parity,
                                changes: changes.clone(),
                            },
                        )
                        .await
                }
                (Ask::Stage, None) => Err(ChunkReply::Refused {
                    code: StoreCode::Invalid,
                    message: format!("position {position} has nothing to stage"),
                }),
            };
            asks.push(answer);
            kinds.push(kind);
        }
        let replies = gather_replies(asks, self.meta.wait).await;
        // a holder holds the stripe after the write: staged, confirmed at the row's label, or
        // holding no bytes
        let ok: Vec<bool> = replies
            .iter()
            .zip(&set.targets)
            .zip(&kinds)
            .map(|((reply, (chunk, _)), kind)| match kind {
                Ask::Empty => true,
                Ask::Confirm => matches!(reply, Some(ChunkReply::Held { label: Some(held) }) if *held == base.named(usize::from(chunk.position))),
                Ask::Stage => matches!(reply, Some(ChunkReply::Done)),
            })
            .collect();
        let staged: Vec<bool> = ok
            .iter()
            .zip(&kinds)
            .map(|(ok, kind)| *ok && *kind == Ask::Stage)
            .collect();
        // a position holds it only if every holder of it does: a moving one's arrival too
        let positions = set.positions(&ok);
        let count = positions.iter().filter(|held| **held).count();
        if count < placement.needed() {
            discard(&store, &set.targets, &staged, label);
            // a holder past the row read: the loop reads the row again after this answer, and only
            // a row unchanged past it is behind its chunks (F93, item 262)
            if self.holders_ahead(&store, &set.targets, row).await {
                return StripeStep::Ahead;
            }
            // the row moved under this write (item 239)
            if self.row_moved(entry.object, part.stripe, Some(row)).await {
                return StripeStep::Retry(RetryWhy::RowMoved);
            }
            let why: Vec<String> = replies
                .iter()
                .zip(&set.targets)
                .zip(&ok)
                .filter(|(_, ok)| !**ok)
                .map(|((reply, (chunk, _)), _)| format!("position {}: {reply:?}", chunk.position))
                .collect();
            return StripeStep::Refused(self.refused(
                ObjectOutcome::NotReady,
                format!(
                    "stripe {} of {} is held by {count} positions after the write and a write needs {}: {}",
                    part.stripe,
                    self.op.path,
                    placement.needed(),
                    why.join("; ")
                ),
            ));
        }
        // a test can hold a stager here, as a pause across a leader change would, or end it
        if let Some(hold) = crate::server::objects::faults::stager_hold() {
            crate::server::objects::faults::hold_this_stager(hold).await;
        }
        crate::server::objects::faults::hit(crate::server::objects::faults::StripeCrashPoint::Answered);
        // the commit: staged positions held or missed, every other left as it was; what it
        // changed is the whole chunk at the positions it staged (the model's rule, F90)
        let touch: Vec<Touch> = (0..set.width)
            .map(|position| {
                if !plan.staged.contains(&position) {
                    Touch::Untouched
                } else if positions[position] {
                    Touch::Held
                } else {
                    Touch::Missed
                }
            })
            .collect();
        let next = base
            .commit(&WriteCommit {
                label,
                positions: touch,
                stamp: entry.epoch,
                length: plan.length,
                pending: None,
                changed: ChangedUnits::whole(ChangedUnits::mask(plan.staged.iter().copied())),
            })
            .claiming(set.epoch);
        match self
            .propose(next, Some(row.sequence), commit_bundle(&self.op.id, part.stripe, attempt))
            .await
        {
            Proposed::Committed => {
                crate::server::objects::faults::hit(crate::server::objects::faults::StripeCrashPoint::Committed);
                apply(&store, &set.targets, &staged, label);
                StripeStep::Committed
            }
            Proposed::CommittedElsewhere => {
                discard(&store, &set.targets, &staged, label);
                StripeStep::Committed
            }
            Proposed::Refused => {
                discard(&store, &set.targets, &staged, label);
                StripeStep::Retry(RetryWhy::Refused)
            }
            Proposed::Placement => {
                self.meta.refresh_placement(set.key).await;
                discard(&store, &set.targets, &staged, label);
                StripeStep::Retry(RetryWhy::Placement)
            }
            Proposed::Failed(code, why) => StripeStep::Failed(code, why),
        }
    }

    /// A whole stripe's new chunks: its visible bytes read, the rest zeros, the write laid over,
    /// encoded once
    ///
    /// # Arguments
    ///
    /// * `store` - The node's object store
    /// * `set` - The stripe's chunks and holders
    /// * `base` - The row read
    /// * `geometry` - The object's geometry
    /// * `held` - How much of the stripe a reader sees
    /// * `part` - The write's part of the stripe
    /// * `plan` - What the write does
    #[allow(clippy::too_many_arguments)]
    async fn stage_plan_whole(
        &self,
        store: &ObjectStore,
        set: &StripeSet,
        base: &StripeRow,
        geometry: &Geometry,
        held: u64,
        part: &StripePart<'_>,
        plan: &WritePlan,
    ) -> Result<BTreeMap<usize, Staging>, StripeRead> {
        let chunk = geometry.chunk_bytes();
        // the stripe as it will be: what a reader sees of it now, zeros past that, then the write
        let mut stripe = vec![0u8; plan.length as usize];
        // the old bytes the write does not cover, a data chunk at a time
        let keep_to = held.min(plan.length);
        let reads = (0..usize::from(geometry.data))
            .filter_map(|position| {
                let from = position as u64 * chunk;
                let to = (from + chunk).min(keep_to);
                // nothing kept here, or every kept byte rewritten
                let covered = part.at <= from && part.at + part.data.len() as u64 >= to;
                (to > from && !covered).then(|| (position, from, to - from))
            })
            .map(|(position, from, len)| async move {
                (position, from, gather(store, set.current(), base, geometry, &[position], 0, len, false).await)
            })
            .collect::<Vec<_>>();
        for (position, from, gathered) in join_all(reads).await {
            let mut gathered = gathered?;
            let bytes = gathered.bytes.remove(&position).unwrap_or_default();
            stripe[from as usize..from as usize + bytes.len()].copy_from_slice(&bytes);
        }
        stripe[part.at as usize..part.at as usize + part.data.len()].copy_from_slice(part.data);
        // encoded, and every chunk that holds bytes cut into its pieces, on an object executor
        let geometry = *geometry;
        let length = plan.length;
        let whole = plan.whole.clone();
        store
            .compute(move || {
                Box::pin(async move {
                    let parity = crate::server::objects::put::encode_parity(&geometry, &stripe, length).await?;
                    let mut staging = BTreeMap::new();
                    for position in whole {
                        let chunk_length = geometry.chunk_length(length, position);
                        let bytes: &[u8] = if geometry.is_data(position) {
                            let from = (position as u64 * geometry.chunk_bytes()) as usize;
                            &stripe[from..from + chunk_length as usize]
                        } else {
                            &parity[position - usize::from(geometry.data)]
                        };
                        staging.insert(position, Staging::Whole(WholePieces::from_bytes(geometry.unit_bytes, bytes).await));
                    }
                    Ok::<_, codec::CodecError>(staging)
                })
            })
            .await
            .map_err(|error| StripeRead::Failed(error.to_string()))?
            .map_err(|error| StripeRead::Failed(error.to_string()))
    }

    /// A partial write's stages: each data chunk it touches, staged whole or by the units it
    /// changes, and every parity chunk's change or new units
    ///
    /// # Arguments
    ///
    /// * `store` - The node's object store
    /// * `set` - The stripe's chunks and holders
    /// * `base` - The row read
    /// * `geometry` - The object's geometry
    /// * `part` - The write's part of the stripe
    /// * `plan` - What the write does
    async fn stage_plan_partial(
        &self,
        store: &ObjectStore,
        set: &StripeSet,
        base: &StripeRow,
        geometry: &Geometry,
        part: &StripePart<'_>,
        plan: &WritePlan,
    ) -> Result<BTreeMap<usize, Staging>, StripeRead> {
        let chunk = geometry.chunk_bytes();
        let unit = u64::from(geometry.unit_bytes);
        let k = usize::from(geometry.data);
        let end = part.at + part.data.len() as u64;
        // the units each touched data chunk's part of the write covers, in its own offsets
        let mut runs: BTreeMap<usize, (u64, u64)> = BTreeMap::new();
        for position in plan.staged.iter().copied().filter(|position| *position < k) {
            let from = position as u64 * chunk;
            let lo = part.at.max(from) - from;
            let hi = end.min(from + chunk) - from;
            runs.insert(position, (lo / unit, (hi - 1) / unit));
        }
        // the unit rows the write reaches, across every touched chunk
        let first = runs.values().map(|(first, _)| *first).min().unwrap_or(0);
        let last = runs.values().map(|(_, last)| *last).max().unwrap_or(0);
        // the old bytes the write needs: every data chunk's rows for reconstruct-write, the
        // touched chunks' own for a delta
        let wanted: Vec<usize> = match plan.mode {
            WriteMode::Reconstruct => (0..k).collect(),
            _ => runs.keys().copied().collect(),
        };
        let offset = first * unit;
        let len = (last + 1 - first) * unit;
        let old = gather(store, set.current(), base, geometry, &wanted, offset, len, false).await?;
        // each touched chunk's rows after the write: the old bytes with the write laid over
        let mut new: BTreeMap<usize, Vec<u8>> = BTreeMap::new();
        for (position, _) in &runs {
            let mut bytes = old.bytes.get(position).map(|old| old.to_vec()).unwrap_or_else(|| vec![0u8; len as usize]);
            let from = *position as u64 * chunk + offset;
            let lo = part.at.max(from);
            let hi = end.min(from + len);
            if hi > lo {
                bytes[(lo - from) as usize..(hi - from) as usize]
                    .copy_from_slice(&part.data[(lo - part.at) as usize..(hi - part.at) as usize]);
            }
            new.insert(*position, bytes);
        }
        let geometry = *geometry;
        let mode = plan.mode;
        let whole = plan.whole.clone();
        let length = plan.length;
        let old_bytes = old.bytes;
        store
            .compute(move || {
                Box::pin(async move {
                    let mut staging: BTreeMap<usize, Staging> = BTreeMap::new();
                    let unit_bytes = geometry.unit_bytes;
                    // each touched data chunk: whole where the plan says, else its changed units
                    for (position, bytes) in &new {
                        let (lo, hi) = runs[position];
                        if whole.contains(position) {
                            // a whole chunk is its rows from zero: the write's rows start at zero
                            // for a chunk that held nothing, or that the write covers end to end
                            let chunk_length = geometry.chunk_length(length, *position);
                            let mut whole_bytes = vec![0u8; chunk_length as usize];
                            let from = (offset as usize).min(whole_bytes.len());
                            let to = (offset as usize + bytes.len()).min(whole_bytes.len());
                            whole_bytes[from..to].copy_from_slice(&bytes[..to - from]);
                            staging.insert(*position, Staging::Whole(WholePieces::from_bytes(unit_bytes, &whole_bytes).await));
                        } else {
                            let units = (lo..=hi).map(|index| {
                                let at = ((index - first) * u64::from(unit_bytes)) as usize;
                                (index as u32, bytes[at..at + unit_bytes as usize].to_vec())
                            });
                            let units: Vec<(u32, Vec<u8>)> = units.collect();
                            staging.insert(*position, Staging::Units(checked_units(units.iter().map(|(index, bytes)| (*index, bytes))).await));
                        }
                    }
                    let parity_positions = usize::from(geometry.data)..geometry.width();
                    match mode {
                        // each parity holder is sent each touched chunk's change, old XOR new
                        WriteMode::Delta => {
                            let mut changes = Vec::new();
                            for (position, bytes) in &new {
                                let (lo, hi) = runs[position];
                                let old = old_bytes.get(position).map(|old| old.to_vec()).unwrap_or_else(|| vec![0u8; bytes.len()]);
                                let mut units = Vec::new();
                                for index in lo..=hi {
                                    let at = ((index - first) * u64::from(unit_bytes)) as usize;
                                    let change: Vec<u8> = old[at..at + unit_bytes as usize]
                                        .iter()
                                        .zip(&bytes[at..at + unit_bytes as usize])
                                        .map(|(old, new)| old ^ new)
                                        .collect();
                                    units.push((index as u32, change));
                                }
                                changes.push(ParityChange {
                                    data: *position as u8,
                                    units: checked_units(units.iter().map(|(index, bytes)| (*index, bytes))).await,
                                });
                            }
                            for position in parity_positions {
                                staging.insert(position, Staging::Delta(changes.clone()));
                            }
                        }
                        // the affected unit rows encoded again from every data chunk's new values
                        _ => {
                            let rows: Vec<&[u8]> = (0..usize::from(geometry.data))
                                .map(|position| match new.get(&position) {
                                    Some(bytes) => bytes.as_slice(),
                                    None => old_bytes.get(&position).map_or(&[][..], |bytes| &bytes[..]),
                                })
                                .collect();
                            let mut parity = vec![vec![0u8; rows[0].len()]; usize::from(geometry.parity)];
                            {
                                let mut outs: Vec<&mut [u8]> = parity.iter_mut().map(Vec::as_mut_slice).collect();
                                codec::encode(geometry.data, geometry.parity, &rows, &mut outs).await?;
                            }
                            for (row, bytes) in parity.iter().enumerate() {
                                // the new parity's units over the rows the write reaches
                                let cut: Vec<(u32, Vec<u8>)> = (first..=last)
                                    .map(|index| {
                                        let at = ((index - first) * u64::from(unit_bytes)) as usize;
                                        (index as u32, bytes[at..at + unit_bytes as usize].to_vec())
                                    })
                                    .collect();
                                staging.insert(
                                    usize::from(geometry.data) + row,
                                    Staging::Units(checked_units(cut.iter().map(|(index, bytes)| (*index, bytes))).await),
                                );
                            }
                        }
                    }
                    Ok::<_, codec::CodecError>(staging)
                })
            })
            .await
            .map_err(|error| StripeRead::Failed(error.to_string()))?
            .map_err(|error| StripeRead::Failed(error.to_string()))
    }
}

/// What a write asks a position's holder
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ask {
    /// Stage what the write makes of its chunk
    Stage,
    /// Confirm it holds the chunk the row names, which the write leaves alone
    Confirm,
    /// Nothing: its chunk holds no bytes, and it holds the stripe by that alone
    Empty,
}

/// What a write stages on a position
#[derive(Debug, Clone)]
pub(super) enum Staging {
    /// The whole chunk, in pieces with their units' checksums
    Whole(WholePieces),
    /// New values of some units, over the label the row names
    Units(Vec<UnitBytes>),
    /// Each touched data chunk's change, which a parity holder folds into its own parity and
    /// stages as new values
    Delta(Vec<ParityChange>),
}

#[cfg(test)]
mod tests;
