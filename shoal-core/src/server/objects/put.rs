//! A put's pipeline: an object's bytes cut into stripes and written to every holder of each
//!
//! A put of an object past its pool's inline threshold writes its stripe chunks straight into
//! place, labelled with the put's label, before the one commit that makes its object current
//! ([S7](../../../../docs/src/object-storage/write-path.md#a-whole-object-in-one-commit)). The
//! coordinating shard reads the client's frames and hands their bytes to this pipeline, which runs
//! on one of the node's object executors, never on a table shard
//! ([X9](../../../../docs/src/object-storage/table-latency.md)):
//!
//! - it cuts the bytes into stripes and each stripe's chunk into pieces of whole units;
//! - it computes each unit's CRC-64/NVME once, as the bytes arrive, and folds every byte into the
//!   put's own checksum, which the shard holds to the one the client named before it commits;
//! - it sends each piece to every holder of its stripe, a holder on this node through its slice's
//!   owner and one on another over the object lane, and lets the pieces go as they are sent, so a
//!   put's bytes leave as they arrive ([F86](../../../../docs/src/features/replicated-pools.md)); a
//!   lane at its bound holds the piece until it has room, so the put goes at the rate its holders
//!   take bytes, and a holder whose lane refused a piece is sent nothing more of the put
//!   ([Resolved #238](../../../../docs/src/appendix/resolved/object-lane-backpressure.md));
//! - a stripe is done once `k + f` holders answered its last piece durably; the rest are waited
//!   for a little longer and then named as missed, for the row that records them.
//!
//! # Invariants
//!
//! **A stripe is acknowledged on its holders' word alone**: a holder counts once it answered the
//! last piece of its chunk, which it does only after the chunk is durable in place
//! ([P11](../../../../docs/src/object-storage/contract.md)).
//!
//! **What the pipeline holds is bounded**: at most `inflight` stripes await their answers, and the
//! bytes ahead of them wait in the bounded channel the shard feeds, so a holder that stops reading
//! stops the client's stream rather than growing a node's memory.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Duration;

use tracing::{event, Level};
use uuid::Uuid;

use super::executor::{answer_of, Answer, Holder, ObjectStore};
use super::store::{ChunkId, StoreError};
use crate::shared::checksum::{crc64, Crc64};
use crate::shared::objects::Label;
use crate::shared::protocol::peer::chunk::{ChunkReply, ChunkRequest, Piece};

/// The most bytes one piece of a chunk carries: an object lane's frame, X11's
pub const PIECE_BYTES: u64 = 1 << 20;

/// What a put's pipeline is to write
#[derive(Debug, Clone)]
pub struct PutPlan {
    /// The put's identity, which every stream it opens is named from
    pub op: Uuid,
    /// The bytes of the object each stripe holds
    pub stripe_bytes: u64,
    /// The bytes of a unit
    pub unit_bytes: u32,
    /// The data chunks of a stripe: one for copies, `k` for an erasure code (F90)
    pub data: u8,
    /// The parity chunks of a stripe, or the copies past the first
    pub parity: u8,
    /// How many bytes the put carries
    pub length: u64,
    /// Each stripe's chunks and the holders they are written to: every position's, then while
    /// its placement group moves, every moving position's arrival (F88)
    pub stripes: Vec<Vec<(ChunkId, Holder)>>,
    /// How many positions of a stripe have to hold it before it counts: `k + f`, a position
    /// holding it only when every holder it is written to does
    pub needed: usize,
    /// How long a stripe's other holders are waited for once `k + f` answered
    pub straggler_wait: Duration,
    /// How long a stripe's holders are waited for at all
    pub deadline: Duration,
    /// How many stripes may await their answers at once
    pub inflight: usize,
    /// The key the bucket records the object under, its path's `ObjectMeta` key, which every
    /// chunk's header carries so a slice can say whose it is (M20)
    pub owner: u64,
}

/// A put whose every stripe is held by enough of its holders
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutDone {
    /// The CRC-64/NVME of every byte the put carried
    pub crc: u64,
    /// How many bytes it carried
    pub bytes: u64,
    /// Each stripe's positions whose holder did not answer that it holds its chunk
    pub missed: BTreeMap<u64, Vec<usize>>,
    /// Each stripe's holders that answered they hold its chunk, in the plan's order
    pub held: BTreeMap<u64, Vec<bool>>,
}

/// Why a put's pipeline did not write it
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PutFailure {
    /// The client's bytes stopped before the put's length
    Source {
        /// How many arrived
        received: u64,
        /// How many the put declared
        declared: u64,
    },
    /// A stripe's holders could not hold it: fewer than `k + f` answered durably
    Short {
        /// The stripe
        stripe: u64,
        /// How many held it
        held: usize,
        /// How many had to
        needed: usize,
        /// What the others answered, for a person
        why: Vec<String>,
    },
    /// The pipeline could not be started on an object executor
    NotStarted(String),
}

impl std::fmt::Display for PutFailure {
    /// Write the failure for a person
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write to
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PutFailure::Source { received, declared } => write!(
                f,
                "the put's bytes stopped at {received} of the {declared} it declared"
            ),
            PutFailure::Short {
                stripe,
                held,
                needed,
                why,
            } => write!(
                f,
                "stripe {stripe} is held by {held} holders and needs {needed}: {}",
                why.join("; ")
            ),
            PutFailure::NotStarted(why) => write!(f, "the put's pipeline did not start: {why}"),
        }
    }
}

/// One stripe whose last pieces are sent, awaiting each holder's answer
struct StripeWait {
    /// The stripe
    stripe: u64,
    /// Each position's answer, if its last piece reached its holder
    answers: Answers,
    /// Each position's refusal, if one was already known
    refused: Vec<Option<String>>,
    /// Each holder that holds its position already: one whose chunk holds no bytes, which an
    /// erasure coded stripe shorter than its data chunks has and never stores (F90)
    held: Vec<bool>,
}

/// Start a put's pipeline on one of the node's object executors
///
/// Returns where its outcome arrives.
///
/// # Arguments
///
/// * `store` - The node's object store
/// * `plan` - What to write
/// * `bytes` - The put's bytes, in order, as the client's frames brought them
pub fn start(
    store: &ObjectStore,
    plan: PutPlan,
    bytes: kanal::AsyncReceiver<Vec<u8>>,
) -> kanal::AsyncReceiver<Result<PutDone, PutFailure>> {
    let (outcome, receiver) = kanal::bounded_async(1);
    let failed = outcome.clone();
    let started = store.run(Box::new(move |store| {
        Box::pin(async move {
            // an erasure coded put encodes each stripe before its parity leaves (F90)
            let done = if plan.data > 1 {
                pipeline_erasure(&store, &plan, &bytes).await
            } else {
                pipeline(&store, &plan, &bytes).await
            };
            let _ = outcome.send(done).await;
        })
    }));
    if let Err(error) = started {
        let _ = failed.try_send(Err(PutFailure::NotStarted(error.to_string())));
    }
    receiver
}

/// The pipeline itself, on an object executor
///
/// # Arguments
///
/// * `store` - The node's object store
/// * `plan` - What to write
/// * `bytes` - The put's bytes, in order
async fn pipeline(
    store: &ObjectStore,
    plan: &PutPlan,
    bytes: &kanal::AsyncReceiver<Vec<u8>>,
) -> Result<PutDone, PutFailure> {
    let unit = u64::from(plan.unit_bytes.max(1));
    // a piece is whole units, at most a frame, and never past a chunk
    let piece_bytes = (PIECE_BYTES / unit).max(1) * unit;
    // every stream a run opens is named from a nonce of its own and its stripe
    let nonce: u64 = rand::random();
    let mut crc = Crc64::new();
    let mut received = 0u64;
    let mut buffer: Vec<u8> = Vec::with_capacity(piece_bytes as usize);
    // where the next piece starts in its stripe, and which stripe
    let mut stripe = 0u64;
    let mut in_stripe = 0u64;
    // the holders of the current stripe that refused a piece already, and their reasons
    let mut refused: Vec<Option<String>> = Vec::new();
    // and every holder whose lane refused a piece of this put, which is sent no more of it
    let mut gone: BTreeSet<Holder> = BTreeSet::new();
    let mut waiting: VecDeque<StripeWait> = VecDeque::new();
    let mut missed = BTreeMap::new();
    let mut held = BTreeMap::new();
    while received < plan.length {
        // the next frame's bytes, folded into the put's checksum in steps
        let Ok(frame) = bytes.recv().await else {
            return Err(PutFailure::Source {
                received,
                declared: plan.length,
            });
        };
        for step in frame.chunks(super::store::STEP) {
            crc.update(step);
            glommio::yield_if_needed().await;
        }
        received += frame.len() as u64;
        let mut rest = &frame[..];
        while !rest.is_empty() {
            // as much of the frame as fills the current piece, never past the stripe
            let left_in_stripe = plan.stripe_bytes - in_stripe;
            let want = (piece_bytes - buffer.len() as u64).min(left_in_stripe - buffer.len() as u64);
            let take = (want as usize).min(rest.len());
            buffer.extend_from_slice(&rest[..take]);
            rest = &rest[take..];
            let stripe_full = in_stripe + buffer.len() as u64 == plan.stripe_bytes;
            let object_done = received == plan.length && rest.is_empty();
            if buffer.len() as u64 == piece_bytes || stripe_full || object_done {
                let last = stripe_full || object_done;
                let piece = std::mem::take(&mut buffer);
                if refused.is_empty() {
                    refused = vec![None; plan.stripes.get(stripe as usize).map_or(0, Vec::len)];
                }
                let (sent, answers) =
                    send_piece(store, plan, nonce, stripe, in_stripe, piece, last, &mut refused, &mut gone).await;
                in_stripe += sent;
                if last {
                    // the last piece's answers are what the stripe waits on
                    let positions = answers.len();
                    waiting.push_back(StripeWait {
                        stripe,
                        answers,
                        refused: std::mem::take(&mut refused),
                        held: vec![false; positions],
                    });
                    stripe += 1;
                    in_stripe = 0;
                    // no more stripes await their answers than the pipeline may hold
                    while waiting.len() > plan.inflight.max(1) {
                        let wait = waiting.pop_front().expect("held");
                        settle(plan, wait, &mut missed, &mut held).await?;
                    }
                }
            }
        }
    }
    // a put of nothing has no stripes to wait for; every other one waits for each
    while let Some(wait) = waiting.pop_front() {
        settle(plan, wait, &mut missed, &mut held).await?;
    }
    Ok(PutDone {
        crc: crc.finish(),
        bytes: received,
        missed,
        held,
    })
}

/// An erasure coded put's pipeline, on an object executor (F90)
///
/// Each stripe's bytes are held until the stripe is whole, since its parity encodes every data
/// chunk's units together. The layout is contiguous, so data chunk `j` is bytes `[j·C, (j+1)·C)`
/// of the stripe and its pieces leave as soon as they fill; once the stripe is whole its parity is
/// encoded here, in steps, and sent. A data chunk past the stripe's bytes holds none and is never
/// stored, and its position is held already ([S8](../../../../docs/src/object-storage/erasure-coding.md#whole-stripes-and-short-ones)).
///
/// # Arguments
///
/// * `store` - The node's object store
/// * `plan` - What to write
/// * `bytes` - The put's bytes, in order
async fn pipeline_erasure(
    store: &ObjectStore,
    plan: &PutPlan,
    bytes: &kanal::AsyncReceiver<Vec<u8>>,
) -> Result<PutDone, PutFailure> {
    let geometry = crate::shared::objects::Geometry {
        stripe_bytes: u32::try_from(plan.stripe_bytes).unwrap_or(u32::MAX),
        unit_bytes: plan.unit_bytes,
        data: plan.data,
        parity: plan.parity,
    };
    let unit = u64::from(plan.unit_bytes.max(1));
    let chunk_bytes = geometry.chunk_bytes();
    // a piece is whole units, at most a frame, and never past its chunk
    let piece_bytes = (PIECE_BYTES / unit).max(1) * unit;
    let nonce: u64 = rand::random();
    let mut crc = Crc64::new();
    let mut received = 0u64;
    let mut stripe = 0u64;
    // the stripe's bytes so far, and how many of them have left as data pieces
    let mut buffer: Vec<u8> = Vec::with_capacity(plan.stripe_bytes.min(plan.length) as usize);
    let mut sent = 0u64;
    let mut sending: Option<StripeSending> = None;
    let mut gone: BTreeSet<Holder> = BTreeSet::new();
    let mut waiting: VecDeque<StripeWait> = VecDeque::new();
    let mut missed = BTreeMap::new();
    let mut held = BTreeMap::new();
    while received < plan.length {
        // the next frame's bytes, folded into the put's checksum in steps
        let Ok(frame) = bytes.recv().await else {
            return Err(PutFailure::Source {
                received,
                declared: plan.length,
            });
        };
        for step in frame.chunks(super::store::STEP) {
            crc.update(step);
            glommio::yield_if_needed().await;
        }
        received += frame.len() as u64;
        let mut rest = &frame[..];
        while !rest.is_empty() {
            // how much of the object this stripe holds, known before its first byte
            let stripe_length = plan
                .length
                .saturating_sub(stripe * plan.stripe_bytes)
                .min(plan.stripe_bytes);
            let current = sending.get_or_insert_with(|| StripeSending::new(plan, &geometry, stripe, stripe_length));
            let take = ((stripe_length - buffer.len() as u64) as usize).min(rest.len());
            buffer.extend_from_slice(&rest[..take]);
            rest = &rest[take..];
            // every data piece the bytes now cover leaves at once, never across its chunk's end
            while sent < buffer.len() as u64 {
                let position = (sent / chunk_bytes) as usize;
                let chunk_start = position as u64 * chunk_bytes;
                let length = geometry.chunk_length(stripe_length, position);
                let offset = sent - chunk_start;
                let end = (offset + piece_bytes).min(length);
                if chunk_start + end > buffer.len() as u64 {
                    break;
                }
                let piece = buffer[(chunk_start + offset) as usize..(chunk_start + end) as usize].to_vec();
                current
                    .send(store, plan, nonce, position, offset, piece, end == length, &mut gone)
                    .await;
                sent = chunk_start + end;
            }
            // the stripe is whole: its parity, encoded here, then sent
            if buffer.len() as u64 == stripe_length {
                let mut done = sending.take().expect("begun");
                let parity = encode_parity(&geometry, &buffer, stripe_length).await;
                let parity = match parity {
                    Ok(parity) => parity,
                    Err(error) => {
                        return Err(PutFailure::Short {
                            stripe,
                            held: 0,
                            needed: plan.needed,
                            why: vec![error.to_string()],
                        })
                    }
                };
                let length = geometry.chunk_length(stripe_length, usize::from(plan.data));
                for (row, bytes) in parity.into_iter().enumerate() {
                    let position = usize::from(plan.data) + row;
                    let mut offset = 0u64;
                    while offset < length {
                        let end = (offset + piece_bytes).min(length);
                        let piece = bytes[offset as usize..end as usize].to_vec();
                        done.send(store, plan, nonce, position, offset, piece, end == length, &mut gone)
                            .await;
                        offset = end;
                    }
                }
                waiting.push_back(done.wait());
                buffer.clear();
                sent = 0;
                stripe += 1;
                // no more stripes await their answers than the pipeline may hold
                while waiting.len() > plan.inflight.max(1) {
                    let wait = waiting.pop_front().expect("held");
                    settle(plan, wait, &mut missed, &mut held).await?;
                }
            }
        }
    }
    // every stripe's answers
    while let Some(wait) = waiting.pop_front() {
        settle(plan, wait, &mut missed, &mut held).await?;
    }
    Ok(PutDone {
        crc: crc.finish(),
        bytes: received,
        missed,
        held,
    })
}

/// A stripe's parity, from its bytes: each parity chunk as long as data chunk zero, in whole units
///
/// A data chunk shorter than the parity is encoded with zeros past its bytes, which is what they
/// read as; a whole stripe's chunks are encoded where they lie in its buffer.
///
/// # Arguments
///
/// * `geometry` - The stripe's geometry
/// * `stripe` - The stripe's bytes
/// * `stripe_length` - How many of them there are
pub(crate) async fn encode_parity(
    geometry: &crate::shared::objects::Geometry,
    stripe: &[u8],
    stripe_length: u64,
) -> Result<Vec<Vec<u8>>, super::codec::CodecError> {
    let unit = u64::from(geometry.unit_bytes.max(1));
    let chunk_bytes = geometry.chunk_bytes();
    // every chunk encoded at the parity's length, whole units
    let parity_length = geometry.chunk_length(stripe_length, usize::from(geometry.data));
    let padded = parity_length.div_ceil(unit) * unit;
    // each data chunk where it lies in the buffer, or copied out with zeros where it is short
    let mut owned: Vec<(usize, Vec<u8>)> = Vec::new();
    for position in 0..usize::from(geometry.data) {
        let start = position as u64 * chunk_bytes;
        let length = geometry.chunk_length(stripe_length, position);
        if length < padded {
            // a chunk past the stripe's bytes holds none, and starts past its buffer
            let mut bytes = vec![0u8; padded as usize];
            if length > 0 {
                bytes[..length as usize].copy_from_slice(&stripe[start as usize..(start + length) as usize]);
            }
            owned.push((position, bytes));
        }
    }
    let data: Vec<&[u8]> = (0..usize::from(geometry.data))
        .map(|position| match owned.iter().find(|(at, _)| *at == position) {
            Some((_, bytes)) => bytes.as_slice(),
            None => {
                let start = (position as u64 * chunk_bytes) as usize;
                &stripe[start..start + padded as usize]
            }
        })
        .collect();
    let mut parity = vec![vec![0u8; padded as usize]; usize::from(geometry.parity)];
    {
        let mut outs: Vec<&mut [u8]> = parity.iter_mut().map(Vec::as_mut_slice).collect();
        super::codec::encode(geometry.data, geometry.parity, &data, &mut outs).await?;
    }
    // each parity chunk holds the parity's length, its last unit's tail zeros
    for chunk in &mut parity {
        chunk.truncate(parity_length as usize);
    }
    Ok(parity)
}

/// One erasure coded stripe's pieces as they leave: each holder's last answer and refusal
struct StripeSending {
    /// The stripe
    stripe: u64,
    /// How much of the stripe holds bytes
    stripe_length: u64,
    /// The stripe's chunks and holders
    targets: Vec<(ChunkId, Holder)>,
    /// Each holder's answer to the last piece sent it
    answers: Answers,
    /// Each holder's refusal, once it refused a piece
    refused: Vec<Option<String>>,
    /// Each holder whose position holds no bytes, held without a piece
    held: Vec<bool>,
    /// The stream every piece of this stripe names, a chunk's own beside it
    stream: u64,
}

impl StripeSending {
    /// A stripe about to leave, with the holders of positions that hold no bytes held already
    ///
    /// # Arguments
    ///
    /// * `plan` - The put
    /// * `geometry` - The stripe's geometry
    /// * `stripe` - The stripe
    /// * `stripe_length` - How much of it holds bytes
    fn new(plan: &PutPlan, geometry: &crate::shared::objects::Geometry, stripe: u64, stripe_length: u64) -> Self {
        let targets = plan.stripes.get(stripe as usize).cloned().unwrap_or_default();
        let held = targets
            .iter()
            .map(|(chunk, _)| geometry.chunk_length(stripe_length, usize::from(chunk.position)) == 0)
            .collect();
        StripeSending {
            stripe,
            stripe_length,
            answers: vec![None; targets.len()],
            refused: vec![None; targets.len()],
            held,
            targets,
            stream: rand::random(),
        }
    }

    /// Send one piece of one position's chunk to every holder of that position
    ///
    /// # Arguments
    ///
    /// * `store` - The node's object store
    /// * `plan` - The put
    /// * `nonce` - The run's nonce
    /// * `position` - The position
    /// * `offset` - Where in its chunk the piece starts
    /// * `bytes` - The piece's bytes, whole units unless it is the chunk's last
    /// * `last` - Whether it is the chunk's last
    /// * `gone` - Every holder sent no more of the put
    #[allow(clippy::too_many_arguments)]
    async fn send(
        &mut self,
        store: &ObjectStore,
        plan: &PutPlan,
        nonce: u64,
        position: usize,
        offset: u64,
        mut bytes: Vec<u8>,
        last: bool,
        gone: &mut BTreeSet<Holder>,
    ) {
        let unit = plan.unit_bytes as usize;
        // the chunk's last unit is padded with zeros to a whole one
        bytes.resize(bytes.len().div_ceil(unit) * unit, 0);
        // each unit's checksum, once, before the piece leaves
        let mut crcs = Vec::with_capacity(bytes.len() / unit);
        for chunk in bytes.chunks(unit) {
            crcs.push(crc64(chunk));
            glommio::yield_if_needed().await;
        }
        // the bytes every holder's piece shares, never copied for each (F106)
        let bytes = bytes::Bytes::from(bytes);
        let geometry = crate::shared::objects::Geometry {
            stripe_bytes: u32::try_from(plan.stripe_bytes).unwrap_or(u32::MAX),
            unit_bytes: plan.unit_bytes,
            data: plan.data,
            parity: plan.parity,
        };
        let length = geometry.chunk_length(self.stripe_length, position);
        let stream = nonce ^ self.stream ^ crate::shared::rendezvous::mix64(self.stripe.wrapping_add(1) ^ ((position as u64) << 56));
        for (index, (chunk, holder)) in self.targets.iter().enumerate() {
            if usize::from(chunk.position) != position {
                continue;
            }
            // a holder that refused a piece of this stripe, or of the put, is sent no more
            if self.refused[index].is_some() {
                continue;
            }
            if gone.contains(holder) {
                self.refused[index] = Some(format!("{holder:?} refused an earlier stripe of this put"));
                self.answers[index] = None;
                continue;
            }
            let piece = Piece {
                stream,
                chunk: *chunk,
                label: Label::PUT,
                chunk_bytes: geometry.chunk_bytes(),
                unit_bytes: plan.unit_bytes,
                length,
                offset,
                last,
                stripe_length: self.stripe_length,
                owner: plan.owner,
                crcs: crcs.clone(),
                bytes: bytes.clone(),
            };
            match store.send(*holder, ChunkRequest::Write(piece)).await {
                Ok(answer) => self.answers[index] = Some(answer),
                Err(reply) => {
                    self.refused[index] = Some(format!("{holder:?} refused: {reply:?}"));
                    self.answers[index] = None;
                    gone.insert(*holder);
                }
            }
        }
    }

    /// The stripe, every piece sent, as the answers it waits on
    fn wait(self) -> StripeWait {
        StripeWait {
            stripe: self.stripe,
            answers: self.answers,
            refused: self.refused,
            held: self.held,
        }
    }
}

/// Where each holder's answer to a piece arrives, if the piece reached it
type Answers = Vec<Option<kanal::AsyncReceiver<Result<Answer, StoreError>>>>;

/// Send one piece of a stripe's chunk to each of its holders
///
/// Returns the bytes of the stripe the piece covered, before any padding, and where each holder's
/// answer arrives; only a chunk's last piece's answers are waited on, since an earlier piece's
/// failure ends its stream, which the last piece's answer then says. A holder that refused an
/// earlier piece is sent no more of the stripe, and one whose send was refused - its lane stalled,
/// or nothing reaches its node - no more of the put, so a stalled holder costs a put one stall
/// rather than one a stripe.
///
/// # Arguments
///
/// * `store` - The node's object store
/// * `plan` - The put
/// * `nonce` - The run's nonce, which names its streams
/// * `stripe` - The stripe
/// * `offset` - Where in the stripe's chunk the piece starts
/// * `bytes` - The piece's bytes, whole units unless it is the object's last
/// * `last` - Whether it is the chunk's last
/// * `refused` - Each position's refusal so far
/// * `gone` - Every holder sent no more of the put
#[allow(clippy::too_many_arguments)]
async fn send_piece(
    store: &ObjectStore,
    plan: &PutPlan,
    nonce: u64,
    stripe: u64,
    offset: u64,
    mut bytes: Vec<u8>,
    last: bool,
    refused: &mut [Option<String>],
    gone: &mut BTreeSet<Holder>,
) -> (u64, Answers) {
    let unit = plan.unit_bytes as usize;
    let covered = bytes.len() as u64;
    // the object's last unit is padded with zeros to a whole one
    bytes.resize(bytes.len().div_ceil(unit) * unit, 0);
    // each unit's checksum, once, before the piece leaves
    let mut crcs = Vec::with_capacity(bytes.len() / unit);
    for chunk in bytes.chunks(unit) {
        crcs.push(crc64(chunk));
        glommio::yield_if_needed().await;
    }
    // the bytes every holder's piece shares, never copied for each (F106)
    let bytes = bytes::Bytes::from(bytes);
    let holders = plan.stripes.get(stripe as usize).cloned().unwrap_or_default();
    // how much of the chunk holds bytes: its stripe's share of the object
    let start = stripe * plan.stripe_bytes;
    let length = plan.length.saturating_sub(start).min(plan.stripe_bytes);
    let stream = nonce ^ crate::shared::rendezvous::mix64(stripe.wrapping_add(1));
    let mut answers = Vec::with_capacity(holders.len());
    for (position, (chunk, holder)) in holders.iter().enumerate() {
        // a holder that refused a piece of this stripe is sent no more of it
        if refused.get(position).is_some_and(Option::is_some) {
            answers.push(None);
            continue;
        }
        // nor is one whose send was refused earlier in the put
        if gone.contains(holder) {
            if let Some(slot) = refused.get_mut(position) {
                *slot = Some(format!("{holder:?} refused an earlier stripe of this put"));
            }
            answers.push(None);
            continue;
        }
        let piece = Piece {
            stream,
            chunk: *chunk,
            label: Label::PUT,
            chunk_bytes: plan.stripe_bytes,
            unit_bytes: plan.unit_bytes,
            length,
            offset,
            last,
            stripe_length: length,
            owner: plan.owner,
            crcs: crcs.clone(),
            bytes: bytes.clone(),
        };
        match store.send(*holder, ChunkRequest::Write(piece)).await {
            Ok(answer) => answers.push(Some(answer)),
            Err(reply) => {
                if let Some(slot) = refused.get_mut(position) {
                    *slot = Some(format!("{holder:?} refused: {reply:?}"));
                }
                gone.insert(*holder);
                answers.push(None);
            }
        }
    }
    (covered, answers)
}

/// Which of a stripe's positions hold it, given which of its holders do
///
/// A position holds it only when every holder it was written to does: while its placement group
/// moves, its old slice and its arrival both (F88).
///
/// # Arguments
///
/// * `targets` - The stripe's chunks and holders, as the plan has them
/// * `held` - Whether each holder holds it
fn positions_held(targets: &[(ChunkId, Holder)], held: &[bool]) -> Vec<bool> {
    let width = targets
        .iter()
        .map(|(chunk, _)| usize::from(chunk.position) + 1)
        .max()
        .unwrap_or(0);
    let mut positions = vec![true; width];
    for (index, (chunk, _)) in targets.iter().enumerate() {
        if !held.get(index).copied().unwrap_or(false) {
            positions[usize::from(chunk.position)] = false;
        }
    }
    positions
}

/// Wait for a stripe's holders: done once `k + f` positions hold it, the rest named missed
///
/// # Arguments
///
/// * `plan` - The put
/// * `wait` - The stripe and its answers
/// * `missed` - Where each stripe's missed positions are recorded
/// * `holding` - Where each stripe's holders that hold it are recorded
async fn settle(
    plan: &PutPlan,
    wait: StripeWait,
    missed: &mut BTreeMap<u64, Vec<usize>>,
    holding: &mut BTreeMap<u64, Vec<bool>>,
) -> Result<(), PutFailure> {
    use futures::future::FutureExt;
    use futures::stream::{FuturesUnordered, StreamExt};
    let targets = plan.stripes.get(wait.stripe as usize).cloned().unwrap_or_default();
    let positions = wait.answers.len().max(wait.refused.len()).max(targets.len());
    let mut held = vec![false; positions];
    // a holder whose position holds no bytes holds it without a piece (F90)
    for (target, already) in wait.held.iter().enumerate() {
        if *already {
            if let Some(slot) = held.get_mut(target) {
                *slot = true;
            }
        }
    }
    let mut why: Vec<String> = wait.refused.iter().flatten().cloned().collect();
    // every answer that can still come, raced as they come
    let mut pending: FuturesUnordered<_> = wait
        .answers
        .into_iter()
        .enumerate()
        .filter_map(|(position, answer)| answer.map(|answer| answer_of(answer).map(move |reply| (position, reply))))
        .collect();
    // the stripe's holders are waited for until its deadline, and once k + f hold it, the rest
    // until a little after
    let deadline = std::time::Instant::now() + plan.deadline;
    let mut until = deadline;
    // the positions held before any answer: those whose chunk holds no bytes
    let mut count = positions_held(&targets, &held).iter().filter(|held| **held).count();
    if count >= plan.needed {
        until = deadline.min(std::time::Instant::now() + plan.straggler_wait);
    }
    loop {
        let left = until.saturating_duration_since(std::time::Instant::now());
        if left.is_zero() {
            break;
        }
        let next = futures::select! {
            answer = pending.next().fuse() => answer,
            () = glommio::timer::sleep(left).fuse() => continue,
        };
        match next {
            // every answer is in
            None => break,
            Some((position, ChunkReply::Done)) => {
                held[position] = true;
                let now = positions_held(&targets, &held).iter().filter(|held| **held).count();
                // the first time k + f hold it, the rest have a little longer
                if now >= plan.needed && count < plan.needed {
                    until = deadline.min(std::time::Instant::now() + plan.straggler_wait);
                }
                count = now;
            }
            Some((position, reply)) => why.push(format!("position {position}: {reply:?}")),
        }
    }
    if count < plan.needed {
        return Err(PutFailure::Short {
            stripe: wait.stripe,
            held: count,
            needed: plan.needed,
            why,
        });
    }
    // every position not every holder of which answered it holds the chunk is missed
    let missing: Vec<usize> = positions_held(&targets, &held)
        .iter()
        .enumerate()
        .filter(|(_, held)| !**held)
        .map(|(position, _)| position)
        .collect();
    if !missing.is_empty() {
        event!(Level::DEBUG, msg = "a put's stripe missed some holders", stripe = wait.stripe, missed = ?missing, why = ?why);
        missed.insert(wait.stripe, missing);
    }
    holding.insert(wait.stripe, held);
    Ok(())
}
