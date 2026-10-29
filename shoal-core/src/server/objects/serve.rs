//! A slice's answer to a chunk request, whoever sent it
//!
//! A request reaches the executor that owns its slice from a shard of this node, through the
//! [`ObjectStore`](super::ObjectStore) handle, or from another node over the object lane; either
//! way it is the object lane's request and is answered here, on the owner, the same way
//! ([F86](../../../../docs/src/features/replicated-pools.md)).
//!
//! A whole chunk arrives in pieces. Its pieces are written in the order they were sent, by one
//! task a stream, so a piece never overtakes the one before it however the requests were spawned.
//!
//! # Invariants
//!
//! **A stream's pieces are written by one task, in order**: the serve loop hands each piece to
//! its stream's task before it reads the next request, so the order a sender wrote them in is the
//! order they reach the file.
//!
//! **Every piece is answered once**: a piece before its chunk's last with done once it is written,
//! the last once the chunk is durable, and every piece of a stream that already failed with a
//! refusal, so a sender on another node never waits on an id nobody answers. A failed stream's file
//! goes back to the pool at its first failure.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use tracing::{event, Level};

use super::store::{ReadAnswer, Slice, Span, StageParity, StageUnits, StoreError, Verdict, WholeWriter};
use crate::shared::objects::Label;
use crate::shared::protocol::peer::chunk::{ChunkReply, ChunkRequest, Piece, StoreCode};

/// What a request's sender is told, once
pub type Replier = Box<dyn FnOnce(ChunkReply)>;

/// What a request is answered with on the executor that served it: an answer to encode whole, or
/// a read's bytes in the buffers its store left them in, which a lane writes after their head
/// without copying them into a body ([F106](../../../../docs/src/features/lane-copies.md))
pub(crate) enum Answered {
    /// An answer, encoded whole
    Reply(ChunkReply),
    /// A read's bytes at a label, where the read left them
    Bytes {
        /// The label they are at
        label: Label,
        /// The bytes, holding their read's draw until they are dropped
        span: Span,
    },
}

impl Answered {
    /// The answer as one that crosses to another thread: ~~a read's bytes copied once into a
    /// buffer of their own (F105)~~ a read's bytes in the heap buffer its read filled, handed over
    /// whole with its draw, or copied once where the device's own buffers hold them (F110)
    pub(crate) async fn into_reply(self) -> ChunkReply {
        match self {
            Answered::Reply(reply) => reply,
            Answered::Bytes { label, span } => ChunkReply::Bytes {
                label,
                bytes: span.into_bytes().await,
            },
        }
    }
}

/// The code a store's refusal crosses the lane as
///
/// # Arguments
///
/// * `error` - The store's refusal
#[must_use]
pub fn code_of(error: &StoreError) -> StoreCode {
    match error {
        StoreError::DeviceFailed { .. } => StoreCode::DeviceFailed,
        StoreError::CannotMake { .. } => StoreCode::CannotMake,
        StoreError::NotStaged { .. } => StoreCode::NotStaged,
        StoreError::NoSpace { .. } => StoreCode::NoSpace,
        StoreError::JournalFull => StoreCode::JournalFull,
        StoreError::Corrupt { .. } => StoreCode::Corrupt,
        StoreError::Budget { .. } => StoreCode::Budget,
        StoreError::UnknownSlice(_) => StoreCode::UnknownSlice,
        StoreError::Invalid(_) => StoreCode::Invalid,
        StoreError::NotSealed => StoreCode::NotSealed,
        StoreError::Removed => StoreCode::Removed,
    }
}

/// A store's refusal as the lane's answer
///
/// # Arguments
///
/// * `error` - The store's refusal
#[must_use]
pub fn refused(error: &StoreError) -> ChunkReply {
    ChunkReply::Refused {
        code: code_of(error),
        message: error.to_string(),
    }
}

/// Refuse units whose bytes do not match the checksums they crossed with
///
/// # Arguments
///
/// * `units` - The units
fn check_units(units: &[crate::shared::protocol::peer::chunk::UnitBytes]) -> Result<(), StoreError> {
    match units
        .iter()
        .find(|unit| crate::shared::checksum::crc64(&unit.bytes) != unit.crc)
    {
        Some(unit) => Err(StoreError::Corrupt { unit: unit.index }),
        None => Ok(()),
    }
}

/// Answer a request that is not a piece of a whole chunk, on the slice it names
///
/// # Arguments
///
/// * `slice` - The slice, on its owner
/// * `request` - The request
pub async fn serve(slice: &Rc<Slice>, request: ChunkRequest) -> ChunkReply {
    // a request as it arrives is a reader's, unless it says it is a rebuild's, a move's or a
    // scrub's, and its answer crosses to another thread, a read's bytes in the buffer it filled
    // (F110)
    serve_as(slice, request, true, true).await.into_reply().await
}

/// Answer a request that is not a piece of a whole chunk, a read's bytes left where its store put
/// them, for an answer written on this executor ([F106](../../../../docs/src/features/lane-copies.md))
///
/// # Arguments
///
/// * `slice` - The slice, on its owner
/// * `request` - The request
pub(crate) async fn answer(slice: &Rc<Slice>, request: ChunkRequest) -> Answered {
    // a request as it arrives is a reader's, unless it says it is a rebuild's, a move's or a scrub's
    serve_as(slice, request, true, false).await
}

/// Answer a request on the slice it names, as a reader's or as recovery's
///
/// A reader's read may be answered from its executor's cache of read units and fills it; a
/// background one's and a scrub's read the device and leave the cache as it was (F102).
///
/// # Arguments
///
/// * `slice` - The slice, on its owner
/// * `request` - The request
/// * `foreground` - Whether it is a reader's
/// * `crosses` - Whether its answer crosses to another thread, so a read fills a buffer that can
///   cross with it (F110)
async fn serve_as(
    slice: &Rc<Slice>,
    request: ChunkRequest,
    foreground: bool,
    crosses: bool,
) -> Answered {
    // a request for what a reclaimer sealed waits for the seal to end, and is never refused for
    // it (M20)
    if request.scope().is_none() {
        slice.until_unsealed(request.chunk()).await;
    }
    // and a stage of what a reclamation just removed is refused, so a late one does not bring it
    // back (item 253)
    if matches!(request, ChunkRequest::StageUnits { .. } | ChunkRequest::StageParity { .. }) {
        if let Err(error) = slice.refuse_removed(request.chunk()) {
            return Answered::Reply(refused(&error));
        }
    }
    // each kind is one of the store's operations
    let served = match request {
        ChunkRequest::Write(_) | ChunkRequest::Stage(_) => Err(StoreError::Invalid(
            "a piece of a whole chunk is served by its stream".to_string(),
        )),
        // a scrub's listing reads its headers under its device's ceiling, a block a chunk (F89)
        ChunkRequest::Scrub(inner) if matches!(*inner, ChunkRequest::List { .. }) => {
            let ChunkRequest::List { after, start, limit } = *inner else {
                unreachable!("matched as a listing")
            };
            slice
                .list(after, start, limit, true)
                .await
                .map(|(chunks, more)| ChunkReply::Listed { chunks, more })
        }
        // a background or a scrub's request reaches here once its slice's gate let it through
        ChunkRequest::Background(inner) | ChunkRequest::Scrub(inner) => {
            return Box::pin(serve_as(slice, *inner, false, crosses)).await
        }
        // some of a chunk's units verified where they lie, the verdict crossing alone (F89)
        ChunkRequest::Verify {
            chunk,
            label,
            offset,
            len,
        } => slice.verify(chunk, label, offset, len).await.map(|verdict| match verdict {
            Verdict::Verified { label, crcs, .. } => ChunkReply::Verified { label, crcs },
            Verdict::Corrupt { label, units } => ChunkReply::Corrupt { label, units },
            Verdict::Held(label) => ChunkReply::Held { label },
        }),
        // the same, with the units' summary for a parity check (F90)
        ChunkRequest::VerifySummary {
            chunk,
            label,
            offset,
            len,
        } => slice
            .verify_summarised(chunk, label, offset, len)
            .await
            .map(|verdict| match verdict {
                Verdict::Verified { label, crcs, summary } => ChunkReply::Summarised {
                    label,
                    crcs,
                    summary: summary.unwrap_or_default(),
                },
                Verdict::Corrupt { label, units } => ChunkReply::Corrupt { label, units },
                Verdict::Held(label) => ChunkReply::Held { label },
            }),
        // a parity holder's fold of a change into its own parity, staged as new values (F90)
        ChunkRequest::StageParity {
            chunk,
            makes,
            expects,
            length,
            data,
            parity,
            changes,
            ..
        } => {
            // a change that changed on the way is never folded
            if let Some(error) = changes.iter().find_map(|change| check_units(&change.units).err()) {
                return Answered::Reply(refused(&error));
            }
            let Some(expects) = expects.first().copied() else {
                return Answered::Reply(refused(&StoreError::Invalid(
                    "a parity stage names no label it is laid over".to_string(),
                )));
            };
            let staged = slice
                .stage_parity(StageParity {
                    id: chunk,
                    makes,
                    expects,
                    length,
                    data,
                    parity,
                    changes: changes
                        .into_iter()
                        .map(|change| {
                            (
                                change.data,
                                change.units.into_iter().map(|unit| (unit.index, Vec::from(unit.bytes))).collect(),
                            )
                        })
                        .collect(),
                })
                .await;
            // a test can end the holder once the stage is synced and before it answers
            if staged.is_ok() {
                super::faults::hit(super::faults::StripeCrashPoint::StageSynced);
            }
            staged.map(|()| ChunkReply::Done)
        }
        // the chunks the slice holds of one placement group, past a place in it (F88)
        ChunkRequest::List {
            after,
            start,
            limit,
        } => slice
            .list(after, start, limit, false)
            .await
            .map(|(chunks, more)| ChunkReply::Listed { chunks, more }),
        ChunkRequest::StageUnits {
            chunk,
            makes,
            expects,
            length,
            units,
            ..
        } => {
            // bytes that changed on the way are never staged
            if let Err(error) = check_units(&units) {
                return Answered::Reply(refused(&error));
            }
            // the first label it may be laid over, then the rest
            let Some((first, also)) = expects.split_first() else {
                return Answered::Reply(refused(&StoreError::Invalid(
                    "a stage names no label it is laid over".to_string(),
                )));
            };
            let staged = slice
                .stage_units(StageUnits {
                    id: chunk,
                    makes,
                    expects: *first,
                    also: also.to_vec(),
                    length,
                    units: units.into_iter().map(|unit| (unit.index, Vec::from(unit.bytes))).collect(),
                })
                .await;
            // a test can end the holder once the stage is synced and before it answers
            if staged.is_ok() {
                super::faults::hit(super::faults::StripeCrashPoint::StageSynced);
            }
            staged.map(|()| ChunkReply::Done)
        }
        ChunkRequest::Apply { chunk, label } => {
            slice.apply(chunk, label).await.map(|()| ChunkReply::Done)
        }
        // a reclaimer's seal, set at once and ended by its reclamation, an unseal or its lapse (M20)
        ChunkRequest::Seal { scope, seal } => {
            slice.seal(scope, seal);
            Ok(ChunkReply::Done)
        }
        ChunkRequest::Unseal { scope, seal } => {
            slice.unseal(scope, seal);
            Ok(ChunkReply::Done)
        }
        // what the seal covers, removed under it
        ChunkRequest::Reclaim { scope, seal } => slice.reclaim(scope, seal).await.map(|_| ChunkReply::Done),
        // a stripe a floor's tombstone committed past, removed through its sequence
        ChunkRequest::DiscardThrough { chunk, through } => {
            slice.discard_through(chunk, through).await.map(|_| ChunkReply::Done)
        }
        ChunkRequest::Discard { chunk, label } => match slice.discard(chunk, label).await {
            // a record already gone is discarded
            Ok(()) | Err(StoreError::NotStaged { .. }) => Ok(ChunkReply::Done),
            Err(error) => Err(error),
        },
        ChunkRequest::Confirm { chunk, labels } => slice
            .confirm(chunk, &labels)
            .await
            .map(|confirmed| ChunkReply::Held {
                label: match confirmed {
                    Ok(label) => Some(label),
                    Err(have) => have,
                },
            }),
        ChunkRequest::Fold {
            chunk,
            makes,
            sources,
            length,
            units,
            ..
        } => match check_units(&units) {
            // bytes that changed on the way are never folded
            Err(error) => Err(error),
            Ok(()) => slice
                .fold(
                    chunk,
                    makes,
                    sources,
                    length,
                    units.into_iter().map(|unit| (unit.index, Vec::from(unit.bytes))).collect(),
                )
                .await
                .map(|()| ChunkReply::Done),
        },
        // a read's bytes, left in the buffers the store read them into (F106)
        ChunkRequest::Read {
            chunk,
            offset,
            len,
            labels,
        } => {
            let read = slice.read_span_as(chunk, offset, len, &labels, foreground, crosses).await;
            return match read {
                Ok(Ok((label, span))) => Answered::Bytes { label, span },
                Ok(Err(have)) => Answered::Reply(ChunkReply::Held { label: have }),
                Err(error) => Answered::Reply(refused(&error)),
            }
        }
    };
    Answered::Reply(served.unwrap_or_else(|error| refused(&error)))
}

/// A read's answer as the lane's, for a caller that asked the store directly
///
/// # Arguments
///
/// * `label` - The label it asked for
/// * `answer` - The store's answer
#[must_use]
pub fn read_reply(label: crate::shared::objects::Label, answer: ReadAnswer) -> ChunkReply {
    match answer {
        ReadAnswer::Bytes(bytes) => ChunkReply::Bytes {
            label,
            bytes: bytes.into(),
        },
        ReadAnswer::Label(have) => ChunkReply::Held { label: have },
    }
}

/// What one stream of pieces has become
enum StreamState {
    /// Its pieces go to its task, in order, each with its units where its lane read them
    Open(futures::channel::mpsc::UnboundedSender<Taken>),
    /// It failed, and every later piece of it is dropped
    Failed,
}

/// A piece as its stream's task takes it: the piece, its units where a lane read them apart for its
/// device if one did (F111), and how its sender is answered
type Taken = (Piece, Option<glommio::io::DmaBuffer>, Replier);

/// A stream's key: its slice, its sender, and the id its sender gave it
type StreamKey = (crate::shared::devices::SliceId, u64, u64);

/// The streams of whole chunks an executor is writing, by slice, sender and stream id
///
/// One sender writes one stripe's chunks to several slices under one stream id, and two of them
/// can be owned by one executor, so a stream is told apart by its slice too.
#[derive(Default, Clone)]
pub struct Streams {
    /// Each stream's state, by its slice, its sender and its id
    open: Rc<RefCell<HashMap<StreamKey, StreamState>>>,
}

/// What becomes of a piece once `Streams::take` has let go of its map
enum Next {
    /// It is refused, with why
    Refuse(Replier, String),
    /// It is queued to its stream's task, which is running
    Queued,
    /// It starts a stream, whose task reads the rest from here
    Start(futures::channel::mpsc::UnboundedReceiver<Taken>),
}

impl Streams {
    /// Hand a piece to its stream's task, starting the task for a stream's first piece
    ///
    /// Called by the serve loop before it reads its next request, so pieces keep their order.
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice, on its owner
    /// * `origin` - Who sent it: a connection, or zero for this node
    /// * `place` - Whether the chunk goes straight into place (a put's) or is staged
    /// * `piece` - The piece
    /// * `aligned` - Its units, where a lane read them apart for its device (F111)
    /// * `reply` - How its sender is answered
    pub fn take(
        &self,
        slice: Rc<Slice>,
        origin: u64,
        place: bool,
        piece: Piece,
        aligned: Option<glommio::io::DmaBuffer>,
        reply: Replier,
    ) {
        let key = (slice.state().id, origin, piece.stream);
        // what becomes of the piece is decided under the map's borrow, which is let go before
        // anything runs: glommio polls a task it spawns before `spawn_local` returns, and a
        // stream's task borrows the map on its way out (item 242)
        let mut open = self.open.borrow_mut();
        let next = match open.get(&key) {
            // a failed stream's later pieces are refused, each once
            Some(StreamState::Failed) => {
                if piece.last {
                    open.remove(&key);
                }
                Next::Refuse(reply, "an earlier piece of this chunk's stream failed".to_string())
            }
            Some(StreamState::Open(sender)) => {
                let last = piece.last;
                // a task gone is a stream that ended; its next piece is no piece of it
                match sender.unbounded_send((piece, aligned, reply)) {
                    Err(error) => {
                        open.remove(&key);
                        let (_, _, reply) = error.into_inner();
                        Next::Refuse(reply, "a piece of a chunk's stream that already ended".to_string())
                    }
                    Ok(()) => {
                        if last {
                            open.remove(&key);
                        }
                        Next::Queued
                    }
                }
            }
            // a stream's first piece starts at the chunk's start, or the stream is lost
            None if piece.offset != 0 => Next::Refuse(
                reply,
                format!("a piece at {} of a stream this slice never began", piece.offset),
            ),
            None => {
                let (sender, receiver) = futures::channel::mpsc::unbounded();
                let last = piece.last;
                let _ = sender.unbounded_send((piece, aligned, reply));
                if !last {
                    open.insert(key, StreamState::Open(sender));
                }
                Next::Start(receiver)
            }
        };
        drop(open);
        // and only then is a refusal answered, or a stream's task started
        match next {
            Next::Refuse(reply, why) => reply(refused(&StoreError::Invalid(why))),
            Next::Queued => {}
            Next::Start(receiver) => {
                let streams = self.clone();
                glommio::spawn_local(async move {
                    streams.write(slice, key, place, receiver).await;
                })
                .detach();
            }
        }
    }

    /// One stream's task: begin the chunk, write each piece in order, finish at the last
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice
    /// * `key` - The stream
    /// * `place` - Whether the chunk goes straight into place
    /// * `pieces` - Its pieces, in order
    async fn write(
        &self,
        slice: Rc<Slice>,
        key: StreamKey,
        place: bool,
        mut pieces: futures::channel::mpsc::UnboundedReceiver<Taken>,
    ) {
        use futures::StreamExt;
        let mut writer: Option<WholeWriter> = None;
        while let Some((piece, aligned, reply)) = pieces.next().await {
            // what the piece was, for the stream's failure if it fails
            let (chunk, offset) = (piece.chunk, piece.offset);
            let written = self.write_piece(&slice, &mut writer, place, piece, aligned).await;
            match written {
                // a piece before the last is answered once it is written
                Ok(false) => reply(ChunkReply::Done),
                Ok(true) => {
                    reply(ChunkReply::Done);
                    return;
                }
                Err(error) => {
                    // said where an operator reads it: every later piece is refused only as "an
                    // earlier piece failed", and a stripe whose width is its `k + f` fails its put
                    event!(
                        Level::WARN,
                        msg = "a whole chunk's stream failed",
                        slice = %key.0,
                        chunk = ?chunk,
                        offset,
                        error = %error
                    );
                    if let Some(writer) = writer.take() {
                        writer.abort().await;
                    }
                    reply(refused(&error));
                    // every later piece of it still to come is refused, until its last
                    if let Some(state) = self.open.borrow_mut().get_mut(&key) {
                        *state = StreamState::Failed;
                    }
                    // and every piece already queued to this task is refused now
                    pieces.close();
                    while let Ok(Some((_, _, reply))) = pieces.try_next() {
                        reply(refused(&StoreError::Invalid(
                            "an earlier piece of this chunk's stream failed".to_string(),
                        )));
                    }
                    return;
                }
            }
        }
        // the sender went before the last piece: the chunk is given up
        self.open.borrow_mut().remove(&key);
        if let Some(writer) = writer.take() {
            writer.abort().await;
        }
    }

    /// Write one piece, beginning the chunk at its first; whether it finished the chunk
    ///
    /// # Arguments
    ///
    /// * `slice` - The slice
    /// * `writer` - The chunk being written, once begun
    /// * `place` - Whether it goes straight into place
    /// * `piece` - The piece
    /// * `aligned` - Its units, where a lane read them apart for its device (F111)
    async fn write_piece(
        &self,
        slice: &Rc<Slice>,
        writer: &mut Option<WholeWriter>,
        place: bool,
        piece: Piece,
        aligned: Option<glommio::io::DmaBuffer>,
    ) -> Result<bool, StoreError> {
        // a piece for what a reclaimer sealed waits for the seal to end (M20), and one of what a
        // reclamation just removed is refused (item 253)
        slice.until_unsealed(&piece.chunk).await;
        slice.refuse_removed(&piece.chunk)?;
        // the chunk, begun at its first piece, in a file of its class once every member sizes files
        // so (F96)
        if writer.is_none() {
            let classed = super::lane::current().is_none_or(|lanes| lanes.classed());
            *writer = Some(
                slice
                    .begin_whole(
                        piece.chunk,
                        piece.label,
                        piece.chunk_bytes,
                        piece.unit_bytes,
                        piece.length,
                        classed,
                    )
                    .await?,
            );
        }
        let current = writer.as_mut().expect("begun");
        // the stripe's length, which a chunk with no row is read by (F90)
        if piece.stripe_length > 0 {
            current.record_stripe_length(piece.stripe_length);
        }
        // and whose chunk it is, which a light scrub asks the consumer about (M20)
        if piece.owner > 0 {
            current.record_owner(piece.owner);
        }
        // a lane's units written as it read them, anything else's copied into the device's buffers
        match aligned {
            Some(units) => current.piece_aligned(piece.offset, &piece.crcs, units).await?,
            None => current.piece(piece.offset, &piece.crcs, &piece.bytes).await?,
        }
        if !piece.last {
            return Ok(false);
        }
        // the last piece finishes it
        let finished = writer.take().expect("begun");
        finished.finish(place).await?;
        // a test can end the holder once a stage is synced and before it answers
        if !place {
            super::faults::hit(super::faults::StripeCrashPoint::StageSynced);
        }
        Ok(true)
    }
}
