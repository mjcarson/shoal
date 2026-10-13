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

use super::store::{ReadAnswer, Slice, StageUnits, StoreError, WholeWriter};
use crate::shared::protocol::peer::chunk::{ChunkReply, ChunkRequest, Piece, StoreCode};

/// What a request's sender is told, once
pub type Replier = Box<dyn FnOnce(ChunkReply)>;

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
    // each kind is one of the store's operations
    let served = match request {
        ChunkRequest::Write(_) | ChunkRequest::Stage(_) => Err(StoreError::Invalid(
            "a piece of a whole chunk is served by its stream".to_string(),
        )),
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
                return refused(&error);
            }
            // the first label it may be laid over, then the rest
            let Some((first, also)) = expects.split_first() else {
                return refused(&StoreError::Invalid("a stage names no label it is laid over".to_string()));
            };
            let staged = slice
                .stage_units(StageUnits {
                    id: chunk,
                    makes,
                    expects: *first,
                    also: also.to_vec(),
                    length,
                    units: units.into_iter().map(|unit| (unit.index, unit.bytes)).collect(),
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
                    units.into_iter().map(|unit| (unit.index, unit.bytes)).collect(),
                )
                .await
                .map(|()| ChunkReply::Done),
        },
        ChunkRequest::Read {
            chunk,
            offset,
            len,
            labels,
        } => slice
            .read_any(chunk, offset, len, &labels)
            .await
            .map(|read| match read {
                Ok((label, bytes)) => ChunkReply::Bytes { label, bytes },
                Err(have) => ChunkReply::Held { label: have },
            }),
    };
    served.unwrap_or_else(|error| refused(&error))
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
        ReadAnswer::Bytes(bytes) => ChunkReply::Bytes { label, bytes },
        ReadAnswer::Label(have) => ChunkReply::Held { label: have },
    }
}

/// What one stream of pieces has become
enum StreamState {
    /// Its pieces go to its task, in order
    Open(futures::channel::mpsc::UnboundedSender<(Piece, Replier)>),
    /// It failed, and every later piece of it is dropped
    Failed,
}

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
    /// * `reply` - How its sender is answered
    pub fn take(&self, slice: Rc<Slice>, origin: u64, place: bool, piece: Piece, reply: Replier) {
        let key = (slice.state().id, origin, piece.stream);
        let mut open = self.open.borrow_mut();
        match open.get(&key) {
            // a failed stream's later pieces are refused, each once
            Some(StreamState::Failed) => {
                if piece.last {
                    open.remove(&key);
                }
                reply(refused(&StoreError::Invalid(
                    "an earlier piece of this chunk's stream failed".to_string(),
                )));
            }
            Some(StreamState::Open(sender)) => {
                let last = piece.last;
                // a task gone is a stream that ended; its next piece is no piece of it
                if let Err(error) = sender.unbounded_send((piece, reply)) {
                    open.remove(&key);
                    let (_, reply) = error.into_inner();
                    reply(refused(&StoreError::Invalid(
                        "a piece of a chunk's stream that already ended".to_string(),
                    )));
                } else if last {
                    open.remove(&key);
                }
            }
            None => {
                // a stream's first piece starts at the chunk's start, or the stream is lost
                if piece.offset != 0 {
                    reply(refused(&StoreError::Invalid(format!(
                        "a piece at {} of a stream this slice never began",
                        piece.offset
                    ))));
                    return;
                }
                let (sender, receiver) = futures::channel::mpsc::unbounded();
                let last = piece.last;
                let _ = sender.unbounded_send((piece, reply));
                if !last {
                    open.insert(key, StreamState::Open(sender));
                }
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
        mut pieces: futures::channel::mpsc::UnboundedReceiver<(Piece, Replier)>,
    ) {
        use futures::StreamExt;
        let mut writer: Option<WholeWriter> = None;
        while let Some((piece, reply)) = pieces.next().await {
            let written = self.write_piece(&slice, &mut writer, place, piece).await;
            match written {
                // a piece before the last is answered once it is written
                Ok(false) => reply(ChunkReply::Done),
                Ok(true) => {
                    reply(ChunkReply::Done);
                    return;
                }
                Err(error) => {
                    event!(Level::DEBUG, msg = "a whole chunk's stream failed", error = %error);
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
                    while let Ok(Some((_, reply))) = pieces.try_next() {
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
    async fn write_piece(
        &self,
        slice: &Rc<Slice>,
        writer: &mut Option<WholeWriter>,
        place: bool,
        piece: Piece,
    ) -> Result<bool, StoreError> {
        // the chunk, begun at its first piece
        if writer.is_none() {
            *writer = Some(
                slice
                    .begin_whole(
                        piece.chunk,
                        piece.label,
                        piece.chunk_bytes,
                        piece.unit_bytes,
                        piece.length,
                    )
                    .await?,
            );
        }
        let current = writer.as_mut().expect("begun");
        current.piece(piece.offset, &piece.crcs, &piece.bytes).await?;
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
