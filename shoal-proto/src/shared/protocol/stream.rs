//! More than one frame for one query: a body carried across bounded frames
//! ([F73](../../../../docs/src/features/bodies-across-frames.md))
//!
//! Until F73 a query's body was one frame and its answer was one frame, so the frame bound was
//! the largest thing either could be, and a receiver read a whole body into one allocation before
//! it acted on any of it. This module is the part of the stream layer both peers agree on, with no
//! I/O in it:
//!
//! - **An opener** is an ordinary frame - a `Queries` or a `Response` - with [`Flags::STREAMED`]
//!   set. Its body carries what it always carried ahead of its payload, then the stream's declared
//!   length, and no payload: the payload follows in data frames.
//! - **A data frame** ([`MessageType::Data`]) carries a sixteen byte id, the offset of its bytes in
//!   the stream, and the bytes. [`Flags::LAST`], reserved since F10, marks the final one. A data
//!   frame's bytes are never an archive and never validated as one; whoever receives them puts them
//!   where they belong and nothing else.
//! - **The id is the opener's id**: a bundle's for a request, a query's for an answer. At most one
//!   stream an id is open in each direction of a connection, which is why a writer interleaves the
//!   frames of different ids and never of one.
//! - **The receiver's state** is [`Inbound`]: it judges every frame against the stream it names -
//!   in order, within its length, `LAST` exactly at its end - and says where the bytes go.
//!
//! Streams are spoken only between peers that agreed to them at the hello:
//! [`CLIENT_CAP_STREAMS`] in the capability byte, and each side's largest assembled body in the
//! byte after it ([`body_bound`]). M13's object frames reuse this layer: an object's bytes are the
//! data frames defined here, behind openers of their own.
//!
//! # Invariants
//!
//! **A stream is judged frame by frame, and a frame that breaks it ends the connection.** An
//! offset other than the bytes received so far, a byte past the declared length, `LAST` anywhere
//! but at the end, or a frame for an id that is not open is a peer out of step, and a peer out of
//! step cannot be resynchronized. A stream the receiver refuses by policy - too long, or no room
//! for it - is answered by name and drained, and the connection goes on.
//!
//! **Bytes a stream reserved never stop the reader.** An assembling stream takes its whole
//! declared length at its opener, so the bytes it holds can only be released by the bytes still to
//! be read. Counting them against [`Inbound::may_read`] would let one stream stop the read that is
//! its only way to finish. Only bytes a sink releases as it consumes them ([`Hold::Windowed`]) are
//! a window.

use uuid::Uuid;

use super::read::{ReadOptions, CLIENT_CAP_LEADER_HINTS, CLIENT_CAP_READ_OPTIONS};
use super::trace::{TraceContext, TRACE_CONTEXT_LEN};
use super::{Flags, Header, MessageType, ProtocolError, RequestHead, HEADER_LEN, QUERY_ID_LEN};

/// The client reads and writes streams: openers and data frames
///
/// Spent from the hello's capability byte after [`CLIENT_CAP_READ_OPTIONS`]; the ack's copy is
/// what the server granted. A server built before F73 grants nothing here, and is sent no stream.
pub const CLIENT_CAP_STREAMS: u8 = 1 << 1;

/// Every capability bit a client of this build asks for
pub const CLIENT_CAPS: u8 = CLIENT_CAP_READ_OPTIONS | CLIENT_CAP_STREAMS | CLIENT_CAP_LEADER_HINTS;

/// The bytes after a data frame's header and before its payload: the id and the offset
pub const DATA_HEAD_LEN: usize = QUERY_ID_LEN + 8;

/// A data frame's header and head together
pub const DATA_PREAMBLE_LEN: usize = HEADER_LEN + DATA_HEAD_LEN;

/// The declared length an opener carries after its other sections
pub const DECLARED_LEN: usize = 8;

/// The body of a `Queries` opener after its trace context and read options: the bundle's id and
/// its declared length
pub const QUERIES_OPENER_LEN: usize = QUERY_ID_LEN + DECLARED_LEN;

/// The most streams one direction of a connection may have open at once
pub const MAX_OPEN_STREAMS: usize = 16;

/// The smallest data frame a sender may be configured to write
pub const MIN_STREAM_FRAME_BYTES: u32 = 4096;

/// The largest body bound the handshake can name: one byte of log2, capped below a `u64`'s top
pub const MAX_BODY_LOG2: u8 = 62;

/// The largest body a hello's or an ack's byte says its sender assembles
///
/// Zero says the sender assembles no stream at all, which is what a peer built before F73 wrote.
///
/// # Arguments
///
/// * `log2` - The byte the handshake carried
#[must_use]
pub const fn body_bound(log2: u8) -> u64 {
    if log2 == 0 {
        0
    } else if log2 > MAX_BODY_LOG2 {
        1 << MAX_BODY_LOG2
    } else {
        1 << log2
    }
}

/// The byte a handshake carries for a body bound: the largest power of two not above it
///
/// # Arguments
///
/// * `bytes` - The bound
#[must_use]
pub const fn log2_floor(bytes: u64) -> u8 {
    if bytes < 2 {
        // a bound of one byte or none is no stream at all
        return 0;
    }
    let log2 = 63 - bytes.leading_zeros() as u8;
    if log2 > MAX_BODY_LOG2 {
        MAX_BODY_LOG2
    } else {
        log2
    }
}

/// How many payload bytes each data frame of a stream carries
///
/// The sender's configured size, never more than the receiver's frame bound leaves after the
/// head, and never less than one byte.
///
/// # Arguments
///
/// * `configured` - The sender's configured data frame size
/// * `peer_max_frame` - The receiver's frame bound
#[must_use]
pub const fn data_body(configured: u32, peer_max_frame: u32) -> usize {
    let room = (peer_max_frame as usize).saturating_sub(DATA_HEAD_LEN);
    let size = if (configured as usize) < room {
        configured as usize
    } else {
        room
    };
    if size == 0 {
        1
    } else {
        size
    }
}

/// Build the bytes ahead of one data frame's payload
///
/// # Arguments
///
/// * `id` - The stream's id
/// * `offset` - Where the payload starts in the stream
/// * `len` - The payload's length
/// * `last` - Whether this is the stream's final frame
/// * `max_frame_bytes` - The receiver's frame bound
///
/// # Errors
///
/// Fails if the frame would be larger than the receiver accepts.
pub fn data_preamble(
    id: &Uuid,
    offset: u64,
    len: usize,
    last: bool,
    max_frame_bytes: u32,
) -> Result<[u8; DATA_PREAMBLE_LEN], ProtocolError> {
    // the head counts towards the frame's length
    let flags = if last { Flags::LAST } else { Flags::NONE };
    let header = Header::new(
        MessageType::Data,
        flags,
        DATA_HEAD_LEN.saturating_add(len),
        max_frame_bytes,
    )?;
    // the header, the id, then the offset
    let mut preamble = [0u8; DATA_PREAMBLE_LEN];
    preamble[..HEADER_LEN].copy_from_slice(&header.encode());
    preamble[HEADER_LEN..HEADER_LEN + QUERY_ID_LEN].copy_from_slice(id.as_bytes());
    preamble[HEADER_LEN + QUERY_ID_LEN..].copy_from_slice(&offset.to_le_bytes());
    Ok(preamble)
}

/// Read a data frame's head: the stream's id and the offset of the payload that follows
///
/// # Arguments
///
/// * `raw` - The head's bytes
#[must_use]
pub fn decode_data_head(raw: &[u8; DATA_HEAD_LEN]) -> (Uuid, u64) {
    // the id, then the offset after it
    let mut id = [0u8; QUERY_ID_LEN];
    id.copy_from_slice(&raw[..QUERY_ID_LEN]);
    let mut offset = [0u8; 8];
    offset.copy_from_slice(&raw[QUERY_ID_LEN..]);
    (Uuid::from_bytes(id), u64::from_le_bytes(offset))
}

/// The payload bytes a data frame carries, from its header
///
/// # Arguments
///
/// * `header` - The data frame's header
///
/// # Errors
///
/// Fails if the frame cannot hold its own head.
pub const fn data_payload_len(header: &Header) -> Result<usize, ProtocolError> {
    match header.body_len().checked_sub(DATA_HEAD_LEN) {
        Some(len) => Ok(len),
        None => Err(ProtocolError::BodyTooShort {
            need: DATA_HEAD_LEN,
            got: header.len,
        }),
    }
}

/// Build the bytes of a `Queries` opener: its sections, the bundle's id and its declared length
///
/// The sections are the ones a bundle framed whole carries - a trace context, read options - in
/// the same order, and the opener carries no archive: the archive follows in data frames.
///
/// # Arguments
///
/// * `trace` - The trace context to carry, if the caller is in a trace
/// * `options` - What the bundle says about its reads, if anything
/// * `id` - The bundle's id, which the data frames and any refusal name
/// * `declared` - The archive's length
/// * `max_frame_bytes` - The server's frame bound
///
/// # Errors
///
/// Fails if the options carry more tokens than a section may, or the opener would not fit a frame.
pub fn queries_opener(
    trace: Option<&TraceContext>,
    options: Option<&ReadOptions>,
    id: &Uuid,
    declared: u64,
    max_frame_bytes: u32,
) -> Result<RequestHead, ProtocolError> {
    // the sections a whole bundle would carry, sized as if the opener's body were its payload
    let options = options.filter(|options| !options.is_empty());
    let section = match options {
        Some(options) => options.encode()?,
        None => Vec::new(),
    };
    let trace_len = if trace.is_some() {
        TRACE_CONTEXT_LEN
    } else {
        0
    };
    let body_len = trace_len + section.len() + QUERIES_OPENER_LEN;
    let mut flags = Flags::STREAMED;
    if trace.is_some() {
        flags = flags.union(Flags::TRACE_CONTEXT);
    }
    if options.is_some() {
        flags = flags.union(Flags::READ_OPTIONS);
    }
    let header = Header::new(MessageType::Queries, flags, body_len, max_frame_bytes)?;
    // the header, then the sections in flag bit order, then the id and the length
    let mut bytes = Vec::with_capacity(HEADER_LEN + body_len);
    bytes.extend_from_slice(&header.encode());
    if let Some(trace) = trace {
        bytes.extend_from_slice(&trace.encode());
    }
    bytes.extend_from_slice(&section);
    bytes.extend_from_slice(id.as_bytes());
    bytes.extend_from_slice(&declared.to_le_bytes());
    Ok(RequestHead::Extended(bytes))
}

/// Read a `Queries` opener's id and declared length
///
/// # Arguments
///
/// * `raw` - The twenty four bytes after the opener's sections
#[must_use]
pub fn decode_queries_opener(raw: &[u8; QUERIES_OPENER_LEN]) -> (Uuid, u64) {
    // the same layout as a data frame's head: an id, then a word
    let mut head = [0u8; DATA_HEAD_LEN];
    head.copy_from_slice(raw);
    decode_data_head(&head)
}

/// Whether a session token and a declared length follow a response opener's id
///
/// # Arguments
///
/// * `flags` - The opener's flags
#[must_use]
pub const fn is_opener(flags: Flags) -> bool {
    flags.contains(Flags::STREAMED)
}

/// One frame's worth of a stream, as a [`Splitter`] cuts it
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Piece {
    /// Where its bytes start in the stream
    pub offset: u64,
    /// How many bytes it carries
    pub len: usize,
    /// Whether it is the stream's final frame
    pub last: bool,
}

/// Cuts a stream of a known length into frames of one size, in order, `LAST` on the final one
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Splitter {
    /// The stream's length
    total: u64,
    /// Where the next frame starts
    next: u64,
    /// Payload bytes a frame
    frame: usize,
}

impl Splitter {
    /// A splitter for a stream
    ///
    /// # Arguments
    ///
    /// * `total` - The stream's length, which is more than zero
    /// * `frame` - Payload bytes a frame, which is more than zero
    #[must_use]
    pub const fn new(total: u64, frame: usize) -> Self {
        Splitter {
            total,
            next: 0,
            frame: if frame == 0 { 1 } else { frame },
        }
    }

    /// The next frame, or nothing once the last has been cut
    pub fn next_piece(&mut self) -> Option<Piece> {
        // nothing is left once the whole length has been cut
        if self.next >= self.total {
            return None;
        }
        let len = (self.total - self.next).min(self.frame as u64) as usize;
        let piece = Piece {
            offset: self.next,
            len,
            last: self.next + len as u64 == self.total,
        };
        self.next += len as u64;
        Some(piece)
    }

    /// Whether every frame has been cut
    #[must_use]
    pub const fn is_done(&self) -> bool {
        self.next >= self.total
    }
}

/// How a receiver holds a stream's bytes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Hold {
    /// Into one buffer of the declared length, reserved whole at the opener
    Reserved,
    /// Through a sink that releases what it has consumed, under the connection's window
    Windowed,
}

/// What is wrong with a stream, which ends the connection it arrived on
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamFault {
    /// An opener named an id that already has a stream open or draining
    Duplicate(Uuid),
    /// An opener would have made more streams open than [`MAX_OPEN_STREAMS`]
    TooMany,
    /// An opener declared a stream of no bytes
    Empty(Uuid),
    /// An opener declared more than the receiver said it assembles
    OverBound {
        /// The stream
        id: Uuid,
        /// What it declared
        declared: u64,
        /// What the receiver advertised
        bound: u64,
    },
    /// A data frame named an id with no stream open
    Unknown(Uuid),
    /// A data frame's offset was not the bytes the stream had received
    Gap {
        /// The stream
        id: Uuid,
        /// The offset the next frame had to have
        expected: u64,
        /// The offset it had
        got: u64,
    },
    /// A data frame carried bytes past the stream's declared length
    PastEnd {
        /// The stream
        id: Uuid,
        /// Its declared length
        declared: u64,
        /// Where the frame's bytes ended
        end: u64,
    },
    /// A data frame carried no bytes
    EmptyFrame(Uuid),
    /// `LAST` was set short of the declared length
    ShortLast {
        /// The stream
        id: Uuid,
        /// Its declared length
        declared: u64,
        /// What it had received
        received: u64,
    },
    /// The declared length was reached without `LAST`
    MissingLast(Uuid),
}

impl std::fmt::Display for StreamFault {
    /// Write what was wrong with the stream
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write too
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamFault::Duplicate(id) => write!(f, "stream {id} was opened while it was open"),
            StreamFault::TooMany => write!(
                f,
                "more than {MAX_OPEN_STREAMS} streams were opened at once"
            ),
            StreamFault::Empty(id) => write!(f, "stream {id} declared no bytes"),
            StreamFault::OverBound {
                id,
                declared,
                bound,
            } => {
                write!(
                    f,
                    "stream {id} declared {declared} bytes past the advertised {bound}"
                )
            }
            StreamFault::Unknown(id) => {
                write!(f, "a data frame named stream {id}, which is not open")
            }
            StreamFault::Gap { id, expected, got } => {
                write!(
                    f,
                    "stream {id} had a data frame at {got} where {expected} was next"
                )
            }
            StreamFault::PastEnd { id, declared, end } => {
                write!(
                    f,
                    "stream {id} carried bytes to {end} past its declared {declared}"
                )
            }
            StreamFault::EmptyFrame(id) => write!(f, "stream {id} had an empty data frame"),
            StreamFault::ShortLast {
                id,
                declared,
                received,
            } => {
                write!(
                    f,
                    "stream {id} ended at {received} of its declared {declared} bytes"
                )
            }
            StreamFault::MissingLast(id) => write!(
                f,
                "stream {id} reached its declared length without its last frame"
            ),
        }
    }
}

/// Where one data frame's bytes go
#[derive(Debug)]
pub enum Step<'a, T> {
    /// Into the stream's sink, at this offset
    Keep {
        /// The stream's sink
        sink: &'a mut T,
        /// The offset the bytes start at
        at: u64,
        /// Whether these are the stream's last bytes, after which the caller takes the sink
        finished: bool,
    },
    /// Nowhere: the stream was refused and is being drained
    Discard {
        /// Whether these were its last bytes, after which the stream is gone
        finished: bool,
    },
}

/// One open stream
#[derive(Debug)]
struct Entry<T> {
    /// The stream's id
    id: Uuid,
    /// Its declared length
    declared: u64,
    /// The bytes received so far
    received: u64,
    /// How its bytes are held
    hold: Hold,
    /// Where they go, or nothing for a refused stream being drained
    sink: Option<T>,
    /// The bytes it holds against the window, for a windowed stream
    held: u64,
}

/// Every stream one direction of a connection has open
///
/// Generic over its sink so that the one map from an id to its stream is this one, and every rule
/// about a stream's frames is judged here rather than beside each reader.
#[derive(Debug)]
pub struct Inbound<T> {
    /// The open streams, at most [`MAX_OPEN_STREAMS`] of them
    open: Vec<Entry<T>>,
    /// The longest stream this receiver assembles
    bound: u64,
    /// Bytes windowed streams hold that their sinks have not released
    windowed: u64,
}

impl<T> Inbound<T> {
    /// No streams yet
    ///
    /// # Arguments
    ///
    /// * `bound` - The longest stream this receiver assembles, as it advertised
    #[must_use]
    pub const fn new(bound: u64) -> Self {
        Inbound {
            open: Vec::new(),
            bound,
            windowed: 0,
        }
    }

    /// The position of an id's stream
    ///
    /// # Arguments
    ///
    /// * `id` - The id
    fn position(&self, id: &Uuid) -> Option<usize> {
        self.open.iter().position(|entry| entry.id == *id)
    }

    /// Check an opener against the rules every stream follows
    ///
    /// # Arguments
    ///
    /// * `id` - The stream's id
    /// * `declared` - Its declared length
    fn admit(&self, id: Uuid, declared: u64) -> Result<(), StreamFault> {
        if self.position(&id).is_some() {
            return Err(StreamFault::Duplicate(id));
        }
        if self.open.len() >= MAX_OPEN_STREAMS {
            return Err(StreamFault::TooMany);
        }
        if declared == 0 {
            return Err(StreamFault::Empty(id));
        }
        Ok(())
    }

    /// Open a stream whose bytes go to a sink
    ///
    /// # Arguments
    ///
    /// * `id` - The stream's id
    /// * `declared` - Its declared length
    /// * `hold` - How its bytes are held
    /// * `sink` - Where they go
    ///
    /// # Errors
    ///
    /// A duplicate id, too many streams, nothing declared, or more declared than this receiver
    /// advertised: each ends the connection.
    pub fn open(
        &mut self,
        id: Uuid,
        declared: u64,
        hold: Hold,
        sink: T,
    ) -> Result<(), StreamFault> {
        self.admit(id, declared)?;
        // a peer that read our bound and declared past it is out of step with us
        if declared > self.bound {
            return Err(StreamFault::OverBound {
                id,
                declared,
                bound: self.bound,
            });
        }
        self.open.push(Entry {
            id,
            declared,
            received: 0,
            hold,
            sink: Some(sink),
            held: 0,
        });
        Ok(())
    }

    /// Open a stream this receiver refuses, whose bytes are read and dropped until its end
    ///
    /// The refusal itself - an error naming the id - is the caller's to send.
    ///
    /// # Arguments
    ///
    /// * `id` - The stream's id
    /// * `declared` - Its declared length
    ///
    /// # Errors
    ///
    /// The structural faults [`Inbound::open`] has, short of the bound.
    pub fn refuse(&mut self, id: Uuid, declared: u64) -> Result<(), StreamFault> {
        self.admit(id, declared)?;
        self.open.push(Entry {
            id,
            declared,
            received: 0,
            hold: Hold::Reserved,
            sink: None,
            held: 0,
        });
        Ok(())
    }

    /// Judge one data frame and say where its bytes go
    ///
    /// # Arguments
    ///
    /// * `id` - The stream the frame names
    /// * `offset` - Its offset
    /// * `len` - Its payload's length
    /// * `last` - Whether it carries `LAST`
    ///
    /// # Errors
    ///
    /// Any frame that breaks its stream, which ends the connection.
    pub fn data(
        &mut self,
        id: Uuid,
        offset: u64,
        len: u64,
        last: bool,
    ) -> Result<Step<'_, T>, StreamFault> {
        let index = self.position(&id).ok_or(StreamFault::Unknown(id))?;
        let entry = &self.open[index];
        // in order, never empty, never past the end, and LAST exactly at the end
        if offset != entry.received {
            return Err(StreamFault::Gap {
                id,
                expected: entry.received,
                got: offset,
            });
        }
        if len == 0 {
            return Err(StreamFault::EmptyFrame(id));
        }
        let end = offset.saturating_add(len);
        if end > entry.declared {
            return Err(StreamFault::PastEnd {
                id,
                declared: entry.declared,
                end,
            });
        }
        if last && end < entry.declared {
            return Err(StreamFault::ShortLast {
                id,
                declared: entry.declared,
                received: end,
            });
        }
        if !last && end == entry.declared {
            return Err(StreamFault::MissingLast(id));
        }
        // the frame is good: count it, and a refused stream's last frame closes it here
        let entry = &mut self.open[index];
        entry.received = end;
        if entry.sink.is_none() {
            if last {
                self.open.remove(index);
            }
            return Ok(Step::Discard { finished: last });
        }
        if entry.hold == Hold::Windowed {
            entry.held += len;
            self.windowed += len;
        }
        let entry = &mut self.open[index];
        Ok(Step::Keep {
            sink: entry.sink.as_mut().expect("a kept stream has a sink"),
            at: offset,
            finished: last,
        })
    }

    /// Take a finished stream's sink and forget the stream
    ///
    /// # Arguments
    ///
    /// * `id` - The stream
    pub fn finish(&mut self, id: &Uuid) -> Option<T> {
        self.remove(id)
    }

    /// Drop a stream before its end, as when the peer says it failed, and give back its sink
    ///
    /// # Arguments
    ///
    /// * `id` - The stream
    pub fn abandon(&mut self, id: &Uuid) -> Option<T> {
        self.remove(id)
    }

    /// Forget a stream, releasing whatever it held against the window
    ///
    /// # Arguments
    ///
    /// * `id` - The stream
    fn remove(&mut self, id: &Uuid) -> Option<T> {
        let index = self.position(id)?;
        let entry = self.open.remove(index);
        self.windowed -= entry.held;
        entry.sink
    }

    /// Say that a windowed stream's sink consumed some of what it held
    ///
    /// # Arguments
    ///
    /// * `id` - The stream
    /// * `bytes` - How many bytes it released
    pub fn release(&mut self, id: &Uuid, bytes: u64) {
        if let Some(index) = self.position(id) {
            let entry = &mut self.open[index];
            let bytes = bytes.min(entry.held);
            entry.held -= bytes;
            self.windowed -= bytes;
        }
    }

    /// Whether the connection may be read on: windowed streams hold less than the window
    ///
    /// Reserved streams never count, for the reason the module's invariants give, and nothing
    /// held at all always may, so a window smaller than one frame cannot stop a reader for good.
    ///
    /// # Arguments
    ///
    /// * `window` - The bytes windowed streams may hold before the reader waits
    #[must_use]
    pub const fn may_read(&self, window: u64) -> bool {
        self.windowed == 0 || self.windowed < window
    }

    /// The longest stream this receiver assembles
    #[must_use]
    pub const fn bound(&self) -> u64 {
        self.bound
    }

    /// Whether an id has a stream open or draining
    ///
    /// # Arguments
    ///
    /// * `id` - The id
    #[must_use]
    pub fn is_open(&self, id: &Uuid) -> bool {
        self.position(id).is_some()
    }

    /// How many streams are open or draining
    #[must_use]
    pub fn len(&self) -> usize {
        self.open.len()
    }

    /// Whether no stream is open
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.open.is_empty()
    }

    /// The ids of every stream open, so a connection that ends can fail each one
    #[must_use]
    pub fn ids(&self) -> Vec<Uuid> {
        self.open.iter().map(|entry| entry.id).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh id for a stream
    fn id(n: u8) -> Uuid {
        Uuid::from_bytes([n; 16])
    }

    /// A stream's frames are kept in order and the last hands its sink back
    #[test]
    fn a_stream_in_order_is_kept_to_its_end() {
        let mut inbound = Inbound::new(1 << 20);
        inbound.open(id(1), 10, Hold::Reserved, "sink").unwrap();
        assert!(matches!(
            inbound.data(id(1), 0, 4, false),
            Ok(Step::Keep {
                at: 0,
                finished: false,
                ..
            })
        ));
        assert!(matches!(
            inbound.data(id(1), 4, 6, true),
            Ok(Step::Keep {
                at: 4,
                finished: true,
                ..
            })
        ));
        assert_eq!(inbound.finish(&id(1)), Some("sink"));
        assert!(inbound.is_empty());
    }

    /// An opener for an id already open ends the connection
    #[test]
    fn an_opener_for_an_open_id_is_a_fault() {
        let mut inbound = Inbound::new(1 << 20);
        inbound.open(id(1), 10, Hold::Reserved, ()).unwrap();
        assert_eq!(
            inbound.open(id(1), 10, Hold::Reserved, ()),
            Err(StreamFault::Duplicate(id(1)))
        );
    }

    /// More than the open bound is a fault
    #[test]
    fn a_stream_past_the_open_bound_is_a_fault() {
        let mut inbound = Inbound::new(1 << 20);
        for n in 0..MAX_OPEN_STREAMS {
            inbound.open(id(n as u8), 10, Hold::Reserved, ()).unwrap();
        }
        assert_eq!(
            inbound.open(id(200), 10, Hold::Reserved, ()),
            Err(StreamFault::TooMany)
        );
    }

    /// A stream of no bytes is a fault, and so is one past the advertised bound
    #[test]
    fn an_empty_or_oversized_declaration_is_a_fault() {
        let mut inbound = Inbound::new(100);
        assert_eq!(
            inbound.open(id(1), 0, Hold::Reserved, ()),
            Err(StreamFault::Empty(id(1)))
        );
        assert!(matches!(
            inbound.open(id(2), 101, Hold::Reserved, ()),
            Err(StreamFault::OverBound {
                declared: 101,
                bound: 100,
                ..
            })
        ));
    }

    /// Data for an id with no stream is a fault
    #[test]
    fn data_for_an_unknown_stream_is_a_fault() {
        let mut inbound: Inbound<()> = Inbound::new(1 << 20);
        assert!(matches!(
            inbound.data(id(9), 0, 1, true),
            Err(StreamFault::Unknown(_))
        ));
    }

    /// A gap, an overlap, bytes past the end and an empty frame are each a fault
    #[test]
    fn a_frame_out_of_order_or_past_the_end_is_a_fault() {
        let mut inbound = Inbound::new(1 << 20);
        inbound.open(id(1), 10, Hold::Reserved, ()).unwrap();
        assert!(matches!(
            inbound.data(id(1), 2, 4, false),
            Err(StreamFault::Gap {
                expected: 0,
                got: 2,
                ..
            })
        ));
        inbound.data(id(1), 0, 4, false).unwrap();
        assert!(matches!(
            inbound.data(id(1), 2, 4, false),
            Err(StreamFault::Gap {
                expected: 4,
                got: 2,
                ..
            })
        ));
        assert!(matches!(
            inbound.data(id(1), 4, 7, true),
            Err(StreamFault::PastEnd { end: 11, .. })
        ));
        assert!(matches!(
            inbound.data(id(1), 4, 0, false),
            Err(StreamFault::EmptyFrame(_))
        ));
    }

    /// `LAST` short of the end, and the end without `LAST`, are each a fault
    #[test]
    fn last_anywhere_but_the_end_is_a_fault() {
        let mut inbound = Inbound::new(1 << 20);
        inbound.open(id(1), 10, Hold::Reserved, ()).unwrap();
        assert!(matches!(
            inbound.data(id(1), 0, 4, true),
            Err(StreamFault::ShortLast { received: 4, .. })
        ));
        assert!(matches!(
            inbound.data(id(1), 0, 10, false),
            Err(StreamFault::MissingLast(_))
        ));
    }

    /// A refused stream's bytes are discarded and its id is free once it ends
    #[test]
    fn a_refused_stream_drains_and_its_id_is_free_after_last() {
        let mut inbound: Inbound<()> = Inbound::new(1 << 20);
        inbound.refuse(id(1), 8).unwrap();
        assert!(matches!(
            inbound.data(id(1), 0, 4, false),
            Ok(Step::Discard { finished: false })
        ));
        assert!(matches!(
            inbound.data(id(1), 4, 4, true),
            Ok(Step::Discard { finished: true })
        ));
        assert!(!inbound.is_open(&id(1)));
        inbound.open(id(1), 8, Hold::Reserved, ()).unwrap();
    }

    /// Reserved bytes never stop the reader
    #[test]
    fn assembling_bytes_never_stop_the_reader() {
        let mut inbound = Inbound::new(1 << 30);
        inbound.open(id(1), 1 << 29, Hold::Reserved, ()).unwrap();
        inbound.data(id(1), 0, 1 << 28, false).unwrap();
        assert!(inbound.may_read(1));
    }

    /// Windowed bytes stop the reader until the sink releases them
    #[test]
    fn windowed_bytes_stop_the_reader_until_released() {
        let mut inbound = Inbound::new(1 << 30);
        inbound.open(id(1), 1 << 20, Hold::Windowed, ()).unwrap();
        inbound.data(id(1), 0, 4096, false).unwrap();
        assert!(!inbound.may_read(4096));
        inbound.release(&id(1), 1024);
        assert!(inbound.may_read(4096));
        inbound.data(id(1), 4096, 4096, false).unwrap();
        assert!(!inbound.may_read(4096));
        inbound.abandon(&id(1));
        assert!(inbound.may_read(4096));
    }

    /// A splitter covers the stream once, in order, with `LAST` on the final piece alone
    #[test]
    fn the_splitter_covers_the_body_once_with_last_on_the_end() {
        let mut splitter = Splitter::new(10, 4);
        let pieces: Vec<Piece> = std::iter::from_fn(|| splitter.next_piece()).collect();
        assert_eq!(
            pieces,
            vec![
                Piece {
                    offset: 0,
                    len: 4,
                    last: false
                },
                Piece {
                    offset: 4,
                    len: 4,
                    last: false
                },
                Piece {
                    offset: 8,
                    len: 2,
                    last: true
                },
            ]
        );
        assert!(splitter.is_done());
    }

    /// A data frame's preamble is thirty two bytes and reads back as it was written
    #[test]
    fn a_data_preamble_round_trips() {
        let preamble = data_preamble(&id(7), 1 << 33, 100, true, 1 << 20).unwrap();
        assert_eq!(preamble.len(), 32);
        let mut header = [0u8; HEADER_LEN];
        header.copy_from_slice(&preamble[..HEADER_LEN]);
        let header = Header::decode(&header, 1 << 20).unwrap();
        assert_eq!(header.kind, MessageType::Data);
        assert!(header.flags.contains(Flags::LAST));
        assert_eq!(data_payload_len(&header), Ok(100));
        let mut head = [0u8; DATA_HEAD_LEN];
        head.copy_from_slice(&preamble[HEADER_LEN..]);
        assert_eq!(decode_data_head(&head), (id(7), 1 << 33));
    }

    /// A body bound survives the handshake's byte as the largest power of two under it
    #[test]
    fn a_body_bound_round_trips_through_its_byte() {
        assert_eq!(body_bound(0), 0);
        assert_eq!(body_bound(log2_floor(64 << 20)), 64 << 20);
        assert_eq!(body_bound(log2_floor((64 << 20) + 1)), 64 << 20);
        assert_eq!(log2_floor(1), 0);
        assert_eq!(body_bound(255), 1 << MAX_BODY_LOG2);
    }

    /// A data frame never carries more than the receiver's frame leaves after its head
    #[test]
    fn a_data_body_fits_the_receivers_frame() {
        assert_eq!(data_body(1 << 20, 64 << 20), 1 << 20);
        assert_eq!(data_body(1 << 20, 64 << 10), (64 << 10) - DATA_HEAD_LEN);
    }
}
