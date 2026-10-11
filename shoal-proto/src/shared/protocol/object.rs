//! Object operations on the client wire ([F80](../../../../docs/src/features/object-wire.md))
//!
//! An object is reached by its bucket and its path, and its bytes are never an archive
//! ([S12](../../../../docs/src/object-storage/wire-and-client.md)). Two message types carry what
//! is not bytes, and F73's [`MessageType::Data`] frames carry what is:
//!
//! - **an [`ObjectOp`]** is a fixed head - the operation's id, the bucket, the kind, the read
//!   level, an offset, a length and a checksum - then the path, then a put's attributes. A put of
//!   any bytes is an opener: [`Flags::STREAMED`] is set, its length is the stream's declared
//!   length, and the bytes follow in data frames named by the operation's id;
//! - **an [`ObjectAnswer`]** is the operation's id, an outcome, what the object is - its id, its
//!   content version, its size and times - and for a read the range answered and its checksum. A
//!   read that answers any bytes is an opener in the same way, and its bytes follow in data frames
//!   named by the operation's id.
//!
//! A failure of the machinery under an operation - the principal holds no grant, the metadata's
//! tablet group has no leader, the outcome of a commit is unknown - is not an outcome here: it is
//! an [`MessageType::Error`] frame under the operation's id, with the [`ErrorCode`] a query would
//! have been answered with, so a client judges a retry of either the same way.
//!
//! **Checksums.** A put's head carries the CRC-64/NVME of every byte that follows it, and the
//! server refuses a put whose bytes do not match before it commits anything. A read's answer
//! carries the CRC-64/NVME of the range it answers, which the client checks as the frames arrive
//! ([`crate::shared::checksum`]).
//!
//! # Invariants
//!
//! **An object frame is sent only on a connection granted [`CLIENT_CAP_OBJECTS`]**, and a server
//! grants it only beside [`stream::CLIENT_CAP_STREAMS`], since a put's and a read's bytes are a
//! stream. A server that receives one on a connection it did not grant closes that connection.
//!
//! **A frame that cannot be right ends its connection**: a head too short for its own lengths, a
//! kind or a flag this build does not read, a put at an offset, a stream declared where none can
//! be. A frame that is well formed but asks for something the bucket refuses - a path too long,
//! a put past the inline threshold - is answered by name, and the connection goes on.
//!
//! [`ErrorCode`]: super::error::ErrorCode

use uuid::Uuid;

use super::read::ReadLevel;
use super::stream;
use super::{Flags, Header, MessageType, ProtocolError, HEADER_LEN, QUERY_ID_LEN};
use crate::shared::identity::BucketId;
use crate::shared::objects::Attributes;

/// The client sends and reads object operations
///
/// Spent from the hello's capability byte after F75's cancel bit. A server grants it only to a
/// client it also grants streams to, since an object's bytes are a stream.
pub const CLIENT_CAP_OBJECTS: u8 = 1 << 4;

/// The bytes of an operation's fixed head, after the frame header
///
/// The id (16), the bucket (8), the kind and the read level (1 each), the path's length (2),
/// the attributes' length (4), then the offset, the length and the checksum (8 each).
pub const OP_HEAD_LEN: usize = 56;

/// The bytes of an answer's fixed head, after the frame header and its id
///
/// The outcome, the kind, whether the tail is attributes, and one reserved (1 each), the tail's
/// length (4), the object (16), then the content version, the size, the offset, the length, the
/// checksum and the two times (8 each).
pub const ANSWER_HEAD_LEN: usize = 80;

/// The most bytes an operation's or an answer's encoded attributes may take
///
/// Structural, not the bucket's bound: a frame past it is malformed. What a bucket accepts is
/// [`crate::shared::objects::MAX_USER_METADATA_BYTES`] and the bounds beside it, refused by name.
pub const MAX_ATTRIBUTES_BYTES: usize = 16 * 1024;

/// The longest refusal message an answer carries
pub const MAX_MESSAGE_BYTES: usize = 4096;

/// What an object operation does
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ObjectOpKind {
    /// Create or replace the whole object from the bytes that follow
    Put = 1,
    /// Read a range of the object
    Read = 2,
    /// Say what the object is, without its bytes
    Stat = 3,
    /// Remove the object
    Delete = 4,
}

impl ObjectOpKind {
    /// Parse a kind from the byte it was written as
    ///
    /// # Arguments
    ///
    /// * `raw` - The byte
    ///
    /// # Errors
    ///
    /// A byte naming no kind this build reads.
    pub const fn from_byte(raw: u8) -> Result<Self, ProtocolError> {
        match raw {
            1 => Ok(ObjectOpKind::Put),
            2 => Ok(ObjectOpKind::Read),
            3 => Ok(ObjectOpKind::Stat),
            4 => Ok(ObjectOpKind::Delete),
            _ => Err(ProtocolError::MalformedObject(
                "an operation names a kind this build does not read",
            )),
        }
    }

    /// The kind's name, for a person
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            ObjectOpKind::Put => "put",
            ObjectOpKind::Read => "read",
            ObjectOpKind::Stat => "stat",
            ObjectOpKind::Delete => "delete",
        }
    }

    /// Whether this kind changes the object
    #[must_use]
    pub const fn writes(self) -> bool {
        matches!(self, ObjectOpKind::Put | ObjectOpKind::Delete)
    }
}

/// How an operation ended, when it ended in an answer rather than an error frame
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ObjectOutcome {
    /// It did what it asked
    Done = 0,
    /// No object is at the path
    NotFound = 1,
    /// The put is longer than the bucket's pool holds inline, and no pool has a device yet
    TooLarge = 2,
    /// The path is empty or longer than [`crate::shared::objects::MAX_PATH_BYTES`]
    BadPath = 3,
    /// The attributes are past the bucket's bounds
    BadAttributes = 4,
    /// The put's bytes do not have the checksum its head named, so nothing was committed
    ChecksumMismatch = 5,
    /// The schema declares no bucket of that id, or no binding makes one a pool's consumer
    UnknownBucket = 6,
}

impl ObjectOutcome {
    /// Parse an outcome from the byte it was written as
    ///
    /// # Arguments
    ///
    /// * `raw` - The byte
    ///
    /// # Errors
    ///
    /// A byte naming no outcome this build reads.
    pub const fn from_byte(raw: u8) -> Result<Self, ProtocolError> {
        match raw {
            0 => Ok(ObjectOutcome::Done),
            1 => Ok(ObjectOutcome::NotFound),
            2 => Ok(ObjectOutcome::TooLarge),
            3 => Ok(ObjectOutcome::BadPath),
            4 => Ok(ObjectOutcome::BadAttributes),
            5 => Ok(ObjectOutcome::ChecksumMismatch),
            6 => Ok(ObjectOutcome::UnknownBucket),
            _ => Err(ProtocolError::MalformedObject(
                "an answer names an outcome this build does not read",
            )),
        }
    }

    /// The outcome's name, for a person
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            ObjectOutcome::Done => "Done",
            ObjectOutcome::NotFound => "NotFound",
            ObjectOutcome::TooLarge => "TooLarge",
            ObjectOutcome::BadPath => "BadPath",
            ObjectOutcome::BadAttributes => "BadAttributes",
            ObjectOutcome::ChecksumMismatch => "ChecksumMismatch",
            ObjectOutcome::UnknownBucket => "UnknownBucket",
        }
    }
}

impl std::fmt::Display for ObjectOutcome {
    /// Write the outcome's name
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write to
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// One operation on one object
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectOp {
    /// The operation's identity: its data frames and its answer name it, and a retry reuses it
    pub id: Uuid,
    /// The bucket the object is in
    pub bucket: BucketId,
    /// What the operation does
    pub kind: ObjectOpKind,
    /// The read level a read or a stat asks for, or the server's default
    pub level: Option<ReadLevel>,
    /// Where a read starts; zero for every other kind
    pub offset: u64,
    /// The bytes a put carries, or the most bytes a read asks for
    pub length: u64,
    /// A put's CRC-64/NVME of every byte it carries; zero for every other kind
    pub checksum: u64,
    /// The object's path
    pub path: String,
    /// What a put says about the object, if anything
    pub attributes: Option<Attributes>,
}

impl ObjectOp {
    /// A put of `length` bytes whose CRC-64/NVME is `checksum`
    ///
    /// # Arguments
    ///
    /// * `id` - The operation's identity
    /// * `bucket` - The bucket
    /// * `path` - The object's path
    /// * `length` - How many bytes follow
    /// * `checksum` - Their CRC-64/NVME
    #[must_use]
    pub fn put(id: Uuid, bucket: BucketId, path: &str, length: u64, checksum: u64) -> Self {
        ObjectOp {
            id,
            bucket,
            kind: ObjectOpKind::Put,
            level: None,
            offset: 0,
            length,
            checksum,
            path: path.to_string(),
            attributes: None,
        }
    }

    /// A read of up to `length` bytes from `offset`
    ///
    /// # Arguments
    ///
    /// * `id` - The operation's identity
    /// * `bucket` - The bucket
    /// * `path` - The object's path
    /// * `offset` - Where the range starts
    /// * `length` - The most bytes wanted; `u64::MAX` reads to the end
    #[must_use]
    pub fn read(id: Uuid, bucket: BucketId, path: &str, offset: u64, length: u64) -> Self {
        ObjectOp {
            id,
            bucket,
            kind: ObjectOpKind::Read,
            level: None,
            offset,
            length,
            checksum: 0,
            path: path.to_string(),
            attributes: None,
        }
    }

    /// A stat or a delete, which carry nothing but the path
    ///
    /// # Arguments
    ///
    /// * `id` - The operation's identity
    /// * `bucket` - The bucket
    /// * `kind` - [`ObjectOpKind::Stat`] or [`ObjectOpKind::Delete`]
    /// * `path` - The object's path
    #[must_use]
    pub fn bare(id: Uuid, bucket: BucketId, kind: ObjectOpKind, path: &str) -> Self {
        ObjectOp {
            id,
            bucket,
            kind,
            level: None,
            offset: 0,
            length: 0,
            checksum: 0,
            path: path.to_string(),
            attributes: None,
        }
    }

    /// Whether data frames follow this operation
    #[must_use]
    pub const fn streams(&self) -> bool {
        matches!(self.kind, ObjectOpKind::Put) && self.length > 0
    }

    /// The bytes of this operation's frame: the header, the head, the path and the attributes
    ///
    /// # Arguments
    ///
    /// * `max_frame_bytes` - The server's frame bound
    ///
    /// # Errors
    ///
    /// Fails if the path or the attributes are longer than a frame's fields can say, or the frame
    /// longer than the server accepts.
    pub fn encode(&self, max_frame_bytes: u32) -> Result<Vec<u8>, ProtocolError> {
        // the path's and the attributes' lengths have fields of a fixed width
        let path = self.path.as_bytes();
        let path_len = u16::try_from(path.len()).map_err(|_| {
            ProtocolError::MalformedObject("a path is longer than an operation can carry")
        })?;
        let attributes = match &self.attributes {
            Some(attributes) => encode_attributes(attributes)?,
            None => Vec::new(),
        };
        // a put of bytes opens a stream, and nothing else does
        let flags = if self.streams() {
            Flags::STREAMED
        } else {
            Flags::NONE
        };
        let body_len = OP_HEAD_LEN + path.len() + attributes.len();
        let header = Header::new(MessageType::ObjectOp, flags, body_len, max_frame_bytes)?;
        // the header, the fixed head, then the path and the attributes
        let mut bytes = Vec::with_capacity(HEADER_LEN + body_len);
        bytes.extend_from_slice(&header.encode());
        bytes.extend_from_slice(self.id.as_bytes());
        bytes.extend_from_slice(&self.bucket.0.to_le_bytes());
        bytes.push(self.kind as u8);
        bytes.push(self.level.map_or(0, ReadLevel::as_byte));
        bytes.extend_from_slice(&path_len.to_le_bytes());
        bytes.extend_from_slice(&(attributes.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&self.offset.to_le_bytes());
        bytes.extend_from_slice(&self.length.to_le_bytes());
        bytes.extend_from_slice(&self.checksum.to_le_bytes());
        bytes.extend_from_slice(path);
        bytes.extend_from_slice(&attributes);
        Ok(bytes)
    }

    /// Read an operation from its frame's header and body
    ///
    /// Judges only what a well formed frame has to be; what the bucket accepts - the path's
    /// length, the attributes' bounds - is the server's to answer by name.
    ///
    /// # Arguments
    ///
    /// * `header` - The frame's header
    /// * `body` - Every byte after the header
    ///
    /// # Errors
    ///
    /// A frame that cannot be right, which ends its connection.
    pub fn decode(header: &Header, body: &[u8]) -> Result<Self, ProtocolError> {
        // the only flag an operation may carry is the one that opens a stream
        if header.flags.bits() & !Flags::STREAMED.bits() != 0 {
            return Err(ProtocolError::MalformedObject(
                "an operation carries a flag other than STREAMED",
            ));
        }
        // the fixed head first
        if body.len() < OP_HEAD_LEN {
            return Err(ProtocolError::BodyTooShort {
                need: OP_HEAD_LEN,
                got: body.len() as u32,
            });
        }
        let id = Uuid::from_slice(&body[..QUERY_ID_LEN]).expect("sixteen bytes are an id");
        let bucket = BucketId(le_u64(&body[16..24]));
        let kind = ObjectOpKind::from_byte(body[24])?;
        let level = ReadLevel::from_byte(body[25])?;
        let path_len = u16::from_le_bytes([body[26], body[27]]) as usize;
        let attributes_len = le_u32(&body[28..32]) as usize;
        let offset = le_u64(&body[32..40]);
        let length = le_u64(&body[40..48]);
        let checksum = le_u64(&body[48..56]);
        // the path and the attributes fill the rest exactly
        if OP_HEAD_LEN + path_len + attributes_len != body.len() {
            return Err(ProtocolError::MalformedObject(
                "an operation's path and attributes do not fill its frame",
            ));
        }
        if attributes_len > MAX_ATTRIBUTES_BYTES {
            return Err(ProtocolError::MalformedObject(
                "an operation's attributes are past what a frame may carry",
            ));
        }
        let path = std::str::from_utf8(&body[OP_HEAD_LEN..OP_HEAD_LEN + path_len])
            .map_err(|_| ProtocolError::MalformedObject("a path is not UTF-8"))?
            .to_string();
        let attributes = if attributes_len == 0 {
            None
        } else {
            Some(decode_attributes(&body[OP_HEAD_LEN + path_len..])?)
        };
        let op = ObjectOp {
            id,
            bucket,
            kind,
            level,
            offset,
            length,
            checksum,
            path,
            attributes,
        };
        // and every field is one this kind can have
        op.judge(header.flags)?;
        Ok(op)
    }

    /// Whether an operation's fields are ones its kind can have
    ///
    /// # Arguments
    ///
    /// * `flags` - The frame's flags
    ///
    /// # Errors
    ///
    /// The first field that cannot be right.
    fn judge(&self, flags: Flags) -> Result<(), ProtocolError> {
        // a stream is opened exactly when a put carries bytes
        if flags.contains(Flags::STREAMED) != self.streams() {
            return Err(ProtocolError::MalformedObject(
                "an operation opens a stream exactly when it is a put of some bytes",
            ));
        }
        match self.kind {
            // a put replaces the whole object, so it starts at the start
            ObjectOpKind::Put => {
                // its identity mints the object's id, which is time-ordered from it
                if self.id.get_version_num() != 7 {
                    return Err(ProtocolError::MalformedObject(
                        "a put's identity is not a v7 UUID",
                    ));
                }
                if self.offset != 0 {
                    return Err(ProtocolError::MalformedObject(
                        "a put names an offset; it replaces the whole object",
                    ));
                }
                if self.level.is_some() {
                    return Err(ProtocolError::MalformedObject("a put names a read level"));
                }
            }
            // a read names a range and nothing it would write
            ObjectOpKind::Read => {
                if self.checksum != 0 || self.attributes.is_some() {
                    return Err(ProtocolError::MalformedObject(
                        "a read carries a checksum or attributes",
                    ));
                }
                if self.length == 0 {
                    return Err(ProtocolError::MalformedObject("a read asks for no bytes"));
                }
            }
            // a stat and a delete carry the path alone
            ObjectOpKind::Stat | ObjectOpKind::Delete => {
                if self.offset != 0
                    || self.length != 0
                    || self.checksum != 0
                    || self.attributes.is_some()
                {
                    return Err(ProtocolError::MalformedObject(
                        "a stat or a delete carries a range, a checksum or attributes",
                    ));
                }
                if self.kind == ObjectOpKind::Delete && self.level.is_some() {
                    return Err(ProtocolError::MalformedObject(
                        "a delete names a read level",
                    ));
                }
            }
        }
        Ok(())
    }
}

/// What an operation came to
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectAnswer {
    /// The operation's id
    pub id: Uuid,
    /// How it ended
    pub outcome: ObjectOutcome,
    /// What it was
    pub kind: ObjectOpKind,
    /// The object at the path, once the operation was done; zero if there is none
    pub object: u128,
    /// The object's content version
    pub content_version: u64,
    /// The object's size
    pub size: u64,
    /// Where a read's range starts
    pub offset: u64,
    /// The bytes a read's data frames carry
    pub length: u64,
    /// A read's CRC-64/NVME of its range, or a put's of the bytes it committed
    pub checksum: u64,
    /// When the object was created, in milliseconds since the Unix epoch, for people only
    pub created_ms: u64,
    /// When the object was last changed, in milliseconds since the Unix epoch, for people only
    pub modified_ms: u64,
    /// Why the operation was refused, for a person; empty when it was done
    pub message: String,
    /// What the object says about itself, on a stat
    pub attributes: Option<Attributes>,
}

impl ObjectAnswer {
    /// An answer refusing an operation by name
    ///
    /// # Arguments
    ///
    /// * `id` - The operation's id
    /// * `kind` - What it was
    /// * `outcome` - Why it was refused
    /// * `message` - The refusal, for a person
    #[must_use]
    pub fn refused(id: Uuid, kind: ObjectOpKind, outcome: ObjectOutcome, message: String) -> Self {
        ObjectAnswer {
            id,
            outcome,
            kind,
            object: 0,
            content_version: 0,
            size: 0,
            offset: 0,
            length: 0,
            checksum: 0,
            created_ms: 0,
            modified_ms: 0,
            message,
            attributes: None,
        }
    }

    /// Whether data frames follow this answer
    #[must_use]
    pub const fn streams(&self) -> bool {
        matches!(self.kind, ObjectOpKind::Read)
            && matches!(self.outcome, ObjectOutcome::Done)
            && self.length > 0
    }

    /// The bytes of this answer's frame: the header, the id, the head and the tail
    ///
    /// # Arguments
    ///
    /// * `max_frame_bytes` - The client's frame bound
    ///
    /// # Errors
    ///
    /// Fails if the tail is past what an answer carries or the frame past the client's bound.
    pub fn encode(&self, max_frame_bytes: u32) -> Result<Vec<u8>, ProtocolError> {
        // the body after the id, framed under the header and the id
        let (flags, body) = self.encode_body()?;
        let mut bytes = Vec::with_capacity(super::RESPONSE_PREAMBLE_LEN + body.len());
        bytes.extend_from_slice(&super::server_preamble(
            MessageType::ObjectAnswer,
            flags,
            &self.id,
            body.len(),
            max_frame_bytes,
        )?);
        bytes.extend_from_slice(&body);
        Ok(bytes)
    }

    /// The bytes of this answer after its frame's header and id, and the flags its frame carries
    ///
    /// A node builds an answer before it knows which connection's frame bound it is written
    /// under, so the head and the tail are made here and framed by the connection's writer.
    ///
    /// # Errors
    ///
    /// Fails if the attributes are past what an answer carries.
    pub fn encode_body(&self) -> Result<(Flags, Vec<u8>), ProtocolError> {
        // a refusal carries its message, a stat its attributes, and nothing else carries a tail
        let tail = match (&self.attributes, self.message.is_empty()) {
            (Some(attributes), _) => encode_attributes(attributes)?,
            (None, false) => {
                let message = self.message.as_bytes();
                message[..message.len().min(MAX_MESSAGE_BYTES)].to_vec()
            }
            (None, true) => Vec::new(),
        };
        // a read of some bytes opens a stream, and nothing else does
        let flags = if self.streams() {
            Flags::STREAMED
        } else {
            Flags::NONE
        };
        // the fixed head, then the tail
        let mut bytes = Vec::with_capacity(ANSWER_HEAD_LEN + tail.len());
        bytes.push(self.outcome as u8);
        bytes.push(self.kind as u8);
        // whether the tail is attributes or a message, so a reader never has to guess
        bytes.push(u8::from(self.attributes.is_some()));
        bytes.push(0);
        bytes.extend_from_slice(&(tail.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&self.object.to_le_bytes());
        for word in [
            self.content_version,
            self.size,
            self.offset,
            self.length,
            self.checksum,
            self.created_ms,
            self.modified_ms,
        ] {
            bytes.extend_from_slice(&word.to_le_bytes());
        }
        bytes.extend_from_slice(&tail);
        Ok((flags, bytes))
    }

    /// Read an answer from what follows its frame's header and id
    ///
    /// # Arguments
    ///
    /// * `id` - The id the frame carried
    /// * `flags` - The frame's flags
    /// * `rest` - Every byte after the id
    ///
    /// # Errors
    ///
    /// An answer that cannot be right, which ends its connection.
    pub fn decode(id: Uuid, flags: Flags, rest: &[u8]) -> Result<Self, ProtocolError> {
        // the only flag an answer may carry is the one that opens a stream
        if flags.bits() & !Flags::STREAMED.bits() != 0 {
            return Err(ProtocolError::MalformedObject(
                "an answer carries a flag other than STREAMED",
            ));
        }
        if rest.len() < ANSWER_HEAD_LEN {
            return Err(ProtocolError::BodyTooShort {
                need: QUERY_ID_LEN + ANSWER_HEAD_LEN,
                got: (QUERY_ID_LEN + rest.len()) as u32,
            });
        }
        let outcome = ObjectOutcome::from_byte(rest[0])?;
        let kind = ObjectOpKind::from_byte(rest[1])?;
        let has_attributes = rest[2] != 0;
        let tail_len = le_u32(&rest[4..8]) as usize;
        if ANSWER_HEAD_LEN + tail_len != rest.len() {
            return Err(ProtocolError::MalformedObject(
                "an answer's tail does not fill its frame",
            ));
        }
        let mut object = [0u8; 16];
        object.copy_from_slice(&rest[8..24]);
        let word = |at: usize| le_u64(&rest[at..at + 8]);
        let tail = &rest[ANSWER_HEAD_LEN..];
        let (message, attributes) = if has_attributes {
            (String::new(), Some(decode_attributes(tail)?))
        } else {
            (String::from_utf8_lossy(tail).into_owned(), None)
        };
        let answer = ObjectAnswer {
            id,
            outcome,
            kind,
            object: u128::from_le_bytes(object),
            content_version: word(24),
            size: word(32),
            offset: word(40),
            length: word(48),
            checksum: word(56),
            created_ms: word(64),
            modified_ms: word(72),
            message,
            attributes,
        };
        // a stream is opened exactly when a read answers some bytes
        if flags.contains(Flags::STREAMED) != answer.streams() {
            return Err(ProtocolError::MalformedObject(
                "an answer opens a stream exactly when a read answers some bytes",
            ));
        }
        Ok(answer)
    }
}

/// Whether a well formed operation asks for something a bucket serves, and why not
///
/// The bounds a bucket refuses past by name, not by ending the connection: the path's length,
/// the attributes' bounds, and a put past the bucket's pool's inline threshold, since no pool has
/// a device before M14.
///
/// # Arguments
///
/// * `op` - The operation
/// * `threshold` - The inline threshold of the pool its bucket is bound to
///
/// # Errors
///
/// The outcome to answer with and a sentence for a person.
pub fn judge_op(op: &ObjectOp, threshold: u64) -> Result<(), (ObjectOutcome, String)> {
    use crate::shared::objects::{MAX_PATH_BYTES, MAX_TAGS, MAX_USER_METADATA_BYTES};
    // a path is not empty and not past the bound
    if op.path.is_empty() {
        return Err((ObjectOutcome::BadPath, "a path is empty".to_string()));
    }
    if op.path.len() > MAX_PATH_BYTES {
        return Err((
            ObjectOutcome::BadPath,
            format!(
                "a path of {} bytes is past the {MAX_PATH_BYTES} a bucket holds",
                op.path.len()
            ),
        ));
    }
    // attributes within their bounds
    if let Some(attributes) = &op.attributes {
        let user: usize = attributes
            .user
            .iter()
            .map(|(key, value)| key.len() + value.len())
            .sum();
        if user > MAX_USER_METADATA_BYTES {
            return Err((
                ObjectOutcome::BadAttributes,
                format!(
                    "{user} bytes of user metadata are past the {MAX_USER_METADATA_BYTES} an object holds"
                ),
            ));
        }
        if attributes.tags.len() > MAX_TAGS {
            return Err((
                ObjectOutcome::BadAttributes,
                format!(
                    "{} tags are past the {MAX_TAGS} an object holds",
                    attributes.tags.len()
                ),
            ));
        }
        for (name, value) in [
            ("content type", &attributes.content_type),
            ("content encoding", &attributes.content_encoding),
            ("cache control", &attributes.cache_control),
        ] {
            if value.len() > MAX_PATH_BYTES {
                return Err((
                    ObjectOutcome::BadAttributes,
                    format!(
                        "a {name} of {} bytes is past the {MAX_PATH_BYTES} an object holds",
                        value.len()
                    ),
                ));
            }
        }
    }
    // and a put no larger than its pool keeps in a row
    if op.kind == ObjectOpKind::Put && op.length > threshold {
        return Err((ObjectOutcome::TooLarge, too_large(op.length, threshold)));
    }
    Ok(())
}

/// The refusal of a put past its pool's inline threshold
///
/// # Arguments
///
/// * `length` - The put's length
/// * `threshold` - The pool's inline threshold
#[must_use]
pub fn too_large(length: u64, threshold: u64) -> String {
    format!(
        "a put of {length} bytes is past its pool's inline threshold of {threshold} bytes, and no \
         storage pool has a device to hold a larger object yet"
    )
}

/// A little endian `u64` from eight bytes
///
/// # Arguments
///
/// * `raw` - Exactly eight bytes
fn le_u64(raw: &[u8]) -> u64 {
    // the caller sliced exactly eight
    let mut word = [0u8; 8];
    word.copy_from_slice(raw);
    u64::from_le_bytes(word)
}

/// A little endian `u32` from four bytes
///
/// # Arguments
///
/// * `raw` - Exactly four bytes
fn le_u32(raw: &[u8]) -> u32 {
    // the caller sliced exactly four
    let mut word = [0u8; 4];
    word.copy_from_slice(raw);
    u32::from_le_bytes(word)
}

/// Write one string with its length ahead of it
///
/// # Arguments
///
/// * `out` - Where to write
/// * `text` - The string
///
/// # Errors
///
/// A string longer than its length's field can say.
fn put_str(out: &mut Vec<u8>, text: &str) -> Result<(), ProtocolError> {
    // a two byte length, then the bytes
    let len = u16::try_from(text.len())
        .map_err(|_| ProtocolError::MalformedObject("an attribute is longer than 64 KiB"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(text.as_bytes());
    Ok(())
}

/// Write a list of pairs with its count ahead of it
///
/// # Arguments
///
/// * `out` - Where to write
/// * `pairs` - The pairs
///
/// # Errors
///
/// A list or a string longer than its field can say.
fn put_pairs(out: &mut Vec<u8>, pairs: &[(String, String)]) -> Result<(), ProtocolError> {
    // a two byte count, then each key and value
    let count = u16::try_from(pairs.len())
        .map_err(|_| ProtocolError::MalformedObject("an attribute list is too long"))?;
    out.extend_from_slice(&count.to_le_bytes());
    for (key, value) in pairs {
        put_str(out, key)?;
        put_str(out, value)?;
    }
    Ok(())
}

/// The bytes an object's attributes travel as: three strings, then two lists of pairs
///
/// Not an archive: a frame's attributes are a few short strings, and a fixed layout is read with
/// no alignment and no validator ([F80](../../../../docs/src/features/object-wire.md)).
///
/// # Arguments
///
/// * `attributes` - The attributes
///
/// # Errors
///
/// Attributes past what a frame may carry.
pub fn encode_attributes(attributes: &Attributes) -> Result<Vec<u8>, ProtocolError> {
    // the three strings, then the user's pairs and the tags
    let mut out = Vec::new();
    put_str(&mut out, &attributes.content_type)?;
    put_str(&mut out, &attributes.content_encoding)?;
    put_str(&mut out, &attributes.cache_control)?;
    put_pairs(&mut out, &attributes.user)?;
    put_pairs(&mut out, &attributes.tags)?;
    if out.len() > MAX_ATTRIBUTES_BYTES {
        return Err(ProtocolError::MalformedObject(
            "the attributes are past what a frame may carry",
        ));
    }
    Ok(out)
}

/// A cursor over attributes' bytes
struct Cursor<'a> {
    /// The bytes
    raw: &'a [u8],
    /// How far into them the next read starts
    at: usize,
}

impl Cursor<'_> {
    /// The next `len` bytes
    ///
    /// # Arguments
    ///
    /// * `len` - How many
    fn take(&mut self, len: usize) -> Result<&[u8], ProtocolError> {
        // refuse to read past the end rather than panic
        let end = self
            .at
            .checked_add(len)
            .filter(|end| *end <= self.raw.len())
            .ok_or(ProtocolError::MalformedObject(
                "attributes end in the middle of a field",
            ))?;
        let bytes = &self.raw[self.at..end];
        self.at = end;
        Ok(bytes)
    }

    /// The next two byte length
    fn len(&mut self) -> Result<usize, ProtocolError> {
        let raw = self.take(2)?;
        Ok(u16::from_le_bytes([raw[0], raw[1]]) as usize)
    }

    /// The next string
    fn string(&mut self) -> Result<String, ProtocolError> {
        // its length, then its bytes, which have to be UTF-8
        let len = self.len()?;
        let raw = self.take(len)?;
        std::str::from_utf8(raw)
            .map(ToString::to_string)
            .map_err(|_| ProtocolError::MalformedObject("an attribute is not UTF-8"))
    }

    /// The next list of pairs
    fn pairs(&mut self) -> Result<Vec<(String, String)>, ProtocolError> {
        // its count, then each key and value
        let count = self.len()?;
        let mut pairs = Vec::with_capacity(count.min(64));
        for _ in 0..count {
            let key = self.string()?;
            let value = self.string()?;
            pairs.push((key, value));
        }
        Ok(pairs)
    }
}

/// Read attributes back from their bytes
///
/// # Arguments
///
/// * `raw` - The bytes, exactly
///
/// # Errors
///
/// Bytes that are not attributes, or that run past or stop short of their end.
pub fn decode_attributes(raw: &[u8]) -> Result<Attributes, ProtocolError> {
    // the fields in the order they were written
    let mut cursor = Cursor { raw, at: 0 };
    let attributes = Attributes {
        content_type: cursor.string()?,
        content_encoding: cursor.string()?,
        cache_control: cursor.string()?,
        user: cursor.pairs()?,
        tags: cursor.pairs()?,
    };
    // and nothing left over
    if cursor.at != raw.len() {
        return Err(ProtocolError::MalformedObject(
            "attributes carry bytes past their last field",
        ));
    }
    Ok(attributes)
}

/// The read half of an answer's frame length check: whether a client of this bound can take one
///
/// An answer's frame is its head and its tail; its bytes are a stream of data frames, each cut to
/// the client's frame, so no answer is refused for the length of the object it describes.
///
/// # Arguments
///
/// * `stream_frame` - The payload bytes each of a read's data frames carries
/// * `client_max_frame` - The client's frame bound
#[must_use]
pub const fn data_frame_bytes(stream_frame: u32, client_max_frame: u32) -> usize {
    stream::data_body(stream_frame, client_max_frame)
}

#[cfg(test)]
mod tests {
    use super::{
        decode_attributes, encode_attributes, ObjectAnswer, ObjectOp, ObjectOpKind, ObjectOutcome,
        OP_HEAD_LEN,
    };
    use crate::shared::identity::BucketId;
    use crate::shared::objects::Attributes;
    use crate::shared::protocol::read::ReadLevel;
    use crate::shared::protocol::{
        Flags, Header, MessageType, ProtocolError, HEADER_LEN, QUERY_ID_LEN,
    };
    use uuid::Uuid;

    /// A frame bound with room for anything these tests build
    const ROOMY: u32 = 1 << 20;

    /// Attributes with every field set
    fn attributes() -> Attributes {
        Attributes {
            content_type: "image/png".to_string(),
            content_encoding: "identity".to_string(),
            cache_control: "max-age=60".to_string(),
            user: vec![("film".to_string(), "alien".to_string())],
            tags: vec![("kind".to_string(), "poster".to_string())],
        }
    }

    /// Split an encoded operation into its header and body and decode it
    ///
    /// # Arguments
    ///
    /// * `bytes` - The encoded frame
    fn decode(bytes: &[u8]) -> Result<ObjectOp, ProtocolError> {
        // the header first, as a server reads it
        let mut raw = [0u8; HEADER_LEN];
        raw.copy_from_slice(&bytes[..HEADER_LEN]);
        let header = Header::decode(&raw, ROOMY)?;
        assert_eq!(header.kind, MessageType::ObjectOp);
        ObjectOp::decode(&header, &bytes[HEADER_LEN..])
    }

    /// Every kind of operation reads back as it was written
    #[test]
    fn operations_round_trip() {
        let bucket = BucketId::of("Posters");
        let mut put = ObjectOp::put(Uuid::now_v7(), bucket, "alien/one-sheet.png", 4096, 77);
        put.attributes = Some(attributes());
        let mut read = ObjectOp::read(Uuid::now_v7(), bucket, "a", 10, u64::MAX);
        read.level = Some(ReadLevel::One);
        let stat = ObjectOp::bare(Uuid::now_v7(), bucket, ObjectOpKind::Stat, "a/b");
        let delete = ObjectOp::bare(Uuid::now_v7(), bucket, ObjectOpKind::Delete, "a/b");
        let empty = ObjectOp::put(Uuid::now_v7(), bucket, "empty", 0, 0);
        for op in [put, read, stat, delete, empty] {
            let bytes = op.encode(ROOMY).unwrap();
            // a put of bytes opens a stream and nothing else does
            assert_eq!(
                Flags::from_bits(u16::from_le_bytes([bytes[2], bytes[3]]))
                    .contains(Flags::STREAMED),
                op.streams()
            );
            assert_eq!(decode(&bytes).unwrap(), op);
        }
    }

    /// An operation whose fields cannot be right is malformed, which ends its connection
    #[test]
    fn an_operation_that_cannot_be_right_is_malformed() {
        let bucket = BucketId::of("Posters");
        // a put at an offset
        let mut at = ObjectOp::put(Uuid::now_v7(), bucket, "a", 10, 1);
        at.offset = 4;
        // a stat with a range
        let mut ranged = ObjectOp::bare(Uuid::now_v7(), bucket, ObjectOpKind::Stat, "a");
        ranged.length = 5;
        // a read of nothing
        let nothing = ObjectOp::read(Uuid::now_v7(), bucket, "a", 0, 0);
        for op in [at, ranged, nothing] {
            assert!(matches!(
                decode(&op.encode(ROOMY).unwrap()),
                Err(ProtocolError::MalformedObject(_))
            ));
        }
        // a put of bytes whose stream flag was taken off
        let mut bytes = ObjectOp::put(Uuid::now_v7(), bucket, "a", 10, 1)
            .encode(ROOMY)
            .unwrap();
        bytes[2] = 0;
        bytes[3] = 0;
        assert!(matches!(
            decode(&bytes),
            Err(ProtocolError::MalformedObject(_))
        ));
        // a kind byte naming nothing
        let mut bytes = ObjectOp::bare(Uuid::now_v7(), bucket, ObjectOpKind::Stat, "a")
            .encode(ROOMY)
            .unwrap();
        bytes[HEADER_LEN + 24] = 9;
        assert!(matches!(
            decode(&bytes),
            Err(ProtocolError::MalformedObject(_))
        ));
        // a path length past the frame
        let mut bytes = ObjectOp::bare(Uuid::now_v7(), bucket, ObjectOpKind::Stat, "a")
            .encode(ROOMY)
            .unwrap();
        bytes[HEADER_LEN + 26] = 200;
        assert!(matches!(
            decode(&bytes),
            Err(ProtocolError::MalformedObject(_))
        ));
        // and a head cut short
        let header = Header::new(MessageType::ObjectOp, Flags::NONE, 10, ROOMY).unwrap();
        assert!(matches!(
            ObjectOp::decode(&header, &[0u8; 10]),
            Err(ProtocolError::BodyTooShort { .. })
        ));
        assert_eq!(OP_HEAD_LEN, 56);
    }

    /// Answers read back as they were written, a stat's attributes and a refusal's message
    #[test]
    fn answers_round_trip() {
        let id = Uuid::now_v7();
        let read = ObjectAnswer {
            id,
            outcome: ObjectOutcome::Done,
            kind: ObjectOpKind::Read,
            object: 0x0192_6f1e_7a00_7000_8000_0000_0000_0001,
            content_version: 3,
            size: 9000,
            offset: 100,
            length: 900,
            checksum: 0xabcd,
            created_ms: 1,
            modified_ms: 2,
            message: String::new(),
            attributes: None,
        };
        let mut stat = read.clone();
        stat.kind = ObjectOpKind::Stat;
        stat.length = 0;
        stat.offset = 0;
        stat.attributes = Some(attributes());
        let refused = ObjectAnswer::refused(
            id,
            ObjectOpKind::Put,
            ObjectOutcome::TooLarge,
            "past 16 KiB".to_string(),
        );
        for answer in [read, stat, refused] {
            let bytes = answer.encode(ROOMY).unwrap();
            let flags = Flags::from_bits(u16::from_le_bytes([bytes[2], bytes[3]]));
            assert_eq!(flags.contains(Flags::STREAMED), answer.streams());
            let rest = &bytes[HEADER_LEN + QUERY_ID_LEN..];
            assert_eq!(ObjectAnswer::decode(id, flags, rest).unwrap(), answer);
        }
    }

    /// What a bucket refuses by name: a path empty or too long, attributes past their bounds,
    /// and a put past its pool's inline threshold
    #[test]
    fn a_bucket_refuses_by_name_what_it_does_not_serve() {
        use super::judge_op;
        use crate::shared::objects::{MAX_PATH_BYTES, MAX_TAGS};
        let bucket = BucketId::of("Posters");
        let put = |path: &str, len| ObjectOp::put(Uuid::now_v7(), bucket, path, len, 0);
        // within every bound is served
        assert!(judge_op(&put("a", 16 << 10), 16 << 10).is_ok());
        // one byte past the threshold is too large, by name
        let (outcome, why) = judge_op(&put("a", (16 << 10) + 1), 16 << 10).unwrap_err();
        assert_eq!(outcome, ObjectOutcome::TooLarge);
        assert!(why.contains("inline threshold"), "{why}");
        // a read is never too large: it is answered with what there is
        let read = ObjectOp::read(Uuid::now_v7(), bucket, "a", 0, u64::MAX);
        assert!(judge_op(&read, 16 << 10).is_ok());
        // paths empty and past the bound
        assert_eq!(
            judge_op(&put("", 1), 16).unwrap_err().0,
            ObjectOutcome::BadPath
        );
        let long = "p".repeat(MAX_PATH_BYTES + 1);
        assert_eq!(
            judge_op(&put(&long, 1), 16).unwrap_err().0,
            ObjectOutcome::BadPath
        );
        assert!(judge_op(&put(&"p".repeat(MAX_PATH_BYTES), 1), 16).is_ok());
        // too many tags
        let mut tagged = put("a", 1);
        tagged.attributes = Some(Attributes {
            tags: vec![("k".to_string(), "v".to_string()); MAX_TAGS + 1],
            ..Attributes::default()
        });
        assert_eq!(
            judge_op(&tagged, 16).unwrap_err().0,
            ObjectOutcome::BadAttributes
        );
    }

    /// A put's identity mints its object's id, so a put whose identity is not a v7 UUID is malformed
    #[test]
    fn a_puts_identity_is_a_v7_uuid() {
        let bucket = BucketId::of("Posters");
        let put = ObjectOp::put(Uuid::new_v4(), bucket, "a", 4, 1);
        assert!(matches!(
            decode(&put.encode(ROOMY).unwrap()),
            Err(ProtocolError::MalformedObject(_))
        ));
        // other kinds name any identity
        let stat = ObjectOp::bare(Uuid::new_v4(), bucket, ObjectOpKind::Stat, "a");
        assert!(decode(&stat.encode(ROOMY).unwrap()).is_ok());
    }

    /// Attributes round trip, and bytes that are not attributes are refused rather than read
    #[test]
    fn attributes_round_trip_and_refuse_garbage() {
        let bytes = encode_attributes(&attributes()).unwrap();
        assert_eq!(decode_attributes(&bytes).unwrap(), attributes());
        // cut short anywhere, or with a byte left over, is refused
        for cut in 0..bytes.len() {
            assert!(decode_attributes(&bytes[..cut]).is_err(), "cut at {cut}");
        }
        let mut long = bytes.clone();
        long.push(0);
        assert!(decode_attributes(&long).is_err());
        // and the empty attributes are five empty fields, not nothing
        assert_eq!(
            decode_attributes(&encode_attributes(&Attributes::default()).unwrap()).unwrap(),
            Attributes::default()
        );
    }
}
