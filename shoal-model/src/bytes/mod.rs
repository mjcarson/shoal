//! The byte ledger: S16's write ledger for ranges of bytes
//!
//! A test records every range it writes to an object, with a checksum of its bytes and the
//! outcome it was told, and every read with the bytes it was answered, and the ledger judges
//! every read against the writes (`docs/src/object-storage/testing.md#the-fixture`). It is
//! C11's ledger ([`crate::oracle`]) for ranges of bytes, and it holds the three outcomes to the
//! same three contracts: an acknowledged write took effect, whole, before it was answered; a
//! refused one never took effect; an unknown one took effect whole or not at all, and never in
//! part.
//!
//! It keeps no bytes. A range is cut into granules of a size the ledger is made with - the
//! object store's unit in a fixture, a few bytes in a history written by hand - and each granule
//! is kept as its CRC-64/NVME ([`crc`]). A range starts and ends on a granule's edge, so a
//! granule of a read is compared with exactly one granule of a write. A granule nothing wrote
//! reads as zeros.
//!
//! Each recorded write is atomic. That is the contract for one stripe and not for an object
//! (P19 promises no atomicity across stripes), so a caller records a write that spans stripes as
//! a write a stripe. A write retried under its identity is recorded once: invoked when its first
//! attempt was sent, and completed when an attempt was answered.
//!
//! Like the rest of this crate it names no shoal crate, so it shares no code with what it judges.

pub mod crc;
mod judge;

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

pub use judge::MAX_OPS_PER_OBJECT;

/// Which recorded write
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct WriteId(pub usize);

/// Which recorded read
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ReadId(pub usize);

/// What a writer was told
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteOutcome {
    /// It was acknowledged: it took effect, whole, before the answer
    Acknowledged,
    /// It was refused: it never took effect
    Refused,
    /// It was given up on: it took effect whole or not at all
    Unknown,
}

/// What a reader asked for ([P12](../../../docs/src/object-storage/contract.md#the-contract))
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadLevel {
    /// A committed state, possibly stale, never mixed
    Default,
    /// A state at least as new as every write acknowledged before the read began
    Strong,
}

/// One recorded write
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteRecord {
    /// The object it wrote to
    pub object: String,
    /// The byte it starts at
    pub offset: u64,
    /// The checksum of each granule it wrote, in order
    pub granules: Vec<u64>,
    /// The step its first attempt was sent at
    pub invoke: u64,
    /// The step it was answered or given up on at, once it has been
    pub complete: Option<u64>,
    /// What it was told, once it has been
    pub outcome: Option<WriteOutcome>,
}

/// One recorded read, which was answered
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadRecord {
    /// The object it read
    pub object: String,
    /// The byte it starts at
    pub offset: u64,
    /// The checksum of each granule it was answered, in order
    pub granules: Vec<u64>,
    /// What it asked for
    pub level: ReadLevel,
    /// The step it was sent at
    pub invoke: u64,
    /// The step it was answered at
    pub complete: u64,
}

/// A record the ledger could not take: the test used it wrongly
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LedgerError {
    /// A granule of no bytes
    ZeroGranule,
    /// A range that does not start and end on a granule's edge
    Unaligned {
        /// The object
        object: String,
        /// Where it starts
        offset: u64,
        /// How long it is
        len: u64,
    },
    /// A range of no bytes
    Empty {
        /// The object
        object: String,
        /// Where it starts
        offset: u64,
    },
    /// A write this ledger never recorded
    NoSuchWrite(WriteId),
    /// A write that was already completed
    AlreadyComplete(WriteId),
    /// A read answered before it was sent
    Backwards {
        /// The object
        object: String,
        /// When it was sent
        invoke: u64,
        /// When it was answered
        complete: u64,
    },
}

impl std::fmt::Display for LedgerError {
    /// Say what the test did wrong
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroGranule => write!(f, "a granule of no bytes"),
            Self::Unaligned {
                object,
                offset,
                len,
            } => write!(
                f,
                "{object} [{offset}, +{len}) does not start and end on a granule's edge"
            ),
            Self::Empty { object, offset } => write!(f, "{object} at {offset} is no bytes"),
            Self::NoSuchWrite(id) => write!(f, "write {} was never recorded", id.0),
            Self::AlreadyComplete(id) => write!(f, "write {} was already completed", id.0),
            Self::Backwards {
                object,
                invoke,
                complete,
            } => write!(
                f,
                "a read of {object} answered at {complete} before it was sent at {invoke}"
            ),
        }
    }
}

impl std::error::Error for LedgerError {}

/// Why a history is not one the writes could have produced
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Violation {
    /// A granule was read that no write wrote and that is not zeros: bytes made up or torn
    Unwritten {
        /// The object
        object: String,
        /// The read
        read: ReadId,
        /// The granule's first byte
        offset: u64,
    },
    /// A granule was read that only a refused write wrote
    RefusedRead {
        /// The object
        object: String,
        /// The read
        read: ReadId,
        /// The refused write
        write: WriteId,
        /// The granule's first byte
        offset: u64,
    },
    /// A strong read does not show a write acknowledged before it began
    Missing {
        /// The object
        object: String,
        /// The read
        read: ReadId,
        /// The acknowledged write
        write: WriteId,
        /// A granule of the write the read shows something older at
        offset: u64,
    },
    /// A read shows part of a write: the write in one granule, and what it replaced in another
    Part {
        /// The object
        object: String,
        /// The read
        read: ReadId,
        /// The write
        write: WriteId,
        /// A granule of the write the read shows something older at
        offset: u64,
    },
    /// No order of the writes explains every read of an object
    NotLinearizable {
        /// The object
        object: String,
        /// Its writes and reads, in invocation order
        detail: String,
    },
    /// An object saw more writes and reads than the search is bounded to
    TooManyOps {
        /// The object
        object: String,
        /// How many
        count: usize,
    },
}

impl std::fmt::Display for Violation {
    /// Say what the history broke
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unwritten {
                object,
                read,
                offset,
            } => write!(
                f,
                "read {} of {object} at {offset} returned bytes no write wrote",
                read.0
            ),
            Self::RefusedRead {
                object,
                read,
                write,
                offset,
            } => write!(
                f,
                "read {} of {object} at {offset} returned refused write {}",
                read.0, write.0
            ),
            Self::Missing {
                object,
                read,
                write,
                offset,
            } => write!(
                f,
                "strong read {} of {object} at {offset} does not show acknowledged write {}",
                read.0, write.0
            ),
            Self::Part {
                object,
                read,
                write,
                offset,
            } => write!(
                f,
                "read {} of {object} shows write {} in part: not at {offset}",
                read.0, write.0
            ),
            Self::NotLinearizable { object, detail } => {
                write!(f, "no order of {object}'s writes explains its reads: {detail}")
            }
            Self::TooManyOps { object, count } => write!(
                f,
                "{object} has {count} writes and reads, past the search's bound of {MAX_OPS_PER_OBJECT}"
            ),
        }
    }
}

impl std::error::Error for Violation {}

/// Every write and read a test made, by object
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ByteLedger {
    /// The bytes a checksum covers
    pub granule: u64,
    /// The writes, in invocation order
    pub writes: Vec<WriteRecord>,
    /// The reads, in the order they were recorded
    pub reads: Vec<ReadRecord>,
}

impl ByteLedger {
    /// An empty ledger
    ///
    /// # Arguments
    ///
    /// * `granule` - The bytes a checksum covers; every range starts and ends on its edge
    ///
    /// # Errors
    ///
    /// A granule of no bytes.
    pub fn new(granule: u64) -> Result<Self, LedgerError> {
        // a granule of nothing cuts nothing
        if granule == 0 {
            return Err(LedgerError::ZeroGranule);
        }
        Ok(Self {
            granule,
            writes: Vec::new(),
            reads: Vec::new(),
        })
    }

    /// Cut a range's bytes into the checksums of its granules
    ///
    /// # Arguments
    ///
    /// * `object` - The object, for an error
    /// * `offset` - Where the range starts
    /// * `bytes` - Its bytes
    fn granules(&self, object: &str, offset: u64, bytes: &[u8]) -> Result<Vec<u64>, LedgerError> {
        let len = bytes.len() as u64;
        // a range of nothing is a mistake, not a write
        if len == 0 {
            return Err(LedgerError::Empty {
                object: object.to_string(),
                offset,
            });
        }
        // both ends on a granule's edge, so granules compare one to one
        if !offset.is_multiple_of(self.granule) || !len.is_multiple_of(self.granule) {
            return Err(LedgerError::Unaligned {
                object: object.to_string(),
                offset,
                len,
            });
        }
        // a checksum a granule
        Ok(bytes
            .chunks(self.granule as usize)
            .map(crc::crc64_nvme)
            .collect())
    }

    /// Record a write being sent
    ///
    /// # Arguments
    ///
    /// * `object` - The object it writes to
    /// * `offset` - The byte it starts at
    /// * `bytes` - What it writes
    /// * `step` - When its first attempt was sent
    ///
    /// # Errors
    ///
    /// A range of no bytes, or one that does not start and end on a granule's edge.
    pub fn write(
        &mut self,
        object: &str,
        offset: u64,
        bytes: &[u8],
        step: u64,
    ) -> Result<WriteId, LedgerError> {
        // checksum it before anything is kept
        let granules = self.granules(object, offset, bytes)?;
        self.writes.push(WriteRecord {
            object: object.to_string(),
            offset,
            granules,
            invoke: step,
            complete: None,
            outcome: None,
        });
        Ok(WriteId(self.writes.len() - 1))
    }

    /// Record what a write was told
    ///
    /// # Arguments
    ///
    /// * `id` - The write
    /// * `outcome` - What it was told
    /// * `step` - When
    ///
    /// # Errors
    ///
    /// A write never recorded, or one already completed.
    pub fn complete(
        &mut self,
        id: WriteId,
        outcome: WriteOutcome,
        step: u64,
    ) -> Result<(), LedgerError> {
        // find the write
        let record = self
            .writes
            .get_mut(id.0)
            .ok_or(LedgerError::NoSuchWrite(id))?;
        // a write is told once; a retry is the same write, completed when it is answered
        if record.outcome.is_some() {
            return Err(LedgerError::AlreadyComplete(id));
        }
        record.complete = Some(step);
        record.outcome = Some(outcome);
        Ok(())
    }

    /// Record a read that was answered
    ///
    /// # Arguments
    ///
    /// * `object` - The object it read
    /// * `offset` - The byte it starts at
    /// * `bytes` - What it was answered
    /// * `level` - What it asked for
    /// * `invoke` - When it was sent
    /// * `complete` - When it was answered
    ///
    /// # Errors
    ///
    /// A range of no bytes, one that does not start and end on a granule's edge, or an answer
    /// before the read was sent.
    pub fn read(
        &mut self,
        object: &str,
        offset: u64,
        bytes: &[u8],
        level: ReadLevel,
        invoke: u64,
        complete: u64,
    ) -> Result<ReadId, LedgerError> {
        // an answer cannot come before its question
        if complete < invoke {
            return Err(LedgerError::Backwards {
                object: object.to_string(),
                invoke,
                complete,
            });
        }
        // checksum what it was answered
        let granules = self.granules(object, offset, bytes)?;
        self.reads.push(ReadRecord {
            object: object.to_string(),
            offset,
            granules,
            level,
            invoke,
            complete,
        });
        Ok(ReadId(self.reads.len() - 1))
    }

    /// Every write still waiting for an answer
    #[must_use]
    pub fn pending(&self) -> Vec<WriteId> {
        self.writes
            .iter()
            .enumerate()
            .filter(|(_, write)| write.outcome.is_none())
            .map(|(index, _)| WriteId(index))
            .collect()
    }

    /// End the history: every write still waiting is unknown
    ///
    /// # Arguments
    ///
    /// * `step` - The final step
    pub fn finish(&mut self, step: u64) {
        for write in &mut self.writes {
            // a write nobody answered may or may not have taken effect
            if write.outcome.is_none() {
                write.complete = Some(step);
                write.outcome = Some(WriteOutcome::Unknown);
            }
        }
    }

    /// Every object the history touched, in order
    #[must_use]
    pub fn objects(&self) -> BTreeSet<&str> {
        self.writes
            .iter()
            .map(|write| write.object.as_str())
            .chain(self.reads.iter().map(|read| read.object.as_str()))
            .collect()
    }

    /// Judge every read against the writes
    ///
    /// A write still waiting is judged as unknown. Each object is judged on its own, since
    /// nothing is promised across objects.
    ///
    /// # Errors
    ///
    /// The first violation found, object by object.
    pub fn judge(&self) -> Result<(), Violation> {
        // the checksum of a granule nothing wrote
        let zeros = crc::crc64_nvme(&vec![0u8; self.granule as usize]);
        for object in self.objects() {
            judge::object(self, object, zeros)?;
        }
        Ok(())
    }
}
