//! The identifiers the stripe model names things by
//!
//! Newtypes over small integers, as the tablet model's are, so a schedule file stays readable and
//! a slice cannot be handed where a device was meant. A client operation's identity is the tablet
//! model's [`OpId`](crate::ids::OpId) and a host is its [`NodeId`](crate::ids::NodeId); everything
//! here is the object store's own vocabulary ([S18](../../../docs/src/object-storage/contract.md)).

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::ids::OpId;

/// A stripe of the one object the model holds, numbered from zero
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StripeIx(pub u8);

/// A position in a placement group: stripe chunk `i` lives at position `i`
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Pos(pub u8);

/// A physical disk: what fails, fills and is swapped, whatever the cluster believes it is
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DiskId(pub u32);

/// A device's identity, minted the first time its path is claimed and kept in a marker there
/// ([S4](../../../docs/src/object-storage/pools-and-devices.md))
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DeviceId(pub u32);

/// A slice's identity, minted with it and kept in a marker inside it; peers name slices
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SliceId(pub u32);

/// How many writes a stripe's row has committed
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct Seq(pub u32);

impl Seq {
    /// The sequence after this one
    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

/// A tag derived from a write's request identity and the stripe it writes
///
/// Tag zero is the object's own, which the put that created it labels every chunk with. A write's
/// tag is a function of its identity alone, so a retry of the write is the same write
/// ([S7](../../../docs/src/object-storage/write-path.md#writes-that-span-stripes)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Tag(pub u32);

impl Tag {
    /// The object's own tag, which every chunk the put wrote carries at sequence zero
    pub const PUT: Tag = Tag(0);

    /// The tag of a write to a stripe
    ///
    /// # Arguments
    ///
    /// * `op` - The write's request identity
    /// * `stripe` - The stripe it writes
    /// * `attempt` - Which try of it, when tags are made per try; zero otherwise
    pub fn of(op: OpId, stripe: StripeIx, attempt: u8) -> Self {
        Self((op.0 * 16 + u32::from(stripe.0)) * 16 + u32::from(attempt % 16) + 1)
    }
}

/// What a stripe chunk is labelled with: the row's sequence and the tag of the write that made it
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Label {
    /// The sequence the write committed at
    pub seq: Seq,
    /// The write's tag
    pub tag: Tag,
}

impl Label {
    /// The label every chunk of the object's put carries
    pub const PUT: Label = Label {
        seq: Seq(0),
        tag: Tag::PUT,
    };
}

impl fmt::Display for Label {
    /// Written as `seq/tag`
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.seq.0, self.tag.0)
    }
}

/// The object's truncate epoch, which every truncate moves
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct Epoch(pub u32);

/// A placement group's generation, held in its tablet group's state beside its positions
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct Generation(pub u32);
