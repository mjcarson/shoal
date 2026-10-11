//! What a bucket's generated rows hold
//!
//! A bucket declared in a schema ([S2](../../../docs/src/object-storage/buckets.md)) generates
//! two persistent unsorted tables, `{Marker}ObjectMeta` and `{Marker}StripeMeta`, whose rows the
//! `#[shoal::db]` macro writes into the schema's own module. The rows' fields are of the types on
//! this page, so their layout is defined once, here, and not in every schema that declares a
//! bucket ([F79](../../../docs/src/features/bucket-schema.md)).
//!
//! The layout follows [S3](../../../docs/src/object-storage/objects.md#the-two-rows) as the
//! contract's decisions left it:
//!
//! - an object's entry keeps its whole path, compared on every read, since the row is keyed by a
//!   hash of the path and two paths can share one ([P14](../../../docs/src/object-storage/contract.md));
//! - an entry keeps a content version, which an ETag is derived from, and a bounded set of
//!   attributes (X14, [Q32](../../../docs/src/object-storage/contract.md));
//! - a stripe's row keeps a label for each chunk, its truncate stamp, a fence and a tombstone (X1)
//!   and the bytes of small writes that rode its commits (X8,
//!   [Q27](../../../docs/src/object-storage/contract.md));
//! - and nothing else: no chunk digest (X1 found no fault one catches), no record of which
//!   holders missed a write (the tablet group derives it, [S10](../../../docs/src/object-storage/recovery.md)),
//!   and no placement group's generation or positions, which are the group's state and not a row's.
//!
//! # Invariants
//!
//! **A generated row is persisted from the first object on.** A change to any type here, or to
//! how [`path_key`] hashes a path, is a change to what every bucket has on disk. The layout is
//! folded into every schema's fingerprint through [`OBJECT_ROWS_LAYOUT`], and
//! `generated_rows_archive_to_frozen_bytes` fails on a change that keeps every type's size;
//! either failing means the change is a new layout, and [`OBJECT_ROWS_VERSION`] moves with it.
//!
//! **The bounds are the object store's to enforce, not the row's.** An unsorted table holds
//! whatever row it is given. The bounds below are what M13's and M15's operations refuse past,
//! named here so the layout and its limits are read in one place.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};

use crate::shared::identity::BucketId;
use crate::shared::protocol::fingerprint::{mix_str, mix_u64, SEED};
use crate::shared::traits::{QuerySupport, ShoalUnsortedTable};

/// The longest path an object can have, in bytes
///
/// S3 bounds a key at 1,024 bytes ([X14](../../../docs/src/object-storage/ceph-and-s3-sources.md)),
/// and so does this.
pub const MAX_PATH_BYTES: usize = 1024;

/// The most bytes of user metadata an object's attributes hold, keys and values together
///
/// S3's bound on user metadata ([Q32](../../../docs/src/object-storage/contract.md)).
pub const MAX_USER_METADATA_BYTES: usize = 2048;

/// The most tags an object can carry
///
/// S3's bound ([Q32](../../../docs/src/object-storage/contract.md)).
pub const MAX_TAGS: usize = 10;

/// The most floors an object's entry keeps before a truncate waits for reclamation
///
/// A floor goes once reclamation has reclaimed the stripes it hides, so the list stays short; a
/// truncate that would push it past this waits ([S3](../../../docs/src/object-storage/objects.md#size-holes-and-truncate)).
pub const MAX_FLOORS: usize = 16;

/// The inline threshold a storage pool takes when its operator names none
///
/// An object at or under its pool's threshold is held in its entry and has no stripes. 16 KiB is
/// the bottom of the knee X10 measured on the lab's cluster
/// ([Q25](../../../docs/src/object-storage/contract.md#q25-in-part-the-metadata-rows-2026-10-04)).
pub const DEFAULT_INLINE_THRESHOLD: usize = 16 * 1024;

/// The largest small write whose bytes ride in its stripe's commit
///
/// Below it, on a device whose sync flushes its cache, a write's bytes are committed into the
/// stripe's row rather than staged, and a row's pending bytes are bounded by it
/// ([Q27](../../../docs/src/object-storage/contract.md#q27-and-q14-in-part-one-small-write-three-ways-2026-10-08)).
pub const SMALL_WRITE_THRESHOLD: usize = 64 * 1024;

/// The seed a path is hashed under to key its object's row
///
/// Frozen: an object's row lives under the hash of its path, so a changed seed loses every object
/// ever written. Zero, as every other seed of a persisted key is.
pub const PATH_KEY_SEED: i64 = 0;

/// The version of the generated rows' layout, moved by hand with every change to it
///
/// [`OBJECT_ROWS_LAYOUT`] already moves when a type here changes size or alignment. This moves
/// for a change that does neither - two fields of one type swapped, a field's meaning changed - so
/// that a schema built before it and one built after refuse each other at the hello.
pub const OBJECT_ROWS_VERSION: u64 = 1;

/// The partition key of the `ObjectMeta` row an object's entry lives in
///
/// The hash of the whole path, which the row stores too: two paths with one key are two entries
/// of one row, told apart by the path ([S3](../../../docs/src/object-storage/objects.md#path-identity)).
///
/// # Arguments
///
/// * `path` - The object's path
#[must_use]
pub fn path_key(path: &str) -> u64 {
    // the path's bytes under the frozen seed, which is the key its row is written under
    gxhash::gxhash64(path.as_bytes(), PATH_KEY_SEED)
}

/// How an object's bytes are laid out in stripes, fixed when the object is created
///
/// A replicated pool is `data: 1` and `parity` one less than its copies: a replicated stripe is
/// the same protocol with `k = 1` and no arithmetic.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Archive,
    Serialize,
    Deserialize,
    DeepSizeOf,
    serde::Serialize,
    serde::Deserialize,
)]
#[rkyv(derive(Debug))]
pub struct Geometry {
    /// The bytes of the object each stripe holds
    pub stripe_bytes: u32,
    /// The unit a stripe chunk is checksummed and written in
    pub unit_bytes: u32,
    /// The data chunks of a stripe, `k`
    pub data: u8,
    /// The parity chunks of a stripe, `m`
    pub parity: u8,
}

/// A mark a truncate leaves: a stripe past `length` stamped below `epoch` is a hole
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Archive,
    Serialize,
    Deserialize,
    DeepSizeOf,
    serde::Serialize,
    serde::Deserialize,
)]
#[rkyv(derive(Debug))]
pub struct Floor {
    /// Where the cut was
    pub length: u64,
    /// The epoch the truncate moved the object to
    pub epoch: u64,
}

/// Where an entry's objects stand
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Default,
    Archive,
    Serialize,
    Deserialize,
    DeepSizeOf,
    serde::Serialize,
    serde::Deserialize,
)]
#[rkyv(derive(Debug))]
pub struct ObjectState {
    /// Whether the entry's object is current; false once it is deleted
    pub live: bool,
    /// The object a replace is writing, registered before any of its stripe chunks is
    pub replacing: Option<u128>,
    /// Objects replaced or deleted whose stripes are not yet reclaimed
    pub retired: Vec<u128>,
}

/// What an object says about itself, for people and for a gateway
///
/// Returned on every stat and changed without a new content version, as S3's are on a change of
/// tags ([Q32](../../../docs/src/object-storage/contract.md)). An empty string is an attribute
/// that was never set.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Default,
    Archive,
    Serialize,
    Deserialize,
    DeepSizeOf,
    serde::Serialize,
    serde::Deserialize,
)]
#[rkyv(derive(Debug))]
pub struct Attributes {
    /// The media type of the object's bytes
    pub content_type: String,
    /// The encoding applied to the object's bytes
    pub content_encoding: String,
    /// How long a cache may keep the object
    pub cache_control: String,
    /// The caller's own metadata, bounded by [`MAX_USER_METADATA_BYTES`]
    pub user: Vec<(String, String)>,
    /// The object's tags, bounded by [`MAX_TAGS`]
    pub tags: Vec<(String, String)>,
}

/// One object at one path: the entry an `ObjectMeta` row holds a list of
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Default,
    Archive,
    Serialize,
    Deserialize,
    DeepSizeOf,
    serde::Serialize,
    serde::Deserialize,
)]
#[rkyv(derive(Debug))]
pub struct ObjectEntry {
    /// The whole path, compared on every read
    pub path: String,
    /// The object at this path, minted when it was created and never reused
    pub object: u128,
    /// Moved by every change to the object's content, and what its ETag is derived from
    pub content_version: u64,
    /// The object's length in bytes
    pub size: u64,
    /// How much of the object the put that created it wrote, cut by every truncate since
    ///
    /// A stripe with no row past it is a hole and nothing is read for it.
    pub put_length: u64,
    /// How the object's bytes are laid out, fixed for its life
    pub geometry: Geometry,
    /// The truncate epoch, which every truncate moves
    pub epoch: u64,
    /// The floors of truncates whose stripes are not yet reclaimed, bounded by [`MAX_FLOORS`]
    pub floors: Vec<Floor>,
    /// Whether the object is live, being replaced, or retired
    pub state: ObjectState,
    /// When the object was created, in milliseconds since the Unix epoch, for people only
    pub created_ms: u64,
    /// When the object was last changed, in milliseconds since the Unix epoch, for people only
    pub modified_ms: u64,
    /// What the object says about itself
    pub attributes: Attributes,
    /// The whole object, when it is no larger than its pool's inline threshold
    pub inline: Option<Vec<u8>>,
}

/// What a stripe chunk is labelled with: the sequence a write committed at and the write's tag
///
/// A sequence alone cannot label a chunk: two writers staged against one row both make "the next
/// sequence" with different bytes ([S18](../../../docs/src/object-storage/contract.md#identity-and-progress)).
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Archive,
    Serialize,
    Deserialize,
    DeepSizeOf,
    serde::Serialize,
    serde::Deserialize,
)]
#[rkyv(derive(Debug))]
pub struct Label {
    /// The row's sequence the write committed at
    pub sequence: u64,
    /// A tag derived from the write's request identity, zero for the put that created the object
    pub tag: u64,
}

/// One data unit of a small write, by its index within the stripe
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Default,
    Archive,
    Serialize,
    Deserialize,
    DeepSizeOf,
    serde::Serialize,
    serde::Deserialize,
)]
#[rkyv(derive(Debug))]
pub struct PendingUnit {
    /// The unit's index within the stripe
    pub index: u32,
    /// The unit's new bytes
    pub bytes: Vec<u8>,
}

/// The bytes of small writes that rode in their commits and not every holder has folded
///
/// A later small write merges its units in, so a holder still at the base, or at the label of an
/// earlier write merged here, makes the latest label from them. They leave the row only by a
/// commit: the leader's clear once `k + f` holders hold the label, a staged write that carried
/// them, or a reclamation ([X1](../../../docs/src/object-storage/stripe-model.md#a-small-write-in-its-commit)).
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Default,
    Archive,
    Serialize,
    Deserialize,
    DeepSizeOf,
    serde::Serialize,
    serde::Deserialize,
)]
#[rkyv(derive(Debug))]
pub struct PendingBytes {
    /// The label they make: the last small write's
    pub label: Label,
    /// The label each position's chunk carried when the first of them committed
    pub base: Vec<Label>,
    /// The labels of the earlier small writes merged here, which a holder may have made already
    pub chain: Vec<Label>,
    /// The data units and their new values, bounded by [`SMALL_WRITE_THRESHOLD`] in all
    pub units: Vec<PendingUnit>,
}

/// Fold one type's archived size and alignment into a layout hash
///
/// # Arguments
///
/// * `hash` - The hash to mix into
/// * `name` - The type's name
/// * `size` - The size of its archived form
/// * `align` - The alignment of its archived form
const fn mix_type(hash: u64, name: &str, size: usize, align: usize) -> u64 {
    // what the type is called, then what it archives to
    let hash = mix_str(hash, name);
    let hash = mix_u64(hash, size as u64);
    mix_u64(hash, align as u64)
}

/// The generated rows' layout, folded into the fingerprint of every schema that declares a bucket
///
/// A generated row's own fingerprint folds each field's spelling and archived size, and a
/// `Vec<ObjectEntry>` is spelled and sized the same whatever an entry holds. This carries what
/// the row's cannot: the archived size and alignment of every type a row nests, and
/// [`OBJECT_ROWS_VERSION`] for every change those miss ([S2](../../../docs/src/object-storage/buckets.md#what-the-macro-generates)).
pub const OBJECT_ROWS_LAYOUT: u64 = {
    // the hand moved version first, so a change that keeps every size still moves this
    let hash = mix_u64(mix_str(SEED, "shoal-object-rows"), OBJECT_ROWS_VERSION);
    // then every type a generated row nests, by what it archives to
    let hash = mix_type(
        hash,
        "Geometry",
        size_of::<ArchivedGeometry>(),
        align_of::<ArchivedGeometry>(),
    );
    let hash = mix_type(
        hash,
        "Floor",
        size_of::<ArchivedFloor>(),
        align_of::<ArchivedFloor>(),
    );
    let hash = mix_type(
        hash,
        "ObjectState",
        size_of::<ArchivedObjectState>(),
        align_of::<ArchivedObjectState>(),
    );
    let hash = mix_type(
        hash,
        "Attributes",
        size_of::<ArchivedAttributes>(),
        align_of::<ArchivedAttributes>(),
    );
    let hash = mix_type(
        hash,
        "ObjectEntry",
        size_of::<ArchivedObjectEntry>(),
        align_of::<ArchivedObjectEntry>(),
    );
    let hash = mix_type(
        hash,
        "Label",
        size_of::<ArchivedLabel>(),
        align_of::<ArchivedLabel>(),
    );
    let hash = mix_type(
        hash,
        "PendingUnit",
        size_of::<ArchivedPendingUnit>(),
        align_of::<ArchivedPendingUnit>(),
    );
    mix_type(
        hash,
        "PendingBytes",
        size_of::<ArchivedPendingBytes>(),
        align_of::<ArchivedPendingBytes>(),
    )
};

/// A bucket's marker, which names the bucket and the two rows it generated
///
/// The `#[shoal::db]` macro implements this for every marker a schema declares a bucket of, in
/// both halves of the schema. It is not generic over the schema, so one marker in two schemas is
/// two conflicting impls, which is the compile error it should be: a marker names one bucket.
pub trait BucketRows: 'static {
    /// The bucket's name, which is its marker's
    const NAME: &'static str;

    /// The client of the schema that declares this bucket
    type Client: QuerySupport;

    /// The row of the bucket's object metadata table, `{Marker}ObjectMeta`
    type ObjectMeta: ShoalUnsortedTable;

    /// The row of the bucket's stripe metadata table, `{Marker}StripeMeta`
    type StripeMeta: ShoalUnsortedTable;

    /// The bucket's stable identity, the hash of its name
    #[must_use]
    fn id() -> BucketId {
        BucketId::of(Self::NAME)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        path_key, Attributes, Floor, Geometry, Label, ObjectEntry, ObjectState, PendingBytes,
        PendingUnit, OBJECT_ROWS_LAYOUT, OBJECT_ROWS_VERSION,
    };
    use crate::shared::protocol::fingerprint::{mix_bytes, SEED};

    /// An entry with every field set, so every field reaches the archived bytes
    fn canonical_entry() -> ObjectEntry {
        ObjectEntry {
            path: "alien/one-sheet.png".to_string(),
            object: 0x0192_6f1e_7a00_7000_8000_0000_0000_0001,
            content_version: 3,
            size: 9_000_000,
            put_length: 8_388_608,
            geometry: Geometry {
                stripe_bytes: 4 << 20,
                unit_bytes: 64 << 10,
                data: 4,
                parity: 2,
            },
            epoch: 1,
            floors: vec![Floor {
                length: 4 << 20,
                epoch: 1,
            }],
            state: ObjectState {
                live: true,
                replacing: Some(7),
                retired: vec![5, 6],
            },
            created_ms: 1_791_000_000_000,
            modified_ms: 1_791_000_000_500,
            attributes: Attributes {
                content_type: "image/png".to_string(),
                content_encoding: "identity".to_string(),
                cache_control: "max-age=60".to_string(),
                user: vec![("film".to_string(), "alien".to_string())],
                tags: vec![("kind".to_string(), "poster".to_string())],
            },
            inline: Some(vec![1, 2, 3]),
        }
    }

    /// Pending bytes with every field set
    fn canonical_pending() -> PendingBytes {
        PendingBytes {
            label: Label {
                sequence: 4,
                tag: 99,
            },
            base: vec![Label::default(); 6],
            chain: vec![Label {
                sequence: 3,
                tag: 42,
            }],
            units: vec![PendingUnit {
                index: 2,
                bytes: vec![9; 16],
            }],
        }
    }

    /// The generated rows' nested types archive to the same bytes they always have
    ///
    /// `OBJECT_ROWS_LAYOUT` sees each type's size and alignment, and nothing else: two fields of
    /// one type swapped keep both. This sees the bytes. If it fails, the layout changed, and the
    /// change is a new one: move `OBJECT_ROWS_VERSION` and these values together.
    #[test]
    fn generated_rows_archive_to_frozen_bytes() {
        // archive a canonical entry and pending bytes, and hash what came out
        let entry =
            rkyv::to_bytes::<rkyv::rancor::Error>(&canonical_entry()).expect("an entry archives");
        let pending = rkyv::to_bytes::<rkyv::rancor::Error>(&canonical_pending())
            .expect("pending bytes archive");
        let frozen = [
            ("entry length", entry.len() as u64, 384),
            (
                "entry bytes",
                mix_bytes(SEED, &entry),
                3_205_224_534_495_473_256,
            ),
            ("pending length", pending.len() as u64, 184),
            (
                "pending bytes",
                mix_bytes(SEED, &pending),
                16_789_441_284_745_714_531,
            ),
            ("layout", OBJECT_ROWS_LAYOUT, 13_376_025_497_120_903_056),
            ("version", OBJECT_ROWS_VERSION, 1),
        ];
        // every value that moved, so one run names all of them
        let moved: Vec<String> = frozen
            .iter()
            .filter(|(_, now, then)| now != then)
            .map(|(name, now, then)| format!("{name}: {now} (frozen at {then})"))
            .collect();
        assert!(
            moved.is_empty(),
            "the generated rows' layout moved: {moved:#?}"
        );
    }

    /// A path's key is the hash of its bytes under a frozen seed
    ///
    /// An object's row lives under this, so a change to it loses every object written before it.
    #[test]
    fn path_keys_are_frozen() {
        // the same path is the same key, and a different one is another
        assert_eq!(
            path_key("alien/one-sheet.png"),
            path_key("alien/one-sheet.png")
        );
        assert_ne!(
            path_key("alien/one-sheet.png"),
            path_key("alien/one-sheet.PNG")
        );
        // a path is opaque bytes: no normalization makes two spellings one
        assert_ne!(path_key("a/b"), path_key("a//b"));
        // and the values are frozen
        assert_eq!(
            path_key("alien/one-sheet.png"),
            910_729_460_091_452_015,
            "path_key moved"
        );
        assert_eq!(
            path_key("posters/a.png"),
            2_562_422_579_418_951_358,
            "path_key moved"
        );
    }
}
