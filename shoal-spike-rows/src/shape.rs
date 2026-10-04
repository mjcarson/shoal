//! The rows the cells write, with representative values stated once
//!
//! Every value a row holds is derived from its key and a number, so a row is the same in every
//! round. The shapes are S3's, filled the way an S3-shaped bucket would fill them:
//!
//! - an object's path is 64 bytes, it has two user map pairs, no floors, nothing retired, and a
//!   geometry of 4 MiB stripes in 64 KiB units at 4+2;
//! - a stripe row has six labels, one a chunk of 4+2, a length, an epoch and a missed mask;
//! - a digest row has the same and six eight byte digests.

use std::sync::OnceLock;

use shoal::shared::queries::{Conditional, ConditionalWrite};

use crate::keys::StripeKey;
use crate::stats::{mix, Rng};
use crate::{
    Filler, Geometry, Label, ObjectEntry, ObjectMeta, ObjectMetaFilter, ObjectMetaUpdate,
    ObjectState, StripeMeta, StripeMetaDigest, StripeMetaFilter, StripeMetaUpdate,
};

/// The bytes of an object's path
pub const PATH_BYTES: usize = 64;

/// The chunks of a stripe: 4+2
pub const CHUNKS: usize = 6;

/// The most inline bytes a row is given, which is also the noise buffer's size
pub const MAX_INLINE: usize = 1 << 20;

/// The bytes of a filler row
pub const FILLER_BYTES: usize = 4096;

/// Seeded noise every payload is cut from, made once
///
/// Shoal compresses nothing, so the bytes only need to be the same in every round.
fn noise() -> &'static [u8] {
    static NOISE: OnceLock<Vec<u8>> = OnceLock::new();
    NOISE.get_or_init(|| {
        // eight bytes of SplitMix64 at a time
        let mut rng = Rng::new(0x5EED_0010);
        let mut bytes = Vec::with_capacity(MAX_INLINE);
        while bytes.len() < MAX_INLINE {
            bytes.extend_from_slice(&rng.next().to_le_bytes());
        }
        bytes
    })
}

/// A path of exactly [`PATH_BYTES`] bytes for an object number
///
/// # Arguments
///
/// * `n` - The object's number
#[must_use]
pub fn path(n: u64) -> String {
    // shaped like a camera's upload, then padded or cut to the stated length
    let mut path = format!(
        "photos/2026/10/{:02}/camera-{:02}/{:016x}/IMG_{:08}.jpg",
        n % 28 + 1,
        n % 7,
        mix(n),
        n % 100_000_000
    );
    path.truncate(PATH_BYTES);
    while path.len() < PATH_BYTES {
        path.push('_');
    }
    path
}

/// One object's entry, holding `inline` bytes of it inline
///
/// # Arguments
///
/// * `n` - The object's number, which every value is derived from
/// * `inline` - How many of its bytes are held inline, zero for an object in stripes
#[must_use]
pub fn object_entry(n: u64, inline: usize) -> ObjectEntry {
    // an object held inline is exactly its inline bytes; one in stripes is 64 MiB of them
    let size = if inline > 0 { inline as u64 } else { 64 << 20 };
    ObjectEntry {
        path: path(n),
        object: (u128::from(mix(n ^ 0xA5A5)) << 64) | u128::from(mix(n)),
        size,
        geometry: Geometry {
            stripe_bytes: 4 << 20,
            unit_bytes: 64 << 10,
            data: 4,
            parity: 2,
        },
        epoch: 0,
        floors: Vec::new(),
        state: ObjectState {
            replacing: None,
            retired: Vec::new(),
        },
        created: 1_790_000_000_000 + n,
        modified: 1_790_000_000_000 + n,
        user: vec![
            ("content-type".to_string(), "image/jpeg".to_string()),
            ("owner".to_string(), "x10".to_string()),
        ],
        inline: noise()[..inline.min(MAX_INLINE)].to_vec(),
    }
}

/// An object row of one entry at a version
///
/// # Arguments
///
/// * `key` - The row's key, the stand-in for its path's hash
/// * `version` - The row's version
/// * `inline` - How many of the object's bytes are held inline
/// * `n` - The object's number
#[must_use]
pub fn object_row(key: u64, version: u64, inline: usize, n: u64) -> ObjectMeta {
    ObjectMeta {
        key,
        version,
        entries: vec![object_entry(n, inline)],
    }
}

/// A change to an object row, applied only if the row is still at the version its writer read
///
/// The entry is written whole, as a change to S3's row would rewrite it, with its time moved.
///
/// # Arguments
///
/// * `key` - The row's key
/// * `read` - The version the writer read
/// * `inline` - How many of the object's bytes the new entry holds inline
/// * `n` - The object's number
#[must_use]
pub fn object_change(key: u64, read: u64, inline: usize, n: u64) -> Conditional<ObjectMetaUpdate> {
    // the entry as it would be after the change, its modification time moved on
    let mut entry = object_entry(n, inline);
    entry.modified += read + 1;
    ObjectMetaUpdate {
        partition_key: key,
        version: Some(read + 1),
        entries: Some(vec![entry]),
    }
    .if_matches(ObjectMetaFilter {
        version: Some(vec![read]),
    })
}

/// The six labels of a stripe whose chunks last changed at a sequence
///
/// # Arguments
///
/// * `key` - The stripe's key, which every tag is derived from
/// * `sequence` - The sequence every chunk carries
fn labels(key: &StripeKey, sequence: u64) -> Vec<Label> {
    // a tag a chunk, as a write's request identity would give it
    (0..CHUNKS as u64)
        .map(|chunk| Label {
            sequence,
            tag: mix(key.2 ^ (key.1 as u64) ^ (chunk << 56) ^ sequence),
        })
        .collect()
}

/// A stripe row at a sequence
///
/// # Arguments
///
/// * `key` - The row's key
/// * `sequence` - Its sequence
#[must_use]
pub fn stripe_row(key: StripeKey, sequence: u64) -> StripeMeta {
    StripeMeta {
        consumer: key.0,
        object: key.1,
        stripe: key.2,
        sequence,
        labels: labels(&key, sequence),
        length: 4 << 20,
        epoch: 0,
        missed: 0,
    }
}

/// A stripe row that keeps a digest of each chunk, at a sequence
///
/// # Arguments
///
/// * `key` - The row's key
/// * `sequence` - Its sequence
#[must_use]
pub fn digest_row(key: StripeKey, sequence: u64) -> StripeMetaDigest {
    StripeMetaDigest {
        consumer: key.0,
        object: key.1,
        stripe: key.2,
        sequence,
        labels: labels(&key, sequence),
        length: 4 << 20,
        epoch: 0,
        missed: 0,
        digests: (0..CHUNKS as u64).map(|chunk| mix(chunk ^ key.2)).collect(),
    }
}

/// A stripe's commit: one chunk's label moved, applied only if the sequence is the one read
///
/// This is S7's commit as an update: the sequence moves, the touched chunk's label names the
/// write, and it is conditional on the sequence the write was staged under.
///
/// # Arguments
///
/// * `key` - The row's key
/// * `read` - The sequence the writer read
#[must_use]
pub fn stripe_commit(key: StripeKey, read: u64) -> Conditional<StripeMetaUpdate> {
    // every chunk as it was, and the one this write touched under its new label
    let mut labels = labels(&key, read);
    let touched = usize::try_from(read % CHUNKS as u64).expect("a chunk index fits");
    labels[touched] = Label {
        sequence: read + 1,
        tag: mix(read ^ key.2 ^ 0xC0DE),
    };
    StripeMetaUpdate {
        partition_key: key,
        sequence: Some(read + 1),
        labels: Some(labels),
        length: Some(4 << 20),
        epoch: Some(0),
        missed: Some(0),
    }
    .if_matches(StripeMetaFilter {
        sequence: Some(vec![read]),
    })
}

/// A filler row
///
/// # Arguments
///
/// * `key` - Its key
#[must_use]
pub fn filler(key: u64) -> Filler {
    Filler {
        key,
        bytes: noise()[..FILLER_BYTES].to_vec(),
    }
}

/// The bytes rkyv archives a row to, which is what the WAL and the archives carry of it
///
/// # Arguments
///
/// * `row` - The row
#[must_use]
pub fn archived_len<T>(row: &T) -> usize
where
    T: for<'a> rkyv::Serialize<
        rkyv::api::high::HighSerializer<
            rkyv::util::AlignedVec,
            rkyv::ser::allocator::ArenaHandle<'a>,
            rkyv::rancor::Error,
        >,
    >,
{
    rkyv::to_bytes::<rkyv::rancor::Error>(row)
        .map(|bytes| bytes.len())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every path is the stated length
    #[test]
    fn paths_are_their_stated_length() {
        for n in [0, 1, 99, 123_456_789] {
            assert_eq!(path(n).len(), PATH_BYTES);
        }
    }

    /// The rows archive to sizes the page can state
    #[test]
    fn rows_archive_to_their_stated_shapes() {
        // a stripe row, and the digest row eight bytes a chunk larger
        let key = crate::keys::stripe_key(1, 2);
        let stripe = archived_len(&stripe_row(key, 0));
        let digest = archived_len(&digest_row(key, 0));
        assert!(stripe > 100 && stripe < 300, "a stripe row archived to {stripe} bytes");
        assert!(digest >= stripe + 48, "{digest} against {stripe}");
        // an object row with nothing inline, and one with a KiB inline
        let empty = archived_len(&object_row(3, 0, 0, 3));
        let kib = archived_len(&object_row(3, 0, 1024, 3));
        assert!(kib >= empty + 1024, "{kib} against {empty}");
    }

    /// A commit names the sequence it read and moves one chunk's label
    #[test]
    fn a_commit_moves_one_label() {
        let key = crate::keys::stripe_key(1, 2);
        let commit = stripe_commit(key, 3);
        let labels = commit.write.labels.expect("a commit sets the labels");
        assert_eq!(labels.iter().filter(|label| label.sequence == 4).count(), 1);
        assert_eq!(commit.write.sequence, Some(4));
    }
}

#[cfg(test)]
mod sizes {
    use super::*;

    /// Print the archived size of each row, for the record page
    #[test]
    #[ignore = "prints the sizes the record page states"]
    fn print_archived_sizes() {
        let key = crate::keys::stripe_key(1, 2);
        println!("stripe {}", archived_len(&stripe_row(key, 0)));
        println!("digest {}", archived_len(&digest_row(key, 0)));
        for inline in [0, 1024, 4096] {
            println!("object inline={inline} {}", archived_len(&object_row(3, 0, inline, 3)));
        }
        println!("filler {}", archived_len(&filler(1)));
    }
}
