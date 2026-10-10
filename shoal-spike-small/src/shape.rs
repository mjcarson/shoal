//! The rows and commits the cells write, with representative values stated once
//!
//! A stripe row is X10's, with one label a copy of a replicated pool of three, a stripe of
//! [`STRIPE_BYTES`], and no bytes pending. A staged commit moves the sequence, names the write's
//! tag in every copy's label and the holders that had not staged when it was proposed; an inline
//! commit does the same with the write's bytes inside it.

use shoal::shared::queries::{Conditional, ConditionalWrite};

use crate::keys::StripeKey;
use crate::stats::mix;
use crate::{
    Filler, Label, StripeHead, StripeMeta, StripeMetaFilter, StripeMetaGet, StripeMetaUpdate, StripeRow,
};

/// The copies of a replicated pool, one a host
pub const COPIES: usize = 3;

/// The stripe a small write lands in: as large as a stripe chunk on a holder
pub const STRIPE_BYTES: u64 = 1 << 20;

/// The bytes of a filler row
pub const FILLER_BYTES: usize = 64 << 10;

/// The labels of a stripe whose copies last changed at a sequence under a tag
///
/// # Arguments
///
/// * `sequence` - The sequence every copy carries
/// * `tag` - The tag of the write that made it
fn labels(sequence: u64, tag: u64) -> Vec<Label> {
    // every copy of a replicated pool holds the same write
    (0..COPIES).map(|_| Label { sequence, tag }).collect()
}

/// The tag of a stripe's first write, which the preload stands for
///
/// # Arguments
///
/// * `key` - The stripe's key
#[must_use]
pub fn first_tag(key: &StripeKey) -> u64 {
    mix(key.2 ^ (key.1 as u64) ^ 0xF125)
}

/// A stripe row at a sequence, as the preload writes it
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
        labels: labels(sequence, first_tag(&key)),
        length: STRIPE_BYTES,
        epoch: 0,
        missed: 0,
        pending: Vec::new(),
    }
}

/// A get of a stripe row without its pending bytes, which is what a writer reads first
///
/// # Arguments
///
/// * `key` - The row's key
#[must_use]
pub fn head_get(key: StripeKey) -> StripeMetaGet {
    StripeMetaGet::new(vec![key]).projection::<StripeHead>()
}

/// B's commit: every copy's label names the write, applied only if the sequence is the one read
///
/// The bytes stay with the holders; the row records which of them had not staged when the
/// commit was proposed.
///
/// # Arguments
///
/// * `key` - The row's key
/// * `read` - The sequence the writer read
/// * `tag` - The write's tag
/// * `missed` - The holders that had not staged, a bit each
#[must_use]
pub fn staged_commit(key: StripeKey, read: u64, tag: u64, missed: u32) -> Conditional<StripeMetaUpdate> {
    StripeMetaUpdate {
        partition_key: key,
        sequence: Some(read + 1),
        labels: Some(labels(read + 1, tag)),
        length: Some(STRIPE_BYTES),
        epoch: Some(0),
        missed: Some(missed),
        pending: None,
    }
    .if_matches(StripeMetaFilter {
        sequence: Some(vec![read]),
    })
}

/// The small-write path's commit: B's with the write's bytes inside it
///
/// # Arguments
///
/// * `key` - The row's key
/// * `read` - The sequence the writer read
/// * `tag` - The write's tag
/// * `bytes` - The write's bytes, held in the row until they are folded
#[must_use]
pub fn inline_commit(key: StripeKey, read: u64, tag: u64, bytes: Vec<u8>) -> Conditional<StripeMetaUpdate> {
    StripeMetaUpdate {
        partition_key: key,
        sequence: Some(read + 1),
        labels: Some(labels(read + 1, tag)),
        length: Some(STRIPE_BYTES),
        epoch: Some(0),
        missed: Some(0),
        pending: Some(bytes),
    }
    .if_matches(StripeMetaFilter {
        sequence: Some(vec![read]),
    })
}

/// The first path's row: a write's bytes and nothing else
///
/// # Arguments
///
/// * `key` - The row's key
/// * `seed` - The write that made the bytes
/// * `size` - How many bytes
#[must_use]
pub fn row(key: u64, seed: u64, size: usize) -> StripeRow {
    StripeRow {
        key,
        bytes: crate::bytes::make(seed, key, size),
    }
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
        bytes: crate::bytes::make(0xF111, key, FILLER_BYTES),
    }
}

/// The bytes rkyv archives a value to, which is what the WAL and the archives carry of it
///
/// # Arguments
///
/// * `value` - The value
#[must_use]
pub fn archived_len<T>(value: &T) -> usize
where
    T: for<'a> rkyv::Serialize<
        rkyv::api::high::HighSerializer<
            rkyv::util::AlignedVec,
            rkyv::ser::allocator::ArenaHandle<'a>,
            rkyv::rancor::Error,
        >,
    >,
{
    rkyv::to_bytes::<rkyv::rancor::Error>(value)
        .map(|bytes| bytes.len())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// B's commit moves the sequence and every label and leaves the pending bytes alone
    #[test]
    fn a_staged_commit_carries_no_bytes() {
        let key = crate::keys::stripe_key(1, 2);
        let commit = staged_commit(key, 3, 99, 0b100);
        assert_eq!(commit.write.sequence, Some(4));
        let labels = commit.write.labels.as_ref().expect("a commit sets the labels");
        assert_eq!(labels.len(), COPIES);
        assert!(labels.iter().all(|label| label.sequence == 4 && label.tag == 99));
        assert_eq!(commit.write.missed, Some(0b100));
        assert!(commit.write.pending.is_none());
        // and it is small: a label a copy and a few integers
        assert!(archived_len(&commit.write) < 300, "{}", archived_len(&commit.write));
    }

    /// The inline commit carries exactly the write's bytes
    #[test]
    fn an_inline_commit_carries_the_write() {
        let key = crate::keys::stripe_key(1, 2);
        let bytes = crate::bytes::make(5, 6, 16 << 10);
        let commit = inline_commit(key, 0, 7, bytes.clone());
        assert_eq!(commit.write.pending.as_deref(), Some(bytes.as_slice()));
        let staged = staged_commit(key, 0, 7, 0);
        let grew = archived_len(&commit.write) - archived_len(&staged.write);
        assert!((16 << 10..(16 << 10) + 64).contains(&grew), "{grew}");
    }

    /// A row archives to its bytes and a little more, and the largest write fits a frame many
    /// times over
    #[test]
    fn rows_archive_to_their_stated_shapes() {
        let small = archived_len(&row(1, 1, 4096));
        assert!((4096..4096 + 64).contains(&small), "{small}");
        let large = archived_len(&row(1, 1, 256 << 10));
        assert!(large < 1 << 20, "{large}");
        // a stripe row's head is what the read before a commit carries
        let head = archived_len(&stripe_row(crate::keys::stripe_key(1, 2), 0));
        assert!(head > 100 && head < 300, "a stripe row archived to {head} bytes");
    }
}

#[cfg(test)]
mod sizes {
    use super::*;

    /// Print the archived size of each row and commit, for the record page
    #[test]
    #[ignore = "prints the sizes the record page states"]
    fn print_archived_sizes() {
        let key = crate::keys::stripe_key(1, 2);
        println!("stripe row {}", archived_len(&stripe_row(key, 0)));
        println!("staged commit {}", archived_len(&staged_commit(key, 0, 1, 0).write));
        for size in [4096usize, 65_536, 262_144] {
            let bytes = crate::bytes::make(1, 2, size);
            println!("inline commit {size}: {}", archived_len(&inline_commit(key, 0, 1, bytes).write));
            println!("row {size}: {}", archived_len(&row(1, 1, size)));
        }
        println!("filler {}", archived_len(&filler(1)));
    }
}
