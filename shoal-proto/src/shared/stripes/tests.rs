//! The stripe row's rules, one at a time
//!
//! The model conformance test (`shoal-proto/tests/stripe_model_conformance.rs`) holds these to the
//! stripe model over every saved schedule; these name each rule on its own, so a failure says
//! which one moved.

use uuid::Uuid;

use super::{
    commit_bundle, hidden_from, is_hole, shown, stripes_of, write_tag, StripeRow, Touch,
    WriteCommit,
};
use crate::shared::objects::{
    Floor, Geometry, Label, ObjectEntry, PendingBytes, PendingUnit, MISSED_BIT,
};

/// A label at a sequence with a tag
fn label(sequence: u64, tag: u64) -> Label {
    Label { sequence, tag }
}

/// A replicated stripe of three positions its put wrote
fn put_row() -> StripeRow {
    StripeRow::put(7, 9, 0, 3, 1 << 20)
}

/// A unit of a small write
fn unit(index: u32, byte: u8) -> PendingUnit {
    PendingUnit {
        index,
        bytes: vec![byte; 8],
    }
}

/// An entry of an object of four 1 MiB stripes, three of them written by its put
fn entry() -> ObjectEntry {
    ObjectEntry {
        size: 4 << 20,
        put_length: 3 << 20,
        geometry: Geometry {
            stripe_bytes: 1 << 20,
            unit_bytes: 64 << 10,
            data: 1,
            parity: 2,
        },
        ..ObjectEntry::default()
    }
}

/// A mark sets the tag's top bit, comes off whole, and no write's tag carries it
#[test]
fn a_mark_is_the_tags_top_bit_and_no_write_carries_it() {
    let named = label(3, 0x1234);
    // marked, it is missed and unmarks to itself
    assert!(named.marked().is_missed());
    assert!(!named.is_missed());
    assert_eq!(named.marked().unmarked(), named);
    assert_eq!(named.marked().tag & MISSED_BIT, MISSED_BIT);
    // no tag a write mints is marked, or the put's
    let op = Uuid::from_u128(0x0192_6f1e_7a00_7000_8000_0000_0000_0001);
    for stripe in 0..64 {
        for attempt in 0..8 {
            let tag = write_tag(&op, stripe, attempt, 11);
            assert_eq!(tag & MISSED_BIT, 0);
            assert_ne!(tag, Label::PUT.tag);
        }
    }
}

/// A tag is a try's and a task's; a commit's identity is a try's and never a task's
#[test]
fn a_tag_is_a_tries_and_a_commit_identity_names_no_task() {
    let op = Uuid::from_u128(0x0192_6f1e_7a00_7000_8000_0000_0000_0001);
    // two tries, two stripes and two tasks make four tags
    assert_ne!(write_tag(&op, 0, 0, 1), write_tag(&op, 0, 1, 1));
    assert_ne!(write_tag(&op, 0, 0, 1), write_tag(&op, 1, 0, 1));
    assert_ne!(write_tag(&op, 0, 0, 1), write_tag(&op, 0, 0, 2));
    // a commit's identity is the same from any task, and differs by try and stripe
    assert_eq!(commit_bundle(&op, 4, 2), commit_bundle(&op, 4, 2));
    assert_ne!(commit_bundle(&op, 4, 2), commit_bundle(&op, 4, 3));
    assert_ne!(commit_bundle(&op, 4, 2), commit_bundle(&op, 5, 2));
    // and is a v8 identity, which no client's v7 bundle is
    assert_eq!(commit_bundle(&op, 4, 2).get_version_num(), 8);
}

/// The identities are frozen: a retry on another build derives the same commit identity
#[test]
fn stripe_identities_are_frozen() {
    let op = Uuid::from_u128(0x0192_6f1e_7a00_7000_8000_0000_0000_0001);
    assert_eq!(
        commit_bundle(&op, 4, 2),
        FROZEN_COMMIT,
        "commit_bundle moved: {:?}",
        commit_bundle(&op, 4, 2)
    );
    assert_eq!(
        write_tag(&op, 4, 2, 9),
        FROZEN_TAG,
        "write_tag moved: {:#x}",
        write_tag(&op, 4, 2, 9)
    );
}

/// The commit identity `stripe_identities_are_frozen` derives
const FROZEN_COMMIT: Uuid = Uuid::from_u128(0x5354_5243_4d54_8fe5_9df6_d110_bdbf_cafd);

/// The tag `stripe_identities_are_frozen` derives
const FROZEN_TAG: u64 = 0x2353_de1a_f1de_500c;

/// A commit moves the sequence, labels what it touched, marks what missed, and stamps the row
#[test]
fn a_commit_labels_what_it_touched_and_marks_what_missed() {
    let row = put_row();
    let write = label(1, 42);
    let next = row.commit(&WriteCommit {
        label: write,
        positions: vec![Touch::Held, Touch::Held, Touch::Missed],
        stamp: 2,
        length: 1 << 20,
        pending: None,
    });
    assert_eq!(next.sequence, 1);
    assert_eq!(next.labels[0], write);
    assert_eq!(next.labels[1], write);
    // the missed position names the write and is not current
    assert_eq!(next.named(2), write);
    assert!(!next.current(2));
    assert!(next.current(0));
    assert_eq!(next.stamp, 2);
    // a position untouched keeps whatever it had, its mark included
    let after = next.commit(&WriteCommit {
        label: label(2, 43),
        positions: vec![Touch::Held, Touch::Held, Touch::Untouched],
        stamp: 2,
        length: 1 << 20,
        pending: None,
    });
    assert!(after.labels[2].is_missed());
    assert_eq!(after.named(2), write);
}

/// A position marked missed still names its write, so a late stage of it is recognised
#[test]
fn a_missed_position_still_names_its_write() {
    let row = put_row().commit(&WriteCommit {
        label: label(1, 42),
        positions: vec![Touch::Held, Touch::Missed, Touch::Held],
        stamp: 0,
        length: 1 << 20,
        pending: None,
    });
    // a holder that staged it late makes the named label, though the row calls it stale
    assert!(row.makes(1, label(1, 42)));
    assert!(!row.current(1));
    assert_eq!(row.current_labels(1), vec![label(1, 42)]);
}

/// The no-op and the fence move the sequence; the fence never lowers
#[test]
fn a_noop_and_a_fence_move_the_sequence() {
    let row = put_row();
    let noop = row.noop();
    assert_eq!(noop.sequence, 1);
    assert_eq!(noop.labels, row.labels);
    let fenced = noop.fence(4).fence(2);
    assert_eq!(fenced.sequence, 3);
    assert_eq!(fenced.fence, 4);
}

/// A small write's bytes merge over what is pending, keeping the base and chaining the earlier
#[test]
fn pending_bytes_merge_and_never_replace() {
    let row = put_row();
    let first = label(1, 10);
    let pending = PendingBytes::after(&row, first, &[unit(2, 1), unit(0, 1)]);
    assert_eq!(pending.base, vec![Label::PUT; 3]);
    assert!(pending.chain.is_empty());
    assert_eq!(
        pending.units.iter().map(|u| u.index).collect::<Vec<_>>(),
        vec![0, 2]
    );
    let row = row.commit(&WriteCommit {
        label: first,
        positions: vec![Touch::Held; 3],
        stamp: 0,
        length: 1 << 20,
        pending: Some(pending),
    });
    // a second small write keeps the first's units it did not write, and chains its label
    let second = label(2, 11);
    let merged = PendingBytes::after(&row, second, &[unit(2, 2), unit(5, 2)]);
    assert_eq!(merged.base, vec![Label::PUT; 3]);
    assert_eq!(merged.chain, vec![first]);
    let units: Vec<(u32, u8)> = merged.units.iter().map(|u| (u.index, u.bytes[0])).collect();
    assert_eq!(units, vec![(0, 1), (2, 2), (5, 2)]);
    let row = row.commit(&WriteCommit {
        label: second,
        positions: vec![Touch::Held; 3],
        stamp: 0,
        length: 1 << 20,
        pending: Some(merged),
    });
    // a chunk at the base, at the first write, or at the second makes the row's label
    for held in [Label::PUT, first, second] {
        assert!(row.makes(0, held), "{held:?} makes the row's label");
    }
    assert!(!row.makes(0, label(1, 99)));
    assert_eq!(row.current_labels(0), vec![second, Label::PUT, first]);
}

/// A clear moves the sequence, takes the bytes out, and marks every holder that did not hold them
#[test]
fn a_clear_marks_every_holder_that_did_not_hold_the_label() {
    let write = label(1, 10);
    let row = put_row().commit(&WriteCommit {
        label: write,
        positions: vec![Touch::Held, Touch::Held, Touch::Missed],
        stamp: 0,
        length: 1 << 20,
        pending: Some(PendingBytes::after(&put_row(), write, &[unit(0, 1)])),
    });
    let cleared = row.clear(&[true, false, true]);
    assert_eq!(cleared.sequence, 2);
    assert!(cleared.pending.is_none());
    // a holder that held it is current again, missed or not before; one that did not is missed
    assert!(cleared.current(0));
    assert!(!cleared.current(1));
    assert!(cleared.current(2));
    assert_eq!(cleared.named(1), write);
}

/// A reclaimed stripe is a tombstone a sequence on, every chunk missed, stamped with its floor
#[test]
fn a_reclaimed_row_is_a_tombstone() {
    let row = put_row().noop();
    let reclaimed = row.reclaimed(5);
    assert!(reclaimed.tombstone);
    assert_eq!(reclaimed.sequence, 2);
    assert!((0..3).all(|pos| !reclaimed.current(pos)));
    assert_eq!(reclaimed.stamp, 5);
    // a write over it un-reclaims it
    let written = reclaimed.commit(&WriteCommit {
        label: label(3, 1),
        positions: vec![Touch::Held; 3],
        stamp: 5,
        length: 1 << 20,
        pending: None,
    });
    assert!(!written.tombstone);
}

/// A stripe past the put with no row, or a row naming only the put, is a hole; a written one is not
#[test]
fn a_hole_is_past_the_put_and_named_by_nothing_since() {
    let entry = entry();
    // stripe 3 starts at the put's length
    assert!(is_hole(&entry, None, 3));
    assert!(!is_hole(&entry, None, 2));
    // a no-op or a fence over it leaves it a hole
    let noop = StripeRow::put(7, 9, 3, 3, 0).noop().fence(1);
    assert!(is_hole(&entry, Some(&noop), 3));
    // a write into it does not
    let written = noop.commit(&WriteCommit {
        label: label(3, 4),
        positions: vec![Touch::Held; 3],
        stamp: 0,
        length: 1 << 20,
        pending: None,
    });
    assert!(!is_hole(&entry, Some(&written), 3));
    // a tombstone is a hole wherever it is
    assert!(is_hole(&entry, Some(&put_row().reclaimed(1)), 0));
}

/// A floor hides what it cut from a stripe stamped below it, and nothing from one stamped past it
#[test]
fn a_floor_hides_a_stripe_stamped_below_it() {
    let mut entry = entry();
    // a truncate to 1.5 MiB at epoch 1, then the object grew back
    entry.floors.push(Floor {
        length: 3 << 19,
        epoch: 1,
    });
    entry.epoch = 1;
    // stripe 1 is cut in half, stripe 2 whole, stripe 0 untouched
    assert_eq!(hidden_from(&entry, None, 0), 1 << 20);
    assert_eq!(hidden_from(&entry, None, 1), 3 << 19);
    assert_eq!(hidden_from(&entry, None, 2), 2 << 20);
    // a row stamped at the floor's epoch is shown whole
    let mut stamped = StripeRow::put(7, 9, 1, 3, 1 << 20).noop();
    stamped.stamp = 1;
    assert_eq!(hidden_from(&entry, Some(&stamped), 1), 2 << 20);
    // and what a reader shows follows
    let half = shown(&entry, None, 1).expect("inside the object");
    assert_eq!((half.start, half.read_to, half.end), (1 << 20, 3 << 19, 2 << 20));
    assert!(shown(&entry, None, 2).expect("inside").is_zeros());
    assert!(shown(&entry, None, 4).is_none());
}

/// A range falls in the stripes it touches, and an empty one in none
#[test]
fn a_range_falls_in_the_stripes_it_touches() {
    assert_eq!(stripes_of(1 << 20, 0, 1), 0..1);
    assert_eq!(stripes_of(1 << 20, (1 << 20) - 1, 2), 0..2);
    assert_eq!(stripes_of(1 << 20, 3 << 20, 1 << 20), 3..4);
    assert_eq!(stripes_of(1 << 20, 5, 0), 0..0);
}
