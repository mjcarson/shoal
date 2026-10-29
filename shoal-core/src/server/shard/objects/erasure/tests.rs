//! The coordinator's erasure coded rules that need no holder: what a decode takes, and how a write
//! is made

use super::{plan_write, take, Taken, WriteMode};
use crate::server::objects::codec;
use crate::shared::objects::{Geometry, Label};
use crate::shared::protocol::peer::chunk::{ChunkReply, StoreCode};
use crate::shared::stripes::StripeRow;

/// A 4+2 stripe of 4 MiB in units of 64 KiB
fn four_plus_two() -> Geometry {
    Geometry {
        stripe_bytes: 4 << 20,
        unit_bytes: 64 << 10,
        data: 4,
        parity: 2,
    }
}

/// A 2+1 stripe of 4 MiB in units of 64 KiB
fn two_plus_one() -> Geometry {
    Geometry {
        data: 2,
        parity: 1,
        ..four_plus_two()
    }
}

/// A label
///
/// # Arguments
///
/// * `sequence` - Its sequence
fn label(sequence: u64) -> Label {
    Label { sequence, tag: sequence * 7 + 1 }
}

/// A row of a stripe with every position at one label
///
/// # Arguments
///
/// * `width` - Its positions
/// * `length` - How much of the stripe holds bytes
/// * `at` - The label every position is at
fn row(width: usize, length: u64, at: Label) -> StripeRow {
    let mut row = StripeRow::put(1, 2, 0, width, length, 0);
    row.sequence = at.sequence;
    row.labels = vec![at; width];
    row
}

/// A decode offered chunks under two labels uses one label's or fails by name (S8, P10, M18)
///
/// Only bytes at the label the row names are taken: a chunk of a write the row has not reached
/// reads the row again, and a chunk of an older write is never a source. A 2+1 stripe whose data
/// chunk 0 is one write ahead and whose parity is one behind has one source of the row's state, so
/// a decode of data chunk 1 is refused rather than made from chunks of three writes.
#[test]
fn decode_never_mixes_labels() {
    let geometry = two_plus_one();
    let state = row(3, 1 << 20, label(5));
    // what each holder answered: D0 a write ahead, D1 the row's, P a write behind
    let answers = [
        ChunkReply::Bytes {
            label: label(6),
            bytes: vec![1; 8].into(),
        },
        ChunkReply::Bytes {
            label: label(5),
            bytes: vec![2; 8].into(),
        },
        ChunkReply::Bytes {
            label: label(4),
            bytes: vec![3; 8].into(),
        },
    ];
    let taken: Vec<Taken> = answers
        .iter()
        .cloned()
        .enumerate()
        .map(|(position, reply)| take(&state, position, reply))
        .collect();
    // a chunk ahead of the row is no source, and neither is one behind it
    assert!(matches!(taken[0], Taken::Missing(_)), "{:?}", taken[0]);
    assert_eq!(taken[1], Taken::Bytes(vec![2; 8].into()));
    assert!(matches!(taken[2], Taken::Missing(_)), "{:?}", taken[2]);
    // one source of the row's state is short of the k a decode needs: it fails by name
    let sources = taken.iter().filter(|taken| matches!(taken, Taken::Bytes(_))).count();
    assert!(sources < usize::from(geometry.data));
    // a holder that says it holds a label past the row reads the row again
    assert_eq!(
        take(&state, 0, ChunkReply::Held { label: Some(label(6)) }),
        Taken::Ahead
    );
    // and one whose unit fails its checksum is reported, never a source
    assert!(matches!(
        take(
            &state,
            0,
            ChunkReply::Refused {
                code: StoreCode::Corrupt,
                message: "unit 3".to_string(),
            }
        ),
        Taken::Corrupt(_)
    ));
    // what mixing would have made: data chunk 1 decoded from D0 of the next write and P of the
    // last is bytes nobody wrote
    let old = [vec![10u8; 8], vec![20u8; 8]];
    let mut parity = vec![vec![0u8; 8]];
    {
        let data: Vec<&[u8]> = old.iter().map(Vec::as_slice).collect();
        let mut outs: Vec<&mut [u8]> = parity.iter_mut().map(Vec::as_mut_slice).collect();
        codec::encode_now(2, 1, &data, &mut outs).expect("an encode");
    }
    let newer_d0 = vec![11u8; 8];
    let mut made = vec![vec![0u8; 8]];
    {
        let sources = [(0u8, newer_d0.as_slice()), (2u8, parity[0].as_slice())];
        let mut outs: Vec<&mut [u8]> = made.iter_mut().map(Vec::as_mut_slice).collect();
        codec::decode_now(2, 1, &sources, &[1], &mut outs).expect("a decode");
    }
    assert_ne!(made[0], old[1], "a decode over two writes' chunks happened to give the old bytes");
}

/// A write that covers a stripe, or leaves old bytes a floor hides, is a whole stripe
#[test]
fn a_whole_stripe_is_encoded_once() {
    let geometry = four_plus_two();
    let mib = 1u64 << 20;
    let base = row(6, 4 * mib, label(3));
    // every byte rewritten
    let whole = plan_write(&geometry, &base, 4 * mib, false, 0, 4 * mib);
    assert_eq!(whole.mode, WriteMode::Whole);
    assert_eq!(whole.staged, vec![0, 1, 2, 3, 4, 5]);
    assert_eq!(whole.whole, whole.staged);
    // a floor hides old bytes: a partial write could revive them, so the stripe is rewritten
    let floored = plan_write(&geometry, &base, 2 * mib, true, 10, 100);
    assert_eq!(floored.mode, WriteMode::Whole);
    // and a write starting past the stripe's end leaves a gap of zeros
    let gap = plan_write(&geometry, &row(6, mib, label(3)), mib, false, 2 * mib, 100);
    assert_eq!(gap.mode, WriteMode::Whole);
}

/// One data chunk of 4+2 touched is a delta, and of 2+1 a reconstruct-write: the delta reads one
/// old range where reconstruct reads the `k - d` others (decided with the user, F90)
#[test]
fn a_partial_write_takes_the_fewer_remote_reads() {
    let mib = 1u64 << 20;
    // 4+2, inside data chunk 1: d = 1 < k - d = 3
    let wide = plan_write(&four_plus_two(), &row(6, 4 * mib, label(3)), 4 * mib, false, mib + 10, 4096);
    assert_eq!(wide.mode, WriteMode::Delta);
    assert_eq!(wide.staged, vec![1, 4, 5]);
    assert!(wide.whole.is_empty());
    assert_eq!(wide.untouched, vec![0, 2, 3]);
    // across two data chunks of 4+2: d = 2, not fewer than k - d
    let across = plan_write(&four_plus_two(), &row(6, 4 * mib, label(3)), 4 * mib, false, mib - 10, 20);
    assert_eq!(across.mode, WriteMode::Reconstruct);
    assert_eq!(across.staged, vec![0, 1, 4, 5]);
    // 2+1, inside data chunk 0: d = 1, k - d = 1
    let narrow = plan_write(&two_plus_one(), &row(3, 4 * mib, label(3)), 4 * mib, false, 10, 4096);
    assert_eq!(narrow.mode, WriteMode::Reconstruct);
    assert_eq!(narrow.untouched, vec![1]);
    // a touched chunk that is stale has no old bytes to diff: reconstruct-write decodes them
    let mut stale = row(6, 4 * mib, label(3));
    stale.labels[1] = stale.labels[1].marked();
    let around = plan_write(&four_plus_two(), &stale, 4 * mib, false, mib + 10, 4096);
    assert_eq!(around.mode, WriteMode::Reconstruct);
}

/// A short stripe's empty data chunks are held with nothing asked, and one a write gives bytes is
/// staged whole
#[test]
fn an_empty_chunk_is_held_and_filled_whole() {
    let geometry = four_plus_two();
    let mib = 1u64 << 20;
    // a stripe of 20,000 bytes: D0 and the parity hold them, D1 to D3 nothing
    let short = row(6, 20_000, label(3));
    let small = plan_write(&geometry, &short, 20_000, false, 100, 100);
    assert_eq!(small.empty, vec![1, 2, 3]);
    assert!(small.untouched.is_empty());
    // an append that reaches data chunk 1 fills it whole, from nothing
    let grown = plan_write(&geometry, &row(6, mib, label(3)), mib, false, mib, 4096);
    assert_eq!(grown.staged, vec![1, 4, 5]);
    assert_eq!(grown.whole, vec![1]);
    assert_eq!(grown.empty, vec![2, 3]);
    assert_eq!(grown.length, mib + 4096);
}

/// A write that takes a chunk holding bytes past its file's class is a whole stripe, and one that
/// stays inside every class is not (F96)
#[test]
fn a_chunk_past_its_class_rewrites_the_stripe_whole() {
    let geometry = four_plus_two();
    let mib = 1u64 << 20;
    let unit = 64u64 << 10;
    // a stripe of a mebibyte and a half: D1 holds eight units, its class; two more take it past
    let base = row(6, mib + mib / 2, label(3));
    let grown = plan_write(&geometry, &base, mib + mib / 2, false, mib + mib / 2, 2 * unit);
    assert_eq!(grown.mode, WriteMode::Whole);
    assert_eq!(grown.staged, vec![0, 1, 4, 5]);
    // a write inside D1's eight units stays a partial write
    let inside = plan_write(&geometry, &base, mib + mib / 2, false, mib + 10, 4096);
    assert_eq!(inside.mode, WriteMode::Delta);
    // a short stripe's chunk zero, and so its parity, past its one unit: the stripe whole
    let short = row(6, 20_000, label(3));
    let past = plan_write(&geometry, &short, 20_000, false, 20_000, unit);
    assert_eq!(past.mode, WriteMode::Whole);
}
