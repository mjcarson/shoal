//! The byte ledger held to histories written by hand (S16, M11)
//!
//! Every history here is a few writes and reads of one or two objects, at a granule of four
//! bytes, with the step each was sent and answered at chosen so that real time says exactly what
//! the case is about. A write's bytes are one letter, so a read's bytes say which write each
//! granule came from.

use shoal_model::bytes::{
    ByteLedger, LedgerError, ReadId, ReadLevel, Violation, WriteId, WriteOutcome,
    MAX_OPS_PER_OBJECT,
};

/// The object most histories write to
const OBJECT: &str = "photos/cat.jpg";

/// The bytes a granule holds
const GRANULE: u64 = 4;

/// An empty ledger at a granule of four bytes
fn ledger() -> ByteLedger {
    ByteLedger::new(GRANULE).expect("a granule of four bytes")
}

/// Granules of one letter each, spelled out: `fill("AB")` is `AAAABBBB`, and `.` is zeros
///
/// # Arguments
///
/// * `granules` - One character a granule
fn fill(granules: &str) -> Vec<u8> {
    granules
        .bytes()
        .flat_map(|letter| {
            let byte = if letter == b'.' { 0 } else { letter };
            std::iter::repeat_n(byte, GRANULE as usize)
        })
        .collect()
}

/// Record a write of one object, sent and answered at the steps given
///
/// # Arguments
///
/// * `ledger` - The ledger
/// * `offset` - Where it starts
/// * `granules` - Its bytes, as `fill` spells them
/// * `invoke` - When it was sent
/// * `answer` - When it was answered and what it was told, if it was
fn write(
    ledger: &mut ByteLedger,
    offset: u64,
    granules: &str,
    invoke: u64,
    answer: Option<(u64, WriteOutcome)>,
) -> WriteId {
    let id = ledger
        .write(OBJECT, offset, &fill(granules), invoke)
        .expect("an aligned write");
    if let Some((step, outcome)) = answer {
        ledger.complete(id, outcome, step).expect("a pending write");
    }
    id
}

/// Record an answered read of the object
///
/// # Arguments
///
/// * `ledger` - The ledger
/// * `offset` - Where it starts
/// * `granules` - What it was answered, as `fill` spells it
/// * `level` - What it asked for
/// * `invoke` - When it was sent
/// * `complete` - When it was answered
fn read(
    ledger: &mut ByteLedger,
    offset: u64,
    granules: &str,
    level: ReadLevel,
    invoke: u64,
    complete: u64,
) -> ReadId {
    ledger
        .read(OBJECT, offset, &fill(granules), level, invoke, complete)
        .expect("an aligned read")
}

/// Whether a verdict is a failure to find an order of the object
///
/// # Arguments
///
/// * `verdict` - What the ledger said
fn not_linearizable(verdict: &Result<(), Violation>) -> bool {
    matches!(verdict, Err(Violation::NotLinearizable { object, .. }) if object == OBJECT)
}

/// Over histories written by hand, an acknowledged range must be read, a refused one must not
/// be, and an unknown one may be whole or absent and never part (S16's row)
#[test]
fn byte_ledger_judges_the_three_outcomes() {
    use ReadLevel::{Default, Strong};
    use WriteOutcome::{Acknowledged, Refused, Unknown};
    let object = OBJECT.to_string();

    // acknowledged, then read whole: right
    let mut history = ledger();
    write(&mut history, 0, "AA", 1, Some((2, Acknowledged)));
    read(&mut history, 0, "AA", Strong, 3, 4);
    assert_eq!(history.judge(), Ok(()));

    // acknowledged, then a strong read that begins after it shows zeros: it must be read
    let mut history = ledger();
    let acked = write(&mut history, 0, "AA", 1, Some((2, Acknowledged)));
    let strong = read(&mut history, 0, "..", Strong, 3, 4);
    assert_eq!(
        history.judge(),
        Err(Violation::Missing {
            object: object.clone(),
            read: strong,
            write: acked,
            offset: 0,
        })
    );

    // the same at the default level is a stale read, which P12 allows
    let mut history = ledger();
    write(&mut history, 0, "AA", 1, Some((2, Acknowledged)));
    read(&mut history, 0, "..", Default, 3, 4);
    assert_eq!(history.judge(), Ok(()));

    // refused, then read: it must not be, at either level
    for level in [Strong, Default] {
        let mut history = ledger();
        let refused = write(&mut history, 0, "AA", 1, Some((2, Refused)));
        let shown = read(&mut history, 0, "AA", level, 3, 4);
        assert_eq!(
            history.judge(),
            Err(Violation::RefusedRead {
                object: object.clone(),
                read: shown,
                write: refused,
                offset: 0,
            })
        );
    }

    // refused, and absent: right
    let mut history = ledger();
    write(&mut history, 0, "AA", 1, Some((2, Refused)));
    read(&mut history, 0, "..", Strong, 3, 4);
    assert_eq!(history.judge(), Ok(()));

    // unknown, then read whole: it may have taken effect
    let mut history = ledger();
    write(&mut history, 0, "AA", 1, Some((2, Unknown)));
    read(&mut history, 0, "AA", Strong, 3, 4);
    assert_eq!(history.judge(), Ok(()));

    // unknown, and absent: it may not have
    let mut history = ledger();
    write(&mut history, 0, "AA", 1, Some((2, Unknown)));
    read(&mut history, 0, "..", Strong, 3, 4);
    assert_eq!(history.judge(), Ok(()));

    // unknown, and read in part over zeros: never
    let mut history = ledger();
    let unknown = write(&mut history, 0, "AA", 1, Some((2, Unknown)));
    let part = read(&mut history, 0, "A.", Default, 3, 4);
    assert_eq!(
        history.judge(),
        Err(Violation::Part {
            object: object.clone(),
            read: part,
            write: unknown,
            offset: 4,
        })
    );

    // or in part over the acknowledged write it would have replaced
    let mut history = ledger();
    write(&mut history, 0, "XX", 1, Some((2, Acknowledged)));
    let unknown = write(&mut history, 0, "AA", 3, None);
    let part = read(&mut history, 0, "AX", Strong, 5, 6);
    history.finish(7);
    assert_eq!(
        history.judge(),
        Err(Violation::Part {
            object: object.clone(),
            read: part,
            write: unknown,
            offset: 4,
        })
    );

    // unknown, seen whole by one strong read and gone from a later one: no order has it both ways
    let mut history = ledger();
    write(&mut history, 0, "AA", 1, None);
    read(&mut history, 0, "AA", Strong, 2, 3);
    read(&mut history, 0, "..", Strong, 4, 5);
    history.finish(6);
    // the message is the object's history in invocation order, with what each granule could be
    assert_eq!(
        history.judge(),
        Err(Violation::NotLinearizable {
            object: object.clone(),
            detail: "write 0 [0, 8) unknown 1..; \
                     read 0 strong [0, 8) 2..3 showing [w0, w0]; \
                     read 1 strong [0, 8) 4..5 showing [zeros, zeros]"
                .to_string(),
        })
    );

    // two writes that overlap in time, read as one granule of each: a mixture, whatever the order
    let mut history = ledger();
    write(&mut history, 0, "AA", 1, Some((4, Acknowledged)));
    write(&mut history, 0, "BB", 2, Some((3, Acknowledged)));
    read(&mut history, 0, "AB", Strong, 5, 6);
    let verdict = history.judge();
    assert!(not_linearizable(&verdict), "{verdict:?}");

    // and bytes no write wrote, as a torn or damaged granule reads, are named as such
    let mut history = ledger();
    write(&mut history, 0, "AA", 1, Some((2, Acknowledged)));
    let damaged = history
        .read(OBJECT, 0, b"AAAAAAZA", Strong, 3, 4)
        .expect("an aligned read");
    assert_eq!(
        history.judge(),
        Err(Violation::Unwritten {
            object,
            read: damaged,
            offset: 4,
        })
    );
}

/// A record the ledger cannot judge is refused when it is made, and leaves the ledger as it was
#[test]
fn byte_ledger_refuses_unaligned_ranges() {
    // a granule of nothing
    assert_eq!(ByteLedger::new(0), Err(LedgerError::ZeroGranule));
    let mut history = ledger();
    // a write that starts inside a granule, or ends inside one
    assert_eq!(
        history.write(OBJECT, 2, b"AAAA", 1),
        Err(LedgerError::Unaligned {
            object: OBJECT.to_string(),
            offset: 2,
            len: 4,
        })
    );
    assert_eq!(
        history.write(OBJECT, 0, b"AAAAAA", 1),
        Err(LedgerError::Unaligned {
            object: OBJECT.to_string(),
            offset: 0,
            len: 6,
        })
    );
    // a read the same
    assert!(matches!(
        history.read(OBJECT, 4, b"AA", ReadLevel::Strong, 1, 2),
        Err(LedgerError::Unaligned { .. })
    ));
    // no bytes at all
    assert_eq!(
        history.write(OBJECT, 0, b"", 1),
        Err(LedgerError::Empty {
            object: OBJECT.to_string(),
            offset: 0,
        })
    );
    // an answer before its question
    assert!(matches!(
        history.read(OBJECT, 0, b"AAAA", ReadLevel::Strong, 5, 4),
        Err(LedgerError::Backwards { .. })
    ));
    // nothing refused was kept
    assert!(history.writes.is_empty() && history.reads.is_empty());
    // a write told twice, and one never recorded
    let id = write(
        &mut history,
        0,
        "A",
        1,
        Some((2, WriteOutcome::Acknowledged)),
    );
    assert_eq!(
        history.complete(id, WriteOutcome::Refused, 3),
        Err(LedgerError::AlreadyComplete(id))
    );
    assert_eq!(
        history.complete(WriteId(9), WriteOutcome::Refused, 3),
        Err(LedgerError::NoSuchWrite(WriteId(9)))
    );
}

/// Each object is judged on its own: a write to one is no read's business on another
#[test]
fn byte_ledger_judges_objects_apart() {
    let mut history = ledger();
    // an acknowledged write to one object
    let id = history.write(OBJECT, 0, &fill("AA"), 1).expect("aligned");
    history
        .complete(id, WriteOutcome::Acknowledged, 2)
        .expect("pending");
    // a strong read of another, after it, sees nothing it did
    history
        .read("photos/dog.jpg", 0, &fill(".."), ReadLevel::Strong, 3, 4)
        .expect("aligned");
    assert_eq!(history.judge(), Ok(()));
    // and the first object's bytes read from the other were never written there
    let wrong = history
        .read("photos/dog.jpg", 0, &fill("AA"), ReadLevel::Strong, 5, 6)
        .expect("aligned");
    assert_eq!(
        history.judge(),
        Err(Violation::Unwritten {
            object: "photos/dog.jpg".to_string(),
            read: wrong,
            offset: 0,
        })
    );
    assert_eq!(
        history.objects().into_iter().collect::<Vec<_>>(),
        vec!["photos/cat.jpg", "photos/dog.jpg"]
    );
}

/// Writes that overlap in time may land in either order, but in one order for every read
#[test]
fn byte_ledger_orders_concurrent_writes() {
    use ReadLevel::{Default, Strong};
    use WriteOutcome::Acknowledged;

    // A covers granules 0 and 1, B covers 1 and 2, and they overlap in time
    let concurrent = || {
        let mut history = ledger();
        let a = write(&mut history, 0, "AA", 1, Some((4, Acknowledged)));
        let b = write(&mut history, 4, "BB", 2, Some((3, Acknowledged)));
        (history, a, b)
    };

    // A then B, or B then A: either is right
    for shown in ["ABB", "AAB"] {
        let (mut history, _, _) = concurrent();
        read(&mut history, 0, shown, Strong, 5, 6);
        assert_eq!(history.judge(), Ok(()), "{shown}");
    }

    // but not both: a later strong read cannot see the other order
    let (mut history, _, _) = concurrent();
    read(&mut history, 0, "ABB", Strong, 5, 6);
    read(&mut history, 0, "AAB", Strong, 7, 8);
    let verdict = history.judge();
    assert!(not_linearizable(&verdict), "{verdict:?}");

    // a strong read missing the granule only B wrote misses an acknowledged write
    let (mut history, _, b) = concurrent();
    let strong = read(&mut history, 0, "AB.", Strong, 5, 6);
    assert_eq!(
        history.judge(),
        Err(Violation::Missing {
            object: OBJECT.to_string(),
            read: strong,
            write: b,
            offset: 8,
        })
    );

    // a default read after both may show the state before either, or after one
    for shown in ["...", "AA.", ".BB"] {
        let (mut history, _, _) = concurrent();
        read(&mut history, 0, shown, Default, 5, 6);
        assert_eq!(history.judge(), Ok(()), "{shown}");
    }

    // a read overlapping a write in time may show it or not
    for shown in ["AA", "BB"] {
        let mut history = ledger();
        write(&mut history, 0, "AA", 1, Some((2, Acknowledged)));
        write(&mut history, 0, "BB", 3, Some((6, Acknowledged)));
        read(&mut history, 0, shown, Strong, 4, 5);
        assert_eq!(history.judge(), Ok(()), "{shown}");
    }

    // and the same bytes written twice need no telling apart
    let mut history = ledger();
    write(&mut history, 0, "AA", 1, Some((2, Acknowledged)));
    write(&mut history, 0, "BB", 3, Some((4, Acknowledged)));
    write(&mut history, 0, "AA", 5, Some((6, Acknowledged)));
    read(&mut history, 0, "AA", Strong, 7, 8);
    assert_eq!(history.judge(), Ok(()));
}

/// An object past the search's bound is refused by name, and one at it is judged
#[test]
fn byte_ledger_bounds_its_search() {
    // writes one after another, each acknowledged before the next is sent, and one read
    let history_of = |writes: usize| {
        let mut history = ledger();
        for index in 0..writes {
            let step = index as u64 * 2;
            let letter = if index.is_multiple_of(2) { "A" } else { "B" };
            write(
                &mut history,
                0,
                letter,
                step,
                Some((step + 1, WriteOutcome::Acknowledged)),
            );
        }
        let last = if (writes - 1).is_multiple_of(2) {
            "A"
        } else {
            "B"
        };
        read(
            &mut history,
            0,
            last,
            ReadLevel::Strong,
            writes as u64 * 2,
            writes as u64 * 2 + 1,
        );
        history
    };
    // at the bound
    assert_eq!(history_of(MAX_OPS_PER_OBJECT - 1).judge(), Ok(()));
    // one past it
    assert_eq!(
        history_of(MAX_OPS_PER_OBJECT).judge(),
        Err(Violation::TooManyOps {
            object: OBJECT.to_string(),
            count: MAX_OPS_PER_OBJECT + 1,
        })
    );
}

/// A history written out as JSON is judged the same when read back
#[test]
fn byte_ledger_is_judged_again_from_json() {
    let mut history = ledger();
    write(&mut history, 0, "AA", 1, Some((2, WriteOutcome::Refused)));
    read(&mut history, 0, "AA", ReadLevel::Strong, 3, 4);
    // a pending write, finished as unknown
    write(&mut history, 8, "C", 5, None);
    assert_eq!(history.pending(), vec![WriteId(1)]);
    history.finish(6);
    assert!(history.pending().is_empty());
    // the same verdict either side of the round trip
    let json = serde_json::to_string(&history).expect("a ledger serializes");
    let back: ByteLedger = serde_json::from_str(&json).expect("and deserializes");
    assert_eq!(back, history);
    assert_eq!(back.judge(), history.judge());
    assert!(matches!(back.judge(), Err(Violation::RefusedRead { .. })));
}
