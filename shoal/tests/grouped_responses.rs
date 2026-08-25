//! Tests for a response that carries the partitions its rows came from, and that borrows them
//!
//! Two changes meet in this file
//! ([F27](../../docs/src/features/grouped-responses.md)). A get's answer carries an index saying
//! which partition each run of its rows came from, so the shard collecting the shares of a split
//! get never has to ask a row where it belongs; and a get whose partitions are all resident is
//! answered out of the rows the shard already holds rather than out of copies of them.
//!
//! The second of those rests entirely on one claim: **that serializing the borrowed shape writes
//! exactly the bytes serializing the owned shape would have written.** If that is ever false the
//! client is reading one wire format with another's layout, and nothing else in the system would
//! notice. So it is checked here, per variant, against the real generated enums of a real schema
//! rather than against a hand-written stand-in.
//!
//! Nothing here starts a server - these are assertions about what the derive generates and what
//! rkyv does with it.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::shared::responses::{GetRows, Response, ResponseAction, ResponseError};
use shoal::shared::protocol::error::ErrorCode;
use shoal::shared::row_ref::RowRef;
use shoal::tables::{EphemeralSortedTable, EphemeralUnsortedTable};
use shoal_derive::{db, ShoalSortedTable, ShoalUnsortedTable};
use uuid::Uuid;

/// A row whose fields are all written out of line, so serializing one writes in two places
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "GroupDb")]
pub struct FlatRow {
    /// The partition this row lands in
    #[shoal(partition)]
    pub key: u64,
    /// A payload with a length of its own
    #[shoal(update)]
    pub value: String,
}

/// A row in a sorted table, so both query kinds are represented in the response enum
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "GroupDb")]
pub struct TieredRow {
    /// The partition this row lands in
    #[shoal(partition)]
    pub key: u64,
    /// The key this row is sorted by within its partition
    #[shoal(sort)]
    pub sort_key: String,
    /// A payload with a length of its own
    #[shoal(update)]
    pub value: String,
}

/// A schema with one table of each kind
#[db]
pub struct GroupDb {
    /// The unsorted table, where a partition holds one row
    pub flat: EphemeralUnsortedTable<FlatRow>,
    /// The sorted table, where a partition holds many
    pub tiered: EphemeralSortedTable<TieredRow>,
}

/// Wrap an answer in the response a shard would have sent
///
/// # Arguments
///
/// * `data` - The answer to wrap
fn response<T>(data: ResponseAction<T>) -> Response<T> {
    Response {
        // a fixed id, so the two shapes differ in nothing but their rows
        id: Uuid::from_u128(0x5c0a1),
        // an index that is not zero, so one dropped on the way through shows
        index: 3,
        data,
        end: true,
    }
}

/// Build the rows of two partitions, and borrows of the same rows
///
/// # Arguments
///
/// * `rows` - The rows to answer with
fn both_shapes<T>(rows: &[T]) -> (GetRows<T>, GetRows<RowRef<'_, T>>)
where
    T: Clone,
{
    // the same two runs either way, so only the ownership differs
    let split = rows.len() / 2;
    let owned = GetRows::from_slots(vec![
        (11, rows[..split].to_vec()),
        (22, rows[split..].to_vec()),
    ]);
    let borrowed = GetRows::from_slots(vec![
        (11, rows[..split].iter().map(RowRef::new).collect()),
        (22, rows[split..].iter().map(RowRef::new).collect()),
    ]);
    (owned, borrowed)
}

/// The rows the flat table's variants are checked with
fn flat_rows() -> Vec<FlatRow> {
    vec![
        FlatRow {
            key: 11,
            value: "the first".to_owned(),
        },
        FlatRow {
            key: 11,
            value: "the second, which is longer".to_owned(),
        },
        FlatRow {
            key: 22,
            value: "the third".to_owned(),
        },
        FlatRow {
            key: 22,
            value: String::new(),
        },
    ]
}

/// The rows the tiered table's variants are checked with
fn tiered_rows() -> Vec<TieredRow> {
    vec![
        TieredRow {
            key: 11,
            sort_key: "a".to_owned(),
            value: "the first".to_owned(),
        },
        TieredRow {
            key: 11,
            sort_key: "b".to_owned(),
            value: "the second".to_owned(),
        },
        TieredRow {
            key: 22,
            sort_key: "a".to_owned(),
            value: "the third".to_owned(),
        },
        TieredRow {
            key: 22,
            sort_key: "b".to_owned(),
            value: "the fourth".to_owned(),
        },
    ]
}

#[test]
/// Every variant of the response enum archives the same whether its rows are owned or borrowed
///
/// This is the claim the whole borrowed reply path rests on, checked against the generated enums
/// rather than against a stand-in for them. A variant added to one enum and not the other, or
/// added in a different place, renames every variant after it - rkyv writes a variant's position
/// as its discriminant - and this is what would catch that.
fn every_response_kinds_variant_is_byte_identical_to_its_ref_mirror() {
    // the unsorted table's own variant
    let rows = flat_rows();
    let (owned, borrowed) = both_shapes(&rows);
    let owned = GroupDbResponseKinds::FlatRow(response(ResponseAction::Get(Some(owned))));
    let borrowed = GroupDbResponseKindsRef::FlatRow(response(ResponseAction::Get(Some(borrowed))));
    assert_eq!(
        rkyv::to_bytes::<rkyv::rancor::Error>(&owned)
            .unwrap()
            .as_slice(),
        rkyv::to_bytes::<rkyv::rancor::Error>(&borrowed)
            .unwrap()
            .as_slice(),
        "the flat table's variant archived differently when its rows were borrowed"
    );
    // and the sorted table's, which sits at a different discriminant
    let rows = tiered_rows();
    let (owned, borrowed) = both_shapes(&rows);
    let owned = GroupDbResponseKinds::TieredRow(response(ResponseAction::Get(Some(owned))));
    let borrowed = GroupDbResponseKindsRef::TieredRow(response(ResponseAction::Get(Some(borrowed))));
    assert_eq!(
        rkyv::to_bytes::<rkyv::rancor::Error>(&owned)
            .unwrap()
            .as_slice(),
        rkyv::to_bytes::<rkyv::rancor::Error>(&borrowed)
            .unwrap()
            .as_slice(),
        "the tiered table's variant archived differently when its rows were borrowed"
    );
}

#[test]
/// The answers that carry no rows archive the same through either enum too
///
/// A borrowed reply is not only used for gets. Every other answer a shard can give goes through
/// the same serializer, and each of them is a variant whose discriminant has to line up.
fn an_answer_with_no_rows_is_byte_identical_through_either_enum() {
    // each answer that carries no rows, checked through both shapes of the same variant
    let answers: Vec<(ResponseAction<FlatRow>, ResponseAction<RowRef<'_, FlatRow>>)> = vec![
        (ResponseAction::Insert(true), ResponseAction::Insert(true)),
        (ResponseAction::Delete(false), ResponseAction::Delete(false)),
        (ResponseAction::Update(true), ResponseAction::Update(true)),
        (ResponseAction::Exists(true), ResponseAction::Exists(true)),
        (ResponseAction::Get(None), ResponseAction::Get(None)),
        (
            ResponseAction::Error(ResponseError::new(ErrorCode::StorageRead, "no partition 7")),
            ResponseAction::Error(ResponseError::new(ErrorCode::StorageRead, "no partition 7")),
        ),
    ];
    for (owned, borrowed) in answers {
        let owned = GroupDbResponseKinds::FlatRow(response(owned));
        let borrowed = GroupDbResponseKindsRef::FlatRow(response(borrowed));
        assert_eq!(
            rkyv::to_bytes::<rkyv::rancor::Error>(&owned)
                .unwrap()
                .as_slice(),
            rkyv::to_bytes::<rkyv::rancor::Error>(&borrowed)
                .unwrap()
                .as_slice(),
            "an answer carrying no rows archived differently through the borrowed enum"
        );
    }
}

#[test]
/// A borrowed reply is read back by the client as the owned response it is
///
/// Byte identity is half of what a client needs. The other half is that the bytes are reachable
/// through the owned enum's archived type, which is what every caller in the tree already uses.
fn a_borrowed_reply_reads_back_through_the_owned_enum() {
    let rows = flat_rows();
    let (_, borrowed) = both_shapes(&rows);
    let borrowed = GroupDbResponseKindsRef::FlatRow(response(ResponseAction::Get(Some(borrowed))));
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&borrowed).unwrap();
    // read what a shard wrote the way a client reads it
    let archived =
        rkyv::access::<ArchivedGroupDbResponseKinds, rkyv::rancor::Error>(&bytes).unwrap();
    let ArchivedGroupDbResponseKinds::FlatRow(answer) = archived else {
        panic!("a borrowed reply came back as the wrong table");
    };
    let shoal::shared::responses::ArchivedResponseAction::Get(rkyv::option::ArchivedOption::Some(
        found,
    )) = &answer.data
    else {
        panic!("a borrowed reply came back with no rows");
    };
    // every row survived, in order, with its payload
    assert_eq!(found.rows.len(), 4);
    assert_eq!(found.rows[1].value.as_str(), "the second, which is longer");
    // and so did the index naming where they came from
    let partitions: Vec<u64> = found
        .groups
        .iter()
        .map(|group| group.partition.to_native())
        .collect();
    assert_eq!(partitions, vec![11, 22]);
}
