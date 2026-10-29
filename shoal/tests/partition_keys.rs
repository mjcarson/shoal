//! A partition key's hash is a persistence format, and this is what freezes it
//!
//! The hash of a `#[shoal(partition)]` field decides which partition a row is in and which
//! tablet owns it, so a change to how it is computed - a different gxhash major, a seed, a
//! feature that alters the algorithm - silently re-homes every row ever written. Until
//! [item 65](../../docs/src/appendix/resolved/gxhash-pin.md) nothing would have noticed. These
//! literals were obtained by running this test once against the tree before the pin moved, with
//! gxhash resolved at 2.3.1 both before and after, and a build that hashes any of them
//! differently is a build that cannot read an existing directory.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::server::ring::TABLET_BITS;
use shoal::shared::traits::PartitionKeySupport;
use shoal::tables::EphemeralUnsortedTable;
use shoal_derive::{db, ShoalUnsortedTable};

/// A row keyed by an integer
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "KeysDb")]
pub struct ByInt {
    /// The partition key
    #[shoal(partition)]
    pub id: u64,
    /// A payload
    #[shoal(update)]
    pub data: String,
}

/// A row keyed by a string
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "KeysDb")]
pub struct ByText {
    /// The partition key
    #[shoal(partition)]
    pub name: String,
    /// A payload
    #[shoal(update)]
    pub data: String,
}

/// A row keyed by a string and an integer, a composite key of two fields
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "KeysDb")]
pub struct ByPair {
    /// The first field of the partition key
    #[shoal(partition)]
    pub name: String,
    /// The second field of the partition key
    #[shoal(partition)]
    pub id: u64,
    /// A payload
    #[shoal(update)]
    pub data: String,
}

/// A row keyed by three integers, the shape an object store's stripe row is keyed by
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "KeysDb")]
pub struct ByTriple {
    /// The first field of the partition key
    #[shoal(partition)]
    pub consumer: u64,
    /// The second field of the partition key
    #[shoal(partition)]
    pub object: u64,
    /// The third field of the partition key
    #[shoal(partition)]
    pub stripe: u64,
    /// A payload
    #[shoal(update)]
    pub data: String,
}

/// A row keyed by a 128 bit integer, the width of an object id
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "KeysDb")]
pub struct ByWide {
    /// The partition key
    #[shoal(partition)]
    pub object: u128,
    /// A payload
    #[shoal(update)]
    pub data: String,
}

/// A row keyed by a uuid, the type an object's id is minted as
///
/// deepsize2 has no `uuid` feature, so this row sizes itself by hand rather than deriving
/// `DeepSizeOf`. A uuid is sixteen inline bytes with no children, so only the payload counts.
#[derive(Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "KeysDb")]
pub struct ByUuid {
    /// The partition key
    #[shoal(partition)]
    pub object: uuid::Uuid,
    /// A payload
    #[shoal(update)]
    pub data: String,
}

impl DeepSizeOf for ByUuid {
    /// The heap bytes this row holds, which are its payload's alone
    ///
    /// # Arguments
    ///
    /// * `context` - What deepsize2 has already counted
    fn deep_size_of_children(&self, context: &mut deepsize2::Context) -> usize {
        // the uuid is inline, so the payload is the only heap this row owns
        self.data.deep_size_of_children(context)
    }
}

/// The schema holding the six key shapes
#[db]
pub struct KeysDb {
    /// Integer keyed rows
    pub by_int: EphemeralUnsortedTable<ByInt>,
    /// String keyed rows
    pub by_text: EphemeralUnsortedTable<ByText>,
    /// Rows keyed by a string and an integer
    pub by_pair: EphemeralUnsortedTable<ByPair>,
    /// Rows keyed by three integers
    pub by_triple: EphemeralUnsortedTable<ByTriple>,
    /// Rows keyed by a 128 bit integer
    pub by_wide: EphemeralUnsortedTable<ByWide>,
    /// Rows keyed by a uuid
    pub by_uuid: EphemeralUnsortedTable<ByUuid>,
}

/// The tablet a partition key lands in, the way `server::ring` derives it
///
/// # Arguments
///
/// * `partition` - The partition key
fn tablet_of(partition: u64) -> u64 {
    partition >> (u64::BITS - TABLET_BITS)
}

/// A fixed set of keys hash to the values they hashed to when this was frozen
///
/// One key shape per kind the derive can produce: a single integer, a single string, two
/// composite keys - a string and an integer, and three integers - and the two types an object's
/// id could be keyed by, a 128 bit integer and a uuid. The tablet is asserted beside the hash
/// because it is the number routing actually reads, and because a change to `TABLET_BITS` would
/// move rows the same way a hash change would.
///
/// The composite literals were not obtained the way the first eight were. No tree before
/// [Resolved #92, #198](../../docs/src/appendix/resolved/composite-partition-key.md) could
/// produce them, since a table with a composite key did not compile; they were printed by the
/// tree that fixed it, the one [`composite_keys_hash_field_by_field`] holds to a definition
/// written without the derive. The 128 bit and uuid literals were printed by the tree of
/// [Resolved #93](../../docs/src/appendix/resolved/archived-partition-hash.md), which deleted the
/// archived hash and left the live one as it was; a uuid key did not compile before it.
///
/// A row and the key naming it are the only two ways a key is hashed, so these are the only
/// numbers frozen. There was a third, from an archived row, which disagreed for a string and a
/// 128 bit integer and had no caller, and it is gone.
#[test]
fn partition_keys_hash_to_frozen_values() {
    // an integer key, which the derive hashes through `Hash for u64`
    let cases_int: &[(u64, u64, u64)] = &[
        (0, 0x20bf_ea67_2338_0d78, 523),
        (1, 0xd0aa_3509_a2d0_dd24, 3338),
        (42, 0x3036_ffe2_cc25_9d89, 771),
        (u64::MAX, 0x4c9f_04f8_29df_45f7, 1225),
    ];
    for (key, expected, tablet) in cases_int {
        let hash = ByInt::get_partition_key_from_values(key);
        assert_eq!(hash, *expected, "u64 key {key} hashed to {hash:#x}");
        assert_eq!(tablet_of(hash), *tablet, "u64 key {key} moved tablet");
    }
    // a string key, which goes through `Hash for str` and so carries its terminator byte
    let cases_text: &[(&str, u64, u64)] = &[
        ("", 0x57b5_8cc6_750e_034c, 1403),
        ("a", 0x5d74_a57f_5e73_eb79, 1495),
        ("shoal", 0x7498_3e29_46f5_ce66, 1865),
        (
            "the quick brown fox jumps over the lazy dog",
            0x8239_6b81_d80f_bddd,
            2083,
        ),
    ];
    for (key, expected, tablet) in cases_text {
        let hash = ByText::get_partition_key_from_values(&(*key).to_string());
        assert_eq!(hash, *expected, "string key {key:?} hashed to {hash:#x}");
        assert_eq!(tablet_of(hash), *tablet, "string key {key:?} moved tablet");
    }
    // a string and an integer, which hash one after the other through their own `Hash`
    let cases_pair: &[(&str, u64, u64, u64)] = &[
        ("", 0, 0x1065_251c_5cfe_765f, 262),
        ("a", 1, 0xe778_6ffd_7668_3c4e, 3703),
        ("shoal", 42, 0x182e_f82c_3530_71b0, 386),
        ("warehouse", u64::MAX, 0x3503_8999_9857_8873, 848),
    ];
    for (name, id, expected, tablet) in cases_pair {
        let hash = ByPair::get_partition_key_from_values(&((*name).to_string(), *id));
        assert_eq!(hash, *expected, "key ({name:?}, {id}) hashed to {hash:#x}");
        assert_eq!(
            tablet_of(hash),
            *tablet,
            "key ({name:?}, {id}) moved tablet"
        );
    }
    // three integers, the shape an object store's stripe row is keyed by
    let cases_triple: &[((u64, u64, u64), u64, u64)] = &[
        ((0, 0, 0), 0xb3d8_d8b3_628f_af2a, 2877),
        ((1, 2, 3), 0x4591_5a46_2242_1929, 1113),
        ((3, 2, 1), 0xb64e_0e65_c1f1_72fc, 2916),
        ((7, u64::MAX, 9), 0x9d7c_62c4_c416_ee37, 2519),
    ];
    for (key, expected, tablet) in cases_triple {
        let hash = ByTriple::get_partition_key_from_values(key);
        assert_eq!(hash, *expected, "key {key:?} hashed to {hash:#x}");
        assert_eq!(tablet_of(hash), *tablet, "key {key:?} moved tablet");
    }
    // a 128 bit integer, through `Hash for u128`. gxhash 2.3 loads a `u64` into both halves of
    // its 128 bit lane, so 0 and `u128::MAX` hash as `u64` 0 and `u64::MAX` do above. That is
    // two tables' keys sharing a number, and it puts no two rows together: a partition belongs
    // to one table
    let cases_wide: &[(u128, u64, u64)] = &[
        (0, 0x20bf_ea67_2338_0d78, 523),
        (1, 0x92b0_88da_3291_bfbe, 2347),
        (u64::MAX as u128 + 1, 0xac4d_ab10_8a44_7644, 2756),
        (u128::MAX, 0x4c9f_04f8_29df_45f7, 1225),
    ];
    for (key, expected, tablet) in cases_wide {
        let hash = ByWide::get_partition_key_from_values(key);
        assert_eq!(hash, *expected, "u128 key {key:#x} hashed to {hash:#x}");
        assert_eq!(tablet_of(hash), *tablet, "u128 key {key:#x} moved tablet");
        // a row holding the key hashes the same, as a single field key always has
        let row = ByWide {
            object: *key,
            data: String::new(),
        };
        assert_eq!(row.get_partition_key(), hash, "u128 row {key:#x}");
    }
    // a uuid, through `Hash for Uuid`, which hashes its bytes and not the integer they spell:
    // the uuid and the `u128` of the same bits are different partitions
    let cases_uuid: &[(u128, u64, u64)] = &[
        (0, 0xce43_5071_e6d0_9e78, 3300),
        (u128::MAX, 0x77c9_62de_bd21_8aa7, 1916),
        (
            0x0192_6f3a_8b7c_7d4e_9f10_1234_5678_9abc,
            0x7d14_2112_8335_8bde,
            2001,
        ),
    ];
    for (bits, expected, tablet) in cases_uuid {
        let key = uuid::Uuid::from_u128(*bits);
        let hash = ByUuid::get_partition_key_from_values(&key);
        assert_eq!(hash, *expected, "uuid key {key} hashed to {hash:#x}");
        assert_eq!(tablet_of(hash), *tablet, "uuid key {key} moved tablet");
        // a row holding the key hashes the same, as a single field key always has
        let row = ByUuid {
            object: key,
            data: String::new(),
        };
        assert_eq!(row.get_partition_key(), hash, "uuid row {key}");
        // and the same bits as an integer are another partition
        assert_ne!(
            ByWide::get_partition_key_from_values(bits),
            hash,
            "uuid {key}"
        );
    }
}

/// Hash a list of values the way a composite key is defined to be hashed: one after another
///
/// This is the definition the derive is held to, written without it: a fresh `GxHasher` fed
/// each field's own `Hash` in declaration order, with nothing between them.
///
/// # Arguments
///
/// * `fields` - The key's fields, in declaration order
fn hash_in_order(fields: &[&dyn Fn(&mut shoal::gxhash::GxHasher)]) -> u64 {
    use std::hash::Hasher;
    // one hasher for the whole key, as the derive builds
    let mut hasher = shoal::gxhash::GxHasher::default();
    // each field writes itself in turn
    for field in fields {
        field(&mut hasher);
    }
    hasher.finish()
}

/// A composite key's row, its values and the definition all hash to the same `u64`
///
/// A row is hashed by `get_partition_key` when it is inserted and a key by
/// `get_partition_key_from_values` when it is read, updated or deleted, so the two disagreeing
/// would put every read in a partition no insert wrote. Both are checked against
/// [`hash_in_order`] rather than only against each other, so a change that moved both the same
/// way is still caught.
#[test]
fn composite_keys_hash_field_by_field() {
    use std::hash::Hash;
    // a string and an integer, which also checks the string's terminator byte is written
    for (name, id) in [("", 0u64), ("a", 1), ("shoal", 42), ("warehouse", u64::MAX)] {
        let row = ByPair {
            name: name.to_owned(),
            id,
            data: String::new(),
        };
        // the definition: the string's `Hash` and then the integer's
        let defined = hash_in_order(&[&|hasher| name.hash(hasher), &|hasher| id.hash(hasher)]);
        // the insert path and the query path agree with it
        assert_eq!(row.get_partition_key(), defined, "row ({name:?}, {id})");
        assert_eq!(
            ByPair::get_partition_key_from_values(&(name.to_owned(), id)),
            defined,
            "values ({name:?}, {id})"
        );
    }
    // three integers, the stripe row's shape
    for (consumer, object, stripe) in [(0u64, 0u64, 0u64), (1, 2, 3), (3, 2, 1), (7, u64::MAX, 9)] {
        let row = ByTriple {
            consumer,
            object,
            stripe,
            data: String::new(),
        };
        // the definition: each integer's `Hash` in declaration order
        let defined = hash_in_order(&[
            &|hasher| consumer.hash(hasher),
            &|hasher| object.hash(hasher),
            &|hasher| stripe.hash(hasher),
        ]);
        // the insert path and the query path agree with it
        assert_eq!(
            row.get_partition_key(),
            defined,
            "row {consumer}/{object}/{stripe}"
        );
        assert_eq!(
            ByTriple::get_partition_key_from_values(&(consumer, object, stripe)),
            defined,
            "values {consumer}/{object}/{stripe}"
        );
    }
    // the order of the fields is part of the key: the same values swapped are another partition
    assert_ne!(
        ByTriple::get_partition_key_from_values(&(1, 2, 3)),
        ByTriple::get_partition_key_from_values(&(3, 2, 1)),
    );
}
