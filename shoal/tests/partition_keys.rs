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

/// The schema holding the three key shapes
#[db]
pub struct KeysDb {
    /// Integer keyed rows
    pub by_int: EphemeralUnsortedTable<ByInt>,
    /// String keyed rows
    pub by_text: EphemeralUnsortedTable<ByText>,
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
/// One key shape per kind the derive can produce today: a single integer and a single string. A
/// composite key is not here because a table declaring one does not compile
/// ([item 92](../../docs/src/appendix/known-issues.md)), which this test found. The tablet is
/// asserted beside the hash because it is the number routing actually reads, and because a
/// change to `TABLET_BITS` would move rows the same way a hash change would.
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
        ("the quick brown fox jumps over the lazy dog", 0x8239_6b81_d80f_bddd, 2083),
    ];
    for (key, expected, tablet) in cases_text {
        let hash = ByText::get_partition_key_from_values(&(*key).to_string());
        assert_eq!(hash, *expected, "string key {key:?} hashed to {hash:#x}");
        assert_eq!(tablet_of(hash), *tablet, "string key {key:?} moved tablet");
    }
}
