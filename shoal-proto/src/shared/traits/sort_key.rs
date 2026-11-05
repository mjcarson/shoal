//! The types a sorted table may order its rows by
//!
//! A sorted table keeps a partition's rows in a tree by their sort key. Resident, that tree is a
//! `BTreeMap` searched in the key's own `Ord`; read from disk, it is the same tree archived, which
//! rkyv wrote out in that order and which a get, an exists and a walk search in the *archived*
//! key's `Ord`, never deserializing the keys they pass. So a sort key's two orders have to be one
//! order, or an archived seek misses a row a resident one finds
//! ([Resolved #207, #42](../../../../docs/src/appendix/resolved/sort-keys-of-any-type.md)).
//!
//! [`SortKey`] is the promise that they are. It is implemented here for every type whose archived
//! form provably orders as it does: the integers, which rend compares by their native values;
//! `bool` and `char`; `String`, whose archived form orders as `str`; `Uuid`, which archives as
//! itself; `Option` of a key, whose archived form puts `None` first as `Option` does; and tuples of
//! keys, whose archived forms compare field by field. A schema's own key type opts in by
//! implementing it, and holds itself to the promise with [`check_sort_key_order`].
//!
//! `usize` and `isize` are left out on purpose: rkyv archives them as 32 bits unless told
//! otherwise, so a key past four billion would not archive at all. Floats are left out by not
//! being `Ord`.

use std::fmt::Debug;
use std::hash::Hash;

use rkyv::de::Pool;
use rkyv::rancor::{Error, Strategy};
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use rkyv::validation::Validator;
use uuid::Uuid;

use super::RkyvSupport;

/// A type a sorted table may order its rows by
///
/// Its bounds are checked where it is implemented, not deep inside the engine: its archived form
/// has to be ordered, printable, validated before it is read - every key a client sends is - and
/// decodable back into the key.
///
/// # The promise an impl makes
///
/// **The archived form orders exactly as the type does.** Every archived seek and range compares
/// archived keys with each other, so an impl whose archived `Ord` disagrees with its own answers a
/// get of an archived partition with the wrong rows and in the wrong order, silently. A derived
/// `Ord` on both sides, over the same fields in the same order, keeps it; a hand-written `Ord` on
/// either side is the way to break it. A schema implementing this for a type of its own should run
/// [`check_sort_key_order`] over that type's edge values in a test.
pub trait SortKey:
    RkyvSupport<
        Archived: Ord
                      + Debug
                      + for<'a> rkyv::bytecheck::CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, Error>,
        > + rkyv::Deserialize<Self, Strategy<Pool, Error>>,
    > + Ord
    + Hash
    + Debug
    + Clone
{
    /// What this key holds on the heap beyond the slot its tree node gives it
    ///
    /// A partition charges every key it holds to the node's memory budget, so a key that holds a
    /// heap block and reports none lets a node grow past its budget unseen
    /// ([Resolved #196](../../../../docs/src/appendix/resolved/row-charge-undercount.md)). That is
    /// why there is no default: an impl says what it holds, even when it holds nothing.
    fn heap_bytes(&self) -> usize;
}

/// Implement [`RkyvSupport`] and [`SortKey`] for every type a sort key may be that holds no heap
///
/// Each of these archives to a form that orders as it does: rend's integers and `char` compare by
/// their native values, `bool` and the bytes archive as themselves, and `Uuid` archives as itself.
macro_rules! sort_keys_without_heap {
    ($($ty:ty),* $(,)?) => {
        $(
            impl RkyvSupport for $ty {}

            impl SortKey for $ty {
                /// A key of this type is held inline in its slot and owns nothing on the heap
                #[inline]
                fn heap_bytes(&self) -> usize {
                    0
                }
            }
        )*
    };
}

sort_keys_without_heap!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, bool, char, Uuid);

impl SortKey for String {
    /// A string owns the block its characters are in, which is its capacity, not its length
    ///
    /// This is the figure deepsize2 counts for it, so a partition's charge for a string key did
    /// not move when the bound changed from `DeepSizeOf` to this.
    #[inline]
    fn heap_bytes(&self) -> usize {
        self.capacity()
    }
}

impl<T: RkyvSupport> RkyvSupport for Option<T> {}

impl<T: SortKey> SortKey for Option<T> {
    /// An absent key holds nothing, and a present one holds what its key holds
    #[inline]
    fn heap_bytes(&self) -> usize {
        // only a key that is there can hold anything
        self.as_ref().map_or(0, SortKey::heap_bytes)
    }
}

/// Implement [`RkyvSupport`] and [`SortKey`] for tuples of keys, two to twelve wide
///
/// Several `#[shoal(sort)]` fields make a tuple, in declaration order. rkyv's archived tuples
/// derive their `Ord` field by field, as std's tuples do, so a tuple of keys orders the way it
/// archives whenever every member does. Twelve is the widest tuple std orders.
macro_rules! sort_key_tuples {
    ($(($($member:ident $index:tt),+)),* $(,)?) => {
        $(
            impl<$($member: RkyvSupport),+> RkyvSupport for ($($member,)+) {}

            impl<$($member: SortKey),+> SortKey for ($($member,)+) {
                /// A tuple holds whatever each of its members holds
                #[inline]
                fn heap_bytes(&self) -> usize {
                    0 $(+ self.$index.heap_bytes())+
                }
            }
        )*
    };
}

sort_key_tuples!(
    (A 0, B 1),
    (A 0, B 1, C 2),
    (A 0, B 1, C 2, D 3),
    (A 0, B 1, C 2, D 3, E 4),
    (A 0, B 1, C 2, D 3, E 4, F 5),
    (A 0, B 1, C 2, D 3, E 4, F 5, G 6),
    (A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7),
    (A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8),
    (A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9),
    (A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10),
    (A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10, L 11),
);

/// Check that a key type's archived form orders its values exactly as the type does
///
/// The values are sorted by the type's own `Ord`, each is archived, and every adjacent pair is
/// compared again in archived form: the two orders agree on the whole set if and only if they
/// agree on every adjacent pair, since both are total orders. Each archived value is decoded too,
/// and has to come back as itself. This is what a schema runs, in a test, over the edge values of
/// a key type of its own that implements [`SortKey`].
///
/// # Arguments
///
/// * `values` - The values to check, in any order, repeats allowed
///
/// # Errors
///
/// Names the first value that would not archive or decode, or the first pair whose two orders
/// disagree.
///
/// # Examples
///
/// ```
/// use shoal_proto::shared::traits::check_sort_key_order;
///
/// check_sort_key_order(&[10u64, 2, u64::MAX, 0]).expect("an integer orders as it archives");
/// check_sort_key_order(&[(2u32, "b".to_string()), (10, "a".to_string())]).unwrap();
/// ```
pub fn check_sort_key_order<K: SortKey>(values: &[K]) -> Result<(), String> {
    // put the values in the order the type itself gives them
    let mut sorted = values.to_vec();
    sorted.sort();
    // archive each of them, as a partition's tree archives its keys
    let mut archived = Vec::with_capacity(sorted.len());
    for value in &sorted {
        let bytes = RkyvSupport::serialize(value)
            .map_err(|error| format!("{value:?} would not archive: {error}"))?;
        archived.push(bytes);
    }
    // every archived value is read back as an archive and decoded into itself
    for (value, bytes) in sorted.iter().zip(&archived) {
        let access =
            K::access(bytes).map_err(|error| format!("{value:?} failed validation: {error}"))?;
        let decoded = K::deserialize(access)
            .map_err(|error| format!("{value:?} would not decode: {error}"))?;
        if decoded != *value {
            return Err(format!("{value:?} decoded as {decoded:?}"));
        }
    }
    // and every adjacent pair compares the same way in both forms
    for (pair, bytes) in sorted.windows(2).zip(archived.windows(2)) {
        // how the type orders the pair
        let native = pair[0].cmp(&pair[1]);
        // how their archived forms order, read where they lie
        let left = K::access(&bytes[0])
            .map_err(|error| format!("{:?} failed validation: {error}", pair[0]))?;
        let right = K::access(&bytes[1])
            .map_err(|error| format!("{:?} failed validation: {error}", pair[1]))?;
        let archived = left.cmp(right);
        if native != archived {
            return Err(format!(
                "{:?} and {:?} order {native:?} but their archived forms order {archived:?}",
                pair[0], pair[1]
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{check_sort_key_order, SortKey};
    use crate::shared::traits::RkyvSupport;
    use deepsize2::DeepSizeOf;
    use rand::distributions::{Distribution, Standard};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use rkyv::{Archive, Deserialize, Serialize};
    use std::cmp::Ordering;
    use uuid::Uuid;

    /// The seed every generated set is drawn from, so a failure names values a rerun finds again
    const SEED: u64 = 0x5eed_0207;

    /// How many generated values each type is checked over, beside its edges
    const GENERATED: usize = 2_000;

    /// Any value of a type, drawn from a seeded generator by the type's standard distribution
    ///
    /// # Arguments
    ///
    /// * `rng` - The seeded generator to draw from
    fn any<T>(rng: &mut StdRng) -> T
    where
        Standard: Distribution<T>,
    {
        rng.sample(Standard)
    }

    /// Draw a seeded set of values beside a type's edges and check the type orders as it archives
    ///
    /// # Arguments
    ///
    /// * `edges` - The values a mistake is most likely to show at
    /// * `draw` - A value drawn from a seeded generator
    fn holds<K: SortKey>(edges: &[K], mut draw: impl FnMut(&mut StdRng) -> K) {
        // the edges, then a seeded set of values between them
        let mut rng = StdRng::seed_from_u64(SEED);
        let mut values = edges.to_vec();
        values.extend((0..GENERATED).map(|_| draw(&mut rng)));
        check_sort_key_order(&values).expect("a key type orders as it archives");
    }

    /// Every integer width orders as it archives, its extremes and its sign change among them
    #[test]
    fn integers_order_as_they_archive() {
        // the unsigned widths, whose decimal strings would put 10 before 2
        holds(&[0u8, 1, 2, 10, u8::MAX], |rng| any(rng));
        holds(&[0u16, 2, 10, 256, u16::MAX], |rng| any(rng));
        holds(&[0u32, 2, 10, 1 << 24, u32::MAX], |rng| any(rng));
        holds(&[0u64, 2, 10, 1 << 56, u64::MAX], |rng| any(rng));
        holds(&[0u128, 2, 10, 1 << 120, u128::MAX], |rng| any(rng));
        // the signed widths, whose archived bytes would put every negative after every positive
        holds(&[i8::MIN, -1, 0, 1, i8::MAX], |rng| any(rng));
        holds(&[i16::MIN, -1, 0, 1, i16::MAX], |rng| any(rng));
        holds(&[i32::MIN, -1, 0, 1, i32::MAX], |rng| any(rng));
        holds(&[i64::MIN, -5, -1, 0, 3, i64::MAX], |rng| any(rng));
        holds(&[i128::MIN, -1, 0, 1, i128::MAX], |rng| any(rng));
    }

    /// `bool`, `char`, strings and uuids order as they archive
    #[test]
    fn scalars_strings_and_uuids_order_as_they_archive() {
        // both booleans
        holds(&[false, true], |rng| any(rng));
        // characters either side of every encoding width
        holds(
            &[
                '\0',
                'a',
                '\u{7f}',
                '\u{80}',
                '\u{7ff}',
                '\u{800}',
                '\u{ffff}',
                '\u{10000}',
                char::MAX,
            ],
            |rng| any(rng),
        );
        // strings, empty, prefixes of each other, and characters of every width
        let strings = ["", "a", "aa", "ab", "b", "é", "z\u{10000}", "10", "2"].map(String::from);
        holds(&strings, |rng| {
            let len = rng.gen_range(0..12);
            (0..len).map(|_| any::<char>(rng)).collect()
        });
        // uuids, which order by their bytes, big end first
        let uuids = [
            Uuid::nil(),
            Uuid::from_u128(1),
            Uuid::from_u128(u128::MAX),
            Uuid::now_v7(),
        ];
        holds(&uuids, |rng| Uuid::from_u128(any(rng)));
    }

    /// An optional key puts its absence first and orders its presence as its key does
    #[test]
    fn optional_keys_order_as_they_archive() {
        // absent, then present at both edges
        holds(
            &[None, Some(0u32), Some(2), Some(10), Some(u32::MAX)],
            |rng| rng.gen_bool(0.1).then(|| any(rng)),
        );
        // and a heap key that may be absent
        holds(&[None, Some(String::new()), Some("a".to_string())], |rng| {
            rng.gen_bool(0.5)
                .then(|| rng.gen_range(0..1_000u32).to_string())
        });
    }

    /// A tuple of keys orders field by field, as its archived form does, at every width
    #[test]
    fn tuples_order_as_they_archive() {
        // a number then a string, the composite key whose strings would order 10 before 2
        let pairs = [(1u64, "b"), (2, "a"), (2, "b"), (10, "a"), (10, "c")]
            .map(|(seq, name)| (seq, name.to_string()));
        holds(&pairs, |rng| {
            (rng.gen_range(0..20u64), rng.gen_range(0..5u8).to_string())
        });
        // a signed key and an optional one, small ranges so many tuples tie on their first field
        holds(
            &[(i32::MIN, None), (-1, Some(0u8)), (0, None), (0, Some(1))],
            |rng| {
                (
                    rng.gen_range(-3..3i32),
                    rng.gen_bool(0.5).then(|| rng.gen_range(0..3u8)),
                )
            },
        );
        // three and twelve wide, every member a different type at the narrow end
        holds(
            &[(0u8, false, 'a'), (0, true, 'a'), (1, false, '\0')],
            |rng| (rng.gen_range(0..3u8), any(rng), any(rng)),
        );
        holds(
            &[(0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 1u8)],
            |rng| {
                let mut field = || rng.gen_range(0..2u8);
                (
                    field(),
                    field(),
                    field(),
                    field(),
                    field(),
                    field(),
                    field(),
                    field(),
                    field(),
                    field(),
                    field(),
                    field(),
                )
            },
        );
    }

    /// A key type whose own order disagrees with its archived form's is reported by name
    ///
    /// The check is only worth running if it can fail, so this is a type built to fail it: its
    /// archived form derives its order, while the type itself orders backwards.
    #[test]
    fn a_key_whose_orders_disagree_is_reported() {
        /// A number that orders backwards, whose archived form orders forwards
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Archive, Serialize, Deserialize)]
        #[rkyv(derive(Debug, PartialEq, Eq, PartialOrd, Ord))]
        struct Backwards(u32);

        impl PartialOrd for Backwards {
            /// The reverse of the number's order, which its archived form does not share
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        impl Ord for Backwards {
            /// The reverse of the number's order, which its archived form does not share
            fn cmp(&self, other: &Self) -> Ordering {
                other.0.cmp(&self.0)
            }
        }

        impl RkyvSupport for Backwards {}

        impl SortKey for Backwards {
            /// A number holds nothing on the heap
            fn heap_bytes(&self) -> usize {
                0
            }
        }

        // a pair is enough to disagree, and the report names both of them
        let refused =
            check_sort_key_order(&[Backwards(1), Backwards(2)]).expect_err("the orders disagree");
        assert!(
            refused.contains("Backwards(2)") && refused.contains("Backwards(1)"),
            "reported as {refused}"
        );
        // while one value alone, or values that are all equal, agree with anything
        check_sort_key_order(&[Backwards(1), Backwards(1)]).expect("equal keys agree");
    }

    /// A key's heap is what deepsize2 counts for it, so no partition's charge moved with the bound
    #[test]
    fn a_keys_heap_is_what_deepsize_counts() {
        // a string's capacity, whatever its length
        let mut grown = String::with_capacity(64);
        grown.push_str("short");
        for key in [
            String::new(),
            "a".to_string(),
            "a longer key than the last".to_string(),
            grown,
        ] {
            // the figure a partition charged before: the whole deep size less the slot's
            let charged = key.deep_size_of() - std::mem::size_of::<String>();
            assert_eq!(key.heap_bytes(), charged, "{key:?}");
        }
        // an inline key holds nothing, and a tuple holds what its members hold
        assert_eq!(7u64.heap_bytes(), 0);
        assert_eq!(Uuid::now_v7().heap_bytes(), 0);
        let pair = (3u64, "twelve bytes".to_string());
        assert_eq!(pair.heap_bytes(), pair.1.capacity());
        let triple = ("a".to_string(), Some("bc".to_string()), None::<String>);
        assert_eq!(triple.heap_bytes(), triple.0.capacity() + 2);
    }
}
