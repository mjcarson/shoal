//! A row that is answered with where it already is, rather than copied to be answered with
//!
//! A get finds its rows in a partition the shard already holds, and then had to clone every one
//! of them into an owned `Vec` purely so that the response type had something to own — after
//! which they were serialized straight back into bytes and the clones dropped. That is
//! [O2](../../../docs/src/appendix/optimizations.md), and the whole of what stands between the
//! rows and the wire is the response needing to own them.
//!
//! [`RowRef`] removes that requirement without changing a single byte of the wire format, and the
//! reason it can is worth stating plainly, because it is the property everything else here rests
//! on: **its archived type is not a copy of the row's archived type, it *is* the row's archived
//! type.** `<RowRef<'_, T> as Archive>::Archived` resolves to `<T as Archive>::Archived`, so a
//! `Vec<RowRef<'_, T>>` archives to the identical `ArchivedVec<Archived<T>>` a `Vec<T>` does, in
//! an identical position, through the identical resolver. Byte identity is a consequence of the
//! type definitions rather than a property somebody has to keep true.
//!
//! A row read off disk is not resident and there is no `T` to point at — what the partition holds
//! is `Archived<T>`. That row is answered through the second variant here, which carries the
//! archived row and writes it back out through [`Rearchive`](super::rearchive::Rearchive). Both
//! variants archive to the same type for the same reason, so a get that named one resident
//! partition and one archived one writes a single response neither half can be told from.
//!
//! The alternative was `#[rkyv(with = Map<Inline>)]` on a mirror struct. That produces a
//! *structurally similar but distinct* archived type, which is a much weaker guarantee — one that
//! could only ever be asserted empirically, and that a field reordered on one of the two shapes
//! would break silently.
use rkyv::{
    rancor::{Fallible, Source},
    ser::{Allocator, Writer},
    Archive, Place, Serialize,
};

use super::rearchive::Rearchive;

/// A row that is still where the table put it, archived as though it were owned
///
/// Serializing one of these writes exactly what serializing the row it points at would have
/// written. It exists so that a response can be built out of rows a partition still holds
/// instead of out of copies of them.
///
/// There is deliberately no `Deserialize`. A `RowRef` cannot be read back into, because there is
/// nothing for the borrow to point at on the far side — the wire carries the row, and the client
/// reads it as the row's own archived type, which is what it always was.
pub enum RowRef<'a, T: Rearchive> {
    /// A row the partition holds, which is answered where it lies
    Resident(&'a T),
    /// A row that is still in the archive it was read from, which is written back out of it
    InArchive(&'a <T as Archive>::Archived),
}

impl<'a, T: Rearchive> RowRef<'a, T> {
    /// Point at a resident row without copying it
    ///
    /// # Arguments
    ///
    /// * `row` - The row to answer with, wherever it currently lives
    #[must_use]
    pub fn new(row: &'a T) -> Self {
        RowRef::Resident(row)
    }

    /// Point at a row that is still in the archive it was read from
    ///
    /// # Arguments
    ///
    /// * `archived` - The archived row to answer with, wherever it currently lies
    #[must_use]
    pub fn archived(archived: &'a <T as Archive>::Archived) -> Self {
        RowRef::InArchive(archived)
    }
}

impl<T: Rearchive> std::fmt::Debug for RowRef<'_, T> {
    /// Say which of the two forms a row is in, without requiring either to be printable
    ///
    /// Deriving this would put a `Debug` bound on the row's *archived* type, which a schema is
    /// not otherwise required to have, so the generated response enum would stop compiling for a
    /// row that did not ask rkyv for one.
    ///
    /// # Arguments
    ///
    /// * `formatter` - The formatter to write this row's form into
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RowRef::Resident(_) => formatter.write_str("RowRef::Resident(..)"),
            RowRef::InArchive(_) => formatter.write_str("RowRef::InArchive(..)"),
        }
    }
}

impl<T: Rearchive> Clone for RowRef<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: Rearchive> Copy for RowRef<'_, T> {}

/// What serializing a row produced, whichever of the two forms it was serialized from
///
/// The two halves cannot share a resolver type: a resident row produces rkyv's own resolver for
/// its type, and an archived one produces the mirror's, which carries anything that had to be
/// materialized on the way. Both resolve into the same archived type, which is the only thing the
/// bytes depend on.
pub enum RowRefResolver<T: Rearchive> {
    /// What serializing a resident row produced
    Resident(<T as Archive>::Resolver),
    /// What writing an archived row back out produced
    InArchive(<T as Rearchive>::ArchivedResolver),
}

impl<T: Rearchive> Archive for RowRef<'_, T> {
    /// The row's own archived type, not a second type shaped like it
    ///
    /// This one line is the whole guarantee. Everything downstream — that a borrowed response
    /// is byte identical to an owned one, that `FromShoal::retrieve` reads back what the server
    /// wrote, that no wire version has to move for this — follows from the two sides naming the
    /// same type rather than two types that happen to agree.
    type Archived = <T as Archive>::Archived;

    /// Whichever of the two resolvers the row this points at produced
    type Resolver = RowRefResolver<T>;

    /// Write the archived row, exactly as the row itself would have written it
    ///
    /// # Arguments
    ///
    /// * `resolver` - What serializing the row produced
    /// * `out` - Where the archived row belongs
    fn resolve(&self, resolver: Self::Resolver, out: Place<Self::Archived>) {
        match (self, resolver) {
            // the row resolves itself - this type adds nothing to the bytes and must not
            (RowRef::Resident(row), RowRefResolver::Resident(resolver)) => row.resolve(resolver, out),
            // and an archived row is written back out of the archive it was read from
            (RowRef::InArchive(archived), RowRefResolver::InArchive(resolver)) => {
                T::resolve_archived(archived, resolver, out);
            }
            // the two are produced together by `serialize`, so neither pairing below can arise
            (RowRef::Resident(_), RowRefResolver::InArchive(_))
            | (RowRef::InArchive(_), RowRefResolver::Resident(_)) => {
                unreachable!("a row was resolved with the other form's resolver")
            }
        }
    }
}

impl<T, S> Serialize<S> for RowRef<'_, T>
where
    T: Rearchive + Serialize<S>,
    S: Fallible + Writer + Allocator + ?Sized,
    S::Error: Source,
{
    /// Serialize the row this points at, writing whatever the row would have written
    ///
    /// # Arguments
    ///
    /// * `serializer` - The serializer to write this row's out of line data into
    fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        match self {
            // forward to the row, so its out of line data lands where it always did
            RowRef::Resident(row) => Ok(RowRefResolver::Resident(row.serialize(serializer)?)),
            // or copy the out of line data straight across from the archive holding it
            RowRef::InArchive(archived) => Ok(RowRefResolver::InArchive(
                T::serialize_archived(archived, serializer)?,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Place, RowRef};
    use crate::shared::rearchive::Rearchive;
    use rkyv::{
        rancor::{Error, Fallible, Source},
        ser::{Allocator, Writer},
        vec::ArchivedVec,
        Archive, Deserialize, Serialize,
    };

    /// Write the mirror `shoal-derive` generates for a row, for a row written by hand here
    ///
    /// These three test rows are not schema tables and no derive runs over them, so the impl the
    /// generated code would carry is written out once as a macro. It is field for field what
    /// `shoal-derive` emits, which is the point: if the shape below stops being writable, so does
    /// the generated one.
    macro_rules! mirror {
        ($row:ty, $archived:ident, $resolver:ident, $($field:ident: $ty:ty),+ $(,)?) => {
            /// What each field of the row produced on its way out of the archive
            struct $resolver {
                $(
                    /// What this field produced
                    $field: <$ty as Rearchive>::ArchivedResolver,
                )+
            }

            impl Rearchive for $row {
                type ArchivedResolver = $resolver;

                fn serialize_archived<S>(
                    archived: &Self::Archived,
                    serializer: &mut S,
                ) -> Result<Self::ArchivedResolver, S::Error>
                where
                    S: Fallible + Writer + Allocator + ?Sized,
                    S::Error: Source,
                {
                    Ok($resolver {
                        $(
                            $field: <$ty as Rearchive>::serialize_archived(
                                &archived.$field,
                                serializer,
                            )?,
                        )+
                    })
                }

                fn resolve_archived(
                    archived: &Self::Archived,
                    resolver: Self::ArchivedResolver,
                    out: Place<Self::Archived>,
                ) {
                    rkyv::munge::munge!(let $archived { $($field),+ } = out);
                    $(
                        <$ty as Rearchive>::resolve_archived(
                            &archived.$field,
                            resolver.$field,
                            $field,
                        );
                    )+
                }
            }
        };
    }

    /// A row whose fields all live out of line, so serializing it writes in two places
    #[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq)]
    struct Strings {
        /// A field whose bytes are written before the struct that names them
        title: String,
        /// A second one, so their relative order is observable
        overview: String,
    }

    /// A row carrying a collection, which archives as a relative pointer and a length
    #[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq)]
    struct Collections {
        /// A key, so the struct is not made only of pointers
        id: u64,
        /// A field that is itself a run of out of line data
        keywords: Vec<String>,
    }

    /// A row of nothing but scalars, which is the shape the two serializers disagree on
    ///
    /// rkyv enables a `memcpy` for a type it can prove has no padding, and takes that branch in
    /// `serialize_from_slice` but never in `serialize_from_iter`. So this row is the one that
    /// exercises *both* code paths against each other rather than the same path twice, and it is
    /// the only reason this test needs three types instead of one.
    #[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq)]
    struct Scalars {
        /// A key
        id: u64,
        /// A second scalar of the same width, so the struct has no padding to preserve
        runtime: u64,
    }

    mirror!(Strings, ArchivedStrings, StringsMirror, title: String, overview: String);
    mirror!(Collections, ArchivedCollections, CollectionsMirror, id: u64, keywords: Vec<String>);
    mirror!(Scalars, ArchivedScalars, ScalarsMirror, id: u64, runtime: u64);

    #[test]
    /// Serializing borrowed rows writes the same bytes as serializing owned ones
    ///
    /// This is the property the whole optimization rests on: `RowRef`'s archived type is the
    /// row's archived type, so a response built out of borrows is not merely compatible with one
    /// built out of owned rows, it is identical. If this ever fails, the wire format has silently
    /// forked in two and the client is reading one of them with the other's layout.
    fn a_borrowed_row_is_byte_identical_to_an_owned_one() {
        // the three shapes, each of which reaches a different part of rkyv's writer
        let strings = vec![
            Strings {
                title: "Arrival".to_owned(),
                overview: "Linguists meet a heptapod".to_owned(),
            },
            Strings {
                title: "Primer".to_owned(),
                overview: "Two engineers build a box".to_owned(),
            },
        ];
        let collections = vec![
            Collections {
                id: 7,
                keywords: vec!["first contact".to_owned(), "linguistics".to_owned()],
            },
            Collections {
                id: 9,
                keywords: Vec::new(),
            },
        ];
        let scalars = vec![
            Scalars { id: 7, runtime: 116 },
            Scalars { id: 9, runtime: 77 },
        ];
        // check each shape by serializing the rows and then borrows of the same rows
        assert_eq!(
            rkyv::to_bytes::<Error>(&strings).unwrap().as_slice(),
            rkyv::to_bytes::<Error>(&borrow(&strings)).unwrap().as_slice(),
            "a row of out of line fields archived differently when it was borrowed"
        );
        assert_eq!(
            rkyv::to_bytes::<Error>(&collections).unwrap().as_slice(),
            rkyv::to_bytes::<Error>(&borrow(&collections))
                .unwrap()
                .as_slice(),
            "a row carrying a collection archived differently when it was borrowed"
        );
        assert_eq!(
            rkyv::to_bytes::<Error>(&scalars).unwrap().as_slice(),
            rkyv::to_bytes::<Error>(&borrow(&scalars)).unwrap().as_slice(),
            "a row rkyv can memcpy archived differently when it was borrowed"
        );
    }

    #[test]
    /// The scalar row really does take two different writers, which is what makes the test above
    /// a comparison rather than a tautology
    ///
    /// `a_borrowed_row_is_byte_identical_to_an_owned_one` is only worth anything if the two sides
    /// reach rkyv's writer by different routes. A `Vec<T>` serializes through
    /// `serialize_from_slice`, which `memcpy`s a type it has proved has no padding; a
    /// `Vec<RowRef<'_, T>>` cannot take that branch, because `RowRef` leaves the optimization at
    /// its default. If rkyv ever stopped enabling it for `Scalars`, both sides would quietly walk
    /// the same field-by-field path and the comparison would pass without proving anything.
    fn the_two_serializers_the_identity_test_compares_are_different_ones() {
        // the owned row is copyable in one shot, which is the branch we want on one side
        assert!(
            <Scalars as Archive>::COPY_OPTIMIZATION.is_enabled(),
            "a row of two u64s stopped being memcpy-able, so the identity test now compares one \
             code path against itself"
        );
        // and the borrow of it is not, which is the branch we want on the other
        assert!(
            !<RowRef<'_, Scalars> as Archive>::COPY_OPTIMIZATION.is_enabled(),
            "a borrowed row became memcpy-able, which would copy the reference rather than the row"
        );
    }

    #[test]
    /// A borrowed row is read back as the row it borrowed, by the row's own archived type
    ///
    /// Byte identity is only half of what the client needs. The other half is that the bytes are
    /// reachable through `Archived<T>` rather than through some `ArchivedRowRef`, which is what
    /// lets `FromShoal::retrieve` keep its return type across this change.
    fn a_borrowed_row_reads_back_as_the_row_itself() {
        let rows = vec![
            Strings {
                title: "Solaris".to_owned(),
                overview: "A station above an ocean".to_owned(),
            },
        ];
        // serialize the borrows, and read them back as though they had been owned all along
        let bytes = rkyv::to_bytes::<Error>(&borrow(&rows)).unwrap();
        let archived = rkyv::access::<rkyv::vec::ArchivedVec<ArchivedStrings>, Error>(&bytes)
            .expect("borrowed rows are readable as the rows they borrowed");
        assert_eq!(archived.len(), 1);
        assert_eq!(archived[0].title.as_str(), "Solaris");
        assert_eq!(archived[0].overview.as_str(), "A station above an ocean");
    }

    #[test]
    /// Rows written back out of an archive are byte identical to owned ones
    ///
    /// The archived half of the same claim. A partition read from disk holds no rows to point at,
    /// so its rows are written straight out of the archive they lie in — and a client cannot tell
    /// which of the two partitions in a table answered it, because the bytes do not say.
    fn an_archived_row_is_byte_identical_to_an_owned_one() {
        let rows = vec![
            Strings {
                title: "Stalker".to_owned(),
                overview: "A guide leads two men into the zone".to_owned(),
            },
            Strings {
                title: "Sunshine".to_owned(),
                overview: "A crew carries a bomb to the sun".to_owned(),
            },
        ];
        // archive the rows the way a partition read off disk holds them
        let bytes = rkyv::to_bytes::<Error>(&rows).unwrap();
        let archived = rkyv::access::<ArchivedVec<ArchivedStrings>, Error>(&bytes).unwrap();
        // then answer with them where they lie, without materializing one
        let pointed: Vec<RowRef<'_, Strings>> =
            archived.iter().map(RowRef::<Strings>::archived).collect();
        assert_eq!(
            bytes.as_slice(),
            rkyv::to_bytes::<Error>(&pointed).unwrap().as_slice(),
            "a row answered out of an archive did not write what the row itself wrote"
        );
    }

    #[test]
    /// A get that read one resident partition and one archived one writes a single response
    ///
    /// A table is a mixture: some partitions are resident and some are still the archive they were
    /// read from, and one get can name both. The two variants have different resolvers and reach
    /// rkyv's writer by different routes, so this is the case where a drift between them would
    /// show as a response that is half one format and half the other.
    fn a_mixture_of_resident_and_archived_rows_writes_what_owned_rows_write() {
        let rows = vec![
            Strings {
                title: "Solaris".to_owned(),
                overview: "A station above an ocean".to_owned(),
            },
            Strings {
                title: "Annihilation".to_owned(),
                overview: "A survey team enters the shimmer".to_owned(),
            },
        ];
        // archive the rows so that the second one can be answered out of an archive
        let bytes = rkyv::to_bytes::<Error>(&rows).unwrap();
        let archived = rkyv::access::<ArchivedVec<ArchivedStrings>, Error>(&bytes).unwrap();
        // answer the first where it lies and the second out of the archive holding it
        let mixed = vec![
            RowRef::new(&rows[0]),
            RowRef::<Strings>::archived(&archived[1]),
        ];
        assert_eq!(
            bytes.as_slice(),
            rkyv::to_bytes::<Error>(&mixed).unwrap().as_slice(),
            "a get that mixed the two forms wrote something neither of them writes"
        );
    }

    /// Point at every row of a slice without copying any of them
    ///
    /// # Arguments
    ///
    /// * `rows` - The rows to borrow
    fn borrow<T: Rearchive>(rows: &[T]) -> Vec<RowRef<'_, T>> {
        rows.iter().map(RowRef::new).collect()
    }
}
