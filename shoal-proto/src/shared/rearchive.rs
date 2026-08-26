//! Writing an archived row back out without materializing it first
//!
//! [`RowRef`](super::row_ref::RowRef) answers a get out of the rows a partition still holds, which
//! closed the resident half of [O2](../../../docs/src/appendix/optimizations.md). A partition read
//! from disk holds no rows to point at — it holds an archive, and what a scan of it walks is
//! `Archived<T>`. Answering from one meant materializing an owned row per row returned and
//! serializing it straight back into the layout it had just been read out of.
//!
//! The reason that was left standing is that **rkyv serializes in one direction only**. There is
//! no `impl Archive for ArchivedString`, none for `ArchivedVec`, and none for any archived type a
//! derive generates; the only archived types that can be written back out are the ones whose
//! archived form is themselves — rkyv's own `rend` scalars.
//!
//! This module is the missing direction. [`Rearchive`] is "this type's archived form can be
//! written back into that same form", and [`ArchivedRef`] is a value of an archived form standing
//! in for the owned value it was archived from: its archived type *is* the original's archived
//! type, so substituting one for the other changes no bytes. That is the same theorem
//! [`RowRef`](super::row_ref::RowRef) rests on, arrived at from the other side.
//!
//! # What the pieces are for
//!
//! Most of a row needs no code at all. A `u64` archives to `rend::u64_le`, which rkyv already
//! implements `Archive<Archived = Self>` and `Serialize` for, so the mirror for a scalar field is
//! rkyv's own impl reached through [`serialize_portable`]. `String` and `Vec` need the two
//! constructors rkyv exposes for exactly this shape — `ArchivedString::serialize_from_str` and
//! `ArchivedVec::serialize_from_slice`/`serialize_from_iter` — and `Option` needs nothing, because
//! substituting `ArchivedRef<'_, T>` for `T` inside rkyv's own generic `impl Archive for Option<T>`
//! produces the archived type we want. That substitution is what makes this compose at all.
//!
//! # The field this cannot see inside
//!
//! A field whose type is declared elsewhere — a `Vec<Tag>` for a `Tag` in another crate, a
//! `HashMap` — has no `Rearchive` impl here and `shoal-derive` sees only the *syntax* of its type,
//! never its definition. Such a field falls back to [`serialize_via_owned`], which materializes
//! **that field** and serializes it, which is what every field does today. The row still takes the
//! fast path for every other field, and no schema stops compiling because of one exotic one. A
//! nested type that implements [`Rearchive`] itself can be opted back in with `#[shoal(rearchive)]`
//! on the field.
use rkyv::{
    de::Pool,
    option::ArchivedOption,
    rancor::{Fallible, Source},
    ser::{Allocator, Writer},
    string::{ArchivedString, StringResolver},
    vec::{ArchivedVec, VecResolver},
    Archive, Deserialize, Place, Serialize,
};

/// A type whose archived form can be written back out into that same form
///
/// The two halves mirror rkyv's own `Archive`/`Serialize` pair, with the source swapped: instead
/// of reading an owned value, they read the archived value the row was found in. What they write
/// is byte for byte what serializing the owned value would have written, which is the property
/// every user of this trait depends on and the one the tests state.
pub trait Rearchive: Archive {
    /// What [`Rearchive::serialize_archived`] hands to [`Rearchive::resolve_archived`]
    ///
    /// This is deliberately *not* `<Self as Archive>::Resolver`. A field that had to be
    /// materialized carries the materialized value here so that it outlives the serialize and is
    /// still there to resolve from, which rkyv's own resolver has nowhere to put.
    type ArchivedResolver;

    /// Write an archived value's out of line data, reading it out of the archive it lies in
    ///
    /// # Arguments
    ///
    /// * `archived` - The archived value to write back out
    /// * `serializer` - The serializer to write the out of line data into
    ///
    /// # Errors
    ///
    /// Returns the serializer's error if any part of the value could not be written.
    fn serialize_archived<S>(
        archived: &Self::Archived,
        serializer: &mut S,
    ) -> Result<Self::ArchivedResolver, S::Error>
    where
        S: Fallible + Writer + Allocator + ?Sized,
        S::Error: Source;

    /// Write the fixed size archived value on top of what was serialized for it
    ///
    /// # Arguments
    ///
    /// * `archived` - The archived value being written back out
    /// * `resolver` - What [`Rearchive::serialize_archived`] produced for it
    /// * `out` - Where the archived value belongs
    fn resolve_archived(
        archived: &Self::Archived,
        resolver: Self::ArchivedResolver,
        out: Place<Self::Archived>,
    );
}

/// An archived value standing in for the owned value it was archived from
///
/// Its archived type is the original's archived type, so anywhere rkyv is generic over a type
/// parameter — `Option<T>`, `Vec<T>`, a tuple — substituting this for that parameter reuses
/// rkyv's own code and writes the identical bytes. That is how a nested collection is served here
/// without this module reimplementing rkyv's containers.
pub struct ArchivedRef<'a, T: Rearchive>(&'a <T as Archive>::Archived);

impl<'a, T: Rearchive> ArchivedRef<'a, T> {
    /// Stand in for the owned value an archived one was archived from
    ///
    /// # Arguments
    ///
    /// * `archived` - The archived value to write back out, wherever it currently lies
    #[must_use]
    pub fn new(archived: &'a <T as Archive>::Archived) -> Self {
        ArchivedRef(archived)
    }
}

impl<T: Rearchive> Clone for ArchivedRef<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: Rearchive> Copy for ArchivedRef<'_, T> {}

impl<T: Rearchive> Archive for ArchivedRef<'_, T> {
    /// The archived type of the value this stands in for, not a second type shaped like it
    type Archived = <T as Archive>::Archived;

    /// What the mirror produced, which is not rkyv's resolver for the same type
    type Resolver = <T as Rearchive>::ArchivedResolver;

    /// Write the archived value, exactly as the owned value would have written it
    ///
    /// # Arguments
    ///
    /// * `resolver` - What serializing this stand-in produced
    /// * `out` - Where the archived value belongs
    fn resolve(&self, resolver: Self::Resolver, out: Place<Self::Archived>) {
        // the mirror writes the value - this type adds nothing to the bytes and must not
        T::resolve_archived(self.0, resolver, out);
    }
}

impl<T, S> Serialize<S> for ArchivedRef<'_, T>
where
    T: Rearchive,
    S: Fallible + Writer + Allocator + ?Sized,
    S::Error: Source,
{
    /// Write this value's out of line data straight out of the archive holding it
    ///
    /// # Arguments
    ///
    /// * `serializer` - The serializer to write the out of line data into
    fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        // forward to the mirror, which reads the archive rather than an owned value
        T::serialize_archived(self.0, serializer)
    }
}

/// The resolver a field that had to be materialized carries
///
/// rkyv resolves a value *after* serializing it and needs the value both times. A field written
/// out of an archive has no owned value to keep, so the one the fallback had to build is kept
/// here until the resolve that needs it.
pub struct OwnedResolver<T: Archive> {
    /// The value that had to be materialized to be written back out
    owned: T,
    /// What serializing it produced
    resolver: <T as Archive>::Resolver,
}

/// Write out a field whose archived form is already its own archived type
///
/// A scalar archives to a `rend` type that rkyv implements `Archive<Archived = Self>` for, so the
/// mirror for one is rkyv's own impl and this is the whole of it.
///
/// # Arguments
///
/// * `archived` - The archived scalar to write back out
/// * `serializer` - The serializer to write it with
///
/// # Errors
///
/// Returns the serializer's error, which a scalar cannot currently produce.
pub fn serialize_portable<A, S>(
    archived: &A,
    serializer: &mut S,
) -> Result<<A as Archive>::Resolver, S::Error>
where
    A: Archive<Archived = A> + Serialize<S>,
    S: Fallible + ?Sized,
{
    // the archived scalar is its own archived type, so it serializes itself
    archived.serialize(serializer)
}

/// Resolve a field whose archived form is already its own archived type
///
/// # Arguments
///
/// * `archived` - The archived scalar being written back out
/// * `resolver` - What serializing it produced
/// * `out` - Where it belongs
pub fn resolve_portable<A>(archived: &A, resolver: <A as Archive>::Resolver, out: Place<A>)
where
    A: Archive<Archived = A>,
{
    // and likewise resolves itself, writing the same bytes it was read from
    archived.resolve(resolver, out);
}

/// Write out a `Vec` whose elements are their own archived type
///
/// This is the one shape that must not go through [`ArchivedRef`]: a slice of elements that
/// archive to themselves reaches rkyv's `memcpy` branch, and walking it element by element would
/// turn a copy of a payload into a loop over it. `shoal-derive` picks this for a `Vec` of scalars
/// and the generic impl for everything else.
///
/// # Arguments
///
/// * `archived` - The archived vector to write back out
/// * `serializer` - The serializer to write its elements into
///
/// # Errors
///
/// Returns the serializer's error if the elements could not be written.
pub fn serialize_portable_vec<A, S>(
    archived: &ArchivedVec<A>,
    serializer: &mut S,
) -> Result<VecResolver, S::Error>
where
    A: Archive<Archived = A> + Serialize<S>,
    S: Fallible + Writer + Allocator + ?Sized,
{
    // hand rkyv the archived elements as a slice, which is the branch it can copy in one go
    ArchivedVec::serialize_from_slice(archived.as_slice(), serializer)
}

/// Resolve a `Vec` written back out of an archive
///
/// # Arguments
///
/// * `archived` - The archived vector being written back out
/// * `resolver` - What serializing its elements produced
/// * `out` - Where the vector belongs
pub fn resolve_vec<A>(archived: &ArchivedVec<A>, resolver: VecResolver, out: Place<ArchivedVec<A>>) {
    // a vector is a relative pointer and a length, and the length is all that is read here
    ArchivedVec::resolve_from_len(archived.len(), resolver, out);
}

/// Write out a field by materializing it first, which is what every field used to do
///
/// The fallback for a field type `shoal-derive` cannot see inside. It costs what the whole row
/// used to cost, for one field, and it is why a row with an exotic field still compiles and still
/// gains on every other field.
///
/// # Arguments
///
/// * `archived` - The archived field to write back out
/// * `serializer` - The serializer to write it with
///
/// # Errors
///
/// Returns the serializer's error, or its own error if the field could not be materialized.
pub fn serialize_via_owned<T, S>(
    archived: &<T as Archive>::Archived,
    serializer: &mut S,
) -> Result<OwnedResolver<T>, S::Error>
where
    T: Archive + Serialize<S>,
    <T as Archive>::Archived: Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>,
    S: Fallible + ?Sized,
    S::Error: Source,
{
    // materialize the field, since nothing here knows how to write its type out of the archive
    let mut pool = Pool::new();
    let owned = rkyv::api::deserialize_using::<T, _, rkyv::rancor::Error>(archived, &mut pool)
        .map_err(S::Error::new)?;
    // then serialize it the way it has always been serialized
    let resolver = owned.serialize(serializer)?;
    Ok(OwnedResolver { owned, resolver })
}

/// Resolve a field that had to be materialized to be written back out
///
/// # Arguments
///
/// * `resolver` - The materialized field and what serializing it produced
/// * `out` - Where the archived field belongs
pub fn resolve_via_owned<T: Archive>(resolver: OwnedResolver<T>, out: Place<<T as Archive>::Archived>) {
    // take the value back out of the resolver that kept it alive for this
    let OwnedResolver { owned, resolver } = resolver;
    owned.resolve(resolver, out);
}

/// Implement [`Rearchive`] for every type whose archived form writes itself
///
/// A scalar's archived type is a `rend` type that rkyv already implements `Archive<Archived =
/// Self>` and `Serialize` for, so the mirror is rkyv's own impl and this macro is only naming the
/// types it applies to.
macro_rules! rearchive_portable {
    ($($ty:ty),* $(,)?) => {
        $(
            impl Rearchive for $ty {
                type ArchivedResolver = <<$ty as Archive>::Archived as Archive>::Resolver;

                fn serialize_archived<S>(
                    archived: &Self::Archived,
                    serializer: &mut S,
                ) -> Result<Self::ArchivedResolver, S::Error>
                where
                    S: Fallible + Writer + Allocator + ?Sized,
                    S::Error: Source,
                {
                    serialize_portable(archived, serializer)
                }

                fn resolve_archived(
                    archived: &Self::Archived,
                    resolver: Self::ArchivedResolver,
                    out: Place<Self::Archived>,
                ) {
                    resolve_portable(archived, resolver, out);
                }
            }
        )*
    };
}

rearchive_portable!(
    (),
    bool,
    char,
    i8,
    i16,
    i32,
    i64,
    i128,
    u8,
    u16,
    u32,
    u64,
    u128,
    f32,
    f64,
    usize,
    isize,
);

impl Rearchive for String {
    /// Where the string's bytes were written, or that they were written inline
    type ArchivedResolver = StringResolver;

    /// Copy the string's bytes out of the archive into the output
    ///
    /// # Arguments
    ///
    /// * `archived` - The archived string to write back out
    /// * `serializer` - The serializer to write its bytes into
    fn serialize_archived<S>(
        archived: &Self::Archived,
        serializer: &mut S,
    ) -> Result<Self::ArchivedResolver, S::Error>
    where
        S: Fallible + Writer + Allocator + ?Sized,
        S::Error: Source,
    {
        // the same constructor `impl Serialize for String` uses, reading the archive as the str
        ArchivedString::serialize_from_str(archived.as_str(), serializer)
    }

    /// Write the string's inline representation or its relative pointer
    ///
    /// # Arguments
    ///
    /// * `archived` - The archived string being written back out
    /// * `resolver` - Where its bytes were written
    /// * `out` - Where the string belongs
    fn resolve_archived(
        archived: &Self::Archived,
        resolver: Self::ArchivedResolver,
        out: Place<Self::Archived>,
    ) {
        // short strings live inline and long ones point, and the length decides which - as always
        ArchivedString::resolve_from_str(archived.as_str(), resolver, out);
    }
}

impl<T: Rearchive> Rearchive for Vec<T> {
    /// Where the elements were written
    type ArchivedResolver = VecResolver;

    /// Write every element out of the archive, through a stand-in for the element type
    ///
    /// # Arguments
    ///
    /// * `archived` - The archived vector to write back out
    /// * `serializer` - The serializer to write its elements into
    fn serialize_archived<S>(
        archived: &Self::Archived,
        serializer: &mut S,
    ) -> Result<Self::ArchivedResolver, S::Error>
    where
        S: Fallible + Writer + Allocator + ?Sized,
        S::Error: Source,
    {
        // point at each element in turn, so rkyv writes them the way it writes owned ones
        ArchivedVec::serialize_from_iter::<ArchivedRef<'_, T>, _, S>(
            archived.as_slice().iter().map(ArchivedRef::<T>::new),
            serializer,
        )
    }

    /// Write the vector's relative pointer and length
    ///
    /// # Arguments
    ///
    /// * `archived` - The archived vector being written back out
    /// * `resolver` - Where its elements were written
    /// * `out` - Where the vector belongs
    fn resolve_archived(
        archived: &Self::Archived,
        resolver: Self::ArchivedResolver,
        out: Place<Self::Archived>,
    ) {
        resolve_vec(archived, resolver, out);
    }
}

impl<T: Rearchive> Rearchive for Option<T> {
    /// The inner value's resolver, when there is an inner value
    type ArchivedResolver = Option<<T as Rearchive>::ArchivedResolver>;

    /// Write the inner value, if there is one, through rkyv's own `Option` impl
    ///
    /// # Arguments
    ///
    /// * `archived` - The archived option to write back out
    /// * `serializer` - The serializer to write the inner value into
    fn serialize_archived<S>(
        archived: &Self::Archived,
        serializer: &mut S,
    ) -> Result<Self::ArchivedResolver, S::Error>
    where
        S: Fallible + Writer + Allocator + ?Sized,
        S::Error: Source,
    {
        // rkyv's `Option` impl is generic in the element, so a stand-in for the element serves
        stand_in::<T>(archived).serialize(serializer)
    }

    /// Write the option's tag and its inner value
    ///
    /// # Arguments
    ///
    /// * `archived` - The archived option being written back out
    /// * `resolver` - What the inner value's serialize produced
    /// * `out` - Where the option belongs
    fn resolve_archived(
        archived: &Self::Archived,
        resolver: Self::ArchivedResolver,
        out: Place<Self::Archived>,
    ) {
        // and the same stand-in resolves it, writing the tag rkyv keeps to itself
        stand_in::<T>(archived).resolve(resolver, out);
    }
}

/// Stand in for an archived option with an option of stand-ins
///
/// `ArchivedOption`'s tag types are private to rkyv, so an archived option cannot be resolved from
/// outside it. Substituting [`ArchivedRef`] for the element type gives rkyv's own generic impl
/// something it can write, and it writes the identical bytes because the two archived types are
/// the same type.
///
/// # Arguments
///
/// * `archived` - The archived option to stand in for
fn stand_in<T: Rearchive>(
    archived: &ArchivedOption<<T as Archive>::Archived>,
) -> Option<ArchivedRef<'_, T>> {
    archived.as_ref().map(ArchivedRef::<T>::new)
}

#[cfg(test)]
mod tests {
    use super::{serialize_via_owned, ArchivedRef, OwnedResolver, Place, Rearchive};
    use rkyv::{
        rancor::{Error, Fallible, Source},
        ser::{Allocator, Writer},
        vec::ArchivedVec,
        Archive, Deserialize, Serialize,
    };

    /// A type this module has no impl for, standing in for a schema's own nested type
    #[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq, Clone)]
    struct Tag {
        /// A field, so the type is not empty
        name: String,
    }

    /// A type whose fields are written out of the archive by hand, as the derive will write them
    #[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq, Clone)]
    struct Row {
        /// A scalar, which writes itself
        id: u64,
        /// A string, which is copied out of the archive
        title: String,
        /// A type with no mirror, which falls back to being materialized
        tag: Tag,
    }

    /// What each of [`Row`]'s fields produced, as the generated resolver will carry it
    struct RowArchivedResolver {
        /// Nothing - a scalar writes itself
        id: <u64 as Rearchive>::ArchivedResolver,
        /// Where the title's bytes were written
        title: <String as Rearchive>::ArchivedResolver,
        /// The materialized tag and what serializing it produced
        tag: OwnedResolver<Tag>,
    }

    impl Rearchive for Row {
        type ArchivedResolver = RowArchivedResolver;

        fn serialize_archived<S>(
            archived: &Self::Archived,
            serializer: &mut S,
        ) -> Result<Self::ArchivedResolver, S::Error>
        where
            S: Fallible + Writer + Allocator + ?Sized,
            S::Error: Source,
        {
            Ok(RowArchivedResolver {
                id: <u64 as Rearchive>::serialize_archived(&archived.id, serializer)?,
                title: <String as Rearchive>::serialize_archived(&archived.title, serializer)?,
                tag: serialize_via_owned::<Tag, S>(&archived.tag, serializer)?,
            })
        }

        fn resolve_archived(
            archived: &Self::Archived,
            resolver: Self::ArchivedResolver,
            out: Place<Self::Archived>,
        ) {
            rkyv::munge::munge!(let ArchivedRow { id, title, tag } = out);
            <u64 as Rearchive>::resolve_archived(&archived.id, resolver.id, id);
            <String as Rearchive>::resolve_archived(&archived.title, resolver.title, title);
            super::resolve_via_owned::<Tag>(resolver.tag, tag);
        }
    }

    #[test]
    /// Rows written back out of an archive are byte identical to the archive they came from
    ///
    /// This is the whole claim. A get answered out of an archive writes what a get answered out of
    /// owned rows writes, so no client can tell the two apart and no wire version moves for it.
    fn an_archived_row_is_byte_identical_to_the_row_it_was_read_from() {
        let rows = vec![
            Row {
                id: 7,
                title: "Arrival".to_owned(),
                tag: Tag { name: "first contact, which is long enough to be written out of line".to_owned() },
            },
            Row {
                id: 9,
                title: "Primer".to_owned(),
                tag: Tag { name: "time travel".to_owned() },
            },
        ];
        // archive the rows the way a partition on disk holds them
        let bytes = rkyv::to_bytes::<Error>(&rows).unwrap();
        let archived =
            rkyv::access::<ArchivedVec<ArchivedRow>, Error>(&bytes).expect("the rows are readable");
        // then write them straight back out of that archive, without materializing one
        let mirrored: Vec<ArchivedRef<'_, Row>> = archived.iter().map(ArchivedRef::new).collect();
        let again = rkyv::to_bytes::<Error>(&mirrored).unwrap();
        assert_eq!(
            bytes.as_slice(),
            again.as_slice(),
            "a row written out of its archive did not write what the row itself wrote"
        );
    }

    #[test]
    /// The container mirrors write what rkyv's own container impls write
    ///
    /// `String`, `Vec` and `Option` are the three shapes with out of line data, and each reaches
    /// rkyv's writer by a different route when it is mirrored than when it is owned. If any of
    /// them drifted, a row carrying that field would archive differently depending on where it
    /// was read from, which is the one thing this must never do.
    fn every_container_writes_what_its_owned_form_writes() {
        // a string, which is inline below a length and out of line above it
        round_trips!(
            String,
            vec![
                "short".to_owned(),
                "a string long enough that its bytes cannot be held inline".to_owned(),
            ]
        );
        // a vector of scalars, which is the shape rkyv copies in one go
        round_trips!(Vec<u8>, vec![vec![1, 2, 3], Vec::new(), vec![9; 64]]);
        // a vector of strings, which is a run of out of line data inside another one
        round_trips!(
            Vec<String>,
            vec![vec!["one".to_owned(), "two".to_owned()], Vec::new()]
        );
        // and an option, whose tag rkyv keeps to itself
        round_trips!(Option<String>, vec![Some("present".to_owned()), None]);
        round_trips!(Option<u64>, vec![Some(7), None]);
    }

    /// Archive a set of values, write them back out of that archive, and compare the bytes
    ///
    /// A macro rather than a function because the bound a value needs to reach `to_bytes` is
    /// three lifetimes and two types wide, and naming it once per call site is worse than not
    /// naming it at all.
    macro_rules! round_trips {
        ($ty:ty, $values:expr) => {{
            let values: Vec<$ty> = $values;
            // archive the values the way a partition on disk holds them
            let bytes = rkyv::to_bytes::<Error>(&values).unwrap();
            let archived =
                rkyv::access::<ArchivedVec<<$ty as Archive>::Archived>, Error>(&bytes).unwrap();
            // then write them straight back out of that archive
            let mirrored: Vec<ArchivedRef<'_, $ty>> =
                archived.iter().map(ArchivedRef::new).collect();
            let again = rkyv::to_bytes::<Error>(&mirrored).unwrap();
            assert_eq!(
                bytes.as_slice(),
                again.as_slice(),
                "a {} written out of its archive did not write what the value itself wrote",
                stringify!($ty)
            );
        }};
    }
    use round_trips;
}
