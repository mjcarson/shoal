//! Different utilites for deriving traits/code
//!
//! # Every path these macros emit
//!
//! Generated code names `::shoal::…` and nothing else. It never names `shoal_core`, `rkyv`,
//! `uuid`, `glommio` or `kanal` directly, because a crate writing a schema has no reason to have
//! heard of any of them - that was known issue 54, and F15 is the fix. The leading `::` is load
//! bearing: it is the crate root form, so a local module called `shoal` cannot shadow it.
//!
//! | Prefix | Comes from | Legal in a `db(client)` half? |
//! | --- | --- | --- |
//! | `::shoal::shared::…` | the protocol | yes |
//! | `::shoal::client::…` | the client | yes |
//! | `::shoal::{rkyv, uuid, gxhash, serde_json, tracing, deepsize2}` | passthroughs | yes |
//! | `::shoal::server::…` | the engine | **no** |
//! | `::shoal::storage::…` | the engine | **no** |
//! | `::shoal::tables::…` | the engine | **no** |
//! | `::shoal::{glommio, kanal, lru}` | the engine | **no** |
//!
//! A new `::shoal::server::` path emitted from the client half is a bug: it compiles here and
//! fails in the caller, which is the worst place to find out. The client half is everything
//! `crate::DbHalf::Client` still emits - see `crate::db`.

use syn::Ident;

// Helper functions to identify types
pub(super) fn is_string_type(ty: &syn::Type) -> bool {
    if let syn::Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            return segment.ident == "String";
        }
    }
    false
}

/// The scalars whose archived form writes itself
///
/// rkyv archives each of these to a `rend` type it already implements `Archive<Archived = Self>`
/// and `Serialize` for, which is what makes the mirror for one of them rkyv's own code.
const PORTABLE_SCALARS: [&str; 17] = [
    "bool", "char", "f32", "f64", "i8", "i16", "i32", "i64", "i128", "isize", "u8", "u16", "u32",
    "u64", "u128", "usize", "()",
];

/// How a field is written back out of the archive it was read from
///
/// `shoal-derive` sees the *syntax* of a field's type and never its definition, so this is what
/// it can decide from a name. A shape it does not recognize is not an error — it is a field that
/// is materialized on its own while the rest of the row is not
/// ([O40](../../../docs/src/appendix/optimizations.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FieldShape {
    /// A `Vec` of elements that archive to themselves, which rkyv can copy in one go
    ///
    /// Kept apart from `Mirrored` because the generic mirror walks a vector element by element,
    /// and doing that to a payload would turn a copy of it into a loop over it.
    PortableVec,
    /// A type this crate's mirror knows how to write out of an archive
    Mirrored,
    /// A type it does not, which is materialized on its own and written the way it always was
    Opaque,
}

impl FieldShape {
    /// Work out how a field has to be written back out, from the syntax of its type
    ///
    /// # Arguments
    ///
    /// * `ty` - The field's type
    /// * `opted_in` - Whether the field declared `#[shoal(rearchive)]`, which asserts an impl
    pub(super) fn of(ty: &syn::Type, opted_in: bool) -> FieldShape {
        // an author who named a type implementing the trait has told us more than syntax can
        if opted_in {
            return FieldShape::Mirrored;
        }
        // a vector of scalars is the one shape that must not go through the generic mirror
        if let Some(inner) = generic_argument(ty, "Vec") {
            if is_portable_scalar(inner) {
                return FieldShape::PortableVec;
            }
        }
        // everything else is either a shape shoal-proto implements the mirror for, or it is not
        if is_mirrored(ty) {
            FieldShape::Mirrored
        } else {
            FieldShape::Opaque
        }
    }
}

/// Determine whether a type archives to itself, which is every scalar rkyv handles
///
/// # Arguments
///
/// * `ty` - The type to check
fn is_portable_scalar(ty: &syn::Type) -> bool {
    // a unit type is written `()` rather than as a path, so it is checked on its own
    if matches!(ty, syn::Type::Tuple(tuple) if tuple.elems.is_empty()) {
        return true;
    }
    if let syn::Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            return PORTABLE_SCALARS.contains(&segment.ident.to_string().as_str());
        }
    }
    false
}

/// Determine whether shoal-proto implements the mirror for a type
///
/// Recursive, because `Vec` and `Option` are mirrored only when what they hold is.
///
/// # Arguments
///
/// * `ty` - The type to check
fn is_mirrored(ty: &syn::Type) -> bool {
    // scalars and strings are the leaves
    if is_portable_scalar(ty) || is_string_type(ty) {
        return true;
    }
    // and the two containers are mirrored exactly as far as what they hold is
    for wrapper in ["Vec", "Option"] {
        if let Some(inner) = generic_argument(ty, wrapper) {
            return is_mirrored(inner);
        }
    }
    false
}

/// Take the type a named container holds, if a type is that container
///
/// # Arguments
///
/// * `ty` - The type to look inside
/// * `wrapper` - The container to look for, by name
fn generic_argument<'a>(ty: &'a syn::Type, wrapper: &str) -> Option<&'a syn::Type> {
    if let syn::Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            // the name has to match, or this is some other container entirely
            if segment.ident != wrapper {
                return None;
            }
            if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                if let Some(syn::GenericArgument::Type(inner)) = args.args.first() {
                    return Some(inner);
                }
            }
        }
    }
    None
}

/// Convert snake case strings to pascal case
///
/// # Arguments
///
/// * `snake_case` - The snake case string to convert
pub fn snake_to_pascal_case(snake_case: &str) -> String {
    snake_case
        .split('_')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => {
                    first.to_uppercase().collect::<String>() + &chars.as_str().to_lowercase()
                }
            }
        })
        .collect()
}
/// Extract the first generic argument from a type like `PersistentUnsortedTable<Movie, ...>`
/// Returns the inner type (e.g., `Movie`)
pub fn extract_inner_table_type(ty: &syn::Type) -> Option<&syn::Type> {
    if let syn::Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.first() {
            if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                if let Some(syn::GenericArgument::Type(inner_type)) = args.args.first() {
                    return Some(inner_type);
                }
            }
        }
    }
    None
}

/// Extract the inner type as an Ident from a type like `PersistentUnsortedTable<Movie, ...>`
pub fn extract_inner_table_ident(ty: &syn::Type) -> Option<Ident> {
    if let Some(inner_type) = extract_inner_table_type(ty) {
        if let syn::Type::Path(type_path) = inner_type {
            if let Some(segment) = type_path.path.segments.last() {
                return Some(segment.ident.clone());
            }
        }
    }
    None
}

/// Take the projections each table of a database declares off of its field
///
/// A projection is declared on the field holding the table it projects, because the database
/// macro is the only thing that sees every projection of every table at once: the response
/// kinds enum needs a variant for each of them, and an enum cannot be added to later.
///
/// The attribute is consumed here rather than left on the struct, since the struct this macro
/// emits has no derive that would know what `shoal` means.
///
/// # Arguments
///
/// * `fields` - The named fields of the database struct
pub fn take_projections(fields: &mut syn::FieldsNamed) -> Vec<Vec<Ident>> {
    // collect the projections declared on each table, in field order
    let mut declared = Vec::with_capacity(fields.named.len());
    for field in fields.named.iter_mut() {
        // the projections this field declared, if it declared any
        let mut projections = Vec::new();
        // keep every attribute that is not a projection list, so nothing else is swallowed
        field.attrs.retain(|attr| {
            // only a `#[shoal(..)]` attribute can hold a projection list
            if !attr.path().is_ident("shoal") {
                return true;
            }
            // whether this attribute turned out to be the projection list
            let mut is_projections = false;
            // walk the arguments looking for `projections(..)`
            let parsed = attr.parse_nested_meta(|meta| {
                // anything other than a projection list belongs to somebody else
                if !meta.path.is_ident("projections") {
                    return Err(meta.error("expected `projections(..)`"));
                }
                is_projections = true;
                // each name in the list is a projection of this fields table
                meta.parse_nested_meta(|inner| {
                    let ident = inner
                        .path
                        .get_ident()
                        .ok_or_else(|| inner.error("a projection has to be a type name"))?;
                    projections.push(ident.clone());
                    Ok(())
                })
            });
            // a `#[shoal(..)]` that is not a projection list is left for whoever owns it
            match parsed {
                Ok(()) if is_projections => false,
                _ => true,
            }
        });
        declared.push(projections);
    }
    declared
}
