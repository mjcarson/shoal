//! One model of a database schema, built once from the fields of a `#[shoal::db]` struct
//!
//! Every generator of the `db` macro reads the tables of a schema, and before buckets every one
//! of them read those tables off the struct's fields itself: a field was a table, its row was its
//! type's first generic argument, and its kind was a substring of its type's name. A bucket breaks
//! that one to one: one field holds two tables nobody declared
//! ([F79](../../../docs/src/features/bucket-schema.md)). So the fields are read here, once, into
//! the tables a schema serves and the fields that hold them, and the generators read this.
//!
//! # Invariants
//!
//! **A declared table is read exactly as it always was.** Its rewritten type, its row and its
//! kind are what the field-by-field generators read before this model existed, so a schema with
//! no bucket expands to the same items and the same fingerprint; `schema_ids_are_frozen` holds
//! that.
//!
//! **Every way a field can fail to be read is a compile error with a span**, never a panic.
//!
//! **A bucket's tables sit in its field's place**, its `ObjectMeta` then its `StripeMeta`, so the
//! order of the table enum is the order a reader of the struct sees.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{FieldsNamed, Ident};

use crate::tables::TableKinds;
use crate::utils;

/// One table a schema serves, declared by its author or generated for a bucket
pub struct Table {
    /// How the dispatch layer reaches this table from `self`: `movies`, or `posters.objects`
    pub access: TokenStream,
    /// The table's whole type, as the server half names it to call its associated functions
    pub table_type: syn::Type,
    /// The row this table holds, which is also its variant in every generated enum
    pub row: Ident,
    /// Whether a partition holds one row or many
    pub kind: TableKinds,
    /// Whether this table has a storage engine
    pub persistent: bool,
    /// The projections this table declared
    pub projections: Vec<Ident>,
    /// The bucket that generated this table, or `None` for one its author declared
    pub bucket: Option<Ident>,
}

/// One field of the database struct, after its type has been rewritten
pub enum Declared {
    /// A field holding one table its author declared
    Table {
        /// The field's name
        ident: Ident,
        /// The field's rewritten type
        ty: syn::Type,
        /// Which of the schema's tables it holds
        table: usize,
    },
    /// A field holding a bucket, and so the two tables it generated
    Bucket {
        /// The field's name
        ident: Ident,
        /// The field's rewritten type
        ty: syn::Type,
        /// Which of the schema's tables is the bucket's `ObjectMeta`
        objects: usize,
        /// Which of the schema's tables is the bucket's `StripeMeta`
        stripes: usize,
    },
}

impl Declared {
    /// The name of this field
    pub fn ident(&self) -> &Ident {
        match self {
            Declared::Table { ident, .. } | Declared::Bucket { ident, .. } => ident,
        }
    }

    /// The rewritten type of this field
    pub fn ty(&self) -> &syn::Type {
        match self {
            Declared::Table { ty, .. } | Declared::Bucket { ty, .. } => ty,
        }
    }
}

/// A bucket a schema declares
pub struct Bucket {
    /// The bucket's marker, whose name is the bucket's
    pub marker: Ident,
    /// The row of its object metadata table
    pub objects: Ident,
    /// The row of its stripe metadata table
    pub stripes: Ident,
}

/// The tables and fields of one database schema
pub struct Schema {
    /// The database struct's name
    pub db: Ident,
    /// Every field of the struct, in declaration order
    pub fields: Vec<Declared>,
    /// Every table the schema serves, in declaration order with a bucket's in its field's place
    pub tables: Vec<Table>,
    /// Every bucket the schema declares, in declaration order
    pub buckets: Vec<Bucket>,
}

impl Schema {
    /// The row of every table, which is its variant in every generated enum
    pub fn variants(&self) -> Vec<Ident> {
        self.tables.iter().map(|table| table.row.clone()).collect()
    }

    /// The projections each table declared, in table order
    pub fn projections(&self) -> Vec<Vec<Ident>> {
        self.tables
            .iter()
            .map(|table| table.projections.clone())
            .collect()
    }

    /// The tables buckets generated
    pub fn generated(&self) -> impl Iterator<Item = &Table> {
        self.tables.iter().filter(|table| table.bucket.is_some())
    }
}

/// What a field's type says it is, read off the first segment of its path
enum FieldKind {
    /// A table, persistent or held in memory
    Table {
        /// Whether it has a storage engine
        persistent: bool,
    },
    /// A bucket
    Bucket,
}

/// Build the model of a schema from its struct's fields, rewriting each field's type
///
/// The projections a field declared are taken off it here, and every field's type is rewritten
/// to name the database and its table enum, in both halves: everything generated reads the
/// rewritten types, and a client half has to see the same ones a server does.
///
/// # Arguments
///
/// * `fields` - The named fields of the database struct, rewritten in place
/// * `db` - The name of the database struct
///
/// # Errors
///
/// A spanned error for the first field that is neither a table nor a bucket the macro can read.
pub fn build(fields: &mut FieldsNamed, db: &Ident) -> syn::Result<Schema> {
    // the table enum every table type names as its last generic
    let table_names = format_ident!("{db}TableNames");
    // take the projections each field declared before anything else reads the field
    let projections = utils::take_projections(fields);
    // the model we build up, field by field
    let mut schema = Schema {
        db: db.clone(),
        fields: Vec::with_capacity(fields.named.len()),
        tables: Vec::with_capacity(fields.named.len()),
        buckets: Vec::new(),
    };
    for (field, declared) in fields.named.iter_mut().zip(projections) {
        // a named field always has a name
        let ident = field
            .ident
            .clone()
            .ok_or_else(|| syn::Error::new_spanned(&*field, "a database field has to be named"))?;
        // work out whether this field is a table or a bucket from its type's name
        match classify(&field.ty)? {
            FieldKind::Table { persistent } => {
                // the row it holds and whether its partitions hold one row or many
                let row = table_row(&field.ty)?;
                let kind = table_kind(&field.ty)?;
                // fill in the database and its table enum, as the rewrite always has
                rewrite_table_type(&mut field.ty, db, &table_names);
                // record the table and the field holding it
                schema.fields.push(Declared::Table {
                    ident: ident.clone(),
                    ty: field.ty.clone(),
                    table: schema.tables.len(),
                });
                schema.tables.push(Table {
                    access: quote!(#ident),
                    table_type: field.ty.clone(),
                    row,
                    kind,
                    persistent,
                    projections: declared,
                    bucket: None,
                });
            }
            FieldKind::Bucket => {
                // a bucket's tables are generated, so there is nothing for a projection to name
                if let Some(projection) = declared.first() {
                    return Err(syn::Error::new_spanned(
                        projection,
                        "a bucket declares no projections: its tables are generated, and no \
                         client's query reaches them",
                    ));
                }
                // the marker naming the bucket
                let marker = bucket_marker(&field.ty)?;
                // a marker names one bucket, so a second field of the same one is refused
                if schema.buckets.iter().any(|bucket| bucket.marker == marker) {
                    return Err(syn::Error::new_spanned(
                        &field.ty,
                        format!("bucket {marker} is declared twice; a marker names one bucket"),
                    ));
                }
                // fill in the database and its table enum, as a table's rewrite does
                rewrite_bucket_type(&mut field.ty, db, &table_names);
                // the two rows it generates, named from the marker alone
                let objects = format_ident!("{}ObjectMeta", marker, span = marker.span());
                let stripes = format_ident!("{}StripeMeta", marker, span = marker.span());
                // record the field and the two tables it holds, in its place
                schema.fields.push(Declared::Bucket {
                    ident: ident.clone(),
                    ty: field.ty.clone(),
                    objects: schema.tables.len(),
                    stripes: schema.tables.len() + 1,
                });
                for (row, inner) in [(&objects, quote!(objects)), (&stripes, quote!(stripes))] {
                    schema.tables.push(Table {
                        access: quote!(#ident.#inner),
                        table_type: syn::parse_quote! {
                            ::shoal::tables::PersistentUnsortedTable<
                                #row,
                                ::shoal::storage::FileSystem<#db>,
                                #table_names
                            >
                        },
                        row: row.clone(),
                        kind: TableKinds::Unsorted,
                        persistent: true,
                        projections: Vec::new(),
                        bucket: Some(marker.clone()),
                    });
                }
                schema.buckets.push(Bucket {
                    marker,
                    objects,
                    stripes,
                });
            }
        }
    }
    // a table is named by its row, so two tables of one row would be one variant twice
    for (index, table) in schema.tables.iter().enumerate() {
        if let Some(first) = schema.tables[..index]
            .iter()
            .find(|earlier| earlier.row == table.row)
        {
            // name the bucket that generated the name, if one did, since that is the surprise
            let message = match (&first.bucket, &table.bucket) {
                (Some(bucket), _) | (_, Some(bucket)) => format!(
                    "a table of row {} is declared, and bucket {bucket} generates a table of that \
                     name; rename the row or the bucket's marker",
                    table.row
                ),
                (None, None) => format!(
                    "two tables hold row {}; a table is named by its row, so each row is held \
                     by one table",
                    table.row
                ),
            };
            return Err(syn::Error::new_spanned(&table.row, message));
        }
    }
    Ok(schema)
}

/// Work out whether a field's type is a table or a bucket
///
/// Read off the first segment of the type's path, as the macro always has: the struct is not
/// emitted for a client, so its name is all there is. A table type is written as one bare
/// segment, `PersistentUnsortedTable<..>`, and so is a bucket's.
///
/// # Arguments
///
/// * `ty` - The field's type
///
/// # Errors
///
/// When the type is not a path, names no kind, or names two.
fn classify(ty: &syn::Type) -> syn::Result<FieldKind> {
    // the first segment's name, which is where a kind is read from
    let Some(name) = first_segment(ty).map(|segment| segment.ident.to_string()) else {
        return Err(syn::Error::new_spanned(
            ty,
            "a database field is a table, such as `PersistentUnsortedTable<Row, FileSystem>`, \
             or a bucket, `Bucket<Marker>`",
        ));
    };
    // a table is persistent or held in memory, and a bucket is neither
    let persistent = name.contains("Persistent");
    let ephemeral = name.contains("Ephemeral");
    let bucket = name.contains("Bucket");
    match (persistent || ephemeral, bucket) {
        (true, false) => Ok(FieldKind::Table { persistent }),
        (false, true) => Ok(FieldKind::Bucket),
        (true, true) => Err(syn::Error::new_spanned(
            ty,
            format!("`{name}` names both a table and a bucket; a field is one or the other"),
        )),
        (false, false) => Err(syn::Error::new_spanned(
            ty,
            format!(
                "`{name}` is neither a table nor a bucket: a table type's name says `Persistent` \
                 or `Ephemeral` and `Sorted` or `Unsorted`, and a bucket is `Bucket<Marker>`"
            ),
        )),
    }
}

/// The first segment of a type's path, if it is a path
///
/// # Arguments
///
/// * `ty` - The type to read
fn first_segment(ty: &syn::Type) -> Option<&syn::PathSegment> {
    match ty {
        syn::Type::Path(type_path) => type_path.path.segments.first(),
        _ => None,
    }
}

/// The row a table field holds: the last segment of its type's first generic argument
///
/// # Arguments
///
/// * `ty` - The table field's type
///
/// # Errors
///
/// When the type names no row.
fn table_row(ty: &syn::Type) -> syn::Result<Ident> {
    utils::extract_inner_table_ident(ty).ok_or_else(|| {
        syn::Error::new_spanned(
            ty,
            "a table names the row it holds as its first generic argument, such as \
             `PersistentUnsortedTable<Movie, FileSystem>`",
        )
    })
}

/// Whether a table field's partitions hold one row or many
///
/// # Arguments
///
/// * `ty` - The table field's type
///
/// # Errors
///
/// When the type's name says both or neither.
fn table_kind(ty: &syn::Type) -> syn::Result<TableKinds> {
    // the name a kind is read from
    let name = first_segment(ty)
        .map(|segment| segment.ident.to_string())
        .unwrap_or_default();
    // `Unsorted` contains `sorted` and not `Sorted`, so the two checks never overlap by accident
    match (name.contains("Unsorted"), name.contains("Sorted")) {
        (true, false) => Ok(TableKinds::Unsorted),
        (false, true) => Ok(TableKinds::Sorted),
        (true, true) => Err(syn::Error::new_spanned(
            ty,
            format!("`{name}` says both `Sorted` and `Unsorted`; a table is one or the other"),
        )),
        (false, false) => Err(syn::Error::new_spanned(
            ty,
            format!("`{name}` says neither `Sorted` nor `Unsorted`, so its kind is unknown"),
        )),
    }
}

/// The marker a bucket field names: the one generic argument of `Bucket<Marker>`
///
/// # Arguments
///
/// * `ty` - The bucket field's type
///
/// # Errors
///
/// When the bucket names no marker, more than one argument, a lifetime or a constant, or a
/// marker with generics of its own.
fn bucket_marker(ty: &syn::Type) -> syn::Result<Ident> {
    // the error every malformed spelling gets, pointing at what was written
    let malformed = |spanned: &dyn quote::ToTokens, found: &str| {
        syn::Error::new_spanned(
            spanned,
            format!(
                "a bucket is declared as `Bucket<Marker>`, where `Marker` is a unit struct whose \
                 name is the bucket's; {found}"
            ),
        )
    };
    // a bucket's segment carries exactly one argument
    let segment = first_segment(ty).ok_or_else(|| malformed(ty, "this is not a path"))?;
    let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
        return Err(malformed(ty, "this names no marker"));
    };
    if args.args.len() != 1 {
        return Err(malformed(
            &args.args,
            "this names more than one argument, and the database and its table enum are \
             filled in by the macro",
        ));
    }
    // the one argument is a type, a plain path to a struct with no generics of its own
    let Some(syn::GenericArgument::Type(syn::Type::Path(marker))) = args.args.first() else {
        return Err(malformed(&args.args, "its argument is not a type"));
    };
    let Some(last) = marker.path.segments.last() else {
        return Err(malformed(marker, "its argument names nothing"));
    };
    if !matches!(last.arguments, syn::PathArguments::None) || marker.qself.is_some() {
        return Err(malformed(
            marker,
            "the marker cannot take generics of its own",
        ));
    }
    Ok(last.ident.clone())
}

/// Rewrite a table field's type to name the database it belongs to and the table enum it mints
///
/// Every table is generic over its row, its storage engine and its database's table name enum, but
/// a schema only writes the parts of that a person could reasonably be asked to write. This fills
/// in the rest.
///
/// A persistent table names its storage engine and the engine is given the database:
///
/// ```text
/// PersistentUnsortedTable<Movie, FileSystem>
///     -> PersistentUnsortedTable<Movie, FileSystem<Tmdb>, TmdbTableNames>
/// ```
///
/// An ephemeral table names no engine at all, because there is only one engine it could mean, so
/// the database is pushed as a new generic rather than folded into one already there:
///
/// ```text
/// EphemeralUnsortedTable<Movie>
///     -> EphemeralUnsortedTable<Movie, Tmdb, TmdbTableNames>
/// ```
///
/// # Arguments
///
/// * `ty` - The table field's type, rewritten in place
/// * `db` - The name of the database struct
/// * `table_names` - The name of its table enum
fn rewrite_table_type(ty: &mut syn::Type, db: &Ident, table_names: &Ident) {
    let syn::Type::Path(type_path) = ty else {
        return;
    };
    let Some(segment) = type_path.path.segments.first_mut() else {
        return;
    };
    // an ephemeral table is written without a storage engine, so its database generic has to be
    // pushed instead of wrapped around one already there
    let ephemeral = segment.ident.to_string().contains("Ephemeral");
    let syn::PathArguments::AngleBracketed(args) = &mut segment.arguments else {
        return;
    };
    if ephemeral {
        // push the database as the 2nd arg, which is the engine's own generic
        if args.args.len() == 1 {
            args.args
                .push(syn::GenericArgument::Type(syn::parse_quote!(#db)));
        }
    } else if let Some(syn::GenericArgument::Type(syn::Type::Path(storage_path))) =
        args.args.iter_mut().nth(1)
    {
        // add the database as the storage engine's generic, unless it names one already
        if let Some(storage_seg) = storage_path.path.segments.last_mut() {
            if matches!(storage_seg.arguments, syn::PathArguments::None) {
                storage_seg.arguments =
                    syn::PathArguments::AngleBracketed(syn::parse_quote!(<#db>));
            }
        }
    }
    // append the table enum as the 3rd generic arg if it is not already there
    if args.args.len() == 2 {
        args.args
            .push(syn::GenericArgument::Type(syn::parse_quote!(#table_names)));
    }
}

/// Rewrite a bucket field's type to name the database and its table enum
///
/// ```text
/// Bucket<Posters>
///     -> Bucket<Posters, Media, MediaTableNames>
/// ```
///
/// This breaks the same circularity a table's rewrite breaks: the bucket's tables are generic over
/// the database's storage engine, which is generic over the database.
///
/// # Arguments
///
/// * `ty` - The bucket field's type, rewritten in place
/// * `db` - The name of the database struct
/// * `table_names` - The name of its table enum
fn rewrite_bucket_type(ty: &mut syn::Type, db: &Ident, table_names: &Ident) {
    let Some(syn::PathArguments::AngleBracketed(args)) = (match ty {
        syn::Type::Path(type_path) => type_path
            .path
            .segments
            .first_mut()
            .map(|segment| &mut segment.arguments),
        _ => None,
    }) else {
        return;
    };
    // the marker was checked to be the only argument, so the database and its enum follow it
    args.args
        .push(syn::GenericArgument::Type(syn::parse_quote!(#db)));
    args.args
        .push(syn::GenericArgument::Type(syn::parse_quote!(#table_names)));
}
