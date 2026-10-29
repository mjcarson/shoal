//! Generate the schema fingerprints the two peers compare before the first query
//!
//! The schema is visible to two different macros and to neither of them whole. A row derive sees
//! its own field names, types and order but not what its table is called in the database; the
//! database macro sees every table and every projection but not one row's fields. So the
//! fingerprint is composed: each row and each projection folds itself into a constant, and the
//! database macro folds those constants together with the table list.
//!
//! Everything here is a `const fn` chain, so a fingerprint costs nothing at runtime — it is a
//! literal by the time the binary exists.

use quote::quote;
use std::collections::HashSet;
use syn::{Ident, Type};

use crate::schema::{Declared, Schema};

/// Build the set of field names in a list, for asking which roles a field plays
///
/// # Arguments
///
/// * `fields` - The fields to collect the names of
fn names(fields: &[(Ident, Type)]) -> HashSet<String> {
    fields.iter().map(|(ident, _)| ident.to_string()).collect()
}

/// Build the constant expression that fingerprints one row or projection
///
/// Each field mixes in its name, the spelling of its type, the size and alignment that type has
/// once it is archived, its position, and the roles it plays in a query. The archived size is what
/// catches a type alias whose definition widened, since the spelling of such a field never
/// changes and two peers agreeing when they should not is the one dangerous direction.
///
/// # Arguments
///
/// * `name` - The name of the type being fingerprinted
/// * `all_fields` - Every field of the type, in declaration order
/// * `partition_fields` - The fields that key this type's partition
/// * `sort_fields` - The fields that sort this type's rows
/// * `filter_fields` - The fields a query can filter on
/// * `update_fields` - The fields an update can set
pub fn row_expr(
    name: &Ident,
    all_fields: &[(Ident, Type)],
    partition_fields: &[(Ident, Type)],
    sort_fields: &[(Ident, Type)],
    filter_fields: &[(Ident, Type)],
    update_fields: &[(Ident, Type)],
) -> proc_macro2::TokenStream {
    // build the name sets we ask which roles each field plays
    let partitions = names(partition_fields);
    let sorts = names(sort_fields);
    let filters = names(filter_fields);
    let updates = names(update_fields);
    // build one mix per field, in the order the fields were declared
    let mixes: Vec<_> = all_fields
        .iter()
        .enumerate()
        .map(|(index, (ident, ty))| {
            // the name of this field and the type it was written as
            let field_name = ident.to_string();
            let type_name = quote!(#ty).to_string();
            // gather the bits for every role this field plays
            let mut roles = 0u8;
            if partitions.contains(&field_name) {
                roles |= 1 << 0;
            }
            if sorts.contains(&field_name) {
                roles |= 1 << 1;
            }
            if filters.contains(&field_name) {
                roles |= 1 << 2;
            }
            if updates.contains(&field_name) {
                roles |= 1 << 3;
            }
            quote! {
                let hash = ::shoal::shared::protocol::fingerprint::mix_field(
                    hash,
                    #field_name,
                    #type_name,
                    ::core::mem::size_of::<
                        <#ty as ::shoal::rkyv::Archive>::Archived
                    >(),
                    ::core::mem::align_of::<
                        <#ty as ::shoal::rkyv::Archive>::Archived
                    >(),
                    #index,
                    #roles,
                );
            }
        })
        .collect();
    // start from the name of the type itself so two identical field lists still differ
    let type_name = name.to_string();
    quote! {
        {
            let hash = ::shoal::shared::protocol::fingerprint::mix_str(
                ::shoal::shared::protocol::fingerprint::SEED,
                #type_name,
            );
            #(#mixes)*
            hash
        }
    }
}

/// Build the constant expression that fingerprints a whole database
///
/// The client lane's wire version is mixed in so that a change to the framing between a client
/// and a server forces every fingerprint to move, while a version the peer lanes alone moved to
/// leaves it where it was ([F48](../../../../docs/src/features/rolling-compatibility.md)). Each
/// table mixes in the name it is held under, the
/// whole type it was declared as — which is what carries the table kind and the storage engine —
/// and the constants of its row and of each projection declared for it. Each bucket mixes in its
/// field's name and type, the constants of the two rows it generated, and the layout of what
/// those rows nest ([F79](../../../../docs/src/features/bucket-schema.md)).
///
/// # Arguments
///
/// * `schema` - The fields of the database, after their types have been rewritten
pub fn db_expr(schema: &Schema) -> proc_macro2::TokenStream {
    db_expr_with(schema, true)
}

/// Build the expression for a database's structural schema id
///
/// [`db_expr`] with the protocol version left out: the same tables, rows and projections in the
/// same order, hashed from the same seed, so that this moves exactly when the schema does and
/// never when the framing does. It is what two nodes compare beside their wire versions
/// ([F38](../../../../docs/src/features/inter-node-transport.md)).
///
/// # Arguments
///
/// * `schema` - The fields of the database, after their types have been rewritten
pub fn db_structural_expr(schema: &Schema) -> proc_macro2::TokenStream {
    db_expr_with(schema, false)
}

/// Build either fingerprint expression
///
/// # Arguments
///
/// * `schema` - The fields of the database, in declaration order
/// * `with_version` - Whether to mix the protocol version in after the database name
fn db_expr_with(schema: &Schema, with_version: bool) -> proc_macro2::TokenStream {
    // the hash every walk starts from, and one block of mixes per field
    let start = start_expr(schema, with_version);
    let mixes = field_mixes(schema);
    quote! {
        {
            #start
            #(#mixes)*
            hash
        }
    }
}

/// Build the expression for a database's lineage: its id after each of its fields
///
/// The walk [`db_expr_with`] takes, with the hash kept after every field. The last element is the
/// id itself, so a schema whose fields only extend another's holds that other's id in its own
/// lineage, at the position of its last field. That is what an append of tables or buckets is
/// judged by: a peer, a joiner, a snapshot's install and a client are admitted where one side's
/// lineage holds the other's id ([F93](../../../../docs/src/features/operations-and-the-real-cluster.md)).
/// A schema with no field has a lineage of its id alone.
///
/// # Arguments
///
/// * `schema` - The fields of the database, after their types have been rewritten
/// * `with_version` - Whether this is the lineage of the fingerprint or of the structural id
pub fn lineage_expr(schema: &Schema, with_version: bool) -> proc_macro2::TokenStream {
    // the hash every walk starts from, and one block of mixes per field
    let start = start_expr(schema, with_version);
    let mixes = field_mixes(schema);
    // a name for the hash after each field
    let kept: Vec<_> = (0..mixes.len())
        .map(|index| quote::format_ident!("after_{index}"))
        .collect();
    // a schema with no field is its id alone
    let elements = if kept.is_empty() {
        quote! { [hash] }
    } else {
        quote! { [#(#kept),*] }
    };
    quote! {
        &{
            #start
            #(
                #mixes
                let #kept = hash;
            )*
            #elements
        }
    }
}

/// Build the fingerprint of every table a schema serves, by its name
///
/// A table's own identity and shape, with nothing of the schema around it: its name, the whole
/// type it is declared as, its row's constant and each of its projections', and for a table a
/// bucket generated the layout of what those rows nest. Two builds of a schema whose fields only
/// extend each other agree on every table they share, so a snapshot, a scrub's digest and a
/// backup's file are judged by this and not by the schema's id
/// ([F93](../../../../docs/src/features/operations-and-the-real-cluster.md)).
///
/// # Arguments
///
/// * `schema` - The tables of the database, in the order its table enum lists them
pub fn tables_expr(schema: &Schema) -> proc_macro2::TokenStream {
    // one name and constant per table, in the order the schema serves them
    let entries: Vec<_> = schema
        .tables
        .iter()
        .map(|table| {
            // the table's name, which its identity is the hash of, and the type it is held as
            let name = table.row.to_string();
            let row = &table.row;
            let table_type = &table.table_type;
            let type_name = quote!(#table_type).to_string();
            // every projection this table declared
            let projected: Vec<_> = table
                .projections
                .iter()
                .map(|projection| {
                    let projection_name = projection.to_string();
                    quote! {
                        let hash = ::shoal::shared::protocol::fingerprint::mix_str(
                            hash,
                            #projection_name,
                        );
                        let hash = ::shoal::shared::protocol::fingerprint::mix_u64(
                            hash,
                            <#projection as ::shoal::shared::traits::ShoalProjection>
                                ::SCHEMA_FINGERPRINT,
                        );
                    }
                })
                .collect();
            // a generated table's rows nest types its row's constant does not fold
            let layout = if table.bucket.is_some() {
                quote! {
                    let hash = ::shoal::shared::protocol::fingerprint::mix_u64(
                        hash,
                        ::shoal::shared::objects::OBJECT_ROWS_LAYOUT,
                    );
                }
            } else {
                quote! {}
            };
            quote! {
                (#name, {
                    let hash = ::shoal::shared::protocol::fingerprint::mix_str(
                        ::shoal::shared::protocol::fingerprint::SEED,
                        #name,
                    );
                    let hash = ::shoal::shared::protocol::fingerprint::mix_str(hash, #type_name);
                    let hash = ::shoal::shared::protocol::fingerprint::mix_u64(
                        hash,
                        <#row as ::shoal::shared::traits::TableSchemaSupport>::SCHEMA_FINGERPRINT,
                    );
                    #(#projected)*
                    #layout
                    hash
                })
            }
        })
        .collect();
    quote! {
        &[#(#entries),*]
    }
}

/// Build the statements a schema's walk starts with, binding `hash`
///
/// The name of the database and, for the fingerprint, the version of the client lane it speaks -
/// `CLIENT_WIRE_VERSION`, not `PROTOCOL_VERSION`, since F48: a version the peer lanes moved to
/// without the client lane does not orphan every client - and the structural id skips that one
/// mix and is otherwise the same walk.
///
/// # Arguments
///
/// * `schema` - The database whose walk this starts
/// * `with_version` - Whether to mix the protocol version in after the database name
fn start_expr(schema: &Schema, with_version: bool) -> proc_macro2::TokenStream {
    // the name of the database, which every walk starts from
    let db_name = schema.db.to_string();
    // the client lane's version, for the fingerprint alone
    let version = if with_version {
        quote! {
            let hash = ::shoal::shared::protocol::fingerprint::mix_u64(
                hash,
                ::shoal::shared::protocol::CLIENT_WIRE_VERSION as u64,
            );
        }
    } else {
        quote! {}
    };
    quote! {
        let hash = ::shoal::shared::protocol::fingerprint::mix_str(
            ::shoal::shared::protocol::fingerprint::SEED,
            #db_name,
        );
        #version
    }
}

/// Build one block of mixes per field of a schema, in declaration order
///
/// # Arguments
///
/// * `schema` - The fields of the database, in declaration order
fn field_mixes(schema: &Schema) -> Vec<proc_macro2::TokenStream> {
    schema
        .fields
        .iter()
        .map(|declared| {
            // the name this field is held under and the whole type it was declared as
            let field_name = declared.ident().to_string();
            let ty = declared.ty();
            let type_name = quote!(#ty).to_string();
            match declared {
                Declared::Table { table, .. } => {
                    // the row this table holds, whose own fields are folded into its constant
                    let table = &schema.tables[*table];
                    let row = &table.row;
                    // every projection this table declared, each of which is a wire type of its own
                    let projected: Vec<_> = table
                        .projections
                        .iter()
                        .map(|projection| {
                            let projection_name = projection.to_string();
                            quote! {
                                let hash = ::shoal::shared::protocol::fingerprint::mix_str(
                                    hash,
                                    #projection_name,
                                );
                                let hash = ::shoal::shared::protocol::fingerprint::mix_u64(
                                    hash,
                                    <#projection as ::shoal::shared::traits::ShoalProjection>
                                        ::SCHEMA_FINGERPRINT,
                                );
                            }
                        })
                        .collect();
                    quote! {
                        let hash = ::shoal::shared::protocol::fingerprint::mix_str(hash, #field_name);
                        let hash = ::shoal::shared::protocol::fingerprint::mix_str(hash, #type_name);
                        let hash = ::shoal::shared::protocol::fingerprint::mix_u64(
                            hash,
                            <#row as ::shoal::shared::traits::TableSchemaSupport>::SCHEMA_FINGERPRINT,
                        );
                        #(#projected)*
                    }
                }
                Declared::Bucket {
                    objects, stripes, ..
                } => {
                    // the two rows the bucket generated, whose fields are folded into their own
                    // constants, and the layout of the types those rows nest, which is not
                    let objects = &schema.tables[*objects].row;
                    let stripes = &schema.tables[*stripes].row;
                    quote! {
                        let hash = ::shoal::shared::protocol::fingerprint::mix_str(hash, #field_name);
                        let hash = ::shoal::shared::protocol::fingerprint::mix_str(hash, #type_name);
                        let hash = ::shoal::shared::protocol::fingerprint::mix_u64(
                            hash,
                            <#objects as ::shoal::shared::traits::TableSchemaSupport>::SCHEMA_FINGERPRINT,
                        );
                        let hash = ::shoal::shared::protocol::fingerprint::mix_u64(
                            hash,
                            <#stripes as ::shoal::shared::traits::TableSchemaSupport>::SCHEMA_FINGERPRINT,
                        );
                        let hash = ::shoal::shared::protocol::fingerprint::mix_u64(
                            hash,
                            ::shoal::shared::objects::OBJECT_ROWS_LAYOUT,
                        );
                    }
                }
            }
        })
        .collect()
}
