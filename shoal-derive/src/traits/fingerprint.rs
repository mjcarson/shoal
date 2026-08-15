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

/// Build the set of field names in a list, for asking which roles a field plays
///
/// # Arguments
///
/// * `fields` - The fields to collect the names of
fn names(fields: &[(Ident, Type)]) -> HashSet<String> {
    fields
        .iter()
        .map(|(ident, _)| ident.to_string())
        .collect()
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
/// The protocol version is mixed in so that a future wire change which does not bump the version
/// byte still forces every fingerprint to move. Each table mixes in the name it is held under, the
/// whole type it was declared as — which is what carries the table kind and the storage engine —
/// and the constants of its row and of each projection declared for it.
///
/// # Arguments
///
/// * `struct_ident` - The name of the database
/// * `fields` - The tables in this database, after their types have been rewritten
/// * `variants` - The row type of each of those tables, in field order
/// * `projections` - The projections each of those tables declared, in field order
pub fn db_expr(
    struct_ident: &Ident,
    fields: &syn::FieldsNamed,
    variants: &[Ident],
    projections: &[Vec<Ident>],
) -> proc_macro2::TokenStream {
    // build one block of mixes per table, in the order they were declared
    let mixes: Vec<_> = fields
        .named
        .iter()
        .enumerate()
        .map(|(index, field)| {
            // the name this table is held under and the whole type it was declared as
            let field_name = field
                .ident
                .as_ref()
                .expect("a database table has to be a named field")
                .to_string();
            let ty = &field.ty;
            let type_name = quote!(#ty).to_string();
            // the row this table holds, whose own fields are folded into its constant
            let row = &variants[index];
            // every projection this table declared, each of which is a wire type of its own
            let projected: Vec<_> = projections
                .get(index)
                .map(Vec::as_slice)
                .unwrap_or_default()
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
        })
        .collect();
    // start from the name of the database and the version of the protocol it speaks
    let db_name = struct_ident.to_string();
    quote! {
        {
            let hash = ::shoal::shared::protocol::fingerprint::mix_str(
                ::shoal::shared::protocol::fingerprint::SEED,
                #db_name,
            );
            let hash = ::shoal::shared::protocol::fingerprint::mix_u64(
                hash,
                ::shoal::shared::protocol::PROTOCOL_VERSION as u64,
            );
            #(#mixes)*
            hash
        }
    }
}
