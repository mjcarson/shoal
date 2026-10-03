//! Generate the PartitionKeySupport trait implementation for a type

use quote::quote;
use syn::Ident;

use super::utils;

/// Extend a token stream with a PartitionKeySuppport for #name implementation
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the type we are extending
/// * `partition_fields` - The fields that make up this type's partition key, in declaration order
pub fn add(
    stream: &mut proc_macro2::TokenStream,
    name: &Ident,
    partition_fields: &[(syn::Ident, syn::Type)],
) {
    // get this tables name
    let table_name = name.to_string();
    // Build the partition key type
    let partition_key_type = if partition_fields.len() == 1 {
        // we have a single partition field so we can just use that type
        let (_, ty) = &partition_fields[0];
        quote! { #ty }
    } else {
        // we have multiple partition fields so well need to wrap it in a tuple
        let types: Vec<_> = partition_fields.iter().map(|(_, ty)| ty).collect();
        quote! { (#(#types),*) }
    };
    // Build hash statements for get_partition_key, one per field of this row in declaration order
    //
    // A row is hashed field by field rather than by building a tuple of its key to hand to
    // get_partition_key_from_values: a tuple of references is not the key type, and a tuple of
    // clones would copy every key on every insert. A tuple's Hash is its members' Hash in order
    // with nothing between them, so this writes the same bytes the values path writes from the
    // tuple, and for a single field it is exactly what that path did (items 92 and 198)
    let hash_row_stmts: Vec<_> = partition_fields
        .iter()
        .map(|(ident, _)| {
            quote! {
                Self::hash_field(&mut hasher, &self.#ident);
            }
        })
        .collect();
    // Build hash statements for get_partition_key_from_values
    let hash_values_stmts: Vec<_> = if partition_fields.len() == 1 {
        vec![quote! {
            Self::hash_field(&mut hasher, values);
        }]
    } else {
        partition_fields
            .iter()
            .enumerate()
            .map(|(idx, _)| {
                let index = syn::Index::from(idx);
                quote! {
                    Self::hash_field(&mut hasher, &values.#index);
                }
            })
            .collect()
    };
    // Build hash statements for archived values
    let hash_archived_stmts: Vec<_> = partition_fields
        .iter()
        .map(|(ident, ty)| {
            // Handle archived rkyv types
            if utils::is_string_type(ty) {
                quote! {
                    hasher.write(intent.#ident.as_bytes());
                }
            } else if utils::is_u64_type(ty) {
                quote! {
                    hasher.write_u64(intent.#ident.to_native());
                }
            } else if utils::is_u32_type(ty) {
                quote! {
                    hasher.write_u32(intent.#ident.to_native());
                }
            } else {
                // For other types, assume they have to_native()
                quote! {
                    hasher.write_u64(intent.#ident.to_native() as u64);
                }
            }
        })
        .collect();
    // extend our token stream
    stream.extend(quote! {
        #[automatically_derived]
        impl ::shoal::shared::traits::PartitionKeySupport for #name {
            /// The partition key type for this data
            type PartitionKey = #partition_key_type;

            /// The name of this table
            #[inline]
            fn name() -> &'static str {
                #table_name
            }

            /// Calculate the partition key for this row
            ///
            /// This hashes each partition field in declaration order, which is what
            /// `get_partition_key_from_values` does with the members of the key's tuple, so a
            /// row and the key naming it always hash to the same partition.
            fn get_partition_key(&self) -> u64 {
                use std::hash::Hasher;
                // hash each of our partition fields in declaration order
                let mut hasher = ::shoal::gxhash::GxHasher::default();
                #(#hash_row_stmts)*
                hasher.finish()
            }

            /// Calculate the partition key for this row
            ///
            /// # Arguments
            ///
            /// * `values` - The values to hash to generate our partition key
            #[inline]
            fn get_partition_key_from_values(values: &Self::PartitionKey) -> u64 {
                use std::hash::{Hash, Hasher};
                let mut hasher = ::shoal::gxhash::GxHasher::default();
                #(#hash_values_stmts)*
                hasher.finish()
            }

            /// Get the partition key for this row from an archived value
            ///
            /// # Arguments
            ///
            /// * `intent` - The intent to get a partition key from
            fn get_partition_key_from_archived_insert(intent: &<Self as Archive>::Archived) -> u64 {
                use std::hash::{Hash, Hasher};
                let mut hasher = ::shoal::gxhash::GxHasher::default();
                #(#hash_archived_stmts)*
                hasher.finish()
            }
        }

        #[automatically_derived]
        impl #name {
            // Helper method to hash different field types
            fn hash_field<H: std::hash::Hasher, T: std::hash::Hash>(hasher: &mut H, value: &T) {
                value.hash(hasher);
            }
        }
    });
}
