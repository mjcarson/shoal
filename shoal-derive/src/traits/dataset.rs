//! Generate the dataset traits for a table ([F66](../../../docs/src/features/dataset-benchmarks.md))
//!
//! A table that opted in with `#[shoal_table(dataset)]` gets `DatasetRow` and a `DatasetTable`
//! that hands a visitor its row type; one that did not gets a `DatasetTable` that refuses by
//! name. Both are generic over the database's query kinds, so nothing here names the database.

use quote::{format_ident, quote};
use syn::Ident;

/// Build the type a list of key fields is held in: the field's own type, or a tuple of several
///
/// # Arguments
///
/// * `fields` - The key fields (ident, type)
fn key_type(fields: &[(Ident, syn::Type)]) -> proc_macro2::TokenStream {
    // one field is its own type, several are a tuple in field order
    if fields.len() == 1 {
        let (_, ty) = &fields[0];
        quote! { #ty }
    } else {
        let types: Vec<_> = fields.iter().map(|(_, ty)| ty).collect();
        quote! { (#(#types),*) }
    }
}

/// Build the expression that clones a list of key fields out of `self`
///
/// # Arguments
///
/// * `fields` - The key fields (ident, type)
fn key_clone(fields: &[(Ident, syn::Type)]) -> proc_macro2::TokenStream {
    // one field is cloned on its own, several into a tuple in field order
    if fields.len() == 1 {
        let (ident, _) = &fields[0];
        quote! { ::std::clone::Clone::clone(&self.#ident) }
    } else {
        let idents: Vec<_> = fields.iter().map(|(ident, _)| ident).collect();
        quote! { (#(::std::clone::Clone::clone(&self.#idents)),*) }
    }
}

/// Extend a token stream with the dataset traits for a table
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the row type
/// * `partition_fields` - The partition key fields (ident, type)
/// * `sort_fields` - The sort key fields (ident, type), empty for an unsorted table
/// * `opted_in` - Whether the table asked for `dataset`
pub fn add(
    stream: &mut proc_macro2::TokenStream,
    name: &Ident,
    partition_fields: &[(Ident, syn::Type)],
    sort_fields: &[(Ident, syn::Type)],
    opted_in: bool,
) {
    // the name the table is refused or visited under
    let table_name = name.to_string();
    // a table that did not opt in refuses by name, with no bound on its row
    if !opted_in {
        stream.extend(quote! {
            #[automatically_derived]
            impl<K> ::shoal::shared::dataset::DatasetTable<K> for #name {
                /// This table did not opt in
                const DATASET: bool = false;

                /// Refuse, naming this table
                fn accept<V: ::shoal::shared::dataset::DatasetVisitor<K>>(
                    _visitor: V,
                ) -> Result<V::Output, ::shoal::shared::dataset::DatasetError> {
                    Err(::shoal::shared::dataset::DatasetError::NotOptedIn { table: #table_name })
                }
            }
        });
        return;
    }
    // the get this table is read back with
    let get_name = format_ident!("{}Get", name);
    // the partition key, which every table has
    let partition_type = key_type(partition_fields);
    let partition_clone = key_clone(partition_fields);
    // an unsorted table is read by its partition key alone, a sorted one by both keys
    let sorted = !sort_fields.is_empty();
    let (read_key_type, read_key_expr, read_query_body) = if sorted {
        let sort_type = key_type(sort_fields);
        let sort_clone = key_clone(sort_fields);
        (
            quote! { (#partition_type, #sort_type) },
            quote! { (#partition_clone, #sort_clone) },
            quote! {
                // name each partition once, in the order its first key named it
                let mut partitions: Vec<#partition_type> = Vec::with_capacity(keys.len());
                for (partition, _) in keys {
                    if !partitions.contains(partition) {
                        partitions.push(::std::clone::Clone::clone(partition));
                    }
                }
                // and every sort key, which the get matches in each named partition
                let sorts: Vec<#sort_type> = keys
                    .iter()
                    .map(|(_, sort)| ::std::clone::Clone::clone(sort))
                    .collect();
                #get_name::new(partitions).sort_keys(sorts).into()
            },
        )
    } else {
        (
            quote! { #partition_type },
            partition_clone,
            quote! {
                // an unsorted get names whole partitions, which are the rows
                #get_name::new(keys.to_vec()).into()
            },
        )
    };
    // the row reads itself, inserts itself, and is read back by its keys
    stream.extend(quote! {
        #[automatically_derived]
        impl<K> ::shoal::shared::dataset::DatasetRow<K> for #name
        where
            K: From<#name> + From<#get_name> + 'static,
        {
            /// The keys that read exactly one row of this table
            type ReadKey = #read_key_type;

            /// Whether this table is sorted
            const SORTED: bool = #sorted;

            /// Clone the keys that read this row back out of it
            fn read_key(&self) -> Self::ReadKey {
                #read_key_expr
            }

            /// Insert this row
            fn insert_query(self) -> K {
                self.into()
            }

            /// Read every row these keys name in one get
            ///
            /// # Arguments
            ///
            /// * `keys` - The keys of the rows to read
            fn read_query(keys: &[Self::ReadKey]) -> K {
                #read_query_body
            }
        }

        #[automatically_derived]
        impl<K> ::shoal::shared::dataset::DatasetTable<K> for #name
        where
            K: From<#name> + From<#get_name> + 'static,
        {
            /// This table opted in
            const DATASET: bool = true;

            /// Hand the visitor this table's row type
            ///
            /// # Arguments
            ///
            /// * `visitor` - The visitor to call back
            fn accept<V: ::shoal::shared::dataset::DatasetVisitor<K>>(
                visitor: V,
            ) -> Result<V::Output, ::shoal::shared::dataset::DatasetError> {
                Ok(visitor.visit::<#name>(#table_name))
            }
        }
    });
}
