//! Generate a get struct for a type

use quote::{format_ident, quote};
use syn::Ident;

/// Extend a token stream with an Get struct definition
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the type we are extending
/// * `update_fields` - The fields to include in the update struct (ident, type)
pub fn add_unsorted(
    stream: &mut proc_macro2::TokenStream,
    name: &Ident,
    partition_fields: &[(syn::Ident, syn::Type)],
) {
    // build our struct names
    let get_name = format_ident!("{}Get", name);
    let filter_name = format_ident!("{}Filter", name);
    // also add the exists struct
    add_unsorted_exists(stream, name, partition_fields);
    // build the partition key type
    let partition_key_type = if partition_fields.len() == 1 {
        // we have a single partition field so we can just use that type
        let (_, ty) = &partition_fields[0];
        quote! { #ty }
    } else {
        // we have multiple partition fields so well need to wrap it in a tuple
        let types: Vec<_> = partition_fields.iter().map(|(_, ty)| ty).collect();
        quote! { (#(#types),*) }
    };
    // build the partition key type
    let partition_args: Vec<_> = partition_fields
        .iter()
        .map(|(ident, ty)| quote! { #ident: #ty })
        .collect();
    // build the partition args to tuple init
    let partition_init = if partition_fields.len() == 1 {
        // we have a single partition key arg so just use type instead of a tuple
        let (ident, _) = &partition_fields[0];
        quote! { #ident }
    } else {
        // we have multiple partition key fields so get all of their idents
        let idents = partition_fields.iter().map(|(ident, _)| {
            quote! { #ident }
        });
        // wrap them in a tuple
        quote! { (#(#idents,)*)  }
    };
    // generate our get struct for this type and its methods
    stream.extend(quote! {
        #[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
        #[rkyv(derive(Debug))]
        pub struct #get_name {
            /// The partition key of the partition to get
            pub partition_key: #partition_key_type,
            /// Any filters to use when deciding what rows to return
            pub filters: Option<#filter_name>,
            /// The number of rows to return
            pub limit: Option<usize>,
        }

        #[automatically_derived]
        impl shoal_core::shared::traits::RkyvSupport for #get_name {}

        #[automatically_derived]
        impl #get_name {
            /// Create a new get query for this type
            pub fn new(#(#partition_args),*) -> Self {
                #get_name {
                    partition_key: #partition_init,
                    filters: None,
                    limit: None,
                }
            }

                /// Set a filter for getting rows
                ///
                /// # Arguments
                ///
                /// * `filter` - The filters to set
                pub fn filters(mut self, filters: #filter_name) -> Self {
                    // set our filters
                    self.filters = Some(filters);
                    self
                }

        }
    });
}

/// Extend a token stream with a sorted Get struct definition
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the type we are extending
/// * `partition_fields` - The partition key fields
/// * `sort_fields` - The sort key fields
pub fn add_sorted(
    stream: &mut proc_macro2::TokenStream,
    name: &Ident,
    partition_fields: &[(syn::Ident, syn::Type)],
    sort_fields: &[(syn::Ident, syn::Type)],
) {
    // build our struct names
    let get_name = format_ident!("{}Get", name);
    let filter_name = format_ident!("{}Filter", name);
    // also add the exists struct
    add_sorted_exists(stream, name, partition_fields, sort_fields);
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
    // Build the sort key type
    let sort_key_type = if sort_fields.len() == 1 {
        // we have a single sort field so we can just use that type
        let (_, ty) = &sort_fields[0];
        quote! { #ty }
    } else {
        // we have multiple sort fields so well need to wrap it in a tuple
        let types: Vec<_> = sort_fields.iter().map(|(_, ty)| ty).collect();
        quote! { (#(#types),*) }
    };
    // generate our get struct for this type and its methods
    stream.extend(quote! {
        #[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
        #[rkyv(derive(Debug))]
        pub struct #get_name {
            /// The partition keys to get data from
            pub partition_keys: Vec<#partition_key_type>,
            /// The sort keys to get data from
            pub sort_keys: Vec<#sort_key_type>,
            /// Any filters to use when deciding what rows to return
            pub filters: Option<#filter_name>,
            /// The number of rows to return
            pub limit: Option<usize>,
        }

        #[automatically_derived]
        impl shoal_core::shared::traits::RkyvSupport for #get_name {}

        #[automatically_derived]
        impl #get_name {
            /// Create a new get query for this type
            ///
            /// # Arguments
            ///
            /// * `partition_keys` - The partitions to get data from
            pub fn new(partition_keys: Vec<#partition_key_type>) -> Self {
                #get_name {
                    partition_keys,
                    sort_keys: Vec::default(),
                    filters: None,
                    limit: None,
                }
            }

            /// Set the sort keys to restrict data returned from partitions too
            ///
            /// # Arguments
            ///
            /// * `sort_keys` - The sort keys to restrict data returned too
            pub fn sort_keys(mut self, sort_keys: Vec<#sort_key_type>) -> Self {
                // set our sort keys
                self.sort_keys = sort_keys;
                self
            }

            /// Set a filter for getting rows
            pub fn filters(mut self, filters: #filter_name) -> Self {
                self.filters = Some(filters);
                self
            }

            /// Set the max number of rows to retrieve
            ///
            /// # Arguments
            ///
            /// * `limit` - The max number of rows to return
            pub fn limit(mut self, limit: usize) -> Self {
                self.limit = Some(limit);
                self
            }
        }
    });
}

/// Extend a token stream with a sorted Exists struct definition
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the type we are extending
/// * `partition_fields` - The partition key fields
/// * `sort_fields` - The sort key fields
fn add_sorted_exists(
    stream: &mut proc_macro2::TokenStream,
    name: &Ident,
    partition_fields: &[(syn::Ident, syn::Type)],
    sort_fields: &[(syn::Ident, syn::Type)],
) {
    // build our struct names
    let exists_name = format_ident!("{}Exists", name);
    let filter_name = format_ident!("{}Filter", name);
    // Build the partition key type
    let partition_key_type = if partition_fields.len() == 1 {
        let (_, ty) = &partition_fields[0];
        quote! { #ty }
    } else {
        let types: Vec<_> = partition_fields.iter().map(|(_, ty)| ty).collect();
        quote! { (#(#types),*) }
    };
    // Build the sort key type
    let sort_key_type = if sort_fields.len() == 1 {
        let (_, ty) = &sort_fields[0];
        quote! { #ty }
    } else {
        let types: Vec<_> = sort_fields.iter().map(|(_, ty)| ty).collect();
        quote! { (#(#types),*) }
    };
    // generate our exists struct for this type and its methods
    stream.extend(quote! {
        #[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
        #[rkyv(derive(Debug))]
        pub struct #exists_name {
            /// The partition keys to check for data in
            pub partition_keys: Vec<#partition_key_type>,
            /// The sort keys to check for data with
            pub sort_keys: Vec<#sort_key_type>,
            /// Any filters to use when deciding what rows to check
            pub filters: Option<#filter_name>,
        }

        #[automatically_derived]
        impl shoal_core::shared::traits::RkyvSupport for #exists_name {}

        #[automatically_derived]
        impl shoal_core::shared::traits::ExistsQuery for #exists_name {}

        #[automatically_derived]
        impl #exists_name {
            /// Create a new exists query for this type
            ///
            /// # Arguments
            ///
            /// * `partition_keys` - The partitions to check for data in
            pub fn new(partition_keys: Vec<#partition_key_type>) -> Self {
                #exists_name {
                    partition_keys,
                    sort_keys: Vec::default(),
                    filters: None,
                }
            }

            /// Set the sort keys to restrict data checked to
            ///
            /// # Arguments
            ///
            /// * `sort_keys` - The sort keys to restrict data checked to
            pub fn sort_keys(mut self, sort_keys: Vec<#sort_key_type>) -> Self {
                self.sort_keys = sort_keys;
                self
            }

            /// Set a filter for checking rows
            pub fn filters(mut self, filters: #filter_name) -> Self {
                self.filters = Some(filters);
                self
            }
        }
    });
}

/// Extend a token stream with a unsorted Exists struct definition
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the type we are extending
/// * `partition_fields` - The partition key fields
/// * `sort_fields` - The sort key fields
fn add_unsorted_exists(
    stream: &mut proc_macro2::TokenStream,
    name: &Ident,
    partition_fields: &[(syn::Ident, syn::Type)],
) {
    // build our struct names
    let exists_name = format_ident!("{}Exists", name);
    let filter_name = format_ident!("{}Filter", name);
    // Build the partition key type
    let partition_key_type = if partition_fields.len() == 1 {
        let (_, ty) = &partition_fields[0];
        quote! { #ty }
    } else {
        let types: Vec<_> = partition_fields.iter().map(|(_, ty)| ty).collect();
        quote! { (#(#types),*) }
    };
    // generate our exists struct for this type and its methods
    stream.extend(quote! {
        #[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
        #[rkyv(derive(Debug))]
        pub struct #exists_name {
            /// The partition key to check for data in
            pub partition_key: #partition_key_type,
            /// Any filters to use when deciding what rows to check
            pub filters: Option<#filter_name>,
        }

        #[automatically_derived]
        impl shoal_core::shared::traits::RkyvSupport for #exists_name {}

        #[automatically_derived]
        impl shoal_core::shared::traits::ExistsQuery for #exists_name {}

        #[automatically_derived]
        impl #exists_name {
            /// Create a new exists query for this type
            ///
            /// # Arguments
            ///
            /// * `partition_key` - The partition to check for data in
            pub fn new(partition_key: #partition_key_type) -> Self {
                #exists_name {
                    partition_key,
                    filters: None,
                }
            }

            /// Set a filter for checking rows
            pub fn filters(mut self, filters: #filter_name) -> Self {
                self.filters = Some(filters);
                self
            }
        }
    });
}
