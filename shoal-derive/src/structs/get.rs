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
pub fn add(
    stream: &mut proc_macro2::TokenStream,
    name: &Ident,
    partition_fields: &[(syn::Ident, syn::Type)],
) {
    // build our struct names
    let get_name = format_ident!("{}Get", name);
    let filter_name = format_ident!("{}Filter", name);
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
