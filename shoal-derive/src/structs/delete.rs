//! Generate a delete struct for a type

use quote::{format_ident, quote};
use syn::Ident;

/// Extend a token stream with an Delete struct definition
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the type we are extending
pub fn add(
    stream: &mut proc_macro2::TokenStream,
    name: &Ident,
    partition_fields: &[(syn::Ident, syn::Type)],
) {
    // build the update struct name
    let delete_name = format_ident!("{}Delete", name);
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
    // generate the delete struct
    stream.extend(quote! {
        #[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
        #[rkyv(derive(Debug))]
        pub struct #delete_name{
            /// The key to the partition to delete
            pub partition_key: #partition_key_type,
        }

        #[automatically_derived]
        impl shoal_core::shared::traits::RkyvSupport for #delete_name {}

        #[automatically_derived]
        impl #delete_name {
            /// Create a new get query for this type
            pub fn new(#(#partition_args),*) -> Self {
                #delete_name {
                    partition_key: #partition_init,
                }
            }
        }
    });
}
