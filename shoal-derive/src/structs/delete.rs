//! Generate a delete struct for a type

use quote::{format_ident, quote};
use syn::Ident;

/// Extend a token stream with an Delete struct definition
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the type we are extending
pub fn add_unsorted(
    stream: &mut proc_macro2::TokenStream,
    name: &Ident,
    partition_fields: &[(syn::Ident, syn::Type)],
) {
    // build the delete struct name
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
        #[derive(Debug, Clone, ::shoal::rkyv::Archive, ::shoal::rkyv::Serialize, ::shoal::rkyv::Deserialize)]
        #[rkyv(derive(Debug))]
        pub struct #delete_name{
            /// The key to the partition to delete
            pub partition_key: #partition_key_type,
        }

        #[automatically_derived]
        impl ::shoal::shared::traits::RkyvSupport for #delete_name {}

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

/// Extend a token stream with a sorted Delete struct definition
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
    // build the delete struct name
    let delete_name = format_ident!("{}Delete", name);
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
    // Build partition key args for the new method
    let partition_args: Vec<_> = partition_fields
        .iter()
        .map(|(ident, ty)| quote! { #ident: #ty })
        .collect();
    // Build sort key args for the new method
    let sort_args: Vec<_> = sort_fields
        .iter()
        .map(|(ident, ty)| quote! { #ident: #ty })
        .collect();
    // Build the partition init
    let partition_init = if partition_fields.len() == 1 {
        // we have a single partition field so we can just use that type
        let (ident, _) = &partition_fields[0];
        quote! { #ident }
    } else {
        // we have multiple partition fields so well need to wrap it in a tuple
        let idents = partition_fields.iter().map(|(ident, _)| quote! { #ident });
        quote! { (#(#idents,)*) }
    };
    // Build the sort init
    let sort_init = if sort_fields.len() == 1 {
        // we have a single sort field so we can just use that type
        let (ident, _) = &sort_fields[0];
        quote! { #ident }
    } else {
        // we have multiple sort fields so well need to wrap it in a tuple
        let idents = sort_fields.iter().map(|(ident, _)| quote! { #ident });
        quote! { (#(#idents,)*) }
    };
    // generate the sorted delete struct and its methods
    stream.extend(quote! {
        #[derive(Debug, Clone, ::shoal::rkyv::Archive, ::shoal::rkyv::Serialize, ::shoal::rkyv::Deserialize)]
        #[rkyv(derive(Debug))]
        pub struct #delete_name {
            /// The key to the partition to delete from
            pub partition_key: #partition_key_type,
            /// The sort key of the row to delete
            pub sort_key: #sort_key_type,
        }

        #[automatically_derived]
        impl ::shoal::shared::traits::RkyvSupport for #delete_name {}

        #[automatically_derived]
        impl #delete_name {
            /// Create a new delete query for this type
            pub fn new(#(#partition_args,)* #(#sort_args),*) -> Self {
                #delete_name {
                    partition_key: #partition_init,
                    sort_key: #sort_init,
                }
            }
        }
    });
}
