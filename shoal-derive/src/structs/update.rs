//! Generate a get struct for a type

use quote::{format_ident, quote};
use syn::Ident;

/// Extend a token stream with an unsorted Update struct definition
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
    update_fields: &[(syn::Ident, syn::Type)],
) {
    // build the update struct name
    let update_name = format_ident!("{}Update", name);
    let update_data_name = format_ident!("{}UpdateData", name);
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
    // if no update fields, create a struct with just the partition key
    if update_fields.is_empty() {
        stream.extend(quote! {
            /// The updates that can be applied to this table
            #[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
            #[rkyv(derive(Debug))]
            pub struct #update_name {
                /// The partition key to update data in
                pub partition_key: #partition_key_type,
            }

            impl shoal_core::shared::traits::RkyvSupport for #update_name {}

            /// The server facing updates that can be applied to this table (just the updates no keys)
            #[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
            #[rkyv(derive(Debug))]
            pub struct #update_data_name;

            #[automatically_derived]
            impl shoal_core::shared::traits::RkyvSupport for #update_data_name {}

            #[automatically_derived]
            impl From<#update_name> for #update_data_name {
                fn from(_update: #update_name) -> Self {
                    #update_data_name
                }
            }
        });
        return;
    }
    // build the fields for the update struct (all optional)
    let fields = update_fields.iter().map(|(ident, ty)| {
        // build the doc string for this field
        let doc_string = format!("The updated value to set for {ident}");
        quote! {
            #[doc = #doc_string]
            pub #ident: Option<#ty>
        }
    });
    // clone our fields for our data struct as well
    let data_fields = fields.clone();
    // build the fields for our conversion from an Update to an UpdateData struct
    let from_fields = update_fields.iter().map(|(ident, _)| {
        quote! { #ident: update.#ident }
    });
    // generate the update struct
    stream.extend(quote! {
        /// The updates that can be applied to this table
        #[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
        #[rkyv(derive(Debug))]
        pub struct #update_name {
            /// The partition key to update data in
            pub partition_key: #partition_key_type,
            #(#fields),*
        }

        #[automatically_derived]
        impl shoal_core::shared::traits::RkyvSupport for #update_name {}

        /// The server facing updates that can be applied to this table (just the updates no keys)
        #[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
        #[rkyv(derive(Debug))]
        pub struct #update_data_name {
            #(#data_fields),*
        }

        #[automatically_derived]
        impl shoal_core::shared::traits::RkyvSupport for #update_data_name {}

        #[automatically_derived]
        impl From<#update_name> for #update_data_name {
            fn from(update: #update_name) -> Self {
                #update_data_name {
                    #(#from_fields),*
                }
            }
        }
    });
}

/// Extend a token stream with a sorted Update struct definition
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the type we are extending
/// * `update_fields` - The fields to include in the update struct (ident, type)
pub fn add_sorted(
    stream: &mut proc_macro2::TokenStream,
    name: &Ident,
    partition_fields: &[(syn::Ident, syn::Type)],
    sort_fields: &[(syn::Ident, syn::Type)],
    update_fields: &[(syn::Ident, syn::Type)],
) {
    // build the update struct name
    let update_name = format_ident!("{}Update", name);
    let update_data_name = format_ident!("{}UpdateData", name);
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
    // build the sort key type
    let sort_key_type = if sort_fields.len() == 1 {
        // we have a single sort field so we can just use that type
        let (_, ty) = &sort_fields[0];
        quote! { #ty }
    } else {
        // we have multiple sort fields so well need to wrap it in a tuple
        let types: Vec<_> = sort_fields.iter().map(|(_, ty)| ty).collect();
        quote! { (#(#types),*) }
    };
    //// if no update fields, create an empty struct
    //if update_fields.is_empty() {
    //    stream.extend(quote! {
    //        #[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Default)]
    //        #[rkyv(derive(Debug))]
    //        pub struct #update_name;

    //        impl shoal_core::shared::traits::RkyvSupport for #update_name {}

    //        /// The server facing updates that can be applied to this table (just the updates no keys)
    //        #[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    //        #[rkyv(derive(Debug))]
    //        pub struct #update_data_name;

    //        #[automatically_derived]
    //        impl shoal_core::shared::traits::RkyvSupport for #update_data_name {}

    //        #[automatically_derived]
    //        impl From<#update_name> for #update_data_name {
    //            fn from(update: #update_name) -> Self {
    //                #update_data_name {}
    //            }
    //        }
    //    });
    //    return;
    //}
    // build the fields for the update struct (all optional)
    let fields = update_fields.iter().map(|(ident, ty)| {
        // build the doc string for this field
        let doc_string = format!("The updated value to set for {ident}");
        quote! {
            #[doc = #doc_string]
            pub #ident: Option<#ty>
        }
    });
    // clone our fields for our data struct as well
    let data_fields = fields.clone();
    // build the fields for our conversion from an Update to an UpdateData struct
    let from_fields = update_fields.iter().map(|(ident, _)| {
        quote! { #ident: update.#ident }
    });
    // generate the update struct
    stream.extend(quote! {
        /// The updates that can be applied to this table
        #[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
        #[rkyv(derive(Debug))]
        pub struct #update_name {
            /// The partition key to update data in
            pub partition_key: #partition_key_type,
            /// The sort key to apply updates too
            pub sort_key: #sort_key_type,
            #(#fields),*
        }

        #[automatically_derived]
        impl shoal_core::shared::traits::RkyvSupport for #update_name {}

        /// The server facing updates that can be applied to this table (just the updates no keys)
        #[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
        #[rkyv(derive(Debug))]
        pub struct #update_data_name {
            #(#data_fields),*
        }

        #[automatically_derived]
        impl shoal_core::shared::traits::RkyvSupport for #update_data_name {}

        #[automatically_derived]
        impl #update_name {
            /// Convert this update into its sort key and update data parts
            pub fn into_update_parts(self) -> (#sort_key_type, #update_data_name) {
                (
                    self.sort_key,
                    #update_data_name {
                        #(#from_fields),*
                    }
                )
            }
        }
    });
}
