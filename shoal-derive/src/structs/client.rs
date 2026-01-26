//! Generate a struct for this databases client

use quote::{format_ident, quote};
use syn::Ident;

// Add a client for this database
pub fn add(stream: &mut proc_macro2::TokenStream, struct_ident: &Ident, variants: &Vec<Ident>) {
    // build our new idents
    let client_ident = format_ident!("{}Client", struct_ident);
    let query_ident = format_ident!("{struct_ident}QueryKinds");
    let response_ident = format_ident!("{struct_ident}ResponseKinds");
    let archived_response_ident = format_ident!("Archived{struct_ident}ResponseKinds");
    // build our succeeded response arms
    let succeeded_arms = variants.iter().map(|variant_ident| {
        // build our evict partition arm for this table
        quote! {
            #archived_response_ident::#variant_ident(response)=> response.succeeded(opts),
        }
    });
    // build our kind arms
    let kind_arms = variants.iter().map(|variant_ident| {
        // build our evict partition arm for this table
        quote! {
            #archived_response_ident::#variant_ident(response)=> response.kind(),
        }
    });
    // build our get_exists arms
    let get_exists_arms = variants.iter().map(|variant_ident| {
        quote! {
            #archived_response_ident::#variant_ident(response)=> response.get_exists(),
        }
    });
    // add our client struct and query support for the client
    stream.extend(quote! {
        pub struct #client_ident {}

        impl shoal_core::shared::traits::QuerySupport for #client_ident {
            /// The different tables or types of queries we will handle
            type QueryKinds = #query_ident;

            /// The different tables we can get responses from
            type ResponseKinds = #response_ident;

            /// Make sure queries have succeeded based on some critiera
            ///
            /// # Arguments
            ///
            /// * `opts` - The options to use when validating query responses
            fn succeeded(
                archived: &<Self::ResponseKinds as rkyv::Archive>::Archived,
                opts: shoal_core::client::QuerySuceededOpts,
            ) -> Result<(), shoal_core::client::Errors> {
                match archived {
                    #(#succeeded_arms)*
                }
            }

            /// Get the kind of query this is a response to
            ///
            /// # Arguments
            ///
            /// * `archived` - The archived query to get the query kind for
            fn kind(archived: &<Self::ResponseKinds as rkyv::Archive>::Archived) -> shoal_core::shared::responses::ResponseActionNames {
                match archived {
                    #(#kind_arms)*
                }
            }

            /// Get the exists result from an Exists response
            ///
            /// # Arguments
            ///
            /// * `archived` - The archived response to get the exists result from
            fn get_exists(archived: &<Self::ResponseKinds as rkyv::Archive>::Archived) -> Option<bool> {
                match archived {
                    #(#get_exists_arms)*
                }
            }

        }
    });
}
