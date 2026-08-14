//! Generate the FromShoal trait implementation for a type

use quote::{quote};
use syn::Ident;

/// Extend a token stream with a FromShoal implementation
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the type we are extending
/// * `db_name` - The name of the database
/// * `response_name` - The name of the response type
pub fn add(
    stream: &mut proc_macro2::TokenStream,
    name: &Ident,
    db_name: &Ident,
    response_name: &Ident,
) {
    // extend our token stream
    stream.extend(
        quote! {
            #[automatically_derived]
            impl shoal_core::FromShoal<#db_name> for #name  {
                type ResponseKinds = <#db_name as shoal_core::shared::traits::QuerySupport>::ResponseKinds;

                fn retrieve(archived: &#response_name) -> Result<&rkyv::option::ArchivedOption<rkyv::vec::ArchivedVec<<Self as rkyv::Archive>::Archived>>, shoal_core::client::Errors> {
                    // make sure its the right data kind
                    if let #response_name::#name(action) = archived {
                        // make sure its a get action
                       if let shoal_core::shared::responses::ArchivedResponseAction::Get(rows) = &action.data {
                            return Ok(rows);
                        }
                        // a query that failed is answered with the failure rather than with
                        // "you asked for the wrong type", which is what it used to look like
                        if let shoal_core::shared::responses::ArchivedResponseAction::Error(error) = &action.data {
                            return Err(shoal_core::client::Errors::Server {
                                query_id: Some(action.id),
                                index: Some(action.index.to_native() as usize),
                                code: error.code(),
                                msg: error.msg().to_owned(),
                            });
                        }
                    }
                    Err(shoal_core::client::Errors::WrongType("Wrong Type!".to_owned()))
                }
            }
        }
    );
}

