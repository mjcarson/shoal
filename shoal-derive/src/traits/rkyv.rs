//! Generate the RkyvSupport trait implementation for a type

use quote::quote;
use syn::Ident;

/// Extend a token stream with a RkyvSupport impl
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the type we are extending
pub fn add(stream: &mut proc_macro2::TokenStream, name: &Ident) {
    // extend our token stream
    stream.extend(quote! {
        #[automatically_derived]
        impl ::shoal::shared::traits::RkyvSupport for #name {}
    });
}
