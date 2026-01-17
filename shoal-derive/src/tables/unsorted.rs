//! generate the traits and code for unsorted tables

use quote::quote;
use syn::Ident;

/// Extend a token stream with an update method for ShoalUnsortedTable
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the type we are extending
/// * `update_fields` - The fields used for updates (ident, type)
pub fn add(
    stream: &mut proc_macro2::TokenStream,
    name: &Ident,
    update_fields: &[(syn::Ident, syn::Type)],
) {
    // build the update assignments
    let update_assignments: Vec<_> = update_fields
        .iter()
        .map(|(ident, _)| {
            quote! {
                if let Some(ref new_val) = update.update.#ident {
                    self.#ident = new_val.clone();
                }
            }
        })
        .collect();
    // generate the ShoalUnsortedTable implementation
    stream.extend(quote! {
        #[automatically_derived]
        impl shoal_core::shared::traits::ShoalUnsortedTable for #name {
            fn update(&mut self, update: &shoal_core::shared::queries::UnsortedUpdate<Self>) {
                #(#update_assignments)*
            }
        }
    });
}
