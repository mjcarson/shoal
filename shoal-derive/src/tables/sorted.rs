//! Generate the traits and code for sorted tables

use quote::quote;
use syn::Ident;

/// Extend a token stream with an update method for ShoalSortedTable
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the type we are extending
/// * `update_fields` - The fields used for updates (ident, type)
pub fn add(
    stream: &mut proc_macro2::TokenStream,
    name: &Ident,
    sort_fields: &[(syn::Ident, syn::Type)],
    update_fields: &[(syn::Ident, syn::Type)],
) {
    //// Must have at least one sort field
    //if sort_fields.is_empty() {
    //    panic!("ShoalSortedTable requires at least one sort field!");
    //}
    // Build the Sort type and get_sort body
    let (sort_type, get_sort_body) = if sort_fields.len() == 1 {
        // Single field - clone and return
        let (ident, ty) = &sort_fields[0];
        (quote! { #ty }, quote! { self.#ident.clone() })
    } else {
        // Multiple fields - build a tuple
        let types: Vec<_> = sort_fields.iter().map(|(_, ty)| ty).collect();
        let clones: Vec<_> = sort_fields
            .iter()
            .map(|(ident, _)| quote! { self.#ident.clone() })
            .collect();
        (quote! { (#(#types),*) }, quote! { (#(#clones),*) })
    };
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
        impl shoal_core::shared::traits::ShoalSortedTable for #name {
            /// The sort type for this data
            type Sort = #sort_type;

            /// Build the sort key for this row
            #[inline]
            fn get_sort(&self) -> Self::Sort {
                #get_sort_body
            }

            /// Apply an update to a single row
            ///
            /// # Arguments
            ///
            /// * `update` - The update to apply to a specific row
            fn update(&mut self, update: &shoal_core::shared::queries::SortedUpdate<Self>) {
                #(#update_assignments)*
            }
        }
    });
}
