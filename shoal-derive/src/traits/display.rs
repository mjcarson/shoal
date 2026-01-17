//! Generate a display trait for a shoal database

use quote::quote;
use syn::Ident;

/// Build the FromStr implementation
pub fn add(stream: &mut proc_macro2::TokenStream, enum_ident: &Ident, variants: &Vec<Ident>) {
    // get the variant names as a string
    let variant_names: Vec<_> = variants
        .iter()
        .map(|variant_name| variant_name.to_string())
        .collect();
    // build our from str arms
    let to_str_arms = variants
        .iter()
        .zip(&variant_names)
        .map(|(variant, variant_name)| {
            quote! {
                #enum_ident::#variant => write!(f, "{}", #variant_name),
            }
        });
    // add our FromStr impl to our token stream
    stream.extend(quote! {
        impl std::fmt::Display for #enum_ident {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    #(#to_str_arms)*
                }
            }
        }
    });
}
