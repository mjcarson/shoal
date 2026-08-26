//! Generates the mirror that writes a row back out of the archive it was read from
//!
//! A get answered off disk reads `Archived<Row>` and had to materialize a `Row` per row returned
//! purely so that there was something rkyv knew how to serialize. rkyv only serializes in one
//! direction and cannot write an archived value back into its own layout, so the missing
//! direction is generated here, field by field, against the archived struct rkyv's own derive
//! emits. That is [O40](../../../docs/src/appendix/optimizations.md).
//!
//! # What it can and cannot see
//!
//! This macro sees the *syntax* of a field's type and never its definition, so a `Vec<Tag>` for a
//! `Tag` declared in another crate is a name and nothing more. Such a field is written the way
//! every field used to be written — materialized on its own — while every field around it is
//! written straight out of the archive. Nothing is refused and no schema stops compiling because
//! of one field this cannot see inside; the row simply keeps paying for that one.
use quote::{format_ident, quote};
use syn::Ident;

use super::utils::FieldShape;

/// Extend a token stream with a `Rearchive` implementation for a row or a projection
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the type we are extending
/// * `fields` - Every field of the type, with whether it opted into the mirror (ident, type, opt in)
pub(crate) fn add(
    stream: &mut proc_macro2::TokenStream,
    name: &Ident,
    fields: &[(syn::Ident, syn::Type, bool)],
) {
    // rkyv names the archived struct after the row, and munging it is how a field is resolved
    let archived_name = format_ident!("Archived{}", name);
    let resolver_name = format_ident!("{}ArchivedResolver", name);
    // work out how each field has to be written, from the syntax of its type
    let shapes: Vec<FieldShape> = fields
        .iter()
        .map(|(_, ty, opted_in)| FieldShape::of(ty, *opted_in))
        .collect();
    // declare what each field hands from its serialize to its resolve
    let resolver_fields = fields.iter().zip(&shapes).map(|((ident, ty, _), shape)| {
        let doc = format!("What `{name}::{ident}` produced on its way out of the archive");
        match shape {
            // a materialized field carries the value it built, since its resolve still needs it
            FieldShape::Opaque => quote! {
                #[doc = #doc]
                #ident: ::shoal::shared::rearchive::OwnedResolver<#ty>,
            },
            // everything else carries whatever the mirror for its type produces
            FieldShape::Mirrored | FieldShape::PortableVec => quote! {
                #[doc = #doc]
                #ident: <#ty as ::shoal::shared::rearchive::Rearchive>::ArchivedResolver,
            },
        }
    });
    // write each field's out of line data, reading it out of the archive rather than a row
    let serialize_fields = fields.iter().zip(&shapes).map(|((ident, ty, _), shape)| match shape {
        FieldShape::PortableVec => quote! {
            #ident: ::shoal::shared::rearchive::serialize_portable_vec(
                &archived.#ident,
                serializer,
            )?,
        },
        FieldShape::Mirrored => quote! {
            #ident: <#ty as ::shoal::shared::rearchive::Rearchive>::serialize_archived(
                &archived.#ident,
                serializer,
            )?,
        },
        FieldShape::Opaque => quote! {
            #ident: ::shoal::shared::rearchive::serialize_via_owned::<#ty, S>(
                &archived.#ident,
                serializer,
            )?,
        },
    });
    // then write each field's fixed size part on top of what was serialized for it
    let resolve_fields = fields.iter().zip(&shapes).map(|((ident, ty, _), shape)| match shape {
        FieldShape::PortableVec => quote! {
            ::shoal::shared::rearchive::resolve_vec(&archived.#ident, resolver.#ident, #ident);
        },
        FieldShape::Mirrored => quote! {
            <#ty as ::shoal::shared::rearchive::Rearchive>::resolve_archived(
                &archived.#ident,
                resolver.#ident,
                #ident,
            );
        },
        FieldShape::Opaque => quote! {
            ::shoal::shared::rearchive::resolve_via_owned::<#ty>(resolver.#ident, #ident);
        },
    });
    // the fields to munge the out place into, which is one per field in declaration order
    let field_idents: Vec<&syn::Ident> = fields.iter().map(|(ident, _, _)| ident).collect();
    let resolver_doc = format!(
        "What each field of a [`{name}`] produced on its way back out of an archive\n\n\
         This is the mirror's resolver, not rkyv's. A field that had to be materialized keeps the \
         value it built here, because rkyv resolves a value after serializing it and an archived \
         row has no owned value to keep."
    );
    stream.extend(quote! {
        #[doc = #resolver_doc]
        #[automatically_derived]
        pub struct #resolver_name {
            #(#resolver_fields)*
        }

        /// A row can be written back out of the archive it was read from
        ///
        /// The rows of a partition that is still the archive it was loaded from cannot be pointed
        /// at the way resident rows can, because what the partition holds is `Archived<Self>` and
        /// not `Self`. This writes those rows straight into the reply instead of materializing
        /// one per row first ([O40](../../../docs/src/appendix/optimizations.md)), and what it
        /// writes is byte for byte what serializing the materialized row would have written.
        #[automatically_derived]
        impl ::shoal::shared::rearchive::Rearchive for #name {
            /// What this row's fields produced, which is not rkyv's resolver for this row
            type ArchivedResolver = #resolver_name;

            /// Write every field's out of line data, straight out of the archive holding it
            ///
            /// # Arguments
            ///
            /// * `archived` - The archived row to write back out
            /// * `serializer` - The serializer to write the out of line data into
            fn serialize_archived<S>(
                archived: &<Self as ::shoal::rkyv::Archive>::Archived,
                serializer: &mut S,
            ) -> Result<Self::ArchivedResolver, <S as ::shoal::rkyv::rancor::Fallible>::Error>
            where
                S: ::shoal::rkyv::rancor::Fallible
                    + ::shoal::rkyv::ser::Writer
                    + ::shoal::rkyv::ser::Allocator
                    + ?Sized,
                <S as ::shoal::rkyv::rancor::Fallible>::Error: ::shoal::rkyv::rancor::Source,
            {
                Ok(#resolver_name {
                    #(#serialize_fields)*
                })
            }

            /// Write the fixed size row on top of what was serialized for its fields
            ///
            /// # Arguments
            ///
            /// * `archived` - The archived row being written back out
            /// * `resolver` - What each of its fields produced
            /// * `out` - Where the archived row belongs
            fn resolve_archived(
                archived: &<Self as ::shoal::rkyv::Archive>::Archived,
                resolver: Self::ArchivedResolver,
                out: ::shoal::rkyv::Place<<Self as ::shoal::rkyv::Archive>::Archived>,
            ) {
                // split the place the row belongs in into one place per field
                ::shoal::rkyv::munge::munge!(let #archived_name { #(#field_idents),* } = out);
                #(#resolve_fields)*
            }
        }
    });
}
