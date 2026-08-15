//! Generate a get struct for a type

use quote::{format_ident, quote};
use syn::Ident;

/// Extend a token stream with an Get struct definition
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
) {
    // build our struct names
    let get_name = format_ident!("{}Get", name);
    let filter_name = format_ident!("{}Filter", name);
    let projection_enum = format_ident!("{}Projection", name);
    // also add the exists struct
    add_unsorted_exists(stream, name, partition_fields);
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
    // generate our get struct for this type and its methods
    stream.extend(quote! {
        #[derive(Debug, Clone, ::shoal::rkyv::Archive, ::shoal::rkyv::Serialize, ::shoal::rkyv::Deserialize)]
        #[rkyv(derive(Debug))]
        pub struct #get_name {
            /// The partition keys of the partitions to get
            pub partition_keys: Vec<#partition_key_type>,
            /// Any filters to use when deciding what rows to return
            pub filters: Option<#filter_name>,
            /// The number of rows to return
            pub limit: Option<usize>,
            /// The subset of each rows fields this get is asking to be answered with
            pub projection: #projection_enum,
        }

        #[automatically_derived]
        impl ::shoal::shared::traits::RkyvSupport for #get_name {}

        #[automatically_derived]
        impl #get_name {
            /// Create a new get query for this type
            ///
            /// The partitions are read in the order they are given here, and that is the
            /// order their rows come back in.
            ///
            /// # Arguments
            ///
            /// * `partition_keys` - The keys of the partitions to read
            pub fn new(partition_keys: Vec<#partition_key_type>) -> Self {
                #get_name {
                    partition_keys,
                    filters: None,
                    limit: None,
                    // a get asks for the whole row until it is told to project it
                    projection: #projection_enum::Full,
                }
            }

                /// Set a filter for getting rows
                ///
                /// # Arguments
                ///
                /// * `filter` - The filters to set
                pub fn filters(mut self, filters: #filter_name) -> Self {
                    // set our filters
                    self.filters = Some(filters);
                    self
                }

                /// Set the max number of rows to retrieve
                ///
                /// An unsorted partition holds exactly one row, so a limit only bites on a
                /// get naming several partitions. The rows it keeps are the ones from the
                /// partitions named first.
                ///
                /// # Arguments
                ///
                /// * `limit` - The max number of rows to return
                pub fn limit(mut self, limit: usize) -> Self {
                    // set our limit
                    self.limit = Some(limit);
                    self
                }

            /// Ask for a subset of each rows fields instead of the whole row
            ///
            /// A projection copies only the fields it names out of each row, which for a wide
            /// row is most of the cost of reading it. The response comes back as the
            /// projection rather than as the row, so it is retrieved with
            /// `response.access::<P>()` and not `response.access::<Self>()`.
            ///
            /// The type parameter is what names the projection, so a projection of another
            /// table will not compile here rather than failing when the query is answered.
            pub fn projection<P>(mut self) -> Self
            where
                P: ::shoal::shared::traits::ShoalProjection<Row = #name>,
            {
                // remember which of this tables projections was asked for
                self.projection = <P as ::shoal::shared::traits::ShoalProjection>::PROJECTION;
                self
            }
        }
    });
}

/// Extend a token stream with a sorted Get struct definition
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
    // build our struct names
    let get_name = format_ident!("{}Get", name);
    let filter_name = format_ident!("{}Filter", name);
    let projection_enum = format_ident!("{}Projection", name);
    // also add the exists struct
    add_sorted_exists(stream, name, partition_fields, sort_fields);
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
    // generate our get struct for this type and its methods
    stream.extend(quote! {
        #[derive(Debug, Clone, ::shoal::rkyv::Archive, ::shoal::rkyv::Serialize, ::shoal::rkyv::Deserialize)]
        #[rkyv(derive(Debug))]
        pub struct #get_name {
            /// The partition keys to get data from
            pub partition_keys: Vec<#partition_key_type>,
            /// Which rows of each partition this get is asking for
            pub sort_select: ::shoal::shared::queries::SortSelect<#sort_key_type>,
            /// Any filters to use when deciding what rows to return
            pub filters: Option<#filter_name>,
            /// The number of rows to return
            pub limit: Option<usize>,
            /// The subset of each rows fields this get is asking to be answered with
            pub projection: #projection_enum,
        }

        #[automatically_derived]
        impl ::shoal::shared::traits::RkyvSupport for #get_name {}

        #[automatically_derived]
        impl #get_name {
            /// Create a new get query for this type
            ///
            /// # Arguments
            ///
            /// * `partition_keys` - The partitions to get data from
            pub fn new(partition_keys: Vec<#partition_key_type>) -> Self {
                #get_name {
                    partition_keys,
                    sort_select: ::shoal::shared::queries::SortSelect::All,
                    filters: None,
                    limit: None,
                    // a get asks for the whole row until it is told to project it
                    projection: #projection_enum::Full,
                }
            }

            /// Set the sort keys of the rows this get should return
            ///
            /// These name rows rather than bound them: the get returns the rows it named
            /// and no others, in sort order rather than in the order they were named. Use
            /// `sort_range` to bound them instead, and leave both unset to return every
            /// row in each of this gets partitions.
            ///
            /// # Arguments
            ///
            /// * `sort_keys` - The sort keys of the rows to return
            pub fn sort_keys(mut self, sort_keys: Vec<#sort_key_type>) -> Self {
                // set our sort keys, replacing whatever this get selected before
                self.sort_select = ::shoal::shared::queries::SortSelect::Keys(sort_keys);
                self
            }

            /// Bound the rows this get should return by a range of sort keys
            ///
            /// The rows inside the range come back in sort order, and a limit stops the
            /// walk early - which is what makes paging a partition cost a page. An
            /// exclusive lower bound of the last row of a page is the cursor onto the next
            /// one, so `SortRange::after(last)` with the same limit reads the next page.
            ///
            /// This replaces any sort keys this get named, since a get either matches rows
            /// against a set of keys or bounds them by a range.
            ///
            /// # Arguments
            ///
            /// * `sort_range` - The range of sort keys to return rows from
            pub fn sort_range(
                mut self,
                sort_range: ::shoal::shared::queries::SortRange<#sort_key_type>,
            ) -> Self {
                // set our range, replacing whatever this get selected before
                self.sort_select = ::shoal::shared::queries::SortSelect::Range(sort_range);
                self
            }

            /// Set a filter for getting rows
            pub fn filters(mut self, filters: #filter_name) -> Self {
                self.filters = Some(filters);
                self
            }

            /// Set the max number of rows to retrieve
            ///
            /// # Arguments
            ///
            /// * `limit` - The max number of rows to return
            pub fn limit(mut self, limit: usize) -> Self {
                self.limit = Some(limit);
                self
            }

            /// Ask for a subset of each rows fields instead of the whole row
            ///
            /// A projection copies only the fields it names out of each row, which for a wide
            /// row is most of the cost of reading it. The response comes back as the
            /// projection rather than as the row, so it is retrieved with
            /// `response.access::<P>()` and not `response.access::<Self>()`.
            ///
            /// The type parameter is what names the projection, so a projection of another
            /// table will not compile here rather than failing when the query is answered.
            pub fn projection<P>(mut self) -> Self
            where
                P: ::shoal::shared::traits::ShoalProjection<Row = #name>,
            {
                // remember which of this tables projections was asked for
                self.projection = <P as ::shoal::shared::traits::ShoalProjection>::PROJECTION;
                self
            }
        }
    });
}

/// Extend a token stream with a sorted Exists struct definition
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the type we are extending
/// * `partition_fields` - The partition key fields
/// * `sort_fields` - The sort key fields
fn add_sorted_exists(
    stream: &mut proc_macro2::TokenStream,
    name: &Ident,
    partition_fields: &[(syn::Ident, syn::Type)],
    sort_fields: &[(syn::Ident, syn::Type)],
) {
    // build our struct names
    let exists_name = format_ident!("{}Exists", name);
    let filter_name = format_ident!("{}Filter", name);
    // Build the partition key type
    let partition_key_type = if partition_fields.len() == 1 {
        let (_, ty) = &partition_fields[0];
        quote! { #ty }
    } else {
        let types: Vec<_> = partition_fields.iter().map(|(_, ty)| ty).collect();
        quote! { (#(#types),*) }
    };
    // Build the sort key type
    let sort_key_type = if sort_fields.len() == 1 {
        let (_, ty) = &sort_fields[0];
        quote! { #ty }
    } else {
        let types: Vec<_> = sort_fields.iter().map(|(_, ty)| ty).collect();
        quote! { (#(#types),*) }
    };
    // generate our exists struct for this type and its methods
    stream.extend(quote! {
        #[derive(Debug, Clone, ::shoal::rkyv::Archive, ::shoal::rkyv::Serialize, ::shoal::rkyv::Deserialize)]
        #[rkyv(derive(Debug))]
        pub struct #exists_name {
            /// The partition keys to check for data in
            pub partition_keys: Vec<#partition_key_type>,
            /// Which rows of each partition this exists is asking about
            pub sort_select: ::shoal::shared::queries::SortSelect<#sort_key_type>,
            /// Any filters to use when deciding what rows to check
            pub filters: Option<#filter_name>,
        }

        #[automatically_derived]
        impl ::shoal::shared::traits::RkyvSupport for #exists_name {}

        #[automatically_derived]
        impl ::shoal::shared::traits::ExistsQuery for #exists_name {}

        #[automatically_derived]
        impl #exists_name {
            /// Create a new exists query for this type
            ///
            /// # Arguments
            ///
            /// * `partition_keys` - The partitions to check for data in
            pub fn new(partition_keys: Vec<#partition_key_type>) -> Self {
                #exists_name {
                    partition_keys,
                    sort_select: ::shoal::shared::queries::SortSelect::All,
                    filters: None,
                }
            }

            /// Set the sort keys of the rows this exists should check for
            ///
            /// An exists naming sort keys asks whether any of those rows is here. One
            /// with neither keys nor a range set asks whether its partitions hold any row
            /// at all.
            ///
            /// # Arguments
            ///
            /// * `sort_keys` - The sort keys of the rows to check for
            pub fn sort_keys(mut self, sort_keys: Vec<#sort_key_type>) -> Self {
                // set our sort keys, replacing whatever this exists selected before
                self.sort_select = ::shoal::shared::queries::SortSelect::Keys(sort_keys);
                self
            }

            /// Bound the rows this exists should check for by a range of sort keys
            ///
            /// This asks whether any row falls inside the range, which a sorted partition
            /// answers with a seek rather than a walk.
            ///
            /// This replaces any sort keys this exists named, since an exists either
            /// matches rows against a set of keys or bounds them by a range.
            ///
            /// # Arguments
            ///
            /// * `sort_range` - The range of sort keys to check for rows in
            pub fn sort_range(
                mut self,
                sort_range: ::shoal::shared::queries::SortRange<#sort_key_type>,
            ) -> Self {
                // set our range, replacing whatever this exists selected before
                self.sort_select = ::shoal::shared::queries::SortSelect::Range(sort_range);
                self
            }

            /// Set a filter for checking rows
            pub fn filters(mut self, filters: #filter_name) -> Self {
                self.filters = Some(filters);
                self
            }
        }
    });
}

/// Extend a token stream with a unsorted Exists struct definition
///
/// # Arguments
///
/// * `stream` - The stream to extend
/// * `name` - The name of the type we are extending
/// * `partition_fields` - The partition key fields
/// * `sort_fields` - The sort key fields
fn add_unsorted_exists(
    stream: &mut proc_macro2::TokenStream,
    name: &Ident,
    partition_fields: &[(syn::Ident, syn::Type)],
) {
    // build our struct names
    let exists_name = format_ident!("{}Exists", name);
    let filter_name = format_ident!("{}Filter", name);
    // Build the partition key type
    let partition_key_type = if partition_fields.len() == 1 {
        let (_, ty) = &partition_fields[0];
        quote! { #ty }
    } else {
        let types: Vec<_> = partition_fields.iter().map(|(_, ty)| ty).collect();
        quote! { (#(#types),*) }
    };
    // generate our exists struct for this type and its methods
    stream.extend(quote! {
        #[derive(Debug, Clone, ::shoal::rkyv::Archive, ::shoal::rkyv::Serialize, ::shoal::rkyv::Deserialize)]
        #[rkyv(derive(Debug))]
        pub struct #exists_name {
            /// The partition key to check for data in
            pub partition_key: #partition_key_type,
            /// Any filters to use when deciding what rows to check
            pub filters: Option<#filter_name>,
        }

        #[automatically_derived]
        impl ::shoal::shared::traits::RkyvSupport for #exists_name {}

        #[automatically_derived]
        impl ::shoal::shared::traits::ExistsQuery for #exists_name {}

        #[automatically_derived]
        impl #exists_name {
            /// Create a new exists query for this type
            ///
            /// # Arguments
            ///
            /// * `partition_key` - The partition to check for data in
            pub fn new(partition_key: #partition_key_type) -> Self {
                #exists_name {
                    partition_key,
                    filters: None,
                }
            }

            /// Set a filter for checking rows
            pub fn filters(mut self, filters: #filter_name) -> Self {
                self.filters = Some(filters);
                self
            }
        }
    });
}
