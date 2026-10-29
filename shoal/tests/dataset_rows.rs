//! Tests for the dataset traits a table derive emits ([F66](../../docs/src/features/dataset-benchmarks.md))
//!
//! A benchmark finds a table by the name of the file its rows are in and is called back with the
//! row type, so these check the three things that path depends on: every table is listed with
//! whether it opted in, a name reaches the right row type or is refused by name, and the queries
//! a row builds are the ones a caller would have built by hand.

use shoal::shared::dataset::{DatasetError, DatasetRow, DatasetSupport, DatasetVisitor};
use shoal::shared::traits::QuerySupport;

/// A schema with a table of each kind that opted in, and one that did not
mod catalog {
    use deepsize2::DeepSizeOf;
    use rkyv::{Archive, Deserialize, Serialize};
    use shoal::tables::{EphemeralSortedTable, EphemeralUnsortedTable};
    use shoal_derive::{db, ShoalSortedTable, ShoalUnsortedTable};

    /// An unsorted row keyed by a number, opted in
    #[derive(
        Debug,
        Archive,
        Serialize,
        Deserialize,
        serde::Deserialize,
        Clone,
        ShoalUnsortedTable,
        PartialEq,
        DeepSizeOf,
    )]
    #[rkyv(derive(Debug))]
    #[shoal_table(db = "Catalog", dataset)]
    pub struct Item {
        /// The item's id
        #[shoal(partition)]
        pub id: u64,
        /// The item's name
        #[shoal(filter)]
        pub name: String,
    }

    /// A sorted row keyed by two strings, opted in
    #[derive(
        Debug,
        Archive,
        Serialize,
        Deserialize,
        serde::Deserialize,
        Clone,
        ShoalSortedTable,
        PartialEq,
        DeepSizeOf,
    )]
    #[rkyv(derive(Debug))]
    #[shoal_table(db = "Catalog", dataset)]
    pub struct Review {
        /// The item reviewed
        #[shoal(partition)]
        pub item: String,
        /// Who reviewed it
        #[shoal(sort)]
        pub author: String,
        /// What they said
        pub body: String,
    }

    /// An unsorted row keyed by two fields, a warehouse and an item, opted in
    #[derive(
        Debug,
        Archive,
        Serialize,
        Deserialize,
        serde::Deserialize,
        Clone,
        ShoalUnsortedTable,
        PartialEq,
        DeepSizeOf,
    )]
    #[rkyv(derive(Debug))]
    #[shoal_table(db = "Catalog", dataset)]
    pub struct Stock {
        /// The warehouse holding the stock
        #[shoal(partition)]
        pub warehouse: String,
        /// The item stocked
        #[shoal(partition)]
        pub item: u64,
        /// How many are held
        pub count: u64,
    }

    /// A row that did not opt in, and has no serde at all
    #[derive(
        Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, DeepSizeOf,
    )]
    #[rkyv(derive(Debug))]
    #[shoal_table(db = "Catalog")]
    pub struct Audit {
        /// The entry's id
        #[shoal(partition)]
        pub id: u64,
    }

    /// The schema
    #[db]
    pub struct Catalog {
        /// Items
        pub items: EphemeralUnsortedTable<Item>,
        /// Reviews of items
        pub reviews: EphemeralSortedTable<Review>,
        /// Stock, by warehouse and item
        pub stock: EphemeralUnsortedTable<Stock>,
        /// An audit log no benchmark may load
        pub audit: EphemeralUnsortedTable<Audit>,
    }
}

use catalog::{CatalogClient, CatalogQueryKinds};

/// A visitor that parses one json row of whatever table it is handed and builds its queries
struct Build {
    /// The row, as json
    json: &'static str,
}

/// What [`Build`] builds: the table it was called for, the insert, and the read back of the row
type Built = (&'static str, bool, String, String);

impl DatasetVisitor<CatalogQueryKinds> for Build {
    /// The table, whether it is sorted, and the two queries as debug text
    type Output = Built;

    /// Parse the row and build its insert and its read back
    ///
    /// # Arguments
    ///
    /// * `table` - The table the row is in
    fn visit<R: DatasetRow<CatalogQueryKinds>>(self, table: &'static str) -> Self::Output {
        // the row parses as the type the table was found to have
        let row: R = serde_json::from_str(self.json).expect("the row parses");
        // its read key reads it back, alone
        let read = R::read_query(&[row.read_key()]);
        let insert = row.insert_query();
        (table, R::SORTED, format!("{insert:?}"), format!("{read:?}"))
    }
}

/// Every table is listed in field order with whether it opted in
#[test]
fn every_table_is_listed_with_whether_it_opted_in() {
    // the flags are read off each table's own derive
    assert_eq!(
        CatalogClient::dataset_tables(),
        &[
            ("Item", true),
            ("Review", true),
            ("Stock", true),
            ("Audit", false)
        ]
    );
    // and the names are the ones the client reports, so a file is named after either
    let names: Vec<&str> = CatalogClient::dataset_tables()
        .iter()
        .map(|(name, _)| *name)
        .collect();
    assert_eq!(names, CatalogClient::table_names());
}

/// An unsorted row builds the insert and the get a caller would have
#[test]
fn an_unsorted_row_builds_its_insert_and_get() {
    // find the table by name and build its queries
    let (table, sorted, insert, read) = CatalogClient::visit_table(
        "Item",
        Build {
            json: r#"{"id": 7, "name": "lamp"}"#,
        },
    )
    .expect("Item opted in");
    assert_eq!(table, "Item");
    assert!(!sorted);
    // the insert is the row's own conversion
    let row = catalog::Item {
        id: 7,
        name: "lamp".to_string(),
    };
    assert_eq!(insert, format!("{:?}", CatalogQueryKinds::from(row)));
    // and the read is a get of its partition
    let get: CatalogQueryKinds = catalog::ItemGet::new(vec![7]).into();
    assert_eq!(read, format!("{get:?}"));
}

/// A sorted row is read back by its partition and its sort key
#[test]
fn a_sorted_row_builds_a_get_of_its_sort_key() {
    // find the table by name and build its queries
    let (table, sorted, _, read) = CatalogClient::visit_table(
        "Review",
        Build {
            json: r#"{"item": "lamp", "author": "ada", "body": "bright"}"#,
        },
    )
    .expect("Review opted in");
    assert_eq!(table, "Review");
    assert!(sorted);
    // the get names the partition and the exact row in it
    let get: CatalogQueryKinds = catalog::ReviewGet::new(vec!["lamp".to_string()])
        .sort_keys(vec!["ada".to_string()])
        .into();
    assert_eq!(read, format!("{get:?}"));
}

/// A row keyed by two fields is read back by both, as one composite key
#[test]
fn a_composite_keyed_row_builds_a_get_of_both_fields() {
    // find the table by name and build its queries
    let (table, sorted, insert, read) = CatalogClient::visit_table(
        "Stock",
        Build {
            json: r#"{"warehouse": "north", "item": 7, "count": 3}"#,
        },
    )
    .expect("Stock opted in");
    assert_eq!(table, "Stock");
    assert!(!sorted);
    // the insert is the row's own conversion
    let row = catalog::Stock {
        warehouse: "north".to_string(),
        item: 7,
        count: 3,
    };
    assert_eq!(insert, format!("{:?}", CatalogQueryKinds::from(row)));
    // and the read is a get of the one partition both fields name together
    let get: CatalogQueryKinds = catalog::StockGet::new(vec![("north".to_string(), 7)]).into();
    assert_eq!(read, format!("{get:?}"));
}

/// One get of several sorted keys names each partition once and every sort key
#[test]
fn a_get_of_several_sorted_keys_names_each_partition_once() {
    // three keys over two partitions
    let keys = vec![
        ("lamp".to_string(), "ada".to_string()),
        ("desk".to_string(), "bo".to_string()),
        ("lamp".to_string(), "cy".to_string()),
    ];
    let read = <catalog::Review as DatasetRow<CatalogQueryKinds>>::read_query(&keys);
    // the partitions in the order they were first named, and every sort key
    let get: CatalogQueryKinds =
        catalog::ReviewGet::new(vec!["lamp".to_string(), "desk".to_string()])
            .sort_keys(vec!["ada".to_string(), "bo".to_string(), "cy".to_string()])
            .into();
    assert_eq!(format!("{read:?}"), format!("{get:?}"));
}

/// A table that did not opt in is refused by its name
#[test]
fn a_table_that_did_not_opt_in_is_refused_by_name() {
    // the table exists, so the refusal names it and says what to add
    let refused = CatalogClient::visit_table("Audit", Build { json: "{}" }).unwrap_err();
    assert_eq!(refused, DatasetError::NotOptedIn { table: "Audit" });
    assert!(refused.to_string().contains("dataset"));
}

/// A name no table has is refused with every name the database does have
#[test]
fn an_unknown_table_is_refused_with_the_known_ones() {
    // names are exact: the field name and a lowercase spelling are both unknown
    for name in ["items", "item", "Nope"] {
        let refused = CatalogClient::visit_table(name, Build { json: "{}" }).unwrap_err();
        assert_eq!(
            refused,
            DatasetError::UnknownTable {
                table: name.to_string(),
                known: vec!["Item", "Review", "Stock", "Audit"],
            }
        );
    }
}
