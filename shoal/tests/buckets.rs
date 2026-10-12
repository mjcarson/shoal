//! Buckets in the schema and the tables they generate
//! ([S2](../../docs/src/object-storage/buckets.md), [F79](../../docs/src/features/bucket-schema.md))
//!
//! A field `posters: Bucket<Posters>` generates two persistent unsorted tables, their rows, a
//! bucket enum and the marker's `BucketRows` impl. These hold what S2's acceptance rows ask of the
//! schema - the tables and their ids, the fingerprint, the ids under a reorder - and what only a
//! running server can show: that a bucket's tables start and stop with the node, and that a client's
//! query of one is refused at the front door whoever sends it.
//!
//! Each schema lives in a module of its own, because `#[db]` mints items named after each row and
//! bucket, so two schemas declaring the same ones collide in one namespace.

// a database struct and a bucket's marker are named as types and never built, which is all a schema
// asks of either
#![allow(dead_code)]

use shoal::client::Shoal;
use shoal::server::conf::Auth;
use shoal::server::ShoalPool;
use shoal::shared::auth::Credentials;
use shoal::shared::grants::{Privileges, EVERY_BUCKET, EVERY_TABLE};
use shoal::shared::identity::{BucketId, TableId};
use shoal::shared::objects::{Attributes, Geometry, Label, ObjectEntry, ObjectState};
use shoal::shared::protocol::error::ErrorCode;
use shoal::shared::traits::QuerySupport;

mod utils;

use utils::TestError;

/// The password every user in these tests authenticates with
const PASSWORD: &str = "hunter2";

/// A schema of one table and nothing else, which the others are read against
mod plain {
    use deepsize2::DeepSizeOf;
    use rkyv::{Archive, Deserialize, Serialize};
    use shoal::storage::FileSystem;
    use shoal::tables::PersistentUnsortedTable;
    use shoal_derive::{db, ShoalUnsortedTable};

    /// A movie, keyed by its id
    #[derive(
        Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, DeepSizeOf,
    )]
    #[rkyv(derive(Debug))]
    #[shoal_table(db = "Media")]
    pub struct Movie {
        /// The movie's id
        #[shoal(partition)]
        pub id: u64,
        /// Its title
        #[shoal(update)]
        pub title: String,
    }

    /// The database our media service is built on, before it kept posters
    #[db]
    pub struct Media {
        /// The movies we know about
        pub movies: PersistentUnsortedTable<Movie, FileSystem>,
    }
}

/// The same schema with a bucket declared after its table
mod bucketed {
    use deepsize2::DeepSizeOf;
    use rkyv::{Archive, Deserialize, Serialize};
    use shoal::storage::FileSystem;
    use shoal::tables::{Bucket, PersistentUnsortedTable};
    use shoal_derive::{db, ShoalUnsortedTable};

    /// A movie, keyed by its id
    #[derive(
        Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, DeepSizeOf,
    )]
    #[rkyv(derive(Debug))]
    #[shoal_table(db = "Media")]
    pub struct Movie {
        /// The movie's id
        #[shoal(partition)]
        pub id: u64,
        /// Its title
        #[shoal(update)]
        pub title: String,
    }

    /// Names the bucket our posters are kept in
    pub struct Posters;

    /// The database our media service is built on
    #[db]
    pub struct Media {
        /// The movies we know about
        pub movies: PersistentUnsortedTable<Movie, FileSystem>,
        /// The posters for those movies
        pub posters: Bucket<Posters>,
    }
}

/// The same schema with its bucket declared ahead of its table
mod reordered {
    use deepsize2::DeepSizeOf;
    use rkyv::{Archive, Deserialize, Serialize};
    use shoal::storage::FileSystem;
    use shoal::tables::{Bucket, PersistentUnsortedTable};
    use shoal_derive::{db, ShoalUnsortedTable};

    /// A movie, keyed by its id
    #[derive(
        Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, DeepSizeOf,
    )]
    #[rkyv(derive(Debug))]
    #[shoal_table(db = "Media")]
    pub struct Movie {
        /// The movie's id
        #[shoal(partition)]
        pub id: u64,
        /// Its title
        #[shoal(update)]
        pub title: String,
    }

    /// Names the bucket our posters are kept in
    pub struct Posters;

    /// The bucketed schema with its fields the other way round
    #[db]
    pub struct Media {
        /// The posters for those movies
        pub posters: Bucket<Posters>,
        /// The movies we know about
        pub movies: PersistentUnsortedTable<Movie, FileSystem>,
    }
}

/// The bucketed schema with its bucket's marker named otherwise
mod renamed {
    use deepsize2::DeepSizeOf;
    use rkyv::{Archive, Deserialize, Serialize};
    use shoal::storage::FileSystem;
    use shoal::tables::{Bucket, PersistentUnsortedTable};
    use shoal_derive::{db, ShoalUnsortedTable};

    /// A movie, keyed by its id
    #[derive(
        Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, DeepSizeOf,
    )]
    #[rkyv(derive(Debug))]
    #[shoal_table(db = "Media")]
    pub struct Movie {
        /// The movie's id
        #[shoal(partition)]
        pub id: u64,
        /// Its title
        #[shoal(update)]
        pub title: String,
    }

    /// Names a bucket of stills rather than posters
    pub struct Stills;

    /// The bucketed schema with another bucket
    #[db]
    pub struct Media {
        /// The movies we know about
        pub movies: PersistentUnsortedTable<Movie, FileSystem>,
        /// The stills from those movies
        pub posters: Bucket<Stills>,
    }
}

/// The answers a bundle came back with, an error code each or none
///
/// # Arguments
///
/// * `client` - The client to send with
/// * `bundle` - The bundle to send
async fn answers_of(
    client: &Shoal<bucketed::MediaClient>,
    bundle: shoal::shared::queries::Queries<bucketed::MediaClient>,
) -> Result<Vec<Option<ErrorCode>>, TestError> {
    let mut stream = client.send(bundle).await?;
    let mut answers = Vec::new();
    while let Some(response) = stream.next().await? {
        answers.push(response.error().map(|error| error.code()));
    }
    Ok(answers)
}

/// A bundle naming the declared table and both of the bucket's, reads and writes of each
///
/// # Arguments
///
/// * `client` - The client to build it with
fn mixed_bundle(
    client: &Shoal<bucketed::MediaClient>,
) -> shoal::shared::queries::Queries<bucketed::MediaClient> {
    client
        .query()
        // a write of the declared table
        .add(bucketed::Movie {
            id: 1,
            title: "Alien".to_string(),
        })
        // a write of each generated table
        .add(bucketed::PostersObjectMeta {
            key: shoal::shared::objects::path_key("alien/one-sheet.png"),
            version: 0,
            entries: Vec::new(),
        })
        .add(bucketed::PostersStripeMeta {
            consumer: 1,
            object: 2,
            stripe: 0,
            sequence: 0,
            labels: Vec::new(),
            length: 0,
            stamp: 0,
            fence: 0,
            tombstone: false,
            pending: None,
        })
        // a read of a generated table, then of the declared one
        .add(bucketed::PostersStripeMetaGet::new(vec![(1, 2, 0)]))
        .add(bucketed::MovieGet::new(vec![1]))
}

/// The answers a mixed bundle has to come back with: the declared table's work done, and every
/// query of a generated table refused by name
const MIXED_ANSWERS: [Option<ErrorCode>; 5] = [
    None,
    Some(ErrorCode::GeneratedTable),
    Some(ErrorCode::GeneratedTable),
    Some(ErrorCode::GeneratedTable),
    None,
];

/// A schema with one bucket has two more table ids, each with tablet groups, and no query of a
/// client reaches either
///
/// S2's `bucket_field_generates_its_two_tables`. The tables are the macro's, read off the schema;
/// that each gets its groups is the map's (`map::tests::a_buckets_two_tables_add_their_groups`,
/// which derives the groups from these same ids); that no client reaches them is a running
/// server's, asked as nobody, as an admin of nothing and as a principal granted every table.
#[tokio::test]
async fn bucket_field_generates_its_two_tables() -> Result<(), TestError> {
    // the bucketed schema is the plain one and exactly two tables more, in the bucket's place
    let plain = <plain::MediaClient as QuerySupport>::table_ids();
    let bucketed = <bucketed::MediaClient as QuerySupport>::table_ids();
    assert_eq!(plain, vec![("Movie", TableId::of("Movie"))]);
    assert_eq!(
        bucketed,
        vec![
            ("Movie", TableId::of("Movie")),
            ("PostersObjectMeta", TableId::of("PostersObjectMeta")),
            ("PostersStripeMeta", TableId::of("PostersStripeMeta")),
        ]
    );
    // both are the bucket's, and both are persistent tables the node keeps on disk
    assert_eq!(
        <bucketed::MediaClient as QuerySupport>::generated_tables(),
        &[
            ("PostersObjectMeta", "Posters"),
            ("PostersStripeMeta", "Posters")
        ]
    );
    assert_eq!(
        <bucketed::Media as shoal::ShoalDatabase>::persistent_tables(),
        vec!["Movie", "PostersObjectMeta", "PostersStripeMeta"]
    );
    // a server requiring no authentication starts the bucket's tables, and refuses every query
    // of them while the declared table's are answered
    let temp_dir = utils::test_dir();
    let conf = utils::bind_buckets(utils::build_config(&temp_dir), &["Posters"]);
    let mut pool = ShoalPool::<bucketed::Media>::start(conf)?;
    let addr = pool.ready(utils::READY_TIMEOUT)?.to_string();
    let client = Shoal::<bucketed::MediaClient>::new(&addr).await?;
    assert_eq!(
        answers_of(&client, mixed_bundle(&client)).await?,
        MIXED_ANSWERS,
        "an open server let a client reach a bucket's table"
    );
    pool.exit()?;
    // and a principal granted everything on every table and bucket is refused the same way, by
    // the front door and not by its grants
    let temp_dir = utils::test_dir();
    let auth = Auth::default()
        .required(true)
        .user("owner", PASSWORD)
        .grant("owner", EVERY_TABLE, Privileges::ALL)
        .grant_bucket("owner", EVERY_BUCKET, Privileges::ALL);
    let conf = utils::bind_buckets(utils::build_config(&temp_dir), &["Posters"]).auth(auth);
    let mut pool = ShoalPool::<bucketed::Media>::start(conf)?;
    let addr = pool.ready(utils::READY_TIMEOUT)?.to_string();
    let owner = Shoal::<bucketed::MediaClient>::with_credentials(
        &addr,
        Credentials::scram("owner", PASSWORD),
    )
    .await?;
    assert_eq!(
        answers_of(&owner, mixed_bundle(&owner)).await?,
        MIXED_ANSWERS,
        "a principal granted everything reached a bucket's table"
    );
    pool.exit()?;
    Ok(())
}

/// Declaring a bucket changes the fingerprint and the schema id; what a deployment says about
/// the bucket changes neither
///
/// S2's `bucket_declaration_moves_the_fingerprint_and_its_binding_does_not`. The first half is a
/// comparison of constants. The second is what a deployment can say about a bucket at M12 - who
/// may read and write it - which a node started with any such grants serves under the same
/// constants, so a client built from the schema is accepted at its hello. M13 commits pool
/// bindings, and this test gains a node bound to another pool then
/// ([F79](../../docs/src/features/bucket-schema.md#limitations)).
#[tokio::test]
async fn bucket_declaration_moves_the_fingerprint_and_its_binding_does_not() -> Result<(), TestError>
{
    // declaring a bucket moves both constants
    assert_ne!(
        <plain::MediaClient as QuerySupport>::SCHEMA_FINGERPRINT,
        <bucketed::MediaClient as QuerySupport>::SCHEMA_FINGERPRINT,
        "declaring a bucket left the fingerprint where it was"
    );
    assert_ne!(
        <plain::MediaClient as QuerySupport>::SCHEMA_ID,
        <bucketed::MediaClient as QuerySupport>::SCHEMA_ID,
        "declaring a bucket left the schema id where it was"
    );
    // and a bucket of another name is another schema, under the same field name
    assert_ne!(
        <bucketed::MediaClient as QuerySupport>::SCHEMA_ID,
        <renamed::MediaClient as QuerySupport>::SCHEMA_ID,
        "renaming a bucket's marker left the schema id where it was"
    );
    // three nodes deployed differently - one granting nobody the bucket, one granting a
    // principal read on it, and one binding it to another storage pool - each accept a client
    // built from the schema, which they would refuse at its hello had either fingerprint moved
    // (F80: a pool binding is deployment, never schema)
    let elsewhere = shoal::shared::pools::PoolSpec::new(
        "hdd",
        shoal::shared::pools::Redundancy::Erasure { data: 2, parity: 1 },
    )
    .inline(4096);
    for (auth, pool_of) in [
        (Auth::default(), None),
        (
            Auth::default()
                .required(true)
                .user("viewer", PASSWORD)
                .grant_bucket("viewer", "Posters", Privileges::READ),
            None,
        ),
        (Auth::default(), Some(elsewhere)),
    ] {
        let required = auth.required;
        let temp_dir = utils::test_dir();
        let conf = match pool_of {
            None => utils::bind_buckets(utils::build_config(&temp_dir), &["Posters"]),
            Some(pool) => {
                let mut conf = utils::build_config(&temp_dir);
                conf.storage = conf.storage.clone().pool("elsewhere", pool).bind("Posters", "elsewhere");
                conf
            }
        }
        .auth(auth);
        let mut pool = ShoalPool::<bucketed::Media>::start(conf)?;
        let addr = pool.ready(utils::READY_TIMEOUT)?.to_string();
        let client = if required {
            Shoal::<bucketed::MediaClient>::with_credentials(
                &addr,
                Credentials::scram("viewer", PASSWORD),
            )
            .await?
        } else {
            Shoal::<bucketed::MediaClient>::new(&addr).await?
        };
        // the hello was accepted, so the node's constants are the client's
        drop(client);
        pool.exit()?;
    }
    Ok(())
}

/// Reordering a schema's fields changes no bucket id and no generated table's id
///
/// S2's `bucket_ids_are_stable_under_field_order`. The schema id moves with the order, since a
/// field's place is wire state; every identity a placement or a binding records does not, since
/// each is the hash of a name derived from the marker alone. The values are frozen, since a
/// placement records them.
#[test]
fn bucket_ids_are_stable_under_field_order() {
    // the same bucket, under the same identity, wherever its field sits
    assert_eq!(
        <bucketed::MediaClient as QuerySupport>::bucket_ids(),
        <reordered::MediaClient as QuerySupport>::bucket_ids()
    );
    // the same tables under the same identities, listed in field order
    let mut bucketed = <bucketed::MediaClient as QuerySupport>::table_ids();
    let mut reordered = <reordered::MediaClient as QuerySupport>::table_ids();
    assert_ne!(
        bucketed, reordered,
        "the order of the tables did not follow the fields"
    );
    bucketed.sort();
    reordered.sort();
    assert_eq!(bucketed, reordered);
    // while the schema id, which folds the order, moved
    assert_ne!(
        <bucketed::MediaClient as QuerySupport>::SCHEMA_ID,
        <reordered::MediaClient as QuerySupport>::SCHEMA_ID
    );
    // and the identities are frozen
    let frozen: [(&str, u64, u64); 3] = [
        (
            "bucket Posters",
            BucketId::of("Posters").0,
            16_548_859_369_553_050_818,
        ),
        (
            "table PostersObjectMeta",
            TableId::of("PostersObjectMeta").0,
            9_556_415_703_307_505_179,
        ),
        (
            "table PostersStripeMeta",
            TableId::of("PostersStripeMeta").0,
            14_431_859_731_920_229_224,
        ),
    ];
    let moved: Vec<String> = frozen
        .iter()
        .filter(|(_, now, then)| now != then)
        .map(|(name, now, then)| format!("{name}: {now} (frozen at {then})"))
        .collect();
    assert!(moved.is_empty(), "identities moved: {moved:#?}");
}

/// The length of a row's rkyv form, which is what the record page calls archived
///
/// # Arguments
///
/// * `row` - The row to archive
fn archived_len<T>(row: &T) -> usize
where
    T: for<'a> rkyv::Serialize<
        rkyv::rancor::Strategy<
            rkyv::ser::Serializer<
                rkyv::util::AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'a>,
                rkyv::ser::sharing::Share,
            >,
            rkyv::rancor::Error,
        >,
    >,
{
    rkyv::to_bytes::<rkyv::rancor::Error>(row)
        .expect("a row archives")
        .len()
}

/// The generated rows archive to the sizes F79 records beside X10's stand-ins
///
/// X10 priced rows shaped like S3's on the lab: a stripe row of six labels archived to 176 bytes,
/// and an object row of one entry with a 64 byte path and nothing inline to 312
/// ([X10](../../docs/src/object-storage/stripe-row-costs.md)). These are the same shapes as
/// generated, frozen here so a change to the layout is a change to this number and to the page:
/// 192, the stand-in less its missed mask and plus a fence, a tombstone and a pointer's width
/// of pending bytes, and 360, plus a content version, the put's length, liveness and the
/// attributes X14 asked for ([F79](../../docs/src/features/bucket-schema.md#performance)).
#[test]
fn generated_rows_archive_to_their_recorded_sizes() {
    // a stripe of a 4+2 pool, every chunk at the put's label, nothing pending
    let stripe = bucketed::PostersStripeMeta {
        consumer: 1,
        object: 0x0192_6f1e_7a00_7000_8000_0000_0000_0001,
        stripe: 7,
        sequence: 0,
        labels: vec![Label::default(); 6],
        length: 4 << 20,
        stamp: 0,
        fence: 0,
        tombstone: false,
        pending: None,
    };
    // one object of 64 MiB in stripes at a 64 byte path, its type and one more pair set
    let object = bucketed::PostersObjectMeta {
        key: shoal::shared::objects::path_key(&"p".repeat(64)),
        version: 0,
        entries: vec![ObjectEntry {
            path: "p".repeat(64),
            object: 0x0192_6f1e_7a00_7000_8000_0000_0000_0001,
            content_version: 0,
            size: 64 << 20,
            put_length: 64 << 20,
            geometry: Geometry {
                stripe_bytes: 4 << 20,
                unit_bytes: 64 << 10,
                data: 4,
                parity: 2,
            },
            epoch: 0,
            floors: Vec::new(),
            state: ObjectState {
                live: true,
                replacing: None,
                retired: Vec::new(),
            },
            created_ms: 1_790_000_000_000,
            modified_ms: 1_790_000_000_000,
            attributes: Attributes {
                content_type: "image/jpeg".to_string(),
                content_encoding: String::new(),
                cache_control: String::new(),
                user: vec![("owner".to_string(), "x10".to_string())],
                tags: Vec::new(),
            },
            inline: None,
        }],
    };
    let frozen = [
        ("stripe row", archived_len(&stripe), 192),
        ("object row", archived_len(&object), 360),
    ];
    let moved: Vec<String> = frozen
        .iter()
        .filter(|(_, now, then)| now != then)
        .map(|(name, now, then)| format!("{name}: {now} bytes (frozen at {then})"))
        .collect();
    assert!(
        moved.is_empty(),
        "the generated rows' sizes moved: {moved:#?}"
    );
}

/// A grant on a table a bucket generated, or on a bucket the schema does not declare, refuses
/// the start by name
///
/// A table grant cannot reach a bucket's rows: the start says which bucket to grant on instead.
/// And a grant on a misspelled bucket would be a grant on nothing, so it is refused the way one on
/// a misspelled table is ([F79](../../docs/src/features/bucket-schema.md)).
#[tokio::test]
async fn a_grant_on_a_generated_table_or_an_unknown_bucket_refuses_the_start() {
    // a table grant naming one of the bucket's tables
    let temp_dir = utils::test_dir();
    let auth = Auth::default()
        .required(true)
        .user("writer", PASSWORD)
        .grant("writer", "PostersStripeMeta", Privileges::WRITE);
    let conf = utils::bind_buckets(utils::build_config(&temp_dir), &["Posters"]).auth(auth);
    match ShoalPool::<bucketed::Media>::start(conf) {
        Ok(_) => panic!("a table grant on a bucket's table was accepted"),
        Err(error) => {
            let said = format!("{error:?}");
            assert!(
                said.contains("PostersStripeMeta") && said.contains("bucket Posters"),
                "{said}"
            );
        }
    }
    // a bucket grant naming a bucket nobody declared
    let temp_dir = utils::test_dir();
    let auth = Auth::default()
        .required(true)
        .user("viewer", PASSWORD)
        .grant_bucket("viewer", "Postres", Privileges::READ);
    let conf = utils::bind_buckets(utils::build_config(&temp_dir), &["Posters"]).auth(auth);
    match ShoalPool::<bucketed::Media>::start(conf) {
        Ok(_) => panic!("a grant on a bucket nobody declared was accepted"),
        Err(error) => {
            let said = format!("{error:?}");
            assert!(
                said.contains("Postres") && said.contains("Posters"),
                "{said}"
            );
        }
    }
}

/// A stripe's key is its generated row's partition key, from the row and from the key's values
///
/// Placement cuts a stripe's placement group from `stripe_key`, so a placement group is a
/// sub-range of the tablet whose group orders the stripe only while the two are one hash
/// ([F85](../../docs/src/features/devices-and-pools.md)).
#[test]
fn stripe_key_is_the_stripe_rows_partition_key() {
    use shoal::shared::objects::stripe_key;
    use shoal::shared::traits::PartitionKeySupport;
    for (consumer, object, stripe) in [
        (1u64, 0u128, 0u64),
        (7, 0x0192_6f1e_7a00_7000_8000_0000_0000_0001, 9),
        (u64::MAX, u128::MAX, u64::MAX),
    ] {
        let row = bucketed::PostersStripeMeta {
            consumer,
            object,
            stripe,
            sequence: 0,
            labels: Vec::new(),
            length: 0,
            stamp: 0,
            fence: 0,
            tombstone: false,
            pending: None,
        };
        assert_eq!(row.get_partition_key(), stripe_key(consumer, object, stripe));
        assert_eq!(
            bucketed::PostersStripeMeta::get_partition_key_from_values(&(consumer, object, stripe)),
            stripe_key(consumer, object, stripe)
        );
    }
}
