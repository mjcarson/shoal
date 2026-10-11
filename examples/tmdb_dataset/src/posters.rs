//! `posters`: the Posters bucket's generated tables, asked for through every member
//!
//! The lab's check of the front door ([F79](../../../docs/src/features/bucket-schema.md)). The
//! schema declares `posters: Bucket<Posters>`, which generates two tables the cluster replicates
//! and no client's query may reach. Through every member in turn this sends one bundle mixing the
//! declared table and the generated ones - a movie's read, a write and a read of each generated
//! table, and the movie's read again - and checks every query of a generated table came back
//! `GeneratedTable` while the movie's reads were answered. It writes nothing that is kept: the
//! bundle's only writes are the generated tables', and those are refused.

use clap::{ArgGroup, Args};
use color_eyre::eyre::bail;
use shoal::shared::objects::path_key;
use shoal::shared::protocol::error::ErrorCode;
use std::path::PathBuf;

use crate::load::connect_targets;
use crate::{MovieGet, PostersObjectMeta, PostersStripeMeta, PostersStripeMetaGet};

/// Ask for the Posters bucket's tables through every member and check each is refused
#[derive(Args, Debug, Clone)]
#[command(group(ArgGroup::new("target").required(true).args(["inventory", "addr"])))]
pub struct PostersArgs {
    /// The inventory of the deployed cluster to ask
    #[clap(long, short)]
    pub inventory: Option<PathBuf>,
    /// A single node's client address, for a node started by hand
    #[clap(long)]
    pub addr: Option<String>,
    /// The movie whose reads stand beside the refused queries
    #[clap(long, default_value_t = 603)]
    pub movie: u64,
}

/// The answers a mixed bundle has to come back with: every query of a generated table refused,
/// and the movie's two reads answered
const EXPECTED: [Option<ErrorCode>; 5] = [
    None,
    Some(ErrorCode::GeneratedTable),
    Some(ErrorCode::GeneratedTable),
    Some(ErrorCode::GeneratedTable),
    None,
];

/// Ask for the bucket's tables through every member and check every answer
///
/// # Arguments
///
/// * `args` - Which cluster to ask, and the movie to read beside the bucket's tables
///
/// # Errors
///
/// When a member cannot be reached, or any member answers other than the front door has to.
pub async fn posters(args: PostersArgs) -> color_eyre::Result<()> {
    // connect to every member as the deployment's user, each sending only through its own
    let clients = connect_targets(
        args.inventory.as_ref(),
        args.addr.as_deref(),
        shoal::client::Routing::Endpoints,
    )
    .await?;
    let mut wrong = Vec::new();
    for (member, client) in clients.iter().enumerate() {
        // a movie's read, then a write and a read of each generated table, then the read again
        let bundle = client
            .query()
            .add(MovieGet::new(vec![args.movie]))
            .add(PostersObjectMeta {
                key: path_key("alien/one-sheet.png"),
                version: 0,
                entries: Vec::new(),
            })
            .add(PostersStripeMeta {
                consumer: 1,
                object: 1,
                stripe: 0,
                sequence: 0,
                labels: Vec::new(),
                length: 0,
                stamp: 0,
                fence: 0,
                tombstone: false,
                pending: None,
            })
            .add(PostersStripeMetaGet::new(vec![(1, 1, 0)]))
            .add(MovieGet::new(vec![args.movie]));
        // every answer's error code, or none for one that was answered
        let mut stream = client.send(bundle).await?;
        let mut answers = Vec::new();
        let mut messages = Vec::new();
        while let Some(response) = stream.next().await? {
            answers.push(response.error().map(|error| error.code()));
            if let Some(error) = response.error() {
                messages.push(error.msg().to_string());
            }
        }
        println!("member {member}: {answers:?}");
        if let Some(message) = messages.first() {
            println!("member {member} said: {message}");
        }
        if answers != EXPECTED {
            wrong.push(format!("member {member} answered {answers:?}"));
        }
    }
    if !wrong.is_empty() {
        bail!(
            "the front door let a query of a bucket's table through, or refused a declared \
             table's: {wrong:?}"
        );
    }
    println!(
        "every member refused every query of the Posters bucket's tables GeneratedTable, and \
         answered the movie's reads"
    );
    Ok(())
}
