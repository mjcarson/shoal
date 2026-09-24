//! The command line a schema's `shoalctl` program runs
//!
//! A schema's program is three lines:
//!
//! ```ignore
//! #[tokio::main]
//! async fn main() -> color_eyre::Result<()> {
//!     shoalctl::cli::main::<MyDbClient>().await
//! }
//! ```
//!
//! With no arguments it opens the terminal UI against `127.0.0.1:12000`, which is what every
//! program built against [`crate::run`] did before it had a command line. `cluster` bootstraps
//! a cluster from an inventory, adds a node to one, and runs the day-to-day around them
//! ([F51](../../docs/src/features/cluster-deployment.md)).

use clap::{Args, Parser, Subcommand};
use rkyv::Archive;
use shoal::client::Shoal;
use shoal::traits::QuerySupport;
use std::path::PathBuf;
use std::sync::Arc;

use crate::deploy::Deployment;

/// Query a Shoal database, or deploy a cluster of it
#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    /// What to do; the terminal UI if nothing is given
    #[clap(subcommand)]
    command: Option<Command>,
}

/// The things a shoalctl program can be asked to do
#[derive(Subcommand, Debug)]
enum Command {
    /// Open the terminal UI
    Tui(TuiArgs),
    /// Deploy and operate a cluster over ssh
    #[clap(subcommand)]
    Cluster(ClusterCommand),
}

/// Where the terminal UI connects
#[derive(Args, Debug)]
struct TuiArgs {
    /// The node to connect to, without credentials
    #[clap(long, default_value = "127.0.0.1:12000", conflicts_with = "inventory")]
    addr: String,
    /// A deployed cluster to connect to as its admin
    #[clap(long, short)]
    inventory: Option<PathBuf>,
}

/// The inventory every cluster command reads
#[derive(Args, Debug)]
struct InventoryArg {
    /// The inventory describing the cluster
    #[clap(long, short)]
    inventory: PathBuf,
}

/// The cluster commands
#[derive(Subcommand, Debug)]
enum ClusterCommand {
    /// Deploy a new cluster over the inventory's bootstrap set, initialize it and wait for writes
    Bootstrap {
        /// The inventory
        #[clap(flatten)]
        inventory: InventoryArg,
        /// Delete a node already claimed on a host rather than refusing it
        #[clap(long)]
        wipe: bool,
    },
    /// Join a node of the inventory to the deployed cluster
    Add {
        /// The inventory
        #[clap(flatten)]
        inventory: InventoryArg,
        /// The node to add, by its inventory name
        node: String,
        /// Delete a node already claimed on its host rather than refusing it
        #[clap(long)]
        wipe: bool,
        /// Move a share of the cluster's data onto it once it has joined
        #[clap(long)]
        rebalance: bool,
    },
    /// Move data onto members holding less than their share, and follow the plan
    Rebalance {
        /// The inventory
        #[clap(flatten)]
        inventory: InventoryArg,
    },
    /// Print every node, its unit, and the cluster as a member sees it
    Status {
        /// The inventory
        #[clap(flatten)]
        inventory: InventoryArg,
    },
    /// Print every member's standing, partitions, bytes and write rates, and every plan's progress
    Stats {
        /// The inventory
        #[clap(flatten)]
        inventory: InventoryArg,
        /// Narrow the figures to one table, by the name the schema spells it
        #[clap(long)]
        table: Option<String>,
        /// Print again every so many seconds, two if none is given, until interrupted
        #[clap(long, num_args = 0..=1, default_missing_value = "2")]
        watch: Option<u64>,
        /// Print the leader's answer as json rather than as lines
        #[clap(long)]
        json: bool,
    },
    /// Start a node's unit, or every node's
    Start {
        /// The inventory
        #[clap(flatten)]
        inventory: InventoryArg,
        /// The node, or every deployed node if none
        node: Option<String>,
    },
    /// Stop a node's unit, or every node's
    Stop {
        /// The inventory
        #[clap(flatten)]
        inventory: InventoryArg,
        /// The node, or every deployed node if none
        node: Option<String>,
    },
    /// Restart a node's unit, or every node's
    Restart {
        /// The inventory
        #[clap(flatten)]
        inventory: InventoryArg,
        /// The node, or every deployed node if none
        node: Option<String>,
    },
    /// Print the end of a node's journal
    Logs {
        /// The inventory
        #[clap(flatten)]
        inventory: InventoryArg,
        /// The node
        node: String,
        /// How many lines
        #[clap(long, short = 'n', default_value_t = 200)]
        lines: usize,
    },
    /// Stop and delete every node of the inventory, its data, and the local state
    Destroy {
        /// The inventory
        #[clap(flatten)]
        inventory: InventoryArg,
        /// Say so: this deletes every row the cluster holds
        #[clap(long)]
        yes: bool,
    },
}

/// Run a shoalctl program for a schema, reading the command from the process arguments
///
/// # Errors
///
/// Whatever the command failed with.
pub async fn main<S>() -> color_eyre::Result<()>
where
    S: QuerySupport + Send + Sync + 'static,
    S::TableNames: Send,
    S::QueryKinds: Send,
    S::ResponseKinds: Send,
    <S::ResponseKinds as Archive>::Archived: Send
        + rkyv::Deserialize<
            S::ResponseKinds,
            rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
        >,
    for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    // parse what we were asked to do, the terminal UI if nothing
    let cli = Cli::parse();
    let command = cli.command.unwrap_or(Command::Tui(TuiArgs {
        addr: "127.0.0.1:12000".to_string(),
        inventory: None,
    }));
    match command {
        Command::Tui(args) => {
            // a deployed cluster is connected to as its admin, anything else as nobody
            let shoal: Arc<Shoal<S>> = match &args.inventory {
                Some(path) => {
                    let deployment = Deployment::open(path)?;
                    let record = deployment.state.record()?;
                    deployment.any_member::<S>(&record).await?
                }
                None => Arc::new(
                    Shoal::<S>::new(args.addr.as_str())
                        .await
                        .map_err(|error| color_eyre::eyre::eyre!("{}: {error:?}", args.addr))?,
                ),
            };
            crate::run(shoal).await
        }
        Command::Cluster(command) => cluster::<S>(command).await,
    }
}

/// Run a cluster command
///
/// # Arguments
///
/// * `command` - The command
async fn cluster<S>(command: ClusterCommand) -> color_eyre::Result<()>
where
    S: QuerySupport + Send + Sync + 'static,
    for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    match command {
        ClusterCommand::Bootstrap { inventory, wipe } => {
            Deployment::open(&inventory.inventory)?.bootstrap::<S>(wipe).await
        }
        ClusterCommand::Add {
            inventory,
            node,
            wipe,
            rebalance,
        } => {
            Deployment::open(&inventory.inventory)?
                .add::<S>(&node, wipe, rebalance)
                .await
        }
        ClusterCommand::Rebalance { inventory } => {
            // any member answers, and the leader drives the plan
            let deployment = Deployment::open(&inventory.inventory)?;
            let record = deployment.state.record()?;
            let shoal = deployment.any_member::<S>(&record).await?;
            deployment.rebalance(&shoal).await
        }
        ClusterCommand::Status { inventory } => {
            Deployment::open(&inventory.inventory)?.status::<S>().await
        }
        ClusterCommand::Stats {
            inventory,
            table,
            watch,
            json,
        } => {
            Deployment::open(&inventory.inventory)?
                .stats::<S>(table.as_deref(), watch, json)
                .await
        }
        ClusterCommand::Start { inventory, node } => {
            Deployment::open(&inventory.inventory)?.systemctl("start", node.as_deref())
        }
        ClusterCommand::Stop { inventory, node } => {
            Deployment::open(&inventory.inventory)?.systemctl("stop", node.as_deref())
        }
        ClusterCommand::Restart { inventory, node } => {
            Deployment::open(&inventory.inventory)?.systemctl("restart", node.as_deref())
        }
        ClusterCommand::Logs {
            inventory,
            node,
            lines,
        } => Deployment::open(&inventory.inventory)?.logs(&node, lines),
        ClusterCommand::Destroy { inventory, yes } => {
            // deleting a cluster's every row is asked for in words
            if !yes {
                color_eyre::eyre::bail!(
                    "destroy deletes every node, every row and the cluster's authority; pass --yes"
                );
            }
            Deployment::open(&inventory.inventory)?.destroy()
        }
    }
}
