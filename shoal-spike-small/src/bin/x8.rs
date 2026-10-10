//! Spike X8's driver, its report, the holders, europa's local nodes, and every `shoaladm` command
//! for its schema
//!
//! ```sh
//! # the lab's cluster, a holder beside each node, every cell, and both taken down
//! x8 bootstrap -i shoal-spike-small/inventory.yml
//! x8 holders up -i shoal-spike-small/inventory.yml --program target/lab/x8/znver1/release/x8-holder
//! x8 spike lab -i shoal-spike-small/inventory.yml --round 1 --out x8.json --lead europa
//! x8 holders down -i shoal-spike-small/inventory.yml
//! x8 destroy -i shoal-spike-small/inventory.yml --yes
//! # europa's three local nodes, which no deployment can make
//! x8 local up -i shoal-spike-small/inventory-loopback.yml --program target/lab/x8/znver1/release/x8-node
//! x8 holders up -i shoal-spike-small/inventory-loopback.yml --program target/lab/x8/znver1/release/x8-holder
//! x8 spike loopback -i shoal-spike-small/inventory-loopback.yml --round 1 --out x8.json
//! # merge every round's records and judge the triggers
//! x8 report x8.json > x8-report.md
//! ```
//!
//! The lab's runs are `shoal-spike-small/results/x8-lab.sh`; the tables are on
//! `docs/src/object-storage/small-writes.md`.

use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Parser, Subcommand, ValueEnum};
use color_eyre::eyre::eyre;
use shoal_spike_small::cluster::Lab;
use shoal_spike_small::holders;
use shoal_spike_small::keys::MAX_WORKERS;
use shoal_spike_small::measure::{self, Ctx, DEPTHS, SIZES};
use shoal_spike_small::paths::Path;
use shoal_spike_small::SmallClient;

/// Run spike X8 against a cluster, report it, or deploy and operate the cluster and its holders
#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    /// The project the schema is in, which every admin command takes
    #[clap(flatten)]
    project: shoaladm::cli::ProjectArgs,
    /// What to do
    #[clap(subcommand)]
    command: Command,
}

/// The spike's own commands and every admin command
#[derive(Subcommand, Debug)]
enum Command {
    /// Run one leg's cells against a cluster that is up with its holders
    Spike(SpikeArgs),
    /// Start, check or stop the holder beside every node
    #[command(subcommand)]
    Holders(HoldersCommand),
    /// Start or stop europa's local nodes, which no deployment can make
    #[command(subcommand)]
    Local(LocalCommand),
    /// Merge rounds' records into intervals and judge the triggers named before the run
    Report(ReportArgs),
    /// The admin commands: bootstrap, status, restart, destroy and the rest
    #[command(flatten)]
    Shoaladm(shoaladm::cli::Command),
}

/// Which leg a run is
#[derive(ValueEnum, Debug, Clone, Copy)]
enum Leg {
    /// europa, titan and hyperion at a factor of three, over 1 GbE
    Lab,
    /// Three nodes on europa over loopback at a factor of three
    Loopback,
}

impl Leg {
    /// The leg's name in a record
    fn name(self) -> &'static str {
        match self {
            Leg::Lab => "lab",
            Leg::Loopback => "loopback",
        }
    }
}

/// What a leg runs against and where its records go
#[derive(Args, Debug)]
struct SpikeArgs {
    /// The leg
    leg: Leg,
    /// The inventory of the cluster that is up
    #[clap(long, short)]
    inventory: PathBuf,
    /// The round, which decides the order the cells run in
    #[clap(long, default_value_t = 1)]
    round: u32,
    /// The file records are added to
    #[clap(long)]
    out: PathBuf,
    /// A few seconds a window, which proves the leg runs and measures nothing
    #[clap(long)]
    quick: bool,
    /// The member whose groups every key is aimed at; none aims at every group
    #[clap(long)]
    lead: Option<String>,
    /// The write sizes, in bytes, comma separated; every size by default
    #[clap(long, value_delimiter = ',')]
    sizes: Vec<usize>,
    /// The depths, comma separated; one and thirty-two by default
    #[clap(long, value_delimiter = ',')]
    depths: Vec<usize>,
    /// The paths, comma separated: row, staged, inline; every one by default
    #[clap(long, value_delimiter = ',')]
    paths: Vec<String>,
    /// A multiple of the holders' gates, for the check that the bound does not decide a rate
    #[clap(long, default_value_t = 1)]
    bound_scale: usize,
    /// How long the heat cell runs, in seconds; zero for none
    #[clap(long, default_value_t = 60)]
    heat_secs: u64,
}

/// The holders
#[derive(Subcommand, Debug)]
enum HoldersCommand {
    /// Install and start a holder beside every node of the inventory
    Up {
        /// The inventory
        #[clap(long, short)]
        inventory: PathBuf,
        /// The holder program, built for the oldest cpu among the hosts
        #[clap(long)]
        program: PathBuf,
        /// How an apply or a fold is made durable: `each` syncs its chunk, as S6 has it; `batch`
        /// covers every one that completed with one flush, for the supplement
        #[clap(long, default_value = "each")]
        in_place_sync: String,
    },
    /// Stage, apply, fold and probe on every holder, over its lanes' TLS
    Check {
        /// The inventory
        #[clap(long, short)]
        inventory: PathBuf,
    },
    /// Stop every holder of the inventory and remove everything it wrote
    Down {
        /// The inventory
        #[clap(long, short)]
        inventory: PathBuf,
    },
}

/// europa's local nodes
#[derive(Subcommand, Debug)]
enum LocalCommand {
    /// Render, claim and start every node of the inventory and form them into one cluster
    Up {
        /// The loopback inventory
        #[clap(long, short)]
        inventory: PathBuf,
        /// The node program, built for europa
        #[clap(long)]
        program: PathBuf,
    },
    /// Stop every node of the inventory and remove everything it wrote
    Down {
        /// The loopback inventory
        #[clap(long, short)]
        inventory: PathBuf,
    },
}

/// What to report
#[derive(Args, Debug)]
struct ReportArgs {
    /// The records, every round's
    files: Vec<PathBuf>,
    /// The line naming where they were measured, under every table
    #[clap(long, default_value = "")]
    label: String,
}

/// Run a leg, operate the holders or the local nodes, report, or run an admin command
#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    // parse what we were asked to do
    let cli = Cli::parse();
    color_eyre::install()?;
    match cli.command {
        Command::Spike(args) => {
            // the cluster, connected to as its admin, and a lane pool to every holder
            let lab = Lab::attach(&args.inventory).await?;
            let bound_scale = args.bound_scale.max(1);
            let pools = holders::connect(&lab.inventory, 2 * MAX_WORKERS * bound_scale).await?;
            let holder_hosts = holders::specs(&lab.inventory)?
                .into_iter()
                .map(|spec| spec.target)
                .collect();
            let paths = if args.paths.is_empty() {
                Path::ALL.to_vec()
            } else {
                args.paths
                    .iter()
                    .map(|name| Path::from_name(name).ok_or_else(|| eyre!("no path is called {name}")))
                    .collect::<color_eyre::Result<Vec<_>>>()?
            };
            let ctx = Ctx {
                lab,
                holders: pools,
                holder_hosts,
                leg: args.leg.name().to_string(),
                round: args.round,
                quick: args.quick,
                out: args.out,
                lead: args.lead,
                sizes: if args.sizes.is_empty() { SIZES.to_vec() } else { args.sizes },
                depths: if args.depths.is_empty() { DEPTHS.to_vec() } else { args.depths },
                paths,
                bound_scale,
                heat: Duration::from_secs(args.heat_secs),
            };
            measure::run(&ctx).await
        }
        Command::Holders(HoldersCommand::Up {
            inventory,
            program,
            in_place_sync,
        }) => holders::up(&inventory, &program, &in_place_sync).await,
        Command::Holders(HoldersCommand::Check { inventory }) => holders::check(&inventory).await,
        Command::Holders(HoldersCommand::Down { inventory }) => holders::down(&inventory),
        Command::Local(LocalCommand::Up { inventory, program }) => {
            shoal_spike_small::local::up(&inventory, &program).await
        }
        Command::Local(LocalCommand::Down { inventory }) => shoal_spike_small::local::down(&inventory),
        Command::Report(args) => {
            // every round's records, merged and judged
            let records = shoal_spike_small::record::read_all(&args.files)?;
            print!("{}", shoal_spike_small::report::render(&records, &args.label));
            Ok(())
        }
        Command::Shoaladm(command) => shoaladm::cli::run::<SmallClient>(&cli.project, command).await,
    }
}

#[cfg(test)]
mod tests {
    use super::Cli;
    use clap::CommandFactory;

    /// The spike's commands and every admin command it flattens have one name each
    #[test]
    fn every_command_has_one_name() {
        Cli::command().debug_assert();
    }
}
