//! Spike X3's driver, its report, europa's local nodes, and every `shoaladm` command for its schema
//!
//! ```sh
//! # every device's ceiling, before any cluster is up
//! x3 fio -i shoal-spike-bytes/inventory.yml --round 1 --out x3.json
//! # deploy a leg's cluster, run it at one row size, and take it down
//! x3 bootstrap -i shoal-spike-bytes/inventory.yml
//! x3 spike lab --size 1048576 -i shoal-spike-bytes/inventory.yml --round 1 --out x3.json
//! x3 destroy -i shoal-spike-bytes/inventory.yml --yes
//! # europa's three local nodes, which no deployment can make
//! x3 local up -i shoal-spike-bytes/inventory-loopback.yml --program target/lab/x3/znver1/release/x3-node
//! x3 spike loopback --size 1048576 -i shoal-spike-bytes/inventory-loopback.yml --round 1 --out x3.json
//! x3 local down -i shoal-spike-bytes/inventory-loopback.yml
//! # merge every round's records and judge the triggers
//! x3 report x3.json > x3-report.md
//! ```
//!
//! The lab's runs are `shoal-spike-bytes/results/x3-lab.sh`; the tables are on
//! `docs/src/object-storage/bytes-through-groups.md`.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use shoal_spike_bytes::cluster::Lab;
use shoal_spike_bytes::measure::{self, Ctx};
use shoal::shared::protocol::read::ReadLevel;
use shoal_spike_bytes::StripesAsRowsClient;

/// Run spike X3 against a cluster, report it, or deploy and operate the cluster
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
    /// Run one leg at one row size against a cluster that is up
    Spike(SpikeArgs),
    /// Measure every device an inventory's roots are on with fio, before any cluster is up
    Fio(FioArgs),
    /// Read every preloaded row back through every member, at `One` and `Quorum`, and say what each found
    Readback(ReadbackArgs),
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
    /// europa, titan and hyperion at a factor of three
    Lab,
    /// Three nodes on europa over loopback at a factor of three
    Loopback,
    /// One node on titan
    Titan,
    /// One node on europa
    Europa,
}

impl Leg {
    /// The leg's name in a record
    fn name(self) -> &'static str {
        match self {
            Leg::Lab => "lab",
            Leg::Loopback => "loopback",
            Leg::Titan => "titan",
            Leg::Europa => "europa",
        }
    }
}

/// What a leg runs against and where its records go
#[derive(Args, Debug)]
struct SpikeArgs {
    /// The leg
    leg: Leg,
    /// The row size, in bytes
    #[clap(long)]
    size: usize,
    /// The inventory of the cluster that is up
    #[clap(long, short)]
    inventory: PathBuf,
    /// The round, which decides the order the arms run in
    #[clap(long, default_value_t = 1)]
    round: u32,
    /// The file records are added to
    #[clap(long)]
    out: PathBuf,
    /// A tenth of every count and a few seconds a window, which proves the leg runs and measures nothing
    #[clap(long)]
    quick: bool,
    /// A multiple of the depth rule, for the quick run's check of it
    #[clap(long, default_value_t = 1)]
    depth_scale: usize,
}

/// Where fio runs and where its records go
#[derive(Args, Debug)]
struct FioArgs {
    /// The inventory whose roots' filesystems are measured
    #[clap(long, short)]
    inventory: PathBuf,
    /// The round
    #[clap(long, default_value_t = 1)]
    round: u32,
    /// The file records are added to
    #[clap(long)]
    out: PathBuf,
    /// A short run of a small file, which proves fio runs
    #[clap(long)]
    quick: bool,
}

/// Which cluster's preloaded rows to read back
#[derive(Args, Debug)]
struct ReadbackArgs {
    /// The inventory of the cluster that is up
    #[clap(long, short)]
    inventory: PathBuf,
    /// The row size its preload wrote, in bytes
    #[clap(long)]
    size: usize,
    /// Whether its preload was a quick run's
    #[clap(long)]
    quick: bool,
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

/// Run a leg, measure the devices, start the local nodes, report, or run an admin command
#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    // parse what we were asked to do
    let cli = Cli::parse();
    color_eyre::install()?;
    match cli.command {
        Command::Spike(args) => {
            // the cluster, connected to as its admin, from its deployment record
            let lab = Lab::attach(&args.inventory).await?;
            let ctx = Ctx {
                lab,
                leg: args.leg.name().to_string(),
                round: args.round,
                quick: args.quick,
                out: args.out,
                depth_scale: args.depth_scale,
            };
            measure::run(&ctx, args.size).await
        }
        Command::Readback(args) => {
            // every member's answer for every preloaded key, at each level
            let lab = Lab::attach(&args.inventory).await?;
            let ctx = Ctx {
                lab,
                leg: "readback".to_string(),
                round: 0,
                quick: args.quick,
                out: PathBuf::new(),
                depth_scale: 1,
            };
            for level in [ReadLevel::One, ReadLevel::Quorum] {
                for (member, found) in measure::readback(&ctx, args.size, level).await? {
                    println!(
                        "{level:?} through {member}: {} found, {} empty, {} failed, {} wrong; empty {:?}; failed {:?}; wrong {:?}",
                        found.found,
                        found.empty.len(),
                        found.failed.len(),
                        found.wrong.len(),
                        &found.empty[..found.empty.len().min(12)],
                        &found.failed[..found.failed.len().min(3)],
                        &found.wrong[..found.wrong.len().min(12)],
                    );
                }
            }
            Ok(())
        }
        Command::Fio(args) => shoal_spike_bytes::ceiling::run(&args.inventory, args.round, args.quick, &args.out).await,
        Command::Local(LocalCommand::Up { inventory, program }) => {
            shoal_spike_bytes::local::up(&inventory, &program).await
        }
        Command::Local(LocalCommand::Down { inventory }) => shoal_spike_bytes::local::down(&inventory),
        Command::Report(args) => {
            // every round's records, merged and judged
            let records = shoal_spike_bytes::record::read_all(&args.files)?;
            print!("{}", shoal_spike_bytes::report::render(&records, &args.label));
            Ok(())
        }
        Command::Shoaladm(command) => shoaladm::cli::run::<StripesAsRowsClient>(&cli.project, command).await,
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
