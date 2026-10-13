//! Spike X10's driver, its report, and every `shoaladm` command for its schema
//!
//! ```sh
//! # deploy the spike's cluster from its inventory, run a leg against it, and take it down
//! x10 bootstrap -i shoal-spike-rows/inventory.yml
//! x10 spike rate -i shoal-spike-rows/inventory.yml --round 1 --out x10.json
//! x10 destroy -i shoal-spike-rows/inventory.yml --yes
//! # merge every round's records and judge the triggers
//! x10 report x10.json > x10-report.md
//! ```
//!
//! The lab's runs are `shoal-spike-rows/results/x10-lab.sh`; the tables are on
//! `docs/src/object-storage/stripe-row-costs.md`.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use shoal_spike_rows::cluster::Lab;
use shoal_spike_rows::measure::{rate, remedy, rows, size, Ctx};
use shoal_spike_rows::RowsClient;

/// Run spike X10 against a deployed cluster, report it, or deploy and operate the cluster
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
    /// Run one leg of the spike against a cluster deployed from an inventory
    Spike(SpikeArgs),
    /// Merge rounds' records into intervals and judge the triggers named before the run
    Report(ReportArgs),
    /// The admin commands: bootstrap, status, restart, destroy and the rest
    #[command(flatten)]
    Shoaladm(shoaladm::cli::Command),
}

/// Which leg to run
#[derive(ValueEnum, Debug, Clone, Copy)]
enum Leg {
    /// Rows a second a group, and one row rewritten
    Rate,
    /// Bytes a row, then the cold commit
    Rows,
    /// An object held inline, from 1 KiB to 1 MiB
    Size,
    /// The supplement: cold commits under load, with and without the read before them
    Remedy,
}

/// What a leg runs against and where its records go
#[derive(Args, Debug)]
struct SpikeArgs {
    /// The leg
    leg: Leg,
    /// The inventory of the deployed cluster
    #[clap(long, short)]
    inventory: PathBuf,
    /// The round, which decides the order cells run in
    #[clap(long, default_value_t = 1)]
    round: u32,
    /// The file records are added to
    #[clap(long)]
    out: PathBuf,
    /// A fraction of every count and window, which proves the leg runs and measures nothing
    #[clap(long)]
    quick: bool,
    /// The member on the fast host, whose groups are labelled apart
    #[clap(long, default_value = "europa")]
    fast: String,
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

/// Run a leg, report, or run an admin command
#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    // parse what we were asked to do
    let cli = Cli::parse();
    color_eyre::install()?;
    match cli.command {
        Command::Spike(args) => {
            // the cluster, connected to as its admin
            let lab = Lab::attach(&args.inventory).await?;
            let mut ctx = Ctx {
                lab,
                round: args.round,
                quick: args.quick,
                out: args.out,
                fast: args.fast,
            };
            match args.leg {
                Leg::Rate => rate::run(&mut ctx).await,
                Leg::Rows => rows::run(&mut ctx).await,
                Leg::Size => size::run(&mut ctx).await,
                Leg::Remedy => remedy::run(&mut ctx).await,
            }
        }
        Command::Report(args) => {
            // every round's records, merged and judged
            let records = shoal_spike_rows::record::read_all(&args.files)?;
            print!("{}", shoal_spike_rows::report::render(&records, &args.label));
            Ok(())
        }
        Command::Shoaladm(command) => shoaladm::cli::run::<RowsClient>(&cli.project, command).await,
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
