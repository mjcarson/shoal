//! Deploy the TMDB dataset database and load it
//!
//! ```sh
//! # build the inventory, deploy the cluster, then fill it
//! tmdb-dataset-loader cluster new -o tmdb.yml
//! tmdb-dataset-loader cluster bootstrap -i tmdb.yml
//! tmdb-dataset-loader load -i tmdb.yml --dataset TMDB_movie_dataset_v11.csv
//! # and query it
//! tmdb-dataset-loader tui -i tmdb.yml
//! ```
//!
//! Every `shoalctl` command is here for this schema, beside `load`
//! ([F54](../../../../docs/src/features/tmdb-dataset-deployment.md)).

use clap::{Parser, Subcommand};
use tmdb_dataset::bench::{BenchArgs, VerifyAcksArgs, VerifyArgs};
use tmdb_dataset::load::LoadArgs;
use tmdb_dataset::TmdbClient;

/// Deploy a cluster of the TMDB dataset database, load the dataset into it, and query it
#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    /// What to do; the terminal UI if nothing is given
    #[clap(subcommand)]
    command: Option<Command>,
}

/// The loader's own command, and every shoalctl command
#[derive(Subcommand, Debug)]
enum Command {
    /// Load the TMDB csv into a deployed cluster
    Load(LoadArgs),
    /// Drive a timed mix of reads and writes and print a line a second
    Bench(BenchArgs),
    /// Read every movie and keyword partition back and compare them with the csv
    Verify(VerifyArgs),
    /// Read back every synthetic insert a bench run was acknowledged for
    VerifyAcks(VerifyAcksArgs),
    /// The terminal UI and the cluster commands
    #[command(flatten)]
    Shoalctl(shoalctl::cli::Command),
}

/// Load the dataset, or run a shoalctl command
#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    // parse what we were asked to do, the terminal UI if nothing
    match Cli::parse().command {
        Some(Command::Load(args)) => {
            // report errors with their context; the terminal UI installs this itself, and a
            // second install is refused, so only the loader's own path does it here
            color_eyre::install()?;
            tmdb_dataset::load::run(args).await
        }
        // the lab's test driver, which reports errors the same way
        Some(Command::Bench(args)) => {
            color_eyre::install()?;
            tmdb_dataset::bench::bench(args).await
        }
        Some(Command::Verify(args)) => {
            color_eyre::install()?;
            tmdb_dataset::bench::verify(args).await
        }
        Some(Command::VerifyAcks(args)) => {
            color_eyre::install()?;
            tmdb_dataset::bench::verify_acks(args).await
        }
        Some(Command::Shoalctl(command)) => shoalctl::cli::run::<TmdbClient>(Some(command)).await,
        None => shoalctl::cli::run::<TmdbClient>(None).await,
    }
}
