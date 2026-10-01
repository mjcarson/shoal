//! Deploy the TMDB dataset database and load it
//!
//! ```sh
//! # build the inventory, deploy the cluster, then fill it
//! tmdb-dataset-loader new -o tmdb.yml
//! tmdb-dataset-loader deploy -i tmdb.yml
//! tmdb-dataset-loader load -i tmdb.yml --dataset TMDB_movie_dataset_v11.csv
//! # and query it
//! tmdb-dataset-loader tui -i tmdb.yml
//! ```
//!
//! Every `shoaladm` command is here for this schema, beside `load`, and the terminal UI as
//! `tui` ([F54](../../../../docs/src/features/tmdb-dataset-deployment.md)). The same cluster
//! is deployed by plain `shoaladm` run in this crate's directory, which builds this schema's
//! own admin program ([F63](../../../../docs/src/features/shoaladm.md)); this loader is that
//! program written by hand, with the load beside it.

use clap::{Parser, Subcommand};
use tmdb_dataset::bench::{BenchArgs, VerifyAcksArgs, VerifyArgs};
use tmdb_dataset::load::LoadArgs;
use tmdb_dataset::TmdbClient;

/// Deploy a cluster of the TMDB dataset database, load the dataset into it, and query it
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

/// The loader's own commands, the terminal UI, and every admin command
#[derive(Subcommand, Debug)]
enum Command {
    /// Load the TMDB csv into a deployed cluster
    Load(LoadArgs),
    /// Drive a timed mix of reads and writes and print a line a second
    ///
    /// Called `bench` until F66, when `shoaladm bench` took that name for the benchmark every
    /// schema gets; this is the lab's fault test driver, which knows the TMDB rows by name.
    Drive(BenchArgs),
    /// Read every movie and keyword partition back and compare them with the csv
    Verify(VerifyArgs),
    /// Read back every synthetic insert a bench run was acknowledged for
    VerifyAcks(VerifyAcksArgs),
    /// Open the terminal UI
    Tui(shoalctl::cli::TuiArgs),
    /// The admin commands: deploy, upgrade, status and the rest
    #[command(flatten)]
    Shoaladm(shoaladm::cli::Command),
}

/// Load the dataset, open the terminal UI, or run an admin command
#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    // parse what we were asked to do
    let cli = Cli::parse();
    match cli.command {
        Command::Load(args) => {
            // report errors with their context; the terminal UI installs this itself, and a
            // second install is refused, so only the loader's own paths do it here
            color_eyre::install()?;
            tmdb_dataset::load::run(args).await
        }
        // the lab's test driver, which reports errors the same way
        Command::Drive(args) => {
            color_eyre::install()?;
            tmdb_dataset::bench::bench(args).await
        }
        Command::Verify(args) => {
            color_eyre::install()?;
            tmdb_dataset::bench::verify(args).await
        }
        Command::VerifyAcks(args) => {
            color_eyre::install()?;
            tmdb_dataset::bench::verify_acks(args).await
        }
        Command::Tui(args) => shoalctl::cli::run::<TmdbClient>(&cli.project, args).await,
        Command::Shoaladm(command) => {
            color_eyre::install()?;
            shoaladm::cli::run::<TmdbClient>(&cli.project, command).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Cli;
    use clap::CommandFactory;

    /// The loader's commands and every admin command it flattens have one name each
    ///
    /// clap only finds a duplicate subcommand when the line is parsed, in a debug build, so a
    /// name shoaladm takes later (as `bench` was in F66) panics the loader the first time it is
    /// run; this finds it when the tests are.
    #[test]
    fn every_command_has_one_name() {
        Cli::command().debug_assert();
    }
}
