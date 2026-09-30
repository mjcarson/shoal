//! The bench schema's admin program and terminal UI: `deploy`, `status` and the rest to run
//! `shoal-node` on hosts over ssh ([F51](../../docs/src/features/cluster-deployment.md)), and
//! `tui` to query the cluster. What `shoaladm` would build for the schema, written by hand,
//! since the bench schema lives in a module of a workspace crate rather than a project's root
//! ([F63](../../docs/src/features/shoaladm.md))

use clap::{Parser, Subcommand};
use shoal_bench::workloads::schema::BenchClient;

/// Deploy a bench cluster, operate it, or query it
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

/// The terminal UI, and every admin command
#[derive(Subcommand, Debug)]
enum Command {
    /// Open the terminal UI
    Tui(shoalctl::cli::TuiArgs),
    /// The admin commands: deploy, upgrade, status and the rest
    #[command(flatten)]
    Shoaladm(shoaladm::cli::Command),
}

/// Query a bench cluster, or deploy one
#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Tui(args) => shoalctl::cli::run::<BenchClient>(&cli.project, args).await,
        Command::Shoaladm(command) => {
            // report errors with their context; the terminal UI installs this itself
            color_eyre::install()?;
            shoaladm::cli::run::<BenchClient>(&cli.project, command).await
        }
    }
}
