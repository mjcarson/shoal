//! shoaladm - deploy, upgrade and operate a Shoal cluster
//!
//! Run in the project that defines a schema, this program builds whatever the command needs
//! and runs it: the node program for every host's cpu, and an admin program for the schema
//! that the command is handed to, since a command that connects to the cluster has to be
//! built against its schema ([F63](../../docs/src/features/shoaladm.md)). The commands that
//! never connect - the inventory wizard, the config, the units, the journal, destroy - run here.

use clap::Parser;
use shoaladm::build::Role;
use shoaladm::cli::{Cli, Command};

/// Run a command here, or through the schema's admin program
fn main() -> shoaladm::Result<()> {
    // report errors with their context
    color_eyre::install()?;
    let cli = Cli::parse();
    // a command that connects is the schema's program's to run
    if cli.command.needs_schema() {
        let inventory = inventory_of(&cli.command);
        let handoff = shoaladm::front::program_for(&cli.project, inventory.as_deref(), Role::Adm)?;
        return shoaladm::front::exec(&handoff);
    }
    // the rest run here, on a runtime of their own
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        // nothing comes back: every command that would has a schema
        shoaladm::cli::run_local(&cli.project, cli.command)
            .await
            .map(|_| ())
    })
}

/// The inventory a command was given, if it takes one
///
/// # Arguments
///
/// * `command` - The command
fn inventory_of(command: &Command) -> Option<std::path::PathBuf> {
    match command {
        Command::Deploy { inventory, .. }
        | Command::Bootstrap { inventory, .. }
        | Command::Add { inventory, .. }
        | Command::Rebuild { inventory, .. }
        | Command::Rebalance { inventory }
        | Command::Admin { inventory, .. }
        | Command::ShipBackup { inventory, .. }
        | Command::Status { inventory }
        | Command::Stats { inventory, .. }
        | Command::Start { inventory, .. }
        | Command::Stop { inventory, .. }
        | Command::Restart { inventory, .. }
        | Command::Upgrade { inventory, .. }
        | Command::Reconfigure { inventory, .. }
        | Command::Logs { inventory, .. }
        | Command::Destroy { inventory, .. } => inventory.inventory.clone(),
        Command::Build { .. } | Command::Config(_) | Command::New { .. } => None,
    }
}
