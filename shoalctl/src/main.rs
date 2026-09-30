//! shoalctl - a terminal UI for querying a Shoal database
//!
//! A schema is a compile-time construct, so the UI is built against one. Run in the project
//! that defines it, this program builds that UI for the schema, installs it, and replaces
//! itself with it ([F63](../../docs/src/features/shoaladm.md)); run elsewhere it opens the one a
//! previous build installed for the cluster the inventory names.

use clap::Parser;
use shoaladm::build::Role;
use shoalctl::cli::Cli;

/// Build the schema's terminal UI, or find it, and run it
fn main() -> shoalctl::Result<()> {
    // report errors with their context; the UI itself installs this again in its own process
    color_eyre::install()?;
    let cli = Cli::parse();
    let handoff = shoaladm::front::program_for(&cli.project, cli.tui.inventory.as_deref(), Role::Ctl)?;
    shoaladm::front::exec(&handoff)
}
