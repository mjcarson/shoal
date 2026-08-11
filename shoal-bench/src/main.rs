//! The `shoal-bench` command line entry point
//!
//! Exit codes are meaningful, because these commands are meant to be scriptable:
//!
//! | Code | Meaning |
//! | --- | --- |
//! | 0 | the command did what it was asked |
//! | 1 | it failed |
//! | 2 | it was asked for something that does not make sense |
//! | 3 | a comparison found a regression, and `--fail-on-regression` was given |

use anyhow::Result;
use clap::Parser;

use shoal_bench::cli::{Cli, Command};
use shoal_bench::store::Store;

/// The exit code for a command that failed
const EXIT_FAILED: i32 = 1;

/// Parses the arguments, runs the command, and turns the outcome into an exit code
fn main() {
    // parse first, so that a bad invocation is rejected before anything touches the filesystem
    let cli = Cli::parse();
    // run it, and report a failure on stderr rather than panicking out of main
    match dispatch(cli) {
        Ok(code) => std::process::exit(code),
        Err(err) => {
            eprintln!("error: {err:#}");
            std::process::exit(EXIT_FAILED);
        }
    }
}

/// Runs one command and reports the exit code it wants
///
/// # Arguments
///
/// * `cli` - The parsed command line
fn dispatch(cli: Cli) -> Result<i32> {
    // locate the repository, either from the flag or by walking up from where we were run
    let store = match &cli.repo {
        Some(root) => Store::new(root),
        None => Store::discover(&std::env::current_dir()?)?,
    };
    // then hand off to whichever command was asked for
    match cli.command {
        Command::List(args) => shoal_bench::registry::run_list(&store, &args),
        Command::Run(args) => shoal_bench::run::run_capture(&store, &args),
        Command::Compare(args) => shoal_bench::compare::run_compare(&store, &args),
        Command::Status(args) => shoal_bench::stale::run_status(&store, &args),
        Command::Render(args) => shoal_bench::render::run_render(&store, &args),
        Command::Promote(args) => shoal_bench::promote::run_promote(&store, &args),
    }
}

