//! The command line args for the shoal database

use clap::Parser;

/// A compile time typed database
#[derive(Parser, Debug)]
#[command(author, version, about)]
pub struct Args {
    /// The path to the config file for shoal
    #[clap(short, long, default_value = "shoal.yml")]
    pub conf: String,
}
