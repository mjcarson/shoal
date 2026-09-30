//! The command line a schema's terminal UI runs
//!
//! A schema's program is one line:
//!
//! ```ignore
//! fn main() -> shoalctl::Result<()> {
//!     shoalctl::cli::main_blocking::<MyDbClient>()
//! }
//! ```
//!
//! With no arguments it opens the terminal UI against the cluster the operator is working
//! with: the inventory named with `-i`, else the project's `inventory.yml`, else the config's
//! `default_inventory` ([F63](../../docs/src/features/shoaladm.md)), else `127.0.0.1:12000`,
//! which is what every program built against [`crate::run`] did before it had a command line.
//! A deployed cluster is connected to as its admin; an address as nobody.
//!
//! A program with commands of its own takes [`TuiArgs`] as one of them and hands it to [`run`],
//! as the TMDB dataset loader does with its `tui`
//! ([F54](../../docs/src/features/tmdb-dataset-deployment.md)).

use clap::{Args, Parser};
use rkyv::Archive;
use shoal::client::Shoal;
use shoal::traits::QuerySupport;
use shoaladm::cli::ProjectArgs;
use shoaladm::config::{self, Config};
use shoaladm::deploy::Deployment;
use std::path::PathBuf;
use std::sync::Arc;

/// The address the terminal UI connects to when nothing names a cluster
pub const DEFAULT_ADDR: &str = "127.0.0.1:12000";

/// Query a Shoal database in a terminal
#[derive(Parser, Debug)]
#[command(author, version, about)]
pub struct Cli {
    /// The project the schema is in; the current directory if not given
    #[clap(flatten)]
    pub project: ProjectArgs,
    /// Where to connect
    #[clap(flatten)]
    pub tui: TuiArgs,
}

/// Where the terminal UI connects
#[derive(Args, Debug, Clone, Default)]
pub struct TuiArgs {
    /// A node to connect to as nobody, without credentials; 127.0.0.1:12000 when nothing names a cluster
    #[clap(long, conflicts_with = "inventory")]
    pub addr: Option<String>,
    /// A deployed cluster to connect to as its admin; the project's inventory.yml or the config's default if not given
    #[clap(long, short)]
    pub inventory: Option<PathBuf>,
}

/// Run a terminal UI for a schema, reading where to connect from the process arguments
///
/// # Errors
///
/// Whatever the UI failed with.
pub async fn main<S>() -> color_eyre::Result<()>
where
    S: QuerySupport + Send + Sync + 'static,
    S::TableNames: Send,
    S::QueryKinds: Send,
    S::ResponseKinds: Send,
    <S::ResponseKinds as Archive>::Archived: Send
        + rkyv::Deserialize<
            S::ResponseKinds,
            rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
        >,
    for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    // parse where we were asked to connect, and open the UI there
    let cli = Cli::parse();
    run::<S>(&cli.project, cli.tui).await
}

/// Run a terminal UI for a schema on a runtime of its own
///
/// The whole of a schema's terminal UI program: what the wrapper `shoalctl` generates calls,
/// and what one written by hand calls, so neither needs a runtime dependency of its own.
///
/// # Errors
///
/// When the runtime cannot be built, or whatever the UI failed with.
pub fn main_blocking<S>() -> color_eyre::Result<()>
where
    S: QuerySupport + Send + Sync + 'static,
    S::TableNames: Send,
    S::QueryKinds: Send,
    S::ResponseKinds: Send,
    <S::ResponseKinds as Archive>::Archived: Send
        + rkyv::Deserialize<
            S::ResponseKinds,
            rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
        >,
    for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    // a runtime like the one `#[tokio::main]` builds; `run` installs the error reporter itself
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(main::<S>())
}

/// Open the terminal UI for a schema where the arguments say
///
/// # Arguments
///
/// * `project` - What the command line said about the project, for the inventory's default
/// * `args` - Where to connect
///
/// # Errors
///
/// Whatever the UI failed with.
pub async fn run<S>(project: &ProjectArgs, args: TuiArgs) -> color_eyre::Result<()>
where
    S: QuerySupport + Send + Sync + 'static,
    S::TableNames: Send,
    S::QueryKinds: Send,
    S::ResponseKinds: Send,
    <S::ResponseKinds as Archive>::Archived: Send
        + rkyv::Deserialize<
            S::ResponseKinds,
            rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
        >,
    for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    // an address connects as nobody; anything else is a deployed cluster, connected to as its
    // admin, or the default address when nothing names one
    let shoal: Arc<Shoal<S>> = match (&args.addr, inventory(project, args.inventory.as_deref())?) {
        (Some(addr), _) => connect::<S>(addr).await?,
        (None, Some(path)) => {
            // only its state is needed, not the program it was deployed with
            let deployment = Deployment::attach(&path)?;
            let record = deployment.state.record()?;
            deployment.any_member::<S>(&record).await?
        }
        (None, None) => connect::<S>(DEFAULT_ADDR).await?,
    };
    crate::run(shoal).await
}

/// Which inventory the UI opens: the flag, the project's file or the config's default, or none
///
/// # Arguments
///
/// * `project` - What the command line said about the project
/// * `flag` - The `-i` given, if any
///
/// # Errors
///
/// When the config cannot be read.
fn inventory(project: &ProjectArgs, flag: Option<&std::path::Path>) -> color_eyre::Result<Option<PathBuf>> {
    let config = Config::load()?;
    let dir = project.dir().ok();
    // nothing naming a cluster is not an error here: the default address is
    Ok(config::inventory(flag, dir.as_deref(), &config).ok())
}

/// Connect to a node as nobody
///
/// # Arguments
///
/// * `addr` - The node
async fn connect<S>(addr: &str) -> color_eyre::Result<Arc<Shoal<S>>>
where
    S: QuerySupport + Send + Sync + 'static,
    for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    Ok(Arc::new(
        Shoal::<S>::new(addr)
            .await
            .map_err(|error| color_eyre::eyre::eyre!("{addr}: {error:?}"))?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse a terminal UI command line
    ///
    /// # Arguments
    ///
    /// * `args` - The arguments after the program's name
    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(std::iter::once("shoalctl").chain(args.iter().copied()))
    }

    /// No subcommand is needed, an address and an inventory exclude each other, and the
    /// project flags are taken
    #[test]
    fn the_ui_needs_no_subcommand() {
        let cli = parse(&[]).unwrap();
        assert_eq!(cli.tui.addr, None);
        assert_eq!(cli.tui.inventory, None);
        let cli = parse(&["-i", "lab.yml", "--project", "/src/demo", "--db", "Demo"]).unwrap();
        assert_eq!(cli.tui.inventory, Some(PathBuf::from("lab.yml")));
        assert_eq!(cli.project.db.as_deref(), Some("Demo"));
        assert_eq!(parse(&["--addr", "10.0.0.1:12000"]).unwrap().tui.addr.as_deref(), Some("10.0.0.1:12000"));
        assert!(parse(&["--addr", "10.0.0.1:12000", "-i", "lab.yml"]).is_err());
        assert!(parse(&["tui"]).is_err());
        assert!(parse(&["cluster", "status"]).is_err());
    }
}
