//! A server program for any schema, in one call
//!
//! A schema is a compile-time construct, so Shoal ships no server binary: a program is built
//! against each schema. This is the whole of that program, so a schema's own is three lines:
//!
//! ```ignore
//! fn main() -> Result<(), shoal::server::ServerError> {
//!     shoal::server::node::main::<MyDb>()
//! }
//! ```
//!
//! It is what `shoalctl cluster` deploys ([F51](../../../docs/src/features/cluster-deployment.md)):
//! `claim` gives a node its identity before its first start, so the deployment can issue it a
//! certificate naming that identity, and `serve` runs the node until it is told to stop.

use clap::{Parser, Subcommand};
use rkyv::bytecheck::CheckBytes;
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::validation::{archive::ArchiveValidator, shared::SharedValidator, Validator};
use rkyv::Archive;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::instrument;

use super::database::ShoalDatabase;
use super::errors::ShoalError;
use super::{trace, Conf, ServerError};
use crate::shared::identity::{ClusterId, NodeId};
use crate::shared::{queries::Queries, traits::QuerySupport};
use crate::ShoalPool;

/// The line `serve` prints once every shard answers, followed by the bound address
pub const NODE_READY_LINE: &str = "SHOAL_NODE_SERVING";

/// How long `serve` waits for its shards and control plane to answer
const READY_TIMEOUT: Duration = Duration::from_secs(120);

/// How often `serve` looks for a stop signal or a dead shard
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Serves one schema as a Shoal node
#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    /// What to do
    #[clap(subcommand)]
    command: Command,
}

/// The things a node program can be asked to do
#[derive(Subcommand, Debug)]
enum Command {
    /// Serve this node until it receives SIGTERM or SIGINT
    Serve(ConfArgs),
    /// Claim this node's storage directory without starting it and print who it is as json
    Claim(ConfArgs),
}

/// The configuration a node command reads
#[derive(Parser, Debug)]
struct ConfArgs {
    /// The path to the node's configuration, with `SHOAL_*` environment variables on top
    #[clap(long, default_value = "shoal.yml")]
    conf: PathBuf,
}

/// Who a claimed directory belongs to, as `claim` prints it
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ClaimReport {
    /// The node the directory belongs to
    pub node: NodeId,
    /// The cluster it was bootstrapped into, or none for a joiner that has not been admitted
    pub cluster: Option<ClusterId>,
    /// How many slots the node claimed
    pub slots: usize,
}

/// Run a node program for a schema, reading the command from the process arguments
///
/// # Errors
///
/// Whatever the configuration, the claim, or the server refused with.
pub fn main<D: ShoalDatabase>() -> Result<(), ServerError>
where
    <<D::ClientType as QuerySupport>::QueryKinds as Archive>::Archived: rkyv::Deserialize<
        <D::ClientType as QuerySupport>::QueryKinds,
        Strategy<Pool, rkyv::rancor::Error>,
    >,
    for<'a> <Queries<D::ClientType> as Archive>::Archived:
        CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
    for<'a> <<D::ClientType as QuerySupport>::QueryKinds as Archive>::Archived:
        CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
{
    // parse what we were asked to do
    let cli = Cli::parse();
    match cli.command {
        Command::Serve(args) => serve::<D>(&load(&args.conf)?),
        Command::Claim(args) => {
            // claim the directory and print who it belongs to on one line
            let report = claim(&load(&args.conf)?)?;
            println!("{}", serde_json::to_string(&report)?);
            Ok(())
        }
    }
}

/// Load a node's configuration, refusing a path that does not exist
///
/// `Conf::from_file` treats a missing file as defaults, which for a deployed node would be a
/// standalone server on the loopback serving nothing anybody asked for.
///
/// # Arguments
///
/// * `path` - The configuration file
fn load(path: &std::path::Path) -> Result<Conf, ServerError> {
    // a missing file is a mistake here rather than a request for the defaults
    if !path.is_file() {
        return Err(ServerError::Shoal(ShoalError::InvalidConfig(format!(
            "{} is not a file",
            path.display()
        ))));
    }
    // read it the way every other server does
    let path = path.to_str().ok_or_else(|| {
        ServerError::Shoal(ShoalError::InvalidConfig(format!(
            "{} is not a utf-8 path",
            path.display()
        )))
    })?;
    Ok(Conf::from_file(path)?)
}

/// Claim a node's directory and report who it belongs to
///
/// # Arguments
///
/// * `conf` - The node's configuration
#[instrument(name = "node::claim", skip_all, err(Debug))]
pub fn claim(conf: &Conf) -> Result<ClaimReport, ServerError> {
    // claim it exactly as the first start would
    let identity = super::claim(conf)?;
    Ok(ClaimReport {
        node: identity.node,
        cluster: identity.cluster,
        slots: identity.slots,
    })
}

/// Serve a node until it is told to stop or a shard dies
///
/// # Arguments
///
/// * `conf` - The node's configuration
fn serve<D: ShoalDatabase>(conf: &Conf) -> Result<(), ServerError>
where
    <<D::ClientType as QuerySupport>::QueryKinds as Archive>::Archived: rkyv::Deserialize<
        <D::ClientType as QuerySupport>::QueryKinds,
        Strategy<Pool, rkyv::rancor::Error>,
    >,
    for<'a> <Queries<D::ClientType> as Archive>::Archived:
        CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
    for<'a> <<D::ClientType as QuerySupport>::QueryKinds as Archive>::Archived:
        CheckBytes<Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>>,
{
    // register for the signals a supervisor stops us with before anything starts
    let stop = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&stop))?;
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&stop))?;
    // tracing, and the guard that flushes the exporter when the process ends
    let traces = trace::setup(conf);
    // start every shard and the control plane, and wait for them to answer
    let mut pool = ShoalPool::<D>::start(conf.clone())?;
    let addr = pool.ready(READY_TIMEOUT)?;
    // say so on one line, for whoever started us
    println!("{NODE_READY_LINE} {addr}");
    std::io::stdout().flush()?;
    // hold the node up until asked to stop, reporting a shard that dies rather than serving on
    let outcome = loop {
        // a dead shard ends the node, so its supervisor restarts it
        if let Some((shard, error)) = pool.failure() {
            break Err(ServerError::ShardFailed { shard, error });
        }
        // a stop signal ends it cleanly
        if stop.load(Ordering::Relaxed) {
            break Ok(());
        }
        std::thread::sleep(POLL_INTERVAL);
    };
    // stop every shard and the control plane, keeping the first error
    let exited = pool.exit();
    trace::shutdown(traces);
    outcome.and(exited)
}
