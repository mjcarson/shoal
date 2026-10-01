//! The command line of an admin program
//!
//! Two programs run it. `shoaladm` itself, which knows no schema: it runs the commands that
//! never connect to a cluster, and for the rest builds an admin program for the schema of the
//! project it was run in and hands the command over ([F63](../../docs/src/features/shoaladm.md)).
//! And that admin program, or one a schema writes by hand, which is one line:
//!
//! ```ignore
//! fn main() -> shoaladm::Result<()> {
//!     shoaladm::cli::main_blocking::<MyDbClient>()
//! }
//! ```
//!
//! `deploy` makes a cluster run the project: a bootstrap the first time, a rolling upgrade after
//! ([F51](../../docs/src/features/cluster-deployment.md),
//! [F55](../../docs/src/features/cluster-upgrade.md)); `new` builds an inventory in a full
//! screen form ([F53](../../docs/src/features/inventory-wizard.md)); the rest are the
//! day-to-day. A program with commands of its own flattens [`Command`] into its own subcommand
//! enum and hands whatever it does not handle itself to [`run`], which is how the TMDB dataset
//! loader carries every admin command beside its `load`
//! ([F54](../../docs/src/features/tmdb-dataset-deployment.md)):
//!
//! ```ignore
//! #[derive(clap::Subcommand)]
//! enum MyCommand {
//!     Load(LoadArgs),
//!     #[command(flatten)]
//!     Shoaladm(shoaladm::cli::Command),
//! }
//! ```

use clap::{Args, Parser, Subcommand};
use rkyv::Archive;
use shoal::shared::dataset::DatasetSupport;
use shoal::shared::traits::QuerySupport;
use std::path::PathBuf;

use crate::build::{self, Role, Target};
use crate::config::{self, Config};
use crate::deploy::{Deployment, ProjectHint};
use crate::project::Project;

/// Deploy, upgrade and operate a Shoal cluster
#[derive(Parser, Debug)]
#[command(author, version, about)]
pub struct Cli {
    /// The project the schema is in, and its programs are built from; the current directory if not given
    #[clap(flatten)]
    pub project: ProjectArgs,
    /// What to do
    #[clap(subcommand)]
    pub command: Command,
}

/// Which project, and which of its databases, a command is for
#[derive(Args, Debug, Clone, Default)]
pub struct ProjectArgs {
    /// The Rust project that defines the schema; the current directory if not given
    #[clap(long, global = true)]
    pub project: Option<PathBuf>,
    /// The `#[shoal::db]` struct to deploy, when the project defines more than one
    #[clap(long, global = true)]
    pub db: Option<String>,
}

impl ProjectArgs {
    /// What the deployment is told about the project
    #[must_use]
    pub fn hint(&self) -> ProjectHint {
        ProjectHint {
            dir: self.project.clone(),
            db: self.db.clone(),
        }
    }

    /// The project directory these arguments name, or the one the command runs in
    ///
    /// # Errors
    ///
    /// When the current directory cannot be read.
    pub fn dir(&self) -> color_eyre::Result<PathBuf> {
        match &self.project {
            Some(dir) => Ok(dir.clone()),
            None => std::env::current_dir().map_err(Into::into),
        }
    }
}

/// The inventory a command reads, if it was named; resolved by [`config::inventory`] otherwise
#[derive(Args, Debug, Clone, Default)]
pub struct InventoryArg {
    /// The inventory describing the cluster; the project's inventory.yml or the config's default if not given
    #[clap(long, short)]
    pub inventory: Option<PathBuf>,
}

impl InventoryArg {
    /// Which inventory this is: the flag, the project's file, or the config's default
    ///
    /// # Arguments
    ///
    /// * `project` - The project the command runs for
    ///
    /// # Errors
    ///
    /// When nothing names one.
    pub fn resolve(&self, project: &ProjectArgs) -> color_eyre::Result<PathBuf> {
        let config = Config::load()?;
        let dir = project.dir().ok();
        config::inventory(self.inventory.as_deref(), dir.as_deref(), &config)
    }
}

/// The things an admin program can be asked to do
#[derive(Subcommand, Debug)]
pub enum Command {
    /// Make the cluster run this project: bootstrap it if it does not exist, upgrade it if it does
    Deploy {
        /// The inventory
        #[clap(flatten)]
        inventory: InventoryArg,
        /// Delete a node already claimed on a host rather than refusing it, when bootstrapping
        #[clap(long)]
        wipe: bool,
    },
    /// Build the project's programs without deploying anything: the node for this machine or
    /// for named cpus, the admin program and the terminal UI
    Build {
        /// The cpus to build the node for; this machine's if none, or every host's with --inventory
        #[clap(long = "target-cpu")]
        target_cpus: Vec<String>,
        /// An inventory whose hosts are probed for their cpus
        #[clap(long, short)]
        inventory: Option<PathBuf>,
    },
    /// Show or set the operator's config: the default inventory and where programs are installed
    #[clap(subcommand)]
    Config(ConfigCommand),
    /// Build an inventory in a full screen form, or edit one with --from
    New {
        /// Where to write the inventory
        #[clap(long, short)]
        out: PathBuf,
        /// An inventory to start from
        #[clap(long)]
        from: Option<PathBuf>,
    },
    /// Deploy a new cluster over the inventory's bootstrap set, initialize it and wait for writes
    Bootstrap {
        /// The inventory
        #[clap(flatten)]
        inventory: InventoryArg,
        /// Delete a node already claimed on a host rather than refusing it
        #[clap(long)]
        wipe: bool,
    },
    /// Join a node of the inventory to the deployed cluster
    Add {
        /// The inventory
        #[clap(flatten)]
        inventory: InventoryArg,
        /// The node to add, by its inventory name
        node: String,
        /// Delete a node already claimed on its host rather than refusing it
        #[clap(long)]
        wipe: bool,
        /// Move a share of the cluster's data onto it once it has joined
        #[clap(long)]
        rebalance: bool,
    },
    /// Rebuild one node from its peers under a new identity: stop it, wipe it, join it as a new
    /// member and move every set the old identity held onto it
    /// ([F56](../../docs/src/features/cluster-rebuild.md))
    Rebuild {
        /// The inventory
        #[clap(flatten)]
        inventory: InventoryArg,
        /// The node to rebuild, by its inventory name
        node: String,
        /// Confirm that everything under the node's storage roots is deleted
        #[clap(long)]
        yes: bool,
    },
    /// Move data onto members holding less than their share, and follow the plan
    Rebalance {
        /// The inventory
        #[clap(flatten)]
        inventory: InventoryArg,
    },
    /// Send an operation the cluster tab's command line takes and follow it until it is done
    ///
    /// `repair <table> [verify|repair]`, `backup [table] <dir>`, `restore <dir>`, `status <op>`
    /// and the rest of the tab's operations, sent without its preview.
    Admin {
        /// The inventory
        #[clap(flatten)]
        inventory: InventoryArg,
        /// How many seconds to follow the operation's record before giving up on it
        #[clap(long, default_value_t = 3600)]
        timeout_secs: u64,
        /// The operation, as the cluster tab takes it
        #[clap(required = true, trailing_var_arg = true)]
        line: Vec<String>,
    },
    /// Copy every host's files of a backup to every host, so a restore finds each group's file
    /// on whichever node leads it ([F59](../../docs/src/features/backup-shipping.md))
    ShipBackup {
        /// The inventory of the cluster whose hosts hold the backup
        #[clap(flatten)]
        inventory: InventoryArg,
        /// The backup's directory, `<path>/<op>`, the same path on every host
        dir: String,
        /// The inventory whose hosts are to hold the whole backup, if not this one's
        #[clap(long)]
        to: Option<PathBuf>,
    },
    /// Print every node, its unit, and the cluster as a member sees it
    Status {
        /// The inventory
        #[clap(flatten)]
        inventory: InventoryArg,
    },
    /// Chart every member's queries, speeds, latency, memory, standing, partitions and write
    /// rates, and every plan's progress, full screen; or print them with --basic
    ///
    /// Members are named by hostname ([F64](../../docs/src/features/stats-tui.md)). The full
    /// screen view opens on a home tab of the cluster's totals, six charts and a table of
    /// members ([F65](../../docs/src/features/query-figures-home-tab.md)), and is drawn only
    /// when stdout is a terminal, so a pipe or a script gets lines.
    Stats {
        /// The inventory
        #[clap(flatten)]
        inventory: InventoryArg,
        /// Narrow the figures to one table, by the name the schema spells it
        #[clap(long)]
        table: Option<String>,
        /// Read the figures every so many seconds, two if none is given: the full screen view's
        /// interval, or with --basic print again until interrupted
        #[clap(long, num_args = 0..=1, default_missing_value = "2")]
        watch: Option<u64>,
        /// Print the leader's answer as json rather than as lines
        #[clap(long)]
        json: bool,
        /// Print the figures as lines instead of the full screen view; implied by --json and
        /// when stdout is not a terminal
        #[clap(long)]
        basic: bool,
    },
    /// Start a node's unit, or every node's
    Start {
        /// The inventory
        #[clap(flatten)]
        inventory: InventoryArg,
        /// The node, or every deployed node if none
        node: Option<String>,
    },
    /// Stop a node's unit, or every node's
    Stop {
        /// The inventory
        #[clap(flatten)]
        inventory: InventoryArg,
        /// The node, or every deployed node if none
        node: Option<String>,
    },
    /// Restart a node's unit, or every node's
    Restart {
        /// The inventory
        #[clap(flatten)]
        inventory: InventoryArg,
        /// The node, or every deployed node if none
        node: Option<String>,
    },
    /// Replace every node's program with this build, one node at a time, the leader last
    Upgrade {
        /// The inventory
        #[clap(flatten)]
        inventory: InventoryArg,
        /// The nodes to upgrade, or every deployed node if none
        nodes: Vec<String>,
        /// Restart a node even when it already runs this program
        #[clap(long)]
        force: bool,
        /// Once every node is done, activate the wire version they all speak: no rollback past it
        #[clap(long)]
        activate: bool,
        /// Swap every node back onto the program its last upgrade replaced
        #[clap(long, conflicts_with_all = ["force", "activate"])]
        rollback: bool,
    },
    /// Render every node's shoal.yml again from the inventory and restart the ones that changed, the leader last
    Reconfigure {
        /// The inventory
        #[clap(flatten)]
        inventory: InventoryArg,
        /// The nodes to reconfigure, or every deployed node if none
        nodes: Vec<String>,
        /// Restart a node even when its file did not change
        #[clap(long)]
        force: bool,
    },
    /// Print the end of a node's journal
    Logs {
        /// The inventory
        #[clap(flatten)]
        inventory: InventoryArg,
        /// The node
        node: String,
        /// How many lines
        #[clap(long, short = 'n', default_value_t = 200)]
        lines: usize,
    },
    /// Benchmark the project's schema against a dataset folder, and list, show and compare the
    /// captures ([F66](../../docs/src/features/dataset-benchmarks.md))
    #[clap(subcommand)]
    Bench(crate::bench::BenchCommand),
    /// Stop and delete every node of the inventory, its data, and the local state
    Destroy {
        /// The inventory
        #[clap(flatten)]
        inventory: InventoryArg,
        /// Say so: this deletes every row the cluster holds
        #[clap(long)]
        yes: bool,
    },
}

/// The config commands
#[derive(Subcommand, Debug)]
pub enum ConfigCommand {
    /// Print the config, and where it is read from
    Show,
    /// Set the inventory every command reads when it is given none
    DefaultInventory {
        /// The inventory
        path: PathBuf,
    },
}

impl Command {
    /// Whether this command connects to the cluster, and so has to be run by a program built
    /// for its schema
    #[must_use]
    pub fn needs_schema(&self) -> bool {
        matches!(
            self,
            Command::Deploy { .. }
                | Command::Bootstrap { .. }
                | Command::Add { .. }
                | Command::Rebuild { .. }
                | Command::Rebalance { .. }
                | Command::Admin { .. }
                | Command::Status { .. }
                | Command::Stats { .. }
                | Command::Upgrade { .. }
                | Command::Reconfigure { .. }
        ) || matches!(self, Command::Bench(command) if command.needs_schema())
    }
}

/// Run an admin program for a schema, reading the command from the process arguments
///
/// # Errors
///
/// Whatever the command failed with.
pub async fn main<S>() -> color_eyre::Result<()>
where
    S: DatasetSupport + Send + Sync + 'static,
    S::QueryKinds: Send + Sync + 'static,
    <S::QueryKinds as Archive>::Archived: Send + Sync,
    S::ResponseKinds: Send + Sync,
    <S::ResponseKinds as Archive>::Archived: Send
        + Sync
        + rkyv::Deserialize<S::ResponseKinds, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
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
    // parse what we were asked to do, and do it
    let cli = Cli::parse();
    run::<S>(&cli.project, cli.command).await
}

/// Run an admin program for a schema on a runtime of its own
///
/// The whole of a schema's admin program: what the wrapper `shoaladm` generates calls, and
/// what one written by hand calls, so neither needs a runtime dependency of its own.
///
/// # Errors
///
/// When the runtime cannot be built, or whatever the command failed with.
pub fn main_blocking<S>() -> color_eyre::Result<()>
where
    S: DatasetSupport + Send + Sync + 'static,
    S::QueryKinds: Send + Sync + 'static,
    <S::QueryKinds as Archive>::Archived: Send + Sync,
    S::ResponseKinds: Send + Sync,
    <S::ResponseKinds as Archive>::Archived: Send
        + Sync
        + rkyv::Deserialize<S::ResponseKinds, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
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
    // report errors with their context
    color_eyre::install()?;
    // a runtime like the one `#[tokio::main]` builds
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(main::<S>())
}

/// Run one admin command for a schema
///
/// # Arguments
///
/// * `project` - What the command line said about the project
/// * `command` - The command, as parsed by this program or by one that flattens [`Command`]
///
/// # Errors
///
/// Whatever the command failed with.
pub async fn run<S>(project: &ProjectArgs, command: Command) -> color_eyre::Result<()>
where
    S: DatasetSupport + Send + Sync + 'static,
    S::QueryKinds: Send + Sync + 'static,
    <S::QueryKinds as Archive>::Archived: Send + Sync,
    S::ResponseKinds: Send + Sync,
    <S::ResponseKinds as Archive>::Archived: Send
        + Sync
        + rkyv::Deserialize<S::ResponseKinds, rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>>,
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
    // the commands that never connect, then the ones that do
    let command = match run_local(project, command).await? {
        Some(command) => command,
        None => return Ok(()),
    };
    let hint = project.hint();
    let open = |inventory: &InventoryArg| -> color_eyre::Result<Deployment> {
        Deployment::open(&inventory.resolve(project)?, hint.clone())
    };
    match command {
        Command::Deploy { inventory, wipe } => open(&inventory)?.deploy::<S>(wipe).await,
        Command::Bootstrap { inventory, wipe } => open(&inventory)?.bootstrap::<S>(wipe).await,
        Command::Add {
            inventory,
            node,
            wipe,
            rebalance,
        } => open(&inventory)?.add::<S>(&node, wipe, rebalance).await,
        Command::Rebuild {
            inventory,
            node,
            yes,
        } => open(&inventory)?.rebuild::<S>(&node, yes).await,
        Command::Rebalance { inventory } => {
            // any member answers, and the leader drives the plan
            let deployment = open(&inventory)?;
            let record = deployment.state.record()?;
            let shoal = deployment.any_member::<S>(&record).await?;
            deployment.rebalance(&shoal).await
        }
        Command::Admin {
            inventory,
            timeout_secs,
            line,
        } => {
            // any member answers, and forwards what only the leader can do
            let deployment = open(&inventory)?;
            let record = deployment.state.record()?;
            let shoal = deployment.any_member::<S>(&record).await?;
            deployment
                .admin(&shoal, &line.join(" "), std::time::Duration::from_secs(timeout_secs))
                .await
        }
        Command::Status { inventory } => open(&inventory)?.status::<S>().await,
        Command::Stats {
            inventory,
            table,
            watch,
            json,
            basic,
        } => {
            open(&inventory)?
                .stats::<S>(table.as_deref(), watch, json, basic)
                .await
        }
        Command::Upgrade {
            inventory,
            nodes,
            force,
            activate,
            rollback,
        } => {
            open(&inventory)?
                .upgrade::<S>(&nodes, force, activate, rollback)
                .await
        }
        Command::Reconfigure {
            inventory,
            nodes,
            force,
        } => open(&inventory)?.reconfigure::<S>(&nodes, force).await,
        Command::Bench(command) => crate::bench::run::<S>(project, command).await,
        // everything else was run above
        other => unreachable!("{other:?} connects to no cluster and was run already"),
    }
}

/// Run a command that connects to no cluster, or hand back one that does
///
/// The generic `shoaladm` program runs these itself, since they need no schema.
///
/// # Arguments
///
/// * `project` - What the command line said about the project
/// * `command` - The command
///
/// # Errors
///
/// Whatever the command failed with.
pub async fn run_local(project: &ProjectArgs, command: Command) -> color_eyre::Result<Option<Command>> {
    match command {
        Command::New { out, from } => crate::wizard::run(out, from).await?,
        Command::Build {
            target_cpus,
            inventory,
        } => build_all(project, &target_cpus, inventory.as_deref())?,
        Command::Config(command) => config_command(command)?,
        Command::ShipBackup { inventory, dir, to } => {
            // only the hosts are needed, not a running cluster: the new one may not exist yet
            let deployment = Deployment::attach(&inventory.resolve(project)?)?;
            let to = to
                .map(|path| crate::deploy::Inventory::read(&path))
                .transpose()?;
            deployment.ship_backup(&dir, to.as_ref())?;
        }
        Command::Start { inventory, node } => {
            Deployment::attach(&inventory.resolve(project)?)?.systemctl("start", node.as_deref())?;
        }
        Command::Stop { inventory, node } => {
            Deployment::attach(&inventory.resolve(project)?)?.systemctl("stop", node.as_deref())?;
        }
        Command::Restart { inventory, node } => {
            Deployment::attach(&inventory.resolve(project)?)?.systemctl("restart", node.as_deref())?;
        }
        Command::Logs {
            inventory,
            node,
            lines,
        } => Deployment::attach(&inventory.resolve(project)?)?.logs(&node, lines)?,
        Command::Destroy { inventory, yes } => {
            // deleting a cluster's every row is asked for in words
            if !yes {
                color_eyre::eyre::bail!(
                    "destroy deletes every node, every row and the cluster's authority; pass --yes"
                );
            }
            Deployment::attach(&inventory.resolve(project)?)?.destroy()?;
        }
        Command::Bench(command) => {
            // a capture is read here; a run connects, and is handed back
            if let Some(run) = crate::bench::store::run_local(project, command)? {
                return Ok(Some(Command::Bench(run)));
            }
        }
        other => return Ok(Some(other)),
    }
    Ok(None)
}

/// Run a config command
///
/// # Arguments
///
/// * `command` - The command
fn config_command(command: ConfigCommand) -> color_eyre::Result<()> {
    let path = Config::path()?;
    match command {
        ConfigCommand::Show => {
            let config = Config::read(&path)?;
            println!(
                "{}: {}",
                path.display(),
                if path.is_file() { "read" } else { "not written yet; the defaults" }
            );
            println!(
                "default_inventory: {}",
                config
                    .default_inventory
                    .as_ref()
                    .map_or("none".to_string(), |inventory| inventory.display().to_string())
            );
            println!("bin_dir: {}", config.bin_dir()?.display());
            Ok(())
        }
        ConfigCommand::DefaultInventory { path: inventory } => {
            // kept absolute, so it means the same file from anywhere
            let inventory = inventory
                .canonicalize()
                .map_err(|error| color_eyre::eyre::eyre!("{}: {error}", inventory.display()))?;
            let mut config = Config::read(&path)?;
            config.default_inventory = Some(inventory.clone());
            config.save(&path)?;
            println!("{}: default_inventory is {}", path.display(), inventory.display());
            Ok(())
        }
    }
}

/// Build every program of the project: the node for this machine, for named cpus or for an
/// inventory's hosts, and the admin program and terminal UI for this machine
///
/// # Arguments
///
/// * `project` - What the command line said about the project
/// * `target_cpus` - The cpus to build the node for, if any were named
/// * `inventory` - An inventory whose hosts are probed for theirs, if one was named
fn build_all(
    project: &ProjectArgs,
    target_cpus: &[String],
    inventory: Option<&std::path::Path>,
) -> color_eyre::Result<()> {
    let located = Project::locate(&project.dir()?)?;
    let schema = located.scan(project.db.as_deref())?;
    let config = Config::load()?;
    // the cpus: named, probed, or this machine's
    let mut targets: Vec<Target> = target_cpus.iter().cloned().map(Target::Cpu).collect();
    if let Some(path) = inventory {
        let inventory = crate::deploy::Inventory::read(path)?;
        let known = crate::cpu::known_names()?;
        for spec in &inventory.nodes {
            let node = inventory.node(&spec.name)?;
            let facts = crate::cpu::probe(&node.target)?;
            let target = crate::cpu::decide(&node.name, node.target_cpu.as_deref(), &facts, &known)?;
            eprintln!("[{}] {}: {target}", node.name, facts.name);
            if !targets.iter().any(|known| *known == Target::Cpu(target.clone())) {
                targets.push(Target::Cpu(target));
            }
        }
    }
    if targets.is_empty() {
        targets.push(Target::Native);
    }
    // the node for each, then the two programs for here
    let total = targets.len() + 2;
    for (index, target) in targets.iter().enumerate() {
        let note = format!("{} of {total}", index + 1);
        build::program(&located, &schema, Role::Node, target, &config, Some(&note))?;
    }
    for (offset, role) in [Role::Adm, Role::Ctl].into_iter().enumerate() {
        let note = format!("{} of {total}", targets.len() + offset + 1);
        build::program(&located, &schema, role, &Target::Native, &config, Some(&note))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse an admin command line
    ///
    /// # Arguments
    ///
    /// * `args` - The arguments after the program's name
    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(std::iter::once("shoaladm").chain(args.iter().copied()))
    }

    /// An upgrade takes a list of nodes, and a rollback neither forces nor activates
    #[test]
    fn upgrade_parses_its_nodes_and_refuses_a_forced_rollback() {
        // the nodes are positional, after the inventory
        let cli = parse(&["upgrade", "-i", "lab.yml", "a", "b", "--activate"]).unwrap();
        let Command::Upgrade { nodes, activate, rollback, .. } = cli.command else {
            panic!("not an upgrade");
        };
        assert_eq!(nodes, vec!["a", "b"]);
        assert!(activate && !rollback);
        // a rollback is only a rollback
        assert!(parse(&["upgrade", "-i", "lab.yml", "--rollback", "--activate"]).is_err());
        assert!(parse(&["upgrade", "-i", "lab.yml", "--rollback", "--force"]).is_err());
        assert!(parse(&["upgrade", "-i", "lab.yml", "--rollback"]).is_ok());
    }

    /// Stats takes --basic beside --watch and --json, and a bare --watch reads every two seconds
    #[test]
    fn stats_takes_basic() {
        // the full screen view is the default
        let cli = parse(&["stats", "-i", "lab.yml"]).unwrap();
        let Command::Stats { basic, watch, json, .. } = cli.command else {
            panic!("not stats");
        };
        assert!(!basic && !json);
        assert_eq!(watch, None);
        // --basic asks for lines, with or without a watch
        let cli = parse(&["stats", "--basic", "--watch", "--table", "Movie"]).unwrap();
        let Command::Stats { basic, watch, table, .. } = cli.command else {
            panic!("not stats");
        };
        assert!(basic);
        assert_eq!(watch, Some(2));
        assert_eq!(table.as_deref(), Some("Movie"));
        // and it still needs the schema's program
        assert!(parse(&["stats", "--basic"]).unwrap().command.needs_schema());
    }

    /// The project and the database are taken anywhere on the line, the inventory is optional,
    /// and the commands that connect are told from the ones that do not
    #[test]
    fn the_project_is_global_and_the_inventory_optional() {
        let cli = parse(&["deploy", "--project", "/src/demo", "--db", "Demo"]).unwrap();
        assert_eq!(cli.project.project, Some(PathBuf::from("/src/demo")));
        assert_eq!(cli.project.db.as_deref(), Some("Demo"));
        let Command::Deploy { inventory, wipe } = cli.command else {
            panic!("not a deploy");
        };
        assert_eq!(inventory.inventory, None);
        assert!(!wipe);
        assert!(cli.project.hint().dir.is_some());
        // the same flags before the command
        let cli = parse(&["--db", "Demo", "status", "-i", "x.yml"]).unwrap();
        assert_eq!(cli.project.db.as_deref(), Some("Demo"));
        assert!(cli.command.needs_schema());
        // what needs a schema and what does not
        assert!(parse(&["bootstrap"]).unwrap().command.needs_schema());
        assert!(parse(&["admin", "-i", "x.yml", "backup", "/b"]).unwrap().command.needs_schema());
        assert!(!parse(&["new", "-o", "x.yml"]).unwrap().command.needs_schema());
        assert!(!parse(&["config", "show"]).unwrap().command.needs_schema());
        assert!(!parse(&["build", "--target-cpu", "znver1", "--target-cpu", "znver4"]).unwrap().command.needs_schema());
        assert!(!parse(&["destroy", "--yes"]).unwrap().command.needs_schema());
        assert!(!parse(&["logs", "a"]).unwrap().command.needs_schema());
        // no `cluster` prefix any more
        assert!(parse(&["cluster", "status"]).is_err());
        assert!(parse(&["tui"]).is_err());
    }
}
