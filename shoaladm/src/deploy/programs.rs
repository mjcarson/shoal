//! Where a deployment's node program comes from, and one per host's cpu when it is built
//!
//! Before [F63](../../../docs/src/features/shoaladm.md) an inventory named one built program
//! and every host got it. Now it may name a project instead, or nothing, and the program is
//! built from that project (or the one the command runs in) once per cpu class the hosts turn
//! out to be: every host is asked what it has, the answers are grouped by the `target-cpu` they
//! map to, cargo builds each group's node once, and each host is shipped its own. The build
//! happens before any host is touched, so a build that fails changes nothing anywhere.

use color_eyre::eyre::{WrapErr, bail, eyre};
use std::cell::{OnceCell, RefCell};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use super::inventory::{Inventory, Node};
use super::ops::step;
use super::state::SchemaRecord;
use crate::build::{self, Role, Target};
use crate::config::Config;
use crate::cpu;
use crate::project::{Project, Schema};

/// What the command line said about the project, for an inventory that names none
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProjectHint {
    /// The project directory, or the one the command runs in
    pub dir: Option<PathBuf>,
    /// The database to deploy, when the project defines more than one
    pub db: Option<String>,
}

/// Where the node program comes from
#[derive(Debug)]
pub enum Programs {
    /// One built program for every host, the inventory's `server`
    Fixed(PathBuf),
    /// Built from a project, once per cpu class
    Built {
        /// The project
        project: Project,
        /// The database
        schema: Schema,
        /// Where builds are installed
        config: Config,
        /// The program each node was built, by node name, once `prepare` has run
        built: RefCell<BTreeMap<String, PathBuf>>,
    },
}

impl Programs {
    /// Decide where an inventory's program comes from
    ///
    /// A `server` is used as it is. A `project` is read and scanned. Neither is the project the
    /// hint names, or the one the command runs in.
    ///
    /// # Arguments
    ///
    /// * `inventory` - The inventory
    /// * `hint` - What the command line said about the project
    ///
    /// # Errors
    ///
    /// When the project cannot be read, or defines no database to deploy.
    pub fn resolve(inventory: &Inventory, hint: &ProjectHint) -> color_eyre::Result<Self> {
        // a program the operator built
        if let Some(server) = &inventory.server {
            return Ok(Programs::Fixed(server.clone()));
        }
        // otherwise a project: the inventory's, the command line's, or here
        let dir = match (&inventory.project, &hint.dir) {
            (Some(project), _) => project.clone(),
            (None, Some(dir)) => dir.clone(),
            (None, None) => std::env::current_dir().wrap_err("no current directory")?,
        };
        let project = Project::locate(&dir).wrap_err_with(|| {
            format!(
                "{} names no server program, so the node is built from a project",
                inventory.name
            )
        })?;
        let db = inventory.db.as_deref().or(hint.db.as_deref());
        let schema = project.scan(db)?;
        Ok(Programs::Built {
            project,
            schema,
            config: Config::load()?,
            built: RefCell::new(BTreeMap::new()),
        })
    }

    /// The file name the program has on every host
    #[must_use]
    pub fn server_name(&self) -> Option<String> {
        match self {
            Programs::Fixed(path) => path
                .file_name()
                .and_then(|name| name.to_str())
                .map(str::to_string),
            Programs::Built { project, schema, .. } => Some(build::server_name(project, schema)),
        }
    }

    /// What the state remembers of the schema, so the tools find its programs later
    #[must_use]
    pub fn schema_record(&self) -> Option<SchemaRecord> {
        match self {
            Programs::Fixed(_) => None,
            Programs::Built { project, schema, .. } => Some(SchemaRecord {
                package: project.name.clone(),
                db: schema.name.clone(),
            }),
        }
    }

    /// Have every node's program ready: the built one, or one build per cpu class among them
    ///
    /// # Arguments
    ///
    /// * `nodes` - The nodes about to be deployed to
    ///
    /// # Errors
    ///
    /// When a host cannot be probed, is another architecture, or a build fails.
    pub fn prepare(&self, nodes: &[Node]) -> color_eyre::Result<()> {
        let Programs::Built {
            project,
            schema,
            config,
            built,
        } = self
        else {
            return Ok(());
        };
        // what each host is, and what to build for it
        let known = cpu::known_names()?;
        let mut classes: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for node in nodes {
            // a node already built for is not asked again
            if built.borrow().contains_key(&node.name) {
                continue;
            }
            let facts = cpu::probe(&node.target)?;
            let target = cpu::decide(&node.name, node.target_cpu.as_deref(), &facts, &known)?;
            step(
                Some(&node.name),
                &format!(
                    "{}: building for {target}{}",
                    if facts.name.is_empty() { facts.arch.clone() } else { facts.name.clone() },
                    if node.target_cpu.is_some() { " (from the inventory)" } else { "" }
                ),
            );
            classes.entry(target).or_default().push(node.name.clone());
        }
        // one build per class, each landing under its own name
        let total = classes.len();
        for (index, (target, names)) in classes.iter().enumerate() {
            let note = format!("{} of {total}", index + 1);
            let program = build::program(
                project,
                schema,
                Role::Node,
                &Target::Cpu(target.clone()),
                config,
                Some(&note),
            )?;
            for name in names {
                built.borrow_mut().insert(name.clone(), program.clone());
            }
        }
        Ok(())
    }

    /// The program a node is shipped
    ///
    /// # Arguments
    ///
    /// * `node` - The node's inventory name
    ///
    /// # Errors
    ///
    /// When the node's program has not been built, which is a bug in the caller's order.
    pub fn program(&self, node: &str) -> color_eyre::Result<PathBuf> {
        match self {
            Programs::Fixed(path) => Ok(path.clone()),
            Programs::Built { built, .. } => built
                .borrow()
                .get(node)
                .cloned()
                .ok_or_else(|| eyre!("no program was built for {node}; prepare comes first")),
        }
    }
}

/// The programs a deployment resolves once, the first time it needs them
///
/// Resolving means reading the project and scanning it, which a command that only talks to
/// the cluster never has to do, and cannot when it runs outside the project.
#[derive(Debug, Default)]
pub struct LazyPrograms {
    /// What the command line said
    pub hint: ProjectHint,
    /// The programs, once resolved
    cell: OnceCell<Programs>,
}

impl LazyPrograms {
    /// Programs that will be resolved from an inventory and a hint on first use
    ///
    /// # Arguments
    ///
    /// * `hint` - What the command line said about the project
    #[must_use]
    pub fn new(hint: ProjectHint) -> Self {
        LazyPrograms {
            hint,
            cell: OnceCell::new(),
        }
    }

    /// Programs that are already known, for a test or a caller with a built program
    ///
    /// # Arguments
    ///
    /// * `programs` - The programs
    #[must_use]
    pub fn known(programs: Programs) -> Self {
        let cell = OnceCell::new();
        let _ = cell.set(programs);
        LazyPrograms {
            hint: ProjectHint::default(),
            cell,
        }
    }

    /// The programs, resolved now if they were not yet
    ///
    /// # Arguments
    ///
    /// * `inventory` - The inventory they are resolved from
    ///
    /// # Errors
    ///
    /// As [`Programs::resolve`].
    pub fn get(&self, inventory: &Inventory) -> color_eyre::Result<&Programs> {
        if let Some(programs) = self.cell.get() {
            return Ok(programs);
        }
        let programs = Programs::resolve(inventory, &self.hint)?;
        let _ = self.cell.set(programs);
        self.cell
            .get()
            .ok_or_else(|| eyre!("the programs were set and are not there"))
    }

    /// The programs if they were resolved already, without resolving them
    #[must_use]
    pub fn peek(&self) -> Option<&Programs> {
        self.cell.get()
    }
}

/// Whether a path is a file this machine could run, for the refusal a fixed program gets
///
/// # Arguments
///
/// * `path` - The program
pub fn check_program(path: &Path) -> color_eyre::Result<()> {
    if !path.is_file() {
        bail!("the server program {} does not exist; build it first", path.display());
    }
    Ok(())
}
