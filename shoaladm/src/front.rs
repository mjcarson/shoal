//! What the two generic programs do before handing a command to one built for the schema
//!
//! `shoaladm` and `shoalctl` know no schema. For a command that connects to a cluster they
//! find the project, build the program for its schema (the admin program or the terminal UI),
//! install it, and replace themselves with it, the arguments passed on whole. Run outside any
//! project they fall back to the program a previous build installed, named by the schema the
//! cluster's state remembers ([F63](../../docs/src/features/shoaladm.md)).

use color_eyre::eyre::{WrapErr, eyre};
use std::ffi::OsString;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::build::{self, Role, Target};
use crate::cli::ProjectArgs;
use crate::config::{self, Config};
use crate::deploy::Deployment;
use crate::project::Project;

/// The program to hand a command to, and what to tell it about the project
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handoff {
    /// The program
    pub program: PathBuf,
    /// The project it was built from and the database, to pass on as `--project` and `--db`
    pub project: Option<(PathBuf, String)>,
}

/// Find or build the program for a role
///
/// The project the arguments name, or the current directory, is read and its schema built
/// for. Where that is not a project, the inventory's cluster state is asked which schema it was
/// deployed from and that schema's installed program is used as it is.
///
/// # Arguments
///
/// * `project` - What the command line said about the project
/// * `inventory` - The `-i` the command was given, if any, for the fallback
/// * `role` - Which program
///
/// # Errors
///
/// When there is neither a project to build from nor an installed program to fall back on.
pub fn program_for(project: &ProjectArgs, inventory: Option<&Path>, role: Role) -> color_eyre::Result<Handoff> {
    let config = Config::load()?;
    let dir = project.dir()?;
    // a project at hand is built from, every time: cargo is quick when nothing changed
    let not_a_project = match Project::locate(&dir) {
        Ok(located) => {
            let schema = located.scan(project.db.as_deref())?;
            let program = build::program(&located, &schema, role, &Target::Native, &config, None)?;
            return Ok(Handoff {
                program,
                project: Some((located.dir.clone(), schema.name.clone())),
            });
        }
        Err(error) => error,
    };
    // otherwise whatever a previous build installed for the cluster's schema
    let installed = installed_for(&config, project, inventory, role)
        .wrap_err("and no installed program could be found to run instead")
        .map_err(|error| error.wrap_err(format!("{not_a_project:#}")))?;
    eprintln!(
        "[build] {} is not a Rust project; running {} as it was last built",
        dir.display(),
        installed.display()
    );
    Ok(Handoff {
        program: installed,
        project: None,
    })
}

/// The installed program of the schema an inventory's cluster was deployed from
///
/// # Arguments
///
/// * `config` - The operator's config, for where programs are installed
/// * `project` - What the command line said about the project, for the inventory's default
/// * `inventory` - The `-i` the command was given, if any
/// * `role` - Which program
///
/// # Errors
///
/// When no inventory can be found, its cluster remembers no schema, or the program is not there.
fn installed_for(
    config: &Config,
    project: &ProjectArgs,
    inventory: Option<&Path>,
    role: Role,
) -> color_eyre::Result<PathBuf> {
    // the cluster's state, through its inventory
    let path = config::inventory(inventory, project.dir().ok().as_deref(), config)?;
    let deployment = Deployment::attach(&path)?;
    let record = deployment.state.record()?;
    let schema = record.schema.ok_or_else(|| {
        eyre!(
            "{} was not deployed from a project, so no program was installed for it",
            deployment.inventory.name
        )
    })?;
    let name = build::installed_name(&schema.package, &schema.db, role, &Target::Native);
    let program = config.bin_dir()?.join(&name);
    if !program.is_file() {
        return Err(eyre!(
            "{} is not installed; run this in {}'s project once to build it",
            program.display(),
            schema.package
        ));
    }
    Ok(program)
}

/// The arguments to hand on: the process's own, with the project made explicit
///
/// # Arguments
///
/// * `handoff` - The program and what it was built from
#[must_use]
pub fn arguments(handoff: &Handoff) -> Vec<OsString> {
    let mut args: Vec<OsString> = std::env::args_os().skip(1).collect();
    if let Some((dir, db)) = &handoff.project {
        args.push("--project".into());
        args.push(dir.clone().into());
        args.push("--db".into());
        args.push(db.clone().into());
    }
    args
}

/// Replace this process with the program, passing the arguments on
///
/// # Arguments
///
/// * `handoff` - The program and what it was built from
///
/// # Errors
///
/// Only when the program could not be started; on success this never returns.
pub fn exec(handoff: &Handoff) -> color_eyre::Result<()> {
    let error = Command::new(&handoff.program).args(arguments(handoff)).exec();
    Err(eyre!("failed to run {}: {error}", handoff.program.display()))
}
