//! Building a schema's programs from its project
//!
//! A schema is a compile-time construct, so every program that serves or talks to it is built
//! against it: the node (`shoal::server::node::main::<Db>()`), the admin program
//! (`shoaladm::cli::main_blocking::<DbClient>()`) and the terminal UI
//! (`shoalctl::cli::main_blocking::<DbClient>()`). Nobody writes those three programs any more:
//! this module generates one wrapper crate under the project's own `target/`, with the three as
//! its binaries and the project's dependencies as its own, and has cargo build whichever is
//! needed. A node is built once per cpu class the hosts turn out to be, under that cpu's
//! `-C target-cpu`, in a target directory of its own so the classes never rebuild each other.
//! What cargo produces is installed under `~/.local/shoal/bin` by a name that says what it is
//! ([F63](../../docs/src/features/shoaladm.md)).
//!
//! A schema in `src/main.rs` is brought into a wrapper as a module by path: the file is compiled
//! where it is, its own `mod x;` declarations resolve under `src/` as they always did, and its
//! `fn main` becomes an unused function. A schema in the library is reached by depending on
//! the package.

use color_eyre::eyre::{WrapErr, bail, eyre};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::config::{self, Config};
use crate::project::{Dependency, Project, Schema, SchemaSource};

/// Which of a schema's programs is meant
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// The node, `shoal::server::node::main`
    Node,
    /// The admin program, `shoaladm::cli::main_blocking`
    Adm,
    /// The terminal UI, `shoalctl::cli::main_blocking`
    Ctl,
}

impl Role {
    /// The word a program's name carries for this role
    #[must_use]
    pub fn word(self) -> &'static str {
        match self {
            Role::Node => "node",
            Role::Adm => "adm",
            Role::Ctl => "ctl",
        }
    }

    /// The wrapper's source file for this role
    #[must_use]
    pub fn source_file(self) -> &'static str {
        match self {
            Role::Node => "node.rs",
            Role::Adm => "adm.rs",
            Role::Ctl => "ctl.rs",
        }
    }
}

/// What cpu a program is built for
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    /// This machine, under whatever flags the project builds with
    Native,
    /// A named cpu, under `-C target-cpu=<name>`
    Cpu(String),
}

impl Target {
    /// The word a program's name and its target directory carry for this target
    #[must_use]
    pub fn word(&self) -> &str {
        match self {
            Target::Native => "native",
            Target::Cpu(name) => name,
        }
    }
}

/// Where a project's wrapper crate is generated
///
/// # Arguments
///
/// * `project` - The project
#[must_use]
pub fn wrapper_dir(project: &Project) -> PathBuf {
    project.target_directory.join("shoal-build").join(&project.name)
}

/// The name a wrapper binary has for a role
///
/// # Arguments
///
/// * `project` - The project
/// * `role` - The role
#[must_use]
pub fn bin_name(project: &Project, role: Role) -> String {
    format!("{}-{}", project.name, role.word())
}

/// The name a built program is installed under: the package, the database and the role, and
/// for a node the cpu it was built for
///
/// # Arguments
///
/// * `project` - The project
/// * `schema` - The database
/// * `role` - The role
/// * `target` - The cpu
#[must_use]
pub fn install_name(project: &Project, schema: &Schema, role: Role, target: &Target) -> String {
    installed_name(&project.name, &schema.name, role, target)
}

/// The same name from the package's and the database's names alone, which is what a cluster's
/// state remembers of the schema it was deployed from
///
/// # Arguments
///
/// * `package` - The package
/// * `db` - The database
/// * `role` - The role
/// * `target` - The cpu
#[must_use]
pub fn installed_name(package: &str, db: &str, role: Role, target: &Target) -> String {
    match role {
        Role::Node => format!("{package}-{db}-node-{}", target.word()),
        other => format!("{package}-{db}-{}", other.word()),
    }
}

/// The name a node program has on every host, whatever cpu each was built for
///
/// # Arguments
///
/// * `project` - The project
/// * `schema` - The database
#[must_use]
pub fn server_name(project: &Project, schema: &Schema) -> String {
    format!("{}-{}-node", project.name, schema.name)
}

/// Quote a string as a TOML basic string
///
/// # Arguments
///
/// * `raw` - The string
#[must_use]
pub fn quoted(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() + 2);
    out.push('"');
    for c in raw.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{:04X}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Quote a path as a TOML basic string
///
/// # Arguments
///
/// * `path` - The path
fn quoted_path(path: &Path) -> String {
    quoted(&path.display().to_string())
}

/// Where a dependency comes from, as a manifest writes it
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Origin {
    /// A directory
    Path(PathBuf),
    /// A git repository at a branch, tag or revision, or its default branch
    Git {
        /// The repository
        url: String,
        /// `branch`, `tag` or `rev`, and its value
        reference: Option<(String, String)>,
    },
    /// A registry, crates.io unless named
    Registry {
        /// The registry's name, when not crates.io
        registry: Option<String>,
    },
}

impl Origin {
    /// Read where a dependency comes from off what cargo printed
    ///
    /// # Arguments
    ///
    /// * `dependency` - The dependency
    #[must_use]
    pub fn of(dependency: &Dependency) -> Origin {
        // a path dependency prints its path and no source
        if let Some(path) = &dependency.path {
            return Origin::Path(path.clone());
        }
        match dependency.source.as_deref() {
            // `git+https://host/repo?branch=main#sha`: the url, then the reference, then the lock
            Some(source) if source.starts_with("git+") => {
                let rest = &source["git+".len()..];
                let rest = rest.split('#').next().unwrap_or(rest);
                let (url, query) = rest.split_once('?').unwrap_or((rest, ""));
                let reference = query
                    .split('&')
                    .filter_map(|pair| pair.split_once('='))
                    .find(|(key, _)| matches!(*key, "branch" | "tag" | "rev"))
                    .map(|(key, value)| (key.to_string(), value.to_string()));
                Origin::Git {
                    url: url.to_string(),
                    reference,
                }
            }
            _ => Origin::Registry {
                registry: dependency.registry.clone(),
            },
        }
    }

    /// The manifest fields that name this origin, without a version
    fn fields(&self) -> Vec<String> {
        match self {
            Origin::Path(path) => vec![format!("path = {}", quoted_path(path))],
            Origin::Git { url, reference } => {
                let mut fields = vec![format!("git = {}", quoted(url))];
                if let Some((key, value)) = reference {
                    fields.push(format!("{key} = {}", quoted(value)));
                }
                fields
            }
            Origin::Registry { registry } => registry
                .iter()
                .map(|registry| format!("registry = {}", quoted(registry)))
                .collect(),
        }
    }

    /// The same origin for a sibling of this package: the crate beside it in the same
    /// checkout or repository, or the same version from the same registry
    ///
    /// # Arguments
    ///
    /// * `sibling` - The sibling's package name
    #[must_use]
    pub fn sibling(&self, sibling: &str) -> Origin {
        match self {
            Origin::Path(path) => Origin::Path(
                path.parent()
                    .map_or_else(|| PathBuf::from(sibling), |parent| parent.join(sibling)),
            ),
            other => other.clone(),
        }
    }
}

/// One dependency line of the wrapper's manifest
///
/// # Arguments
///
/// * `dependency` - The dependency, as cargo printed it
#[must_use]
pub fn dependency_line(dependency: &Dependency) -> String {
    let origin = Origin::of(dependency);
    // the name it is used under, and the package it really is
    let key = dependency.rename.as_deref().unwrap_or(&dependency.name);
    let mut fields = Vec::new();
    if dependency.rename.is_some() {
        fields.push(format!("package = {}", quoted(&dependency.name)));
    }
    // a version for anything that is not a path; cargo prints `*` for none
    if !matches!(origin, Origin::Path(_)) || dependency.req != "*" {
        if dependency.req != "*" {
            fields.push(format!("version = {}", quoted(&dependency.req)));
        }
    }
    fields.extend(origin.fields());
    if !dependency.features.is_empty() {
        let features: Vec<String> = dependency.features.iter().map(|feature| quoted(feature)).collect();
        fields.push(format!("features = [{}]", features.join(", ")));
    }
    if !dependency.uses_default_features {
        fields.push("default-features = false".to_string());
    }
    if dependency.optional {
        fields.push("optional = true".to_string());
    }
    format!("{} = {{ {} }}", quoted_key(key), fields.join(", "))
}

/// A key as a manifest writes it: bare when it can be, quoted otherwise
///
/// # Arguments
///
/// * `key` - The key
fn quoted_key(key: &str) -> String {
    if !key.is_empty()
        && key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        key.to_string()
    } else {
        quoted(key)
    }
}

/// The `[patch]` table of a workspace manifest, with its relative paths made absolute
///
/// # Arguments
///
/// * `workspace_root` - The workspace root, which the paths are relative to
///
/// # Errors
///
/// When the manifest cannot be read.
pub fn patches(workspace_root: &Path) -> color_eyre::Result<Option<String>> {
    let manifest = workspace_root.join("Cargo.toml");
    // a root without a manifest patches nothing
    if !manifest.is_file() {
        return Ok(None);
    }
    let raw = std::fs::read_to_string(&manifest)
        .wrap_err_with(|| format!("failed to read {}", manifest.display()))?;
    let value: toml::Value = raw
        .parse()
        .wrap_err_with(|| format!("{} is not a manifest", manifest.display()))?;
    let Some(patch) = value.get("patch").and_then(toml::Value::as_table) else {
        return Ok(None);
    };
    // every entry of every registry, its path rewritten against the root
    let mut rewritten = toml::value::Table::new();
    for (registry, entries) in patch {
        let Some(entries) = entries.as_table() else {
            rewritten.insert(registry.clone(), entries.clone());
            continue;
        };
        let mut fixed = toml::value::Table::new();
        for (name, entry) in entries {
            let mut entry = entry.clone();
            if let Some(fields) = entry.as_table_mut() {
                if let Some(toml::Value::String(path)) = fields.get_mut("path") {
                    let joined = workspace_root.join(path.as_str());
                    *path = joined.display().to_string();
                }
            }
            fixed.insert(name.clone(), entry);
        }
        rewritten.insert(registry.clone(), toml::Value::Table(fixed));
    }
    let mut wrapped = toml::value::Table::new();
    wrapped.insert("patch".to_string(), toml::Value::Table(rewritten));
    Ok(Some(toml::to_string(&toml::Value::Table(wrapped))?))
}

/// The wrapper crate's manifest
///
/// # Arguments
///
/// * `project` - The project
/// * `schema` - The database, whose programs are the binaries
///
/// # Errors
///
/// When the project does not depend on `shoal`, or its workspace manifest cannot be read.
pub fn manifest(project: &Project, schema: &Schema) -> color_eyre::Result<String> {
    let shoal = project.shoal_dependency()?;
    let shoal_origin = Origin::of(shoal);
    let mut text = String::new();
    text.push_str(&format!(
        "# Generated by shoaladm for the {} database of {}; do not edit. Every change is made in\n\
         # the project, and this file is written again on the next build.\n\n",
        schema.name, project.name
    ));
    text.push_str(&format!(
        "[package]\nname = {}\nversion = \"0.0.0\"\nedition = {}\npublish = false\n\n",
        quoted(&format!("{}-shoal", project.name)),
        quoted(&project.edition)
    ));
    // its own workspace, whatever it was generated under
    text.push_str("[workspace]\n\n");
    for role in [Role::Node, Role::Adm, Role::Ctl] {
        text.push_str(&format!(
            "[[bin]]\nname = {}\npath = {}\n\n",
            quoted(&bin_name(project, role)),
            quoted(role.source_file())
        ));
    }
    // the project's dependencies, grouped by the target each is limited to
    let mut groups: BTreeMap<Option<String>, Vec<String>> = BTreeMap::new();
    for dependency in project
        .dependencies
        .iter()
        .filter(|dependency| dependency.kind.is_none())
    {
        groups
            .entry(dependency.target.clone())
            .or_default()
            .push(dependency_line(dependency));
    }
    // and the wrapper's own: the project's library, the allocator, and the two tools beside shoal
    let own = groups.entry(None).or_default();
    if project.lib.is_some() {
        own.push(format!(
            "{} = {{ path = {} }}",
            quoted_key(&project.name),
            quoted_path(&project.dir)
        ));
    }
    own.push("mimalloc = \"0.1\"".to_string());
    for tool in ["shoaladm", "shoalctl"] {
        let mut fields = Vec::new();
        if shoal.req != "*" {
            fields.push(format!("version = {}", quoted(&shoal.req)));
        }
        fields.extend(shoal_origin.sibling(tool).fields());
        own.push(format!("{tool} = {{ {} }}", fields.join(", ")));
    }
    for (target, lines) in &groups {
        match target {
            None => text.push_str("[dependencies]\n"),
            Some(target) => text.push_str(&format!("[target.{}.dependencies]\n", quoted(target))),
        }
        for line in lines {
            text.push_str(line);
            text.push('\n');
        }
        text.push('\n');
    }
    // the project's features, so `cfg(feature = …)` in its schema file keeps its meaning
    if !project.features.is_empty() {
        text.push_str("[features]\n");
        for (name, enables) in &project.features {
            let enables: Vec<String> = enables.iter().map(|enabled| quoted(enabled)).collect();
            text.push_str(&format!("{} = [{}]\n", quoted_key(name), enables.join(", ")));
        }
        text.push('\n');
    }
    // whatever the workspace patches, the wrapper patches too
    if let Some(patch) = patches(&project.workspace_root)? {
        text.push_str(&patch);
        text.push('\n');
    }
    Ok(text)
}

/// The lines that bring the schema into a wrapper source, and the path its types are named by
///
/// # Arguments
///
/// * `project` - The project
/// * `schema` - The database
///
/// # Errors
///
/// When a library schema's package has no library, which cannot happen for a scanned one.
pub fn include(project: &Project, schema: &Schema) -> color_eyre::Result<(String, String)> {
    match schema.source {
        // the file itself, as a module; its own main and imports are not this program's
        SchemaSource::Main => Ok((
            format!(
                "#[path = {}]\n#[allow(dead_code, unused_imports, unused_variables, unused_mut, unreachable_pub, clippy::all)]\nmod schema;\n",
                quoted_path(&schema.file)
            ),
            "schema".to_string(),
        )),
        // the library, by its crate name
        SchemaSource::Lib => {
            let lib = project
                .lib_crate()
                .ok_or_else(|| eyre!("{} has no library to reach {} through", project.name, schema.name))?;
            Ok((String::new(), lib))
        }
    }
}

/// A wrapper's source for a role
///
/// # Arguments
///
/// * `project` - The project
/// * `schema` - The database
/// * `role` - The role
///
/// # Errors
///
/// When the schema cannot be reached, as [`include`] says.
pub fn source(project: &Project, schema: &Schema, role: Role) -> color_eyre::Result<String> {
    let (include, root) = include(project, schema)?;
    let db = format!("{root}::{}", schema.path());
    let client = format!("{root}::{}", schema.client_path());
    let header = format!(
        "//! The {} program for the {} database of {}, generated by shoaladm; do not edit\n\n",
        role.word(),
        schema.name,
        project.name
    );
    Ok(match role {
        Role::Node => format!(
            "{header}{include}\n/// The allocator every Shoal server program runs with\n#[global_allocator]\nstatic GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;\n\n/// Serve or claim a node of the schema\nfn main() -> Result<(), shoal::server::ServerError> {{\n    shoal::server::node::main::<{db}>()\n}}\n"
        ),
        Role::Adm => format!(
            "{header}{include}\n/// Deploy and operate a cluster of the schema\nfn main() -> shoaladm::Result<()> {{\n    shoaladm::cli::main_blocking::<{client}>()\n}}\n"
        ),
        Role::Ctl => format!(
            "{header}{include}\n/// Query the schema in a terminal\nfn main() -> shoalctl::Result<()> {{\n    shoalctl::cli::main_blocking::<{client}>()\n}}\n"
        ),
    })
}

/// Write a file only when its text changed, so cargo sees a change only when there is one
///
/// # Arguments
///
/// * `path` - The file
/// * `text` - What it should hold
fn write_if_changed(path: &Path, text: &str) -> color_eyre::Result<()> {
    if std::fs::read_to_string(path).is_ok_and(|current| current == text) {
        return Ok(());
    }
    std::fs::write(path, text).wrap_err_with(|| format!("failed to write {}", path.display()))
}

/// Generate the wrapper crate for a schema, or bring it up to date
///
/// # Arguments
///
/// * `project` - The project
/// * `schema` - The database
///
/// # Errors
///
/// When the manifest cannot be built or a file cannot be written.
pub fn write_wrapper(project: &Project, schema: &Schema) -> color_eyre::Result<PathBuf> {
    let dir = wrapper_dir(project);
    std::fs::create_dir_all(&dir).wrap_err_with(|| format!("failed to create {}", dir.display()))?;
    // the manifest and the three sources
    write_if_changed(&dir.join("Cargo.toml"), &manifest(project, schema)?)?;
    for role in [Role::Node, Role::Adm, Role::Ctl] {
        write_if_changed(&dir.join(role.source_file()), &source(project, schema, role)?)?;
    }
    // the project's lockfile, so every version is the project's own; copied again only when
    // the project's changed, since cargo adds the wrapper's own dependencies to the copy
    let lock = project.workspace_root.join("Cargo.lock");
    if lock.is_file() {
        let text = std::fs::read_to_string(&lock)?;
        let shadow = dir.join("Cargo.lock.project");
        if std::fs::read_to_string(&shadow).ok().as_deref() != Some(text.as_str()) {
            std::fs::write(dir.join("Cargo.lock"), &text)?;
            std::fs::write(&shadow, &text)?;
        }
    }
    Ok(dir)
}

/// The `RUSTFLAGS` a build for a cpu runs under: what was set, less any cpu, plus this one
///
/// # Arguments
///
/// * `current` - The `RUSTFLAGS` in the environment, if any
/// * `cpu` - The cpu to build for
#[must_use]
pub fn rustflags(current: Option<&str>, cpu: &str) -> String {
    let mut kept = Vec::new();
    let mut words = current.unwrap_or_default().split_whitespace().peekable();
    while let Some(word) = words.next() {
        // `-C target-cpu=x` as two words
        if word == "-C" {
            match words.peek() {
                Some(next) if next.starts_with("target-cpu=") => {
                    words.next();
                    continue;
                }
                _ => {
                    kept.push(word.to_string());
                    continue;
                }
            }
        }
        // or as one
        if word.starts_with("-Ctarget-cpu=") {
            continue;
        }
        kept.push(word.to_string());
    }
    kept.push("-C".to_string());
    kept.push(format!("target-cpu={cpu}"));
    kept.join(" ")
}

/// The cargo to run, which is the one running us when there is one
fn cargo() -> String {
    std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string())
}

/// Build one of a schema's programs and install it
///
/// # Arguments
///
/// * `project` - The project
/// * `schema` - The database
/// * `role` - Which program
/// * `target` - Which cpu
/// * `config` - The operator's config, for where it is installed
/// * `note` - What to say beside the step, such as `1 of 2`
///
/// # Errors
///
/// When the wrapper cannot be written, cargo fails, or the program cannot be installed.
pub fn program(
    project: &Project,
    schema: &Schema,
    role: Role,
    target: &Target,
    config: &Config,
    note: Option<&str>,
) -> color_eyre::Result<PathBuf> {
    let wrapper = write_wrapper(project, schema)?;
    let bin = bin_name(project, role);
    let target_dir = wrapper.join(target.word());
    let note = note.map_or(String::new(), |note| format!(" ({note})"));
    eprintln!(
        "[build] {bin} for {}{note}",
        match target {
            Target::Native => "this machine".to_string(),
            Target::Cpu(cpu) => cpu.clone(),
        }
    );
    // cargo, run in the project so its toolchain file and cargo config apply, writing into the
    // target's own directory so two cpus never rebuild each other
    let mut command = Command::new(cargo());
    command
        .args(["build", "--release", "--manifest-path"])
        .arg(wrapper.join("Cargo.toml"))
        .args(["--bin", &bin, "--target-dir"])
        .arg(&target_dir)
        .current_dir(&project.dir)
        .stdin(std::process::Stdio::null());
    // a cpu build replaces whatever cpu the environment names, and nothing else in it
    if let Target::Cpu(cpu) = target {
        let flags = rustflags(std::env::var("RUSTFLAGS").ok().as_deref(), cpu);
        command.env("RUSTFLAGS", flags);
        command.env_remove("CARGO_ENCODED_RUSTFLAGS");
    }
    let status = command.status().wrap_err("failed to run cargo")?;
    if !status.success() {
        bail!("cargo build of {bin} failed with {status}; the errors above name what to fix in {}", project.dir.display());
    }
    // what it wrote, installed under its name
    let built = target_dir.join("release").join(&bin);
    if !built.is_file() {
        bail!("cargo built {bin} but {} is not there", built.display());
    }
    let installed = config::install(&built, &config.bin_dir()?, &install_name(project, schema, role, target))?;
    eprintln!("[build] installed {}", installed.display());
    Ok(installed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::project::Target as ProjectTarget;

    /// A dependency as cargo would print it
    fn dependency(name: &str, source: Option<&str>, req: &str) -> Dependency {
        Dependency {
            name: name.to_string(),
            source: source.map(str::to_string),
            req: req.to_string(),
            kind: None,
            rename: None,
            optional: false,
            uses_default_features: true,
            features: Vec::new(),
            target: None,
            registry: None,
            path: None,
        }
    }

    /// A project with a main.rs schema and three kinds of dependency
    fn project(dir: &Path) -> Project {
        let mut shoal = dependency("shoal", None, "*");
        shoal.path = Some(PathBuf::from("/src/shoal/shoal"));
        let mut serde = dependency("serde", Some("registry+https://github.com/rust-lang/crates.io-index"), "^1");
        serde.features = vec!["derive".to_string()];
        let mut fancy = dependency("fancy-hash", Some("git+https://example.com/fancy.git?branch=main#abc123"), "*");
        fancy.rename = Some("hash".to_string());
        fancy.uses_default_features = false;
        fancy.optional = true;
        let mut dev = dependency("tempfile", Some("registry+https://github.com/rust-lang/crates.io-index"), "^3");
        dev.kind = Some("dev".to_string());
        let mut unix = dependency("libc", Some("registry+https://github.com/rust-lang/crates.io-index"), "^0.2");
        unix.target = Some("cfg(unix)".to_string());
        Project {
            dir: dir.to_path_buf(),
            name: "demo".to_string(),
            edition: "2021".to_string(),
            target_directory: dir.join("target"),
            workspace_root: dir.to_path_buf(),
            lib: None,
            dependencies: vec![shoal, serde, fancy, dev, unix],
            features: BTreeMap::from([
                ("default".to_string(), vec!["fast".to_string()]),
                ("fast".to_string(), vec!["dep:hash".to_string()]),
            ]),
        }
    }

    /// The schema of that project
    fn schema(dir: &Path) -> Schema {
        Schema {
            name: "Demo".to_string(),
            module_path: vec!["tables".to_string()],
            source: SchemaSource::Main,
            file: dir.join("src").join("main.rs"),
        }
    }

    /// Every kind of dependency is written the way cargo reads it, and a dev one is left out
    #[test]
    fn the_manifest_carries_the_projects_dependencies() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("Cargo.toml"), "[package]\nname = \"demo\"\n\n[patch.crates-io]\nglommio = { path = \"../glommio/glommio\" }\n").unwrap();
        let text = manifest(&project(dir.path()), &schema(dir.path())).expect("a manifest");
        // the package, its own workspace, and the three programs
        assert!(text.contains("name = \"demo-shoal\""), "{text}");
        assert!(text.contains("edition = \"2021\""), "{text}");
        assert!(text.contains("[workspace]\n"), "{text}");
        assert!(text.contains("[[bin]]\nname = \"demo-node\"\npath = \"node.rs\""), "{text}");
        assert!(text.contains("name = \"demo-adm\""), "{text}");
        assert!(text.contains("name = \"demo-ctl\""), "{text}");
        // a path, a registry with features, a renamed optional git dependency, and a targeted one
        assert!(text.contains("shoal = { path = \"/src/shoal/shoal\" }"), "{text}");
        assert!(text.contains("serde = { version = \"^1\", features = [\"derive\"] }"), "{text}");
        assert!(
            text.contains("hash = { package = \"fancy-hash\", git = \"https://example.com/fancy.git\", branch = \"main\", default-features = false, optional = true }"),
            "{text}"
        );
        assert!(text.contains("[target.\"cfg(unix)\".dependencies]\nlibc = { version = \"^0.2\" }"), "{text}");
        assert!(!text.contains("tempfile"), "{text}");
        // the wrapper's own: the allocator and the two tools beside shoal
        assert!(text.contains("mimalloc = \"0.1\""), "{text}");
        assert!(text.contains("shoaladm = { path = \"/src/shoal/shoaladm\" }"), "{text}");
        assert!(text.contains("shoalctl = { path = \"/src/shoal/shoalctl\" }"), "{text}");
        // the features, and the workspace's patch with its path made absolute
        assert!(text.contains("[features]\ndefault = [\"fast\"]\nfast = [\"dep:hash\"]"), "{text}");
        assert!(text.contains("[patch.crates-io.glommio]"), "{text}");
        assert!(text.contains(&format!("path = \"{}/../glommio/glommio\"", dir.path().display())), "{text}");
    }

    /// The tools follow shoal to a git repository and to a registry version
    #[test]
    fn the_tools_come_from_beside_shoal() {
        let git = dependency("shoal", Some("git+https://example.com/shoal.git?rev=abc#abc"), "*");
        assert_eq!(
            Origin::of(&git).sibling("shoaladm").fields(),
            vec!["git = \"https://example.com/shoal.git\"", "rev = \"abc\""]
        );
        let registry = dependency("shoal", Some("registry+https://github.com/rust-lang/crates.io-index"), "^0.1.0");
        assert_eq!(Origin::of(&registry).sibling("shoalctl").fields(), Vec::<String>::new());
        let mut other = registry.clone();
        other.registry = Some("internal".to_string());
        assert_eq!(Origin::of(&other).fields(), vec!["registry = \"internal\""]);
        // a project without shoal has nothing to build
        let dir = tempfile::tempdir().unwrap();
        let mut project = project(dir.path());
        project.dependencies.retain(|dependency| dependency.name != "shoal");
        let error = manifest(&project, &schema(dir.path())).unwrap_err().to_string();
        assert!(error.contains("does not depend on shoal"), "{error}");
    }

    /// The three sources name the database through the module for a main.rs schema and
    /// through the crate for a library one
    #[test]
    fn the_sources_name_the_database() {
        let dir = tempfile::tempdir().unwrap();
        let project = project(dir.path());
        let schema = schema(dir.path());
        let node = source(&project, &schema, Role::Node).unwrap();
        assert!(node.contains(&format!("#[path = \"{}/src/main.rs\"]", dir.path().display())), "{node}");
        assert!(node.contains("mod schema;"), "{node}");
        assert!(node.contains("shoal::server::node::main::<schema::tables::Demo>()"), "{node}");
        assert!(node.contains("mimalloc::MiMalloc"), "{node}");
        let adm = source(&project, &schema, Role::Adm).unwrap();
        assert!(adm.contains("shoaladm::cli::main_blocking::<schema::tables::DemoClient>()"), "{adm}");
        let ctl = source(&project, &schema, Role::Ctl).unwrap();
        assert!(ctl.contains("shoalctl::cli::main_blocking::<schema::tables::DemoClient>()"), "{ctl}");
        // a library schema is reached through the crate
        let mut lib_project = project.clone();
        lib_project.name = "tmdb-dataset".to_string();
        lib_project.lib = Some(ProjectTarget {
            name: "tmdb_dataset".to_string(),
            kind: vec!["lib".to_string()],
            src_path: dir.path().join("src").join("lib.rs"),
        });
        let lib_schema = Schema {
            name: "Tmdb".to_string(),
            module_path: Vec::new(),
            source: SchemaSource::Lib,
            file: dir.path().join("src").join("lib.rs"),
        };
        let node = source(&lib_project, &lib_schema, Role::Node).unwrap();
        assert!(!node.contains("mod schema"), "{node}");
        assert!(node.contains("shoal::server::node::main::<tmdb_dataset::Tmdb>()"), "{node}");
        let text = manifest(&lib_project, &lib_schema).unwrap();
        assert!(text.contains(&format!("tmdb-dataset = {{ path = \"{}\" }}", dir.path().display())), "{text}");
    }

    /// A cpu replaces whatever cpu the environment named and keeps everything else
    #[test]
    fn rustflags_replace_the_cpu() {
        assert_eq!(rustflags(None, "znver1"), "-C target-cpu=znver1");
        assert_eq!(rustflags(Some("-C target-cpu=native -C debuginfo=1"), "znver1"), "-C debuginfo=1 -C target-cpu=znver1");
        assert_eq!(rustflags(Some("-Ctarget-cpu=native --cfg foo"), "x86-64-v3"), "--cfg foo -C target-cpu=x86-64-v3");
        assert_eq!(rustflags(Some("-C opt-level=3"), "znver4"), "-C opt-level=3 -C target-cpu=znver4");
    }

    /// The installed names say the package, the database, the role and the cpu
    #[test]
    fn names_are_descriptive() {
        let dir = tempfile::tempdir().unwrap();
        let project = project(dir.path());
        let schema = schema(dir.path());
        assert_eq!(install_name(&project, &schema, Role::Node, &Target::Cpu("znver1".into())), "demo-Demo-node-znver1");
        assert_eq!(install_name(&project, &schema, Role::Node, &Target::Native), "demo-Demo-node-native");
        assert_eq!(install_name(&project, &schema, Role::Adm, &Target::Native), "demo-Demo-adm");
        assert_eq!(install_name(&project, &schema, Role::Ctl, &Target::Native), "demo-Demo-ctl");
        assert_eq!(server_name(&project, &schema), "demo-Demo-node");
        assert_eq!(bin_name(&project, Role::Node), "demo-node");
        assert_eq!(quoted("a \"b\"\\c"), "\"a \\\"b\\\"\\\\c\"");
        assert_eq!(quoted_key("cfg(unix)"), "\"cfg(unix)\"");
    }

    /// A main.rs brought in by path keeps its own module tree: `mod extra;` in it resolves to
    /// `src/extra.rs`, and a pub struct in it is nameable from the wrapper's root
    ///
    /// This is what the node, admin and terminal UI wrappers rely on, checked by cargo itself
    /// on a crate with no dependencies.
    #[test]
    fn a_main_included_by_path_keeps_its_modules() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("project").join("src");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(
            src.join("main.rs"),
            "#![allow(dead_code)]\nmod extra;\nmod nested;\n/// The database\npub struct Db(pub extra::Extra, pub nested::leaf::Leaf);\nfn main() {}\n",
        )
        .unwrap();
        std::fs::write(src.join("extra.rs"), "pub struct Extra;\n").unwrap();
        std::fs::create_dir_all(src.join("nested")).unwrap();
        std::fs::write(src.join("nested").join("mod.rs"), "pub mod leaf;\n").unwrap();
        std::fs::write(src.join("nested").join("leaf.rs"), "pub struct Leaf;\n").unwrap();
        // the wrapper, with the same include the generated sources use
        let wrapper = dir.path().join("wrapper");
        std::fs::create_dir_all(&wrapper).unwrap();
        std::fs::write(
            wrapper.join("Cargo.toml"),
            "[package]\nname = \"wrapper\"\nversion = \"0.0.0\"\nedition = \"2021\"\n\n[workspace]\n\n[[bin]]\nname = \"wrapper-node\"\npath = \"node.rs\"\n",
        )
        .unwrap();
        let schema = Schema {
            name: "Db".to_string(),
            module_path: Vec::new(),
            source: SchemaSource::Main,
            file: src.join("main.rs"),
        };
        let (include, root) = include(&project(dir.path()), &schema).unwrap();
        std::fs::write(
            wrapper.join("node.rs"),
            format!("{include}\nfn main() {{ let _db: Option<{root}::Db> = None; }}\n"),
        )
        .unwrap();
        let output = Command::new(cargo())
            .args(["check", "--quiet", "--manifest-path"])
            .arg(wrapper.join("Cargo.toml"))
            .args(["--target-dir"])
            .arg(wrapper.join("target"))
            .env_remove("RUSTFLAGS")
            .output()
            .expect("cargo");
        assert!(
            output.status.success(),
            "cargo check failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
