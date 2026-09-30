//! Finding the schema a Rust project defines
//!
//! A deployment starts from a project directory: the one `shoaladm` was run in, or `--project`.
//! Its `Cargo.toml` is read through `cargo metadata`, so what cargo thinks the package is called,
//! depends on and puts its artifacts under is what this module thinks too, and its `src/main.rs`
//! (then its `src/lib.rs`) is parsed for a struct carrying `#[shoal::db]`. That struct is the
//! database, and everything [`build`](crate::build) generates names it
//! ([F63](../../docs/src/features/shoaladm.md)).
//!
//! The scan follows `mod x;` declarations into `src/x.rs` and `src/x/mod.rs`, so a schema kept in
//! its own module is found, and refuses to guess between two: a project with two databases says
//! which one with `--db`.

use color_eyre::eyre::{WrapErr, bail, eyre};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

/// One dependency of the project, as cargo resolved its manifest
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Dependency {
    /// The package's name
    pub name: String,
    /// Where it comes from: none for a path, `git+…` or `registry+…` otherwise
    #[serde(default)]
    pub source: Option<String>,
    /// The version requirement, as cargo prints it (`^1.0`)
    pub req: String,
    /// `dev`, `build`, or none for a normal dependency
    #[serde(default)]
    pub kind: Option<String>,
    /// The name it is used under in code, when it differs from the package's
    #[serde(default)]
    pub rename: Option<String>,
    /// Whether a feature has to turn it on
    #[serde(default)]
    pub optional: bool,
    /// Whether its default features are wanted
    #[serde(default = "default_true")]
    pub uses_default_features: bool,
    /// The features asked of it
    #[serde(default)]
    pub features: Vec<String>,
    /// The `cfg` or triple it is limited to, if any
    #[serde(default)]
    pub target: Option<String>,
    /// The registry it comes from, when not crates.io
    #[serde(default)]
    pub registry: Option<String>,
    /// Where it is, for a path dependency; absolute, as cargo prints it
    #[serde(default)]
    pub path: Option<PathBuf>,
}

/// serde's default for a flag that is on when absent
fn default_true() -> bool {
    true
}

/// One target of the project, as cargo lists it
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Target {
    /// Its name; for a library, the crate name with dashes kept
    pub name: String,
    /// What it is: `lib`, `bin`, `test`, and the rest
    pub kind: Vec<String>,
    /// Its root source file, absolute
    pub src_path: PathBuf,
}

/// One package of the metadata
#[derive(Deserialize, Debug, Clone)]
struct Package {
    /// Its name
    name: String,
    /// Its edition
    edition: String,
    /// Its manifest, absolute
    manifest_path: PathBuf,
    /// Its targets
    targets: Vec<Target>,
    /// Its dependencies, every kind
    dependencies: Vec<Dependency>,
    /// Its features, each naming what it turns on
    #[serde(default)]
    features: BTreeMap<String, Vec<String>>,
}

/// What `cargo metadata --no-deps` prints, the part read here
#[derive(Deserialize, Debug)]
struct Metadata {
    /// Every package of the workspace
    packages: Vec<Package>,
    /// Where cargo puts its artifacts
    target_directory: PathBuf,
    /// The workspace root, where the lockfile is
    workspace_root: PathBuf,
}

/// A Rust project a schema is deployed from
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Project {
    /// The directory, absolute
    pub dir: PathBuf,
    /// The package's name, as `Cargo.toml` spells it
    pub name: String,
    /// The package's edition
    pub edition: String,
    /// Where cargo puts this project's artifacts
    pub target_directory: PathBuf,
    /// The workspace root, which holds the lockfile
    pub workspace_root: PathBuf,
    /// The library target, if the package has one
    pub lib: Option<Target>,
    /// Every dependency, of every kind
    pub dependencies: Vec<Dependency>,
    /// The features, each naming what it turns on
    pub features: BTreeMap<String, Vec<String>>,
}

/// Which root file a schema was found under, which decides how the wrapper reaches it
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaSource {
    /// `src/main.rs`, which a wrapper includes as a module by path
    Main,
    /// The library, which a wrapper depends on by path
    Lib,
}

/// The database struct a project defines
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    /// The struct's name
    pub name: String,
    /// The modules between the root file and the struct, outermost first
    pub module_path: Vec<String>,
    /// Which root file it was found under
    pub source: SchemaSource,
    /// The file it is written in
    pub file: PathBuf,
}

impl Schema {
    /// The struct's path from the root file, as `a::b::Db`
    #[must_use]
    pub fn path(&self) -> String {
        self.module_path
            .iter()
            .map(String::as_str)
            .chain(std::iter::once(self.name.as_str()))
            .collect::<Vec<_>>()
            .join("::")
    }

    /// The generated client type's path from the root file, as `a::b::DbClient`
    #[must_use]
    pub fn client_path(&self) -> String {
        format!("{}Client", self.path())
    }
}

/// One struct a scan found with the attribute, whatever it turned out to be
#[derive(Debug, Clone, PartialEq, Eq)]
struct Candidate {
    /// The schema, if it were chosen
    schema: Schema,
    /// Whether it was written `#[shoal::db(client)]`, the half that cannot be served
    client_only: bool,
    /// Whether the struct is nameable from another module
    public: bool,
}

/// The cargo to run, which is the one running us when there is one
fn cargo() -> String {
    std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string())
}

impl Project {
    /// Read a project from its directory
    ///
    /// # Arguments
    ///
    /// * `dir` - The directory, as given
    ///
    /// # Errors
    ///
    /// When it holds no `Cargo.toml`, cargo cannot read it, or it is a workspace root with no
    /// package of its own.
    pub fn locate(dir: &Path) -> color_eyre::Result<Self> {
        // the directory has to be a project
        let dir = dir
            .canonicalize()
            .wrap_err_with(|| format!("{} is not a directory", dir.display()))?;
        let manifest = dir.join("Cargo.toml");
        if !manifest.is_file() {
            bail!(
                "{} is not a Rust project: it has no Cargo.toml; run this in the project that \
                 defines the schema, or pass --project <dir>",
                dir.display()
            );
        }
        // what cargo makes of it
        let output = Command::new(cargo())
            .args(["metadata", "--format-version", "1", "--no-deps", "--manifest-path"])
            .arg(&manifest)
            .current_dir(&dir)
            .output()
            .wrap_err("failed to run cargo metadata")?;
        if !output.status.success() {
            bail!(
                "cargo metadata failed for {}: {}",
                manifest.display(),
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        let metadata: Metadata = serde_json::from_slice(&output.stdout)
            .wrap_err("cargo metadata printed something that is not its metadata")?;
        Self::from_metadata(&dir, &manifest, metadata)
    }

    /// Pick this directory's package out of the metadata
    ///
    /// # Arguments
    ///
    /// * `dir` - The directory
    /// * `manifest` - Its manifest
    /// * `metadata` - What cargo printed
    fn from_metadata(dir: &Path, manifest: &Path, metadata: Metadata) -> color_eyre::Result<Self> {
        // the package whose manifest this is, by canonical path since cargo prints them resolved
        let wanted = manifest.canonicalize().unwrap_or_else(|_| manifest.to_path_buf());
        let package = metadata
            .packages
            .into_iter()
            .find(|package| {
                package
                    .manifest_path
                    .canonicalize()
                    .map_or(package.manifest_path == wanted, |path| path == wanted)
            })
            .ok_or_else(|| {
                eyre!(
                    "{} is a workspace with no package of its own; pass --project <member>",
                    dir.display()
                )
            })?;
        // the library target, if there is one
        let lib = package
            .targets
            .iter()
            .find(|target| target.kind.iter().any(|kind| kind == "lib" || kind == "rlib"))
            .cloned();
        Ok(Project {
            dir: dir.to_path_buf(),
            name: package.name,
            edition: package.edition,
            target_directory: metadata.target_directory,
            workspace_root: metadata.workspace_root,
            lib,
            dependencies: package.dependencies,
            features: package.features,
        })
    }

    /// The library's crate name, as code names it
    #[must_use]
    pub fn lib_crate(&self) -> Option<String> {
        self.lib.as_ref().map(|lib| lib.name.replace('-', "_"))
    }

    /// The dependency on `shoal`, which every schema has
    ///
    /// # Errors
    ///
    /// When the project does not depend on `shoal`.
    pub fn shoal_dependency(&self) -> color_eyre::Result<&Dependency> {
        self.dependencies
            .iter()
            .filter(|dependency| dependency.kind.is_none())
            .find(|dependency| dependency.name == "shoal")
            .ok_or_else(|| {
                eyre!(
                    "{} does not depend on shoal, so it defines no schema to deploy",
                    self.name
                )
            })
    }

    /// Find the database this project defines
    ///
    /// `src/main.rs` and the modules it declares first; the library's root and its modules if
    /// the binary defines none.
    ///
    /// # Arguments
    ///
    /// * `db` - The struct to deploy, by name or by `a::b::Name`, when the project defines more than one
    ///
    /// # Errors
    ///
    /// When no struct carries `#[shoal::db]`, only client halves do, two do and none was named,
    /// the named one is not there, or the one found is not `pub`.
    pub fn scan(&self, db: Option<&str>) -> color_eyre::Result<Schema> {
        // the binary's root, then the library's
        let mut roots = Vec::new();
        let main = self.dir.join("src").join("main.rs");
        if main.is_file() {
            roots.push((main, SchemaSource::Main));
        }
        if let Some(lib) = &self.lib {
            roots.push((lib.src_path.clone(), SchemaSource::Lib));
        }
        if roots.is_empty() {
            bail!(
                "{} has neither src/main.rs nor a library; there is no schema to look for",
                self.name
            );
        }
        // every candidate under the first root that has any
        let mut candidates = Vec::new();
        let mut looked = Vec::new();
        for (root, source) in &roots {
            looked.push(root.display().to_string());
            scan_file(root, source, &mut Vec::new(), &mut candidates)?;
            if candidates.iter().any(|candidate| !candidate.client_only) {
                break;
            }
        }
        choose(&self.name, candidates, db, &looked)
    }
}

/// Pick the schema out of what a scan found
///
/// # Arguments
///
/// * `package` - The project, for messages
/// * `candidates` - Every struct with the attribute
/// * `db` - The one asked for, if any
/// * `looked` - The files that were read, for the refusal
fn choose(
    package: &str,
    candidates: Vec<Candidate>,
    db: Option<&str>,
    looked: &[String],
) -> color_eyre::Result<Schema> {
    // the halves that can be served
    let (deployable, clients): (Vec<Candidate>, Vec<Candidate>) = candidates
        .into_iter()
        .partition(|candidate| !candidate.client_only);
    if deployable.is_empty() {
        if clients.is_empty() {
            bail!(
                "{package} defines no `#[shoal::db]` struct in {}; the schema to deploy is the \
                 struct that attribute is on",
                looked.join(" or ")
            );
        }
        let names: Vec<String> = clients.iter().map(|candidate| candidate.schema.path()).collect();
        bail!(
            "{package} defines only the client half of a schema ({}): `#[shoal::db(client)]` \
             cannot be served, so there is no node to deploy",
            names.join(", ")
        );
    }
    // the one named, or the only one
    let chosen = match db {
        Some(wanted) => deployable
            .iter()
            .find(|candidate| candidate.schema.path() == wanted || candidate.schema.name == wanted)
            .ok_or_else(|| {
                let names: Vec<String> = deployable.iter().map(|candidate| candidate.schema.path()).collect();
                eyre!(
                    "{package} defines no database named {wanted:?}; it defines {}",
                    names.join(", ")
                )
            })?,
        None => match deployable.as_slice() {
            [one] => one,
            many => {
                let names: Vec<String> = many.iter().map(|candidate| candidate.schema.path()).collect();
                bail!(
                    "{package} defines {} databases ({}); say which to deploy with --db <name>",
                    many.len(),
                    names.join(", ")
                );
            }
        },
    };
    // a private struct cannot be named by the program built around it
    if !chosen.public {
        bail!(
            "{} in {} is private; make it `pub` so the node program can name it",
            chosen.schema.path(),
            chosen.schema.file.display()
        );
    }
    Ok(chosen.schema.clone())
}

/// Read one source file for structs with the attribute, following its module declarations
///
/// # Arguments
///
/// * `file` - The file
/// * `source` - Which root it is under
/// * `path` - The modules above it, outermost first
/// * `found` - Where the candidates go
fn scan_file(
    file: &Path,
    source: &SchemaSource,
    path: &mut Vec<String>,
    found: &mut Vec<Candidate>,
) -> color_eyre::Result<()> {
    // the file as items
    let text = std::fs::read_to_string(file)
        .wrap_err_with(|| format!("failed to read {}", file.display()))?;
    let parsed = syn::parse_file(&text)
        .wrap_err_with(|| format!("{} does not parse as Rust", file.display()))?;
    // a root file and a mod.rs own their directory; any other file owns a directory of its name
    let dir = file.parent().unwrap_or(Path::new("."));
    let stem = file.file_stem().and_then(|stem| stem.to_str()).unwrap_or_default();
    let owns_dir = path.is_empty() || stem == "mod";
    scan_items(&parsed.items, file, source, dir, owns_dir, stem, path, found)
}

/// Read a list of items, recursing into modules
///
/// # Arguments
///
/// * `items` - The items
/// * `file` - The file they are in
/// * `source` - Which root it is under
/// * `dir` - The directory a declared module's file is looked for under
/// * `owns_dir` - Whether declared modules sit beside this file rather than under its name
/// * `stem` - This file's name without its extension
/// * `path` - The modules above these items, outermost first
/// * `found` - Where the candidates go
#[allow(clippy::too_many_arguments)]
fn scan_items(
    items: &[syn::Item],
    file: &Path,
    source: &SchemaSource,
    dir: &Path,
    owns_dir: bool,
    stem: &str,
    path: &mut Vec<String>,
    found: &mut Vec<Candidate>,
) -> color_eyre::Result<()> {
    for item in items {
        match item {
            // a struct with the attribute
            syn::Item::Struct(item) => {
                if let Some(client_only) = db_attribute(&item.attrs) {
                    found.push(Candidate {
                        schema: Schema {
                            name: item.ident.to_string(),
                            module_path: path.clone(),
                            source: *source,
                            file: file.to_path_buf(),
                        },
                        client_only,
                        public: !matches!(item.vis, syn::Visibility::Inherited),
                    });
                }
            }
            // an inline module is its items under its name
            syn::Item::Mod(module) if module.content.is_some() => {
                let items = &module.content.as_ref().expect("checked").1;
                path.push(module.ident.to_string());
                // its declared modules live under a directory of its name
                let inner = if owns_dir {
                    dir.join(module.ident.to_string())
                } else {
                    dir.join(stem).join(module.ident.to_string())
                };
                scan_items(items, file, source, &inner, true, "mod", path, found)?;
                path.pop();
            }
            // a declared module is a file beside this one or under its name; a `#[path]` is
            // not followed, which the page says
            syn::Item::Mod(module) => {
                let name = module.ident.to_string();
                let base = if owns_dir { dir.to_path_buf() } else { dir.join(stem) };
                let candidates = [base.join(format!("{name}.rs")), base.join(&name).join("mod.rs")];
                if let Some(next) = candidates.iter().find(|candidate| candidate.is_file()) {
                    path.push(name);
                    scan_file(next, source, path, found)?;
                    path.pop();
                }
            }
            _ => (),
        }
    }
    Ok(())
}

/// Whether a struct's attributes include the database attribute, and if so whether it names
/// the client half
///
/// `#[shoal::db]` and a bare `#[db]` after `use shoal::db` are the two spellings.
///
/// # Arguments
///
/// * `attrs` - The struct's attributes
fn db_attribute(attrs: &[syn::Attribute]) -> Option<bool> {
    attrs.iter().find_map(|attr| {
        // the path is `db`, or `shoal::db`
        let segments: Vec<String> = attr
            .path()
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect();
        let is_db = match segments.as_slice() {
            [db] => db == "db",
            [shoal, db] => shoal == "shoal" && db == "db",
            _ => false,
        };
        if !is_db {
            return None;
        }
        // the only argument the macro takes is `client`, so any argument list is that half
        Some(matches!(attr.meta, syn::Meta::List(_)))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A project directory with the given files under `src/`
    fn project(files: &[(&str, &str)]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("a temp dir");
        for (name, text) in files {
            let path = dir.path().join("src").join(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, text).unwrap();
        }
        dir
    }

    /// A project with a main.rs and, optionally, a lib.rs
    fn describe(dir: &tempfile::TempDir, lib: bool) -> Project {
        Project {
            dir: dir.path().to_path_buf(),
            name: "demo".to_string(),
            edition: "2024".to_string(),
            target_directory: dir.path().join("target"),
            workspace_root: dir.path().to_path_buf(),
            lib: lib.then(|| Target {
                name: "demo".to_string(),
                kind: vec!["lib".to_string()],
                src_path: dir.path().join("src").join("lib.rs"),
            }),
            dependencies: Vec::new(),
            features: BTreeMap::new(),
        }
    }

    /// One database in main.rs is found, with its path and its client type
    #[test]
    fn one_database_is_found_in_main() {
        let dir = project(&[(
            "main.rs",
            "use shoal::PersistentUnsortedTable;\n#[shoal::db]\npub struct Demo { pub rows: PersistentUnsortedTable<Row, FileSystem> }\nfn main() {}\n",
        )]);
        let schema = describe(&dir, false).scan(None).expect("a schema");
        assert_eq!(schema.name, "Demo");
        assert_eq!(schema.path(), "Demo");
        assert_eq!(schema.client_path(), "DemoClient");
        assert_eq!(schema.source, SchemaSource::Main);
        assert_eq!(schema.file, dir.path().join("src").join("main.rs"));
    }

    /// Two databases are refused without `--db`, and picked by name or by path with it
    #[test]
    fn two_databases_need_a_name() {
        let dir = project(&[(
            "main.rs",
            "#[shoal::db]\npub struct One {}\n#[shoal::db]\npub struct Two {}\nfn main() {}\n",
        )]);
        let project = describe(&dir, false);
        let error = project.scan(None).unwrap_err().to_string();
        assert!(error.contains("2 databases (One, Two)"), "{error}");
        assert!(error.contains("--db"), "{error}");
        assert_eq!(project.scan(Some("Two")).unwrap().name, "Two");
        let error = project.scan(Some("Three")).unwrap_err().to_string();
        assert!(error.contains("no database named \"Three\""), "{error}");
    }

    /// A client half is named as such, a private struct is refused, and a file with neither
    /// says where it looked
    #[test]
    fn halves_and_privacy_are_judged() {
        let dir = project(&[("main.rs", "#[shoal::db(client)]\npub struct Demo {}\nfn main() {}\n")]);
        let error = describe(&dir, false).scan(None).unwrap_err().to_string();
        assert!(error.contains("only the client half"), "{error}");
        assert!(error.contains("Demo"), "{error}");
        let dir = project(&[("main.rs", "#[shoal::db]\nstruct Demo {}\nfn main() {}\n")]);
        let error = describe(&dir, false).scan(None).unwrap_err().to_string();
        assert!(error.contains("Demo in"), "{error}");
        assert!(error.contains("make it `pub`"), "{error}");
        let dir = project(&[("main.rs", "fn main() {}\n")]);
        let error = describe(&dir, false).scan(None).unwrap_err().to_string();
        assert!(error.contains("no `#[shoal::db]` struct"), "{error}");
        assert!(error.contains("main.rs"), "{error}");
    }

    /// A schema in a declared module is found with its module path, through both file layouts
    /// and an inline module, and a bare `#[db]` counts
    #[test]
    fn declared_modules_are_followed() {
        let dir = project(&[
            ("main.rs", "mod schema;\nmod deep;\nfn main() {}\n"),
            ("schema.rs", "pub mod inner { #[db] pub struct Demo {} }\n"),
            ("deep/mod.rs", "pub mod leaf;\n"),
            ("deep/leaf.rs", "// no schema here\n"),
        ]);
        let schema = describe(&dir, false).scan(None).expect("a schema");
        assert_eq!(schema.path(), "schema::inner::Demo");
        assert_eq!(schema.client_path(), "schema::inner::DemoClient");
        // a non-mod.rs file's own modules sit under a directory of its name
        let dir = project(&[
            ("main.rs", "mod outer;\nfn main() {}\n"),
            ("outer.rs", "pub mod tables;\n"),
            ("outer/tables.rs", "#[shoal::db]\npub(crate) struct Demo {}\n"),
        ]);
        let schema = describe(&dir, false).scan(Some("outer::tables::Demo")).expect("a schema");
        assert_eq!(schema.path(), "outer::tables::Demo");
    }

    /// The library is read when the binary defines nothing, and a client half in main.rs does
    /// not stop the library being read
    #[test]
    fn the_library_is_the_fallback() {
        let dir = project(&[
            ("main.rs", "#[shoal::db(client)]\npub struct DemoView {}\nfn main() {}\n"),
            ("lib.rs", "#[shoal::db]\npub struct Demo {}\n"),
        ]);
        let schema = describe(&dir, true).scan(None).expect("a schema");
        assert_eq!(schema.name, "Demo");
        assert_eq!(schema.source, SchemaSource::Lib);
        // and a project with only a library
        let dir = project(&[("lib.rs", "#[shoal::db]\npub struct Demo {}\n")]);
        assert_eq!(describe(&dir, true).scan(None).unwrap().source, SchemaSource::Lib);
        assert_eq!(describe(&dir, true).lib_crate().as_deref(), Some("demo"));
    }

    /// The metadata of this very crate reads back, and names no library for a workspace root
    #[test]
    fn cargo_metadata_is_read() {
        let here = Path::new(env!("CARGO_MANIFEST_DIR"));
        let project = Project::locate(here).expect("this crate");
        assert_eq!(project.name, "shoaladm");
        assert!(project.lib.is_some());
        assert!(project.dependencies.iter().any(|dependency| dependency.name == "shoal"));
        assert!(project.shoal_dependency().unwrap().path.is_some());
        assert!(project.target_directory.ends_with("target"), "{:?}", project.target_directory);
        // the workspace root is a workspace, not a package
        let root = here.parent().unwrap();
        let error = Project::locate(root).unwrap_err().to_string();
        assert!(error.contains("no package of its own"), "{error}");
        // and a directory that is not a project at all
        let dir = tempfile::tempdir().unwrap();
        let error = Project::locate(dir.path()).unwrap_err().to_string();
        assert!(error.contains("not a Rust project"), "{error}");
    }
}
