//! The operator's own settings, and where the tools keep what they build
//!
//! `~/.config/shoal/config.yaml` names the inventory a command reads when it is given none, so
//! `shoalctl` on its own opens the cluster the operator is working with, and `~/.local/shoal/bin`
//! holds every program the two tools built: a node per cpu class, an admin program and a
//! terminal UI per schema, each under a name that says what it is
//! ([F63](../../docs/src/features/shoaladm.md)). Neither exists until something is written to
//! it, and a missing config file is the defaults.

use color_eyre::eyre::{WrapErr, bail, eyre};
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

/// The environment variable that moves the config directory, as the XDG base directory spec names it
pub const CONFIG_HOME_ENV: &str = "XDG_CONFIG_HOME";

/// The environment variable that moves the installed programs, for tests and for an operator
/// who keeps them elsewhere
pub const BIN_DIR_ENV: &str = "SHOAL_BIN_DIR";

/// The file an inventory is looked for under in a project when none is named
pub const PROJECT_INVENTORY: &str = "inventory.yml";

/// What the config file holds
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The inventory a command reads when it is given none and its project holds none
    ///
    /// A relative path is relative to the config file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_inventory: Option<PathBuf>,
    /// Where the programs the tools build are installed; `~/.local/shoal/bin` if absent
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bin_dir: Option<PathBuf>,
}

impl Config {
    /// Where the config file lives: `$XDG_CONFIG_HOME/shoal/config.yaml`, or `~/.config/shoal/config.yaml`
    ///
    /// # Errors
    ///
    /// When neither the override nor `HOME` is set.
    pub fn path() -> color_eyre::Result<PathBuf> {
        // the spec's variable wins, otherwise the operator's home
        let root = match std::env::var_os(CONFIG_HOME_ENV).filter(|root| !root.is_empty()) {
            Some(root) => PathBuf::from(root),
            None => match std::env::var_os("HOME") {
                Some(home) => PathBuf::from(home).join(".config"),
                None => return Err(eyre!("neither {CONFIG_HOME_ENV} nor HOME is set")),
            },
        };
        Ok(root.join("shoal").join("config.yaml"))
    }

    /// Read the config file, or the defaults if there is none
    ///
    /// # Errors
    ///
    /// When the file exists and is not a config, or its location cannot be decided.
    pub fn load() -> color_eyre::Result<Self> {
        Self::read(&Self::path()?)
    }

    /// Read a config file, or the defaults if there is none
    ///
    /// # Arguments
    ///
    /// * `path` - The file
    ///
    /// # Errors
    ///
    /// When the file exists and cannot be read as a config.
    pub fn read(path: &Path) -> color_eyre::Result<Self> {
        // no file is the defaults
        if !path.is_file() {
            return Ok(Config::default());
        }
        let raw = std::fs::read_to_string(path)
            .wrap_err_with(|| format!("failed to read {}", path.display()))?;
        let mut config: Config = serde_yaml::from_str(&raw)
            .wrap_err_with(|| format!("{} is not a shoal config", path.display()))?;
        // a relative inventory is relative to the file, not to wherever we were run
        if let Some(inventory) = &config.default_inventory {
            if inventory.is_relative() {
                if let Some(parent) = path.parent() {
                    config.default_inventory = Some(parent.join(inventory));
                }
            }
        }
        Ok(config)
    }

    /// Write this config to its file, creating the directory, private to the operator
    ///
    /// # Arguments
    ///
    /// * `path` - The file
    ///
    /// # Errors
    ///
    /// When the directory or the file cannot be written.
    pub fn save(&self, path: &Path) -> color_eyre::Result<()> {
        // the directory, and everything above it
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .wrap_err_with(|| format!("failed to create {}", parent.display()))?;
        }
        // written beside the target and renamed, so a reader never sees half of it
        let partial = path.with_extension("yaml.partial");
        let document = serde_yaml::to_string(self)?;
        {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(&partial)
                .wrap_err_with(|| format!("failed to write {}", partial.display()))?;
            file.write_all(HEADER.as_bytes())?;
            file.write_all(document.as_bytes())?;
        }
        std::fs::rename(&partial, path)
            .wrap_err_with(|| format!("failed to rename {} into place", partial.display()))?;
        Ok(())
    }

    /// Where the tools install what they build
    ///
    /// `$SHOAL_BIN_DIR` first, then the config's `bin_dir`, then `~/.local/shoal/bin`.
    ///
    /// # Errors
    ///
    /// When nothing names a directory and `HOME` is not set.
    pub fn bin_dir(&self) -> color_eyre::Result<PathBuf> {
        // the override, for tests and for an operator who keeps them elsewhere
        if let Some(dir) = std::env::var_os(BIN_DIR_ENV).filter(|dir| !dir.is_empty()) {
            return Ok(PathBuf::from(dir));
        }
        // then the file's own
        if let Some(dir) = &self.bin_dir {
            return Ok(dir.clone());
        }
        // then the default under the operator's home
        match std::env::var_os("HOME") {
            Some(home) => Ok(PathBuf::from(home).join(".local").join("shoal").join("bin")),
            None => Err(eyre!("neither {BIN_DIR_ENV}, bin_dir nor HOME names where programs are installed")),
        }
    }
}

/// The note every written config opens with
const HEADER: &str = "\
# Written by `shoaladm config`. `default_inventory` is the inventory every command reads when it
# is given none and its project holds no inventory.yml; `bin_dir` is where the programs the tools
# build are installed (~/.local/shoal/bin if absent).
";

/// Decide which inventory a command reads
///
/// The flag wins; then `inventory.yml` in the project the command runs for, if there is one and
/// the file exists; then the config's `default_inventory`.
///
/// # Arguments
///
/// * `flag` - The `-i` the command was given, if any
/// * `project` - The project directory the command runs for, if one is at hand
/// * `config` - The operator's config
///
/// # Errors
///
/// When nothing names an inventory, saying how to set one.
pub fn inventory(
    flag: Option<&Path>,
    project: Option<&Path>,
    config: &Config,
) -> color_eyre::Result<PathBuf> {
    // the flag, as given
    if let Some(path) = flag {
        return Ok(path.to_path_buf());
    }
    // the project's own file, when it has one
    if let Some(project) = project {
        let local = project.join(PROJECT_INVENTORY);
        if local.is_file() {
            return Ok(local);
        }
    }
    // the operator's default
    if let Some(path) = &config.default_inventory {
        return Ok(path.clone());
    }
    let file = Config::path().map_or_else(|_| "~/.config/shoal/config.yaml".to_string(), |path| path.display().to_string());
    bail!(
        "no inventory: pass -i <file>, keep one as {PROJECT_INVENTORY} in the project, or set a default \
         in {file} with `shoaladm config default-inventory <file>`"
    );
}

/// Install a built program under a directory, atomically and executable
///
/// # Arguments
///
/// * `built` - The program cargo wrote
/// * `bin_dir` - Where the tools install what they build
/// * `name` - The name it is installed under
///
/// # Errors
///
/// When the directory cannot be made or the copy fails.
pub fn install(built: &Path, bin_dir: &Path, name: &str) -> color_eyre::Result<PathBuf> {
    // the directory, and everything above it
    std::fs::create_dir_all(bin_dir)
        .wrap_err_with(|| format!("failed to create {}", bin_dir.display()))?;
    // copied beside its target and renamed, so a program being run is never half written
    let target = bin_dir.join(name);
    let partial = bin_dir.join(format!("{name}.partial"));
    std::fs::copy(built, &partial).wrap_err_with(|| {
        format!("failed to copy {} to {}", built.display(), partial.display())
    })?;
    std::fs::set_permissions(&partial, std::fs::Permissions::from_mode(0o755))?;
    std::fs::rename(&partial, &target)
        .wrap_err_with(|| format!("failed to rename {} into place", partial.display()))?;
    Ok(target)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A missing file is the defaults, a relative inventory is resolved against the file, and an
    /// unknown key is refused
    #[test]
    fn a_config_reads_back_with_its_defaults() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let path = dir.path().join("shoal").join("config.yaml");
        // nothing there yet
        assert_eq!(Config::read(&path).unwrap(), Config::default());
        // a relative default is relative to the file
        let config = Config {
            default_inventory: Some(PathBuf::from("../lab.yml")),
            bin_dir: None,
        };
        config.save(&path).expect("saved");
        let read = Config::read(&path).unwrap();
        assert_eq!(read.default_inventory, Some(dir.path().join("shoal").join("../lab.yml")));
        // the file is the operator's alone
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        // an unknown key is a mistake, not a setting
        std::fs::write(&path, "default_inventroy: x\n").unwrap();
        assert!(Config::read(&path).is_err());
    }

    /// The flag, then the project's file, then the default, then an error that says how to set one
    #[test]
    fn an_inventory_is_resolved_in_order() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let project = dir.path().join("project");
        std::fs::create_dir_all(&project).unwrap();
        let config = Config {
            default_inventory: Some(dir.path().join("default.yml")),
            bin_dir: None,
        };
        // the flag wins over everything
        let flag = dir.path().join("flag.yml");
        assert_eq!(inventory(Some(&flag), Some(&project), &config).unwrap(), flag);
        // no project file: the default
        assert_eq!(
            inventory(None, Some(&project), &config).unwrap(),
            dir.path().join("default.yml")
        );
        // a project file beats the default
        let local = project.join(PROJECT_INVENTORY);
        std::fs::write(&local, "name: x\n").unwrap();
        assert_eq!(inventory(None, Some(&project), &config).unwrap(), local);
        // nothing at all names the config command
        let error = inventory(None, None, &Config::default()).unwrap_err().to_string();
        assert!(error.contains("default-inventory"), "{error}");
    }

    /// An installed program lands executable under its name, and nothing partial is left
    #[test]
    fn a_program_is_installed_whole_and_executable() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let built = dir.path().join("built");
        std::fs::write(&built, b"#!/bin/sh\necho shoal\n").unwrap();
        let bin = dir.path().join("bin");
        let installed = install(&built, &bin, "demo-Db-node-znver1").expect("installed");
        assert_eq!(installed, bin.join("demo-Db-node-znver1"));
        let mode = std::fs::metadata(&installed).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o755);
        assert!(!bin.join("demo-Db-node-znver1.partial").exists());
        // installing again replaces it in place
        std::fs::write(&built, b"#!/bin/sh\necho again\n").unwrap();
        install(&built, &bin, "demo-Db-node-znver1").expect("installed again");
        assert_eq!(std::fs::read(&installed).unwrap(), b"#!/bin/sh\necho again\n");
    }

    /// The bin dir is the override, then the file's, then the home default
    #[test]
    fn the_bin_dir_has_three_sources() {
        let config = Config::default();
        // the default is under the home this test runs with
        let home = std::env::var("HOME").expect("HOME");
        assert_eq!(
            config.bin_dir().unwrap(),
            PathBuf::from(home).join(".local").join("shoal").join("bin")
        );
        // the file's own
        let config = Config {
            default_inventory: None,
            bin_dir: Some(PathBuf::from("/opt/shoal/bin")),
        };
        assert_eq!(config.bin_dir().unwrap(), PathBuf::from("/opt/shoal/bin"));
    }
}
