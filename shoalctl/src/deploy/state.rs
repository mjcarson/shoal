//! What a deployment minted and has to remember between runs
//!
//! Kept on the operator's machine under `~/.shoal/clusters/<name>/`, or under
//! `$SHOAL_DEPLOY_HOME/<name>/`: the cluster's certificate authority and its key, the admin
//! password, and a record of every node that was deployed with the identity its claim printed.
//! The directory is `0700` and every secret in it `0600`. Losing it loses the authority, and so
//! the ability to issue a leaf to a node added later; it does not lose the cluster.

use color_eyre::eyre::{eyre, WrapErr};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// The environment variable that overrides where every cluster's state lives
pub const HOME_ENV: &str = "SHOAL_DEPLOY_HOME";

/// The environment variable that overrides the admin password
pub const PASSWORD_ENV: &str = "SHOAL_ADMIN_PASSWORD";

/// One node the deployment placed, as its claim reported it
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct NodeRecord {
    /// The node id its directory was claimed with
    pub node: String,
    /// The address it was deployed to advertise
    pub address: String,
    /// What ssh reaches it through
    pub target: String,
}

/// Everything a deployment remembers about its cluster
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct ClusterRecord {
    /// The cluster id the bootstrapping node minted
    pub cluster: Option<String>,
    /// Every node deployed, by name
    pub nodes: BTreeMap<String, NodeRecord>,
    /// The operation `Initialize` was sent as, once it was decided
    ///
    /// Written before the request goes out, so a run that dies after sending it resends the
    /// same operation and is answered `Repeated` rather than refused as a second placement.
    pub initialize_op: Option<Uuid>,
    /// Whether `Initialize` was answered
    #[serde(default)]
    pub initialized: bool,
}

/// A cluster's local state directory
#[derive(Debug, Clone)]
pub struct State {
    /// The directory
    dir: PathBuf,
}

impl State {
    /// Where a cluster's state lives
    ///
    /// # Arguments
    ///
    /// * `name` - The cluster's name
    ///
    /// # Errors
    ///
    /// When neither the override nor `HOME` is set.
    pub fn locate(name: &str) -> color_eyre::Result<Self> {
        // the override wins, otherwise the operator's home
        let root = match std::env::var_os(HOME_ENV) {
            Some(root) => PathBuf::from(root),
            None => match std::env::var_os("HOME") {
                Some(home) => PathBuf::from(home).join(".shoal").join("clusters"),
                None => return Err(eyre!("neither {HOME_ENV} nor HOME is set")),
            },
        };
        Ok(State {
            dir: root.join(name),
        })
    }

    /// The state directory
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Whether anything has been deployed from this state
    #[must_use]
    pub fn exists(&self) -> bool {
        self.dir.join("cluster.json").is_file()
    }

    /// Create the directory, private to the operator
    ///
    /// # Errors
    ///
    /// When the directory cannot be created or its permissions set.
    pub fn create(&self) -> color_eyre::Result<()> {
        // make the directory and everything above it
        std::fs::create_dir_all(&self.dir)
            .wrap_err_with(|| format!("failed to create {}", self.dir.display()))?;
        // and keep it to ourselves, since it holds a private key and a password
        std::fs::set_permissions(&self.dir, std::fs::Permissions::from_mode(0o700))?;
        Ok(())
    }

    /// Read the cluster record, or an empty one if nothing was deployed
    ///
    /// # Errors
    ///
    /// When the record exists and cannot be read.
    pub fn record(&self) -> color_eyre::Result<ClusterRecord> {
        // no record is a cluster that has not been deployed yet
        let path = self.dir.join("cluster.json");
        if !path.is_file() {
            return Ok(ClusterRecord::default());
        }
        let raw = std::fs::read_to_string(&path)?;
        serde_json::from_str(&raw).wrap_err_with(|| format!("{} is not a cluster record", path.display()))
    }

    /// Write the cluster record whole
    ///
    /// # Arguments
    ///
    /// * `record` - The record
    ///
    /// # Errors
    ///
    /// When it cannot be written.
    pub fn save(&self, record: &ClusterRecord) -> color_eyre::Result<()> {
        // write beside it and rename, so a crash leaves the old record or the new one
        let raw = serde_json::to_string_pretty(record)?;
        write_private(&self.dir.join("cluster.json"), raw.as_bytes())
    }

    /// Read a secret, if it has been written
    ///
    /// # Arguments
    ///
    /// * `name` - The file under the state directory
    ///
    /// # Errors
    ///
    /// When it exists and cannot be read.
    pub fn read_secret(&self, name: &str) -> color_eyre::Result<Option<String>> {
        // a secret that is not there has not been minted yet
        let path = self.dir.join(name);
        if !path.is_file() {
            return Ok(None);
        }
        Ok(Some(std::fs::read_to_string(path)?))
    }

    /// Write a secret, readable only by the operator
    ///
    /// # Arguments
    ///
    /// * `name` - The file under the state directory
    /// * `contents` - What to write
    ///
    /// # Errors
    ///
    /// When it cannot be written.
    pub fn write_secret(&self, name: &str, contents: &str) -> color_eyre::Result<()> {
        write_private(&self.dir.join(name), contents.as_bytes())
    }

    /// The admin password: the override if set, else the one minted for this cluster
    ///
    /// Minted the first time it is asked for, from two random UUIDs, and kept.
    ///
    /// # Errors
    ///
    /// When the minted password cannot be read or written.
    pub fn password(&self) -> color_eyre::Result<String> {
        // an operator who names a password gets that one
        if let Ok(password) = std::env::var(PASSWORD_ENV) {
            if !password.is_empty() {
                return Ok(password);
            }
        }
        // otherwise the one this cluster already has
        if let Some(password) = self.read_secret("admin.password")? {
            return Ok(password.trim().to_string());
        }
        // or a new one, 244 random bits of it
        let password = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        self.write_secret("admin.password", &password)?;
        Ok(password)
    }

    /// Delete the whole state directory
    ///
    /// # Errors
    ///
    /// When it exists and cannot be removed.
    pub fn delete(&self) -> color_eyre::Result<()> {
        // nothing to delete is not an error
        if self.dir.exists() {
            std::fs::remove_dir_all(&self.dir)
                .wrap_err_with(|| format!("failed to remove {}", self.dir.display()))?;
        }
        Ok(())
    }
}

/// Write a file readable only by its owner, atomically
///
/// # Arguments
///
/// * `path` - Where to write
/// * `contents` - What to write
fn write_private(path: &Path, contents: &[u8]) -> color_eyre::Result<()> {
    // write the whole file beside the target with owner-only permissions
    let partial = path.with_extension("partial");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&partial)
        .wrap_err_with(|| format!("failed to write {}", partial.display()))?;
    file.write_all(contents)?;
    file.sync_all()?;
    // then put it in place in one step
    std::fs::rename(&partial, path)
        .wrap_err_with(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The record round-trips, the password is minted once, and secrets are private
    #[test]
    fn state_is_private_and_minted_once() {
        // a state directory under this test's own temp dir
        let root = std::env::temp_dir().join(format!("shoalctl-state-{}", Uuid::new_v4()));
        let state = State {
            dir: root.join("lab"),
        };
        state.create().expect("a state dir");
        assert!(!state.exists());
        // the record round-trips
        let mut record = ClusterRecord::default();
        record.cluster = Some("c".into());
        record.nodes.insert(
            "a".into(),
            NodeRecord {
                node: "n".into(),
                address: "10.0.0.1".into(),
                target: "a".into(),
            },
        );
        state.save(&record).expect("a saved record");
        assert!(state.exists());
        assert_eq!(state.record().expect("a record"), record);
        // the password is minted once and kept, unless the override is set
        if std::env::var_os(PASSWORD_ENV).is_none() {
            let first = state.password().expect("a password");
            assert_eq!(first.len(), 64);
            assert_eq!(state.password().expect("the same password"), first);
        }
        // every file and the directory are the operator's alone
        let mode = |path: &Path| std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(state.dir()), 0o700);
        assert_eq!(mode(&state.dir().join("cluster.json")), 0o600);
        state.delete().expect("deleted");
        assert!(!state.dir().exists());
        std::fs::remove_dir_all(root).ok();
    }
}
