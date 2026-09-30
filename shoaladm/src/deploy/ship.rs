//! Shipping a backup's files between a deployment's hosts
//!
//! A backup writes each group's file on the disk of the node that led the group
//! ([F49](../../../docs/src/features/backup-and-recovery.md)), and a restore reads each group's
//! file on the node that leads the group in the new cluster. Nothing in the cluster moves them,
//! so before this command an operator gathered every host's files and copied them to every host
//! by hand ([runbook 10](../../../docs/src/operations/runbooks.md#10-backup-and-restore)).
//! `shoaladm ship-backup` does that: it lists what every host holds of one backup, and pipes each
//! host the files it lacks from a host that has them, through this machine, as a tar stream. No
//! file is stored here, and no file a host already holds is sent again.

use color_eyre::eyre::{bail, eyre, WrapErr};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::process::{Command, Stdio};

use super::inventory::Inventory;
use super::ops::{step, Deployment};
use super::remote::{quote, Host};

/// One host a backup's files are read from or written to
#[derive(Debug, Clone, PartialEq, Eq)]
struct Holder {
    /// The node's name in its inventory
    name: String,
    /// The host, as ssh reaches it
    host: Host,
}

/// Split a backup directory into the directory it is in and its own name
///
/// # Arguments
///
/// * `dir` - The backup's directory, `<path>/<op>`
fn split(dir: &str) -> color_eyre::Result<(String, String)> {
    // an absolute path, with a parent, whatever slashes it ends in
    let trimmed = dir.trim_end_matches('/');
    if !trimmed.starts_with('/') {
        bail!("the backup directory {dir:?} has to be an absolute path, as the restore names it");
    }
    let (parent, name) = trimmed
        .rsplit_once('/')
        .ok_or_else(|| eyre!("the backup directory {dir:?} names no directory"))?;
    if name.is_empty() || name == "." || name == ".." {
        bail!("the backup directory {dir:?} names no directory");
    }
    let parent = if parent.is_empty() { "/" } else { parent };
    Ok((parent.to_string(), name.to_string()))
}

/// Every node of an inventory as a host a backup's files can be read from or written to
///
/// # Arguments
///
/// * `inventory` - The inventory
fn holders(inventory: &Inventory) -> color_eyre::Result<Vec<Holder>> {
    // every node the inventory lists, deployed or not: a new cluster's hosts may not be yet
    inventory
        .nodes
        .iter()
        .map(|spec| {
            let node = inventory.node(&spec.name)?;
            Ok(Holder {
                name: node.name,
                host: Host {
                    target: node.target,
                },
            })
        })
        .collect()
}

/// The script that lists the files a host holds of a backup, relative to its parent
///
/// # Arguments
///
/// * `parent` - The directory the backup is in
/// * `name` - The backup's own directory
fn list_script(parent: &str, name: &str) -> String {
    // nothing at all is an empty list, not a failure: that host led none of the groups
    let dir = quote(&format!("{parent}/{name}"));
    format!(
        "if sudo -n test -d {dir}; then sudo -n find {dir} -type f -printf {}; fi",
        quote(&format!("{name}/%P\\n"))
    )
}

/// The script that writes a tar stream of the files named on its stdin
///
/// # Arguments
///
/// * `parent` - The directory the backup is in
fn send_script(parent: &str) -> String {
    format!("sudo -n tar -C {} -cf - -T -", quote(parent))
}

/// The script that unpacks a tar stream under the backup's parent, keeping what is there
///
/// # Arguments
///
/// * `parent` - The directory the backup is in
fn receive_script(parent: &str) -> String {
    // a file the host already holds is never rewritten, and the owners the sender's files had
    // are kept, which is the node's user on every host of a deployment
    let parent = quote(parent);
    format!("sudo -n mkdir -p {parent} && sudo -n tar -C {parent} --skip-old-files -xf -")
}

/// Which files each receiver lacks, and which sender each is read from
///
/// Each file is read from the first holder that has it, in inventory order, so a transfer is one
/// stream per sender and receiver.
///
/// # Arguments
///
/// * `held` - What each holder holds, by name
/// * `order` - The senders, by name, in the order a file is looked for in them
/// * `receivers` - The holders that are to hold every file, by name
fn plan(
    held: &BTreeMap<String, BTreeSet<String>>,
    order: &[String],
    receivers: &[String],
) -> BTreeMap<(String, String), Vec<String>> {
    // every file any holder has, and the first holder that has it
    let mut source: BTreeMap<&String, &String> = BTreeMap::new();
    for name in order {
        for file in held.get(name).into_iter().flatten() {
            source.entry(file).or_insert(name);
        }
    }
    // each receiver's missing files, grouped by the sender they are read from
    let mut transfers: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
    let empty = BTreeSet::new();
    for receiver in receivers {
        let has = held.get(receiver).unwrap_or(&empty);
        for (file, sender) in &source {
            if !has.contains(*file) {
                transfers
                    .entry(((*sender).clone(), receiver.clone()))
                    .or_default()
                    .push((*file).clone());
            }
        }
    }
    transfers
}

/// Pipe a list of files from one host to another as a tar stream through this machine
///
/// # Arguments
///
/// * `from` - The host that holds them
/// * `to` - The host that lacks them
/// * `parent` - The directory the backup is in on both
/// * `files` - The files, relative to `parent`
fn pipe(from: &Host, to: &Host, parent: &str, files: &[String]) -> color_eyre::Result<()> {
    // the sender reads the list on its stdin and writes the stream on its stdout
    let send = from.ssh_command(&send_script(parent));
    let mut sender = Command::new(&send[0])
        .args(&send[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .wrap_err("failed to start ssh")?;
    // the receiver reads the stream straight from the sender's stdout
    let receive = to.ssh_command(&receive_script(parent));
    let stream = sender.stdout.take().expect("a piped stdout");
    let receiver = Command::new(&receive[0])
        .args(&receive[1..])
        .stdin(Stdio::from(stream))
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .wrap_err("failed to start ssh")?;
    // the list, then the end of it, so tar starts
    {
        let mut list = sender.stdin.take().expect("a piped stdin");
        list.write_all(files.join("\n").as_bytes())?;
        list.write_all(b"\n")?;
    }
    // both have to succeed: a sender that failed leaves a receiver with half a stream
    let sent = sender.wait_with_output()?;
    let received = receiver.wait_with_output()?;
    if !sent.status.success() {
        bail!(
            "reading {} files from {} failed: {}",
            files.len(),
            from.target,
            String::from_utf8_lossy(&sent.stderr).trim()
        );
    }
    if !received.status.success() {
        bail!(
            "writing {} files to {} failed: {}",
            files.len(),
            to.target,
            String::from_utf8_lossy(&received.stderr).trim()
        );
    }
    Ok(())
}

impl Deployment {
    /// List what every holder holds of a backup, by name
    ///
    /// # Arguments
    ///
    /// * `holders` - The hosts to ask
    /// * `parent` - The directory the backup is in
    /// * `name` - The backup's own directory
    fn held(
        holders: &[Holder],
        parent: &str,
        name: &str,
    ) -> color_eyre::Result<BTreeMap<String, BTreeSet<String>>> {
        let script = list_script(parent, name);
        holders
            .iter()
            .map(|holder| {
                let listed = holder.host.run(&script)?;
                let files = listed
                    .lines()
                    .filter(|line| !line.is_empty())
                    .map(str::to_string)
                    .collect();
                Ok((holder.name.clone(), files))
            })
            .collect()
    }

    /// Copy every host's files of a backup to every host, so a restore finds each group's file
    /// on whichever node leads the group
    ///
    /// # Arguments
    ///
    /// * `dir` - The backup's directory, `<path>/<op>`, the same path on every host
    /// * `to` - The inventory whose hosts receive the files, or this one's
    ///
    /// # Errors
    ///
    /// When no host holds the backup, a host cannot be reached, a transfer fails, or a host
    /// does not hold every file afterwards.
    pub fn ship_backup(&self, dir: &str, to: Option<&Inventory>) -> color_eyre::Result<()> {
        let (parent, name) = split(dir)?;
        // the hosts that wrote the backup, and the ones that are to hold it whole
        let senders = holders(&self.inventory)?;
        let receivers = match to {
            Some(inventory) => holders(inventory)?,
            None => senders.clone(),
        };
        // a host in both lists is one holder, by its ssh target
        let mut every: Vec<Holder> = senders.clone();
        for receiver in &receivers {
            if !every.iter().any(|holder| holder.host == receiver.host) {
                every.push(receiver.clone());
            }
        }
        let held = Self::held(&every, &parent, &name)?;
        for holder in &every {
            let count = held.get(&holder.name).map_or(0, BTreeSet::len);
            step(Some(&holder.name), &format!("holds {count} files of {dir}"));
        }
        // what each receiver lacks, and from which sender
        let order: Vec<String> = senders.iter().map(|holder| holder.name.clone()).collect();
        let names: Vec<String> = receivers.iter().map(|holder| holder.name.clone()).collect();
        let transfers = plan(&held, &order, &names);
        let total: BTreeSet<&String> = held.values().flatten().collect();
        if total.is_empty() {
            bail!("no host holds any file of {dir}; name the directory the backup wrote, <path>/<op>");
        }
        let host = |name: &str| {
            every
                .iter()
                .find(|holder| holder.name == name)
                .map(|holder| holder.host.clone())
                .expect("a holder of the plan")
        };
        for ((from, to), files) in &transfers {
            step(Some(to), &format!("receiving {} files from {from}", files.len()));
            pipe(&host(from), &host(to), &parent, files)?;
        }
        // every receiver holds every file now, or the command says which do not
        let after = Self::held(&receivers, &parent, &name)?;
        for receiver in &names {
            let has = after.get(receiver).map_or(0, BTreeSet::len);
            if has < total.len() {
                bail!("{receiver} holds {has} of the backup's {} files after the copy", total.len());
            }
        }
        step(
            None,
            &format!(
                "every host holds all {} files of {dir}; restore it with `restore {dir}`",
                total.len()
            ),
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A backup directory splits into its parent and its name, and a relative one is refused
    #[test]
    fn a_backup_directory_splits() {
        assert_eq!(
            split("/optane/backup/1234/").unwrap(),
            ("/optane/backup".to_string(), "1234".to_string())
        );
        assert_eq!(split("/b").unwrap(), ("/".to_string(), "b".to_string()));
        assert!(split("backup/1234").is_err());
        assert!(split("/").is_err());
    }

    /// Each receiver is sent what it lacks, from the first holder in order, and nothing else
    #[test]
    fn each_receiver_is_sent_what_it_lacks() {
        let set = |files: &[&str]| files.iter().map(|file| (*file).to_string()).collect();
        let held = BTreeMap::from([
            ("a".to_string(), set(&["op/t/1.snap", "op/t/1.json"])),
            ("b".to_string(), set(&["op/t/2.snap"])),
            ("c".to_string(), set(&["op/t/2.snap", "op/t/3.snap"])),
        ]);
        let order = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let transfers = plan(&held, &order, &order);
        // a lacks 2 (first held by b) and 3 (only c)
        assert_eq!(transfers[&("b".into(), "a".into())], vec!["op/t/2.snap"]);
        assert_eq!(transfers[&("c".into(), "a".into())], vec!["op/t/3.snap"]);
        // b lacks a's two and c's third
        assert_eq!(
            transfers[&("a".into(), "b".into())],
            vec!["op/t/1.json", "op/t/1.snap"]
        );
        assert_eq!(transfers[&("c".into(), "b".into())], vec!["op/t/3.snap"]);
        // c lacks a's two; its 2 is not sent again from b
        assert_eq!(
            transfers[&("a".into(), "c".into())],
            vec!["op/t/1.json", "op/t/1.snap"]
        );
        assert!(!transfers.contains_key(&("b".into(), "c".into())));
        // a new host that holds nothing is sent everything
        let transfers = plan(&held, &order, &["d".to_string()]);
        let sent: usize = transfers.values().map(Vec::len).sum();
        assert_eq!(sent, 4);
    }

    /// The scripts quote the paths they are given, and the lister prints paths under the name
    #[test]
    fn the_scripts_name_the_backup() {
        assert!(list_script("/b", "op").contains("-printf 'op/%P\\n'"));
        assert!(receive_script("/b dir").contains("'/b dir'"));
        assert!(send_script("/b").ends_with("-cf - -T -"));
    }
}
