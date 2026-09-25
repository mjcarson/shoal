//! `shoalctl cluster new`: building an inventory in a full screen form
//!
//! Writing an inventory by hand means learning its keys from the docs and finding out what is
//! wrong with it only when `bootstrap` refuses it. The wizard walks the same keys a page at a
//! time - the cluster, its shape, the defaults every node gets, the groups nodes can share
//! settings through, and the hosts - judging the draft on every key the way `bootstrap` will,
//! showing what each node resolves to and from where, and writing the file only once nothing
//! is wrong with it ([F53](../../docs/src/features/inventory-wizard.md)).
//!
//! - [`form`] holds the draft and decides what every key does, and draws nothing;
//! - [`view`] draws it;
//! - [`probe`] asks a host over ssh what it has, off the loop.

pub mod form;
pub mod probe;
pub mod view;

use color_eyre::eyre::WrapErr;
use crossterm::event::{Event, EventStream, KeyEventKind};
use futures::StreamExt;
use std::path::{Path, PathBuf};

use crate::deploy::inventory::Inventory;
use form::{Draft, Outcome, ProbeState, Resolution, Wizard};

/// The note every written inventory opens with
const HEADER: &str = "\
# Written by `shoalctl cluster new`. Edit it by hand, or again with `cluster new --from <this file>`.
#
# Build the server program for the oldest cpu among the hosts, never native - a build for a newer
# one dies of SIGILL at its claim:
#
#   RUSTFLAGS=\"-C target-cpu=<oldest host's cpu>\" cargo build --release --bin <your node program>
#
# then deploy it with:
#
#   <your shoalctl program> cluster bootstrap -i <this file>
";

/// Write an inventory as the file the wizard saves
///
/// # Arguments
///
/// * `inventory` - The inventory
///
/// # Errors
///
/// When the inventory cannot be written as YAML.
pub fn document(inventory: &Inventory) -> color_eyre::Result<String> {
    // the note, then the inventory
    let body = serde_yaml::to_string(inventory)?;
    Ok(format!("{HEADER}{body}"))
}

/// Write the inventory next to where it goes and move it into place
///
/// # Arguments
///
/// * `path` - Where the inventory goes
/// * `inventory` - The inventory
///
/// # Errors
///
/// When the file cannot be written or moved.
pub fn save(path: &Path, inventory: &Inventory) -> color_eyre::Result<()> {
    // a partial file first, so an interrupted write never leaves half an inventory
    let body = document(inventory)?;
    let partial = path.with_extension("yml.partial");
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)
            .wrap_err_with(|| format!("failed to create {}", parent.display()))?;
    }
    std::fs::write(&partial, body)
        .wrap_err_with(|| format!("failed to write {}", partial.display()))?;
    std::fs::rename(&partial, path)
        .wrap_err_with(|| format!("failed to move {} into place", path.display()))?;
    Ok(())
}

/// Keep a relative server path pointing at the same program from another inventory's directory
///
/// `Inventory::load` reads a relative `server` against the inventory's own directory. Written to
/// the same directory it was read from, the path is kept as it was written; written anywhere
/// else, it is made absolute rather than silently naming another file.
///
/// # Arguments
///
/// * `server` - The server path as the source inventory wrote it
/// * `from` - The inventory it was read from
/// * `out` - The inventory it will be written to
#[must_use]
pub fn rebase(server: &Path, from: &Path, out: &Path) -> PathBuf {
    // an absolute path means the same thing from anywhere
    if server.is_absolute() {
        return server.to_path_buf();
    }
    // the directories the two files are in, compared as the filesystem sees them
    let dir = |file: &Path| {
        let parent = file
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty());
        let parent = parent.unwrap_or(Path::new("."));
        std::fs::canonicalize(parent).unwrap_or_else(|_| parent.to_path_buf())
    };
    let (from_dir, out_dir) = (dir(from), dir(out));
    if from_dir == out_dir {
        return server.to_path_buf();
    }
    // otherwise the path it meant, without the `..` a join leaves in it
    let mut absolute = PathBuf::new();
    for component in from_dir.join(server).components() {
        match component {
            std::path::Component::ParentDir => {
                absolute.pop();
            }
            std::path::Component::CurDir => (),
            other => absolute.push(other),
        }
    }
    absolute
}

/// Run the wizard until the operator saves or leaves
///
/// # Arguments
///
/// * `out` - Where the inventory is written
/// * `from` - An inventory to start from, if any
///
/// # Errors
///
/// When the inventory to start from cannot be read, the terminal fails, or the file cannot be
/// written.
pub async fn run(out: PathBuf, from: Option<PathBuf>) -> color_eyre::Result<()> {
    // the draft to start from: an inventory as written, without judging it, or nothing
    let draft = match &from {
        Some(path) => {
            let raw = std::fs::read_to_string(path)
                .wrap_err_with(|| format!("failed to read {}", path.display()))?;
            let mut inventory: Inventory = serde_yaml::from_str(&raw)
                .wrap_err_with(|| format!("{} is not an inventory", path.display()))?;
            // a relative program is relative to the file it was read from, so one written
            // elsewhere names it by where it is
            inventory.server = rebase(&inventory.server, path, &out);
            Draft::from_inventory(&inventory)
        }
        None => Draft::default(),
    };
    let mut wizard = Wizard::new(draft, out.clone(), out.exists());
    // the terminal, restored whatever the loop comes to
    let mut terminal = ratatui::init();
    let result = event_loop(&mut terminal, &mut wizard).await;
    ratatui::restore();
    // say what happened once the screen is back
    match result? {
        true => {
            println!("wrote {}", out.display());
            println!("next: build the server program it names, then");
            println!("  <this program> cluster bootstrap -i {}", out.display());
        }
        false => println!("left without writing {}", out.display()),
    }
    Ok(())
}

/// Draw and handle keys until the operator saves or leaves, returning whether it saved
///
/// # Arguments
///
/// * `terminal` - The terminal
/// * `wizard` - The wizard
async fn event_loop(
    terminal: &mut ratatui::DefaultTerminal,
    wizard: &mut Wizard,
) -> color_eyre::Result<bool> {
    // keys from the terminal, and probes and lookups coming back from their blocking threads
    let mut events = EventStream::new();
    let (probe_tx, probe_rx) = kanal::unbounded_async::<(String, ProbeState)>();
    let (resolve_tx, resolve_rx) = kanal::unbounded_async::<(String, Resolution)>();
    loop {
        // every name bootstrap would resolve is looked up here too, once, off the loop
        spawn_resolutions(wizard, &resolve_tx);
        terminal.draw(|frame| view::render(frame, wizard))?;
        tokio::select! {
            event = events.next() => {
                let Some(event) = event else {
                    return Ok(false);
                };
                // only a press is a key: a release would type every character twice
                let Event::Key(key) = event? else {
                    continue;
                };
                if key.kind != KeyEventKind::Press {
                    continue;
                }
                match wizard.handle_key(key) {
                    Outcome::Continue => (),
                    Outcome::Quit => return Ok(false),
                    Outcome::Save => {
                        // judged again, since a save is only ever of a draft with no error
                        let (inventory, _) = wizard.build();
                        match save(&wizard.out, &inventory) {
                            Ok(()) => return Ok(true),
                            Err(error) => {
                                wizard.message = Some((form::Severity::Error, format!("{error:#}")));
                            }
                        }
                    }
                    Outcome::Probe(index) => spawn_probe(wizard, index, probe_tx.clone()),
                }
            }
            Ok((name, state)) = probe_rx.recv() => {
                wizard.probes.insert(name, state);
            }
            Ok((name, resolution)) = resolve_rx.recv() => {
                wizard.resolutions.insert(name, resolution);
            }
        }
    }
}

/// Resolve every node name not yet looked up on a blocking thread each, sending the answers back
///
/// A resolver can take seconds on a name it does not know, so the screen never waits on one.
///
/// # Arguments
///
/// * `wizard` - The wizard
/// * `tx` - Where to send what each name resolved to
fn spawn_resolutions(wizard: &mut Wizard, tx: &kanal::AsyncSender<(String, Resolution)>) {
    for name in wizard.unresolved() {
        // marked first, so the next pass does not ask again
        wizard.resolutions.insert(name.clone(), Resolution::Running);
        let tx = tx.clone();
        tokio::spawn(async move {
            // the lookup blocks, so it runs off the loop
            let lookup = name.clone();
            let resolution = tokio::task::spawn_blocking(move || probe::resolution(&lookup))
                .await
                .unwrap_or_else(|error| Resolution::Failed(error.to_string()));
            let _ = tx.send((name, resolution)).await;
        });
    }
}

/// Probe a node's host on a blocking thread, sending what it found back to the loop
///
/// # Arguments
///
/// * `wizard` - The wizard
/// * `index` - The node to probe
/// * `tx` - Where to send what the probe found
fn spawn_probe(wizard: &mut Wizard, index: usize, tx: kanal::AsyncSender<(String, ProbeState)>) {
    // the node as the draft resolves it: its ssh target and its storage directories
    let (inventory, _) = wizard.build();
    let Some(spec) = inventory.nodes.get(index) else {
        return;
    };
    if spec.name.is_empty() {
        wizard.message = Some((
            form::Severity::Error,
            "name the node before probing it".to_string(),
        ));
        return;
    }
    let name = spec.name.clone();
    let target = spec.target().to_string();
    let (storage, _) = inventory.resolve_storage(spec);
    let roots = storage.roots();
    wizard.probes.insert(name.clone(), ProbeState::Running);
    // ssh blocks, so it runs off the loop and the screen keeps drawing
    tokio::spawn(async move {
        let result = tokio::task::spawn_blocking(move || probe::probe(&target, &roots)).await;
        let state = match result {
            Ok(Ok(report)) => ProbeState::Done(report),
            Ok(Err(error)) => ProbeState::Failed(format!("{error:#}")),
            Err(error) => ProbeState::Failed(error.to_string()),
        };
        let _ = tx.send((name, state)).await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A relative program keeps naming the same file when its inventory is written elsewhere
    #[test]
    fn a_relative_server_is_rebased_onto_the_new_inventory() {
        // a source inventory two directories down, naming a program relative to itself
        let root = tempfile::tempdir().expect("a temp dir");
        let from_dir = root.path().join("shoalctl/inventories");
        std::fs::create_dir_all(&from_dir).unwrap();
        let root_dir = std::fs::canonicalize(root.path()).unwrap();
        let from = from_dir.join("lab.yml");
        let server = Path::new("../../target/deploy/release/shoal-node");
        // written beside it, the path is kept as written
        assert_eq!(rebase(server, &from, &from_dir.join("copy.yml")), server);
        // written elsewhere, it is the file it meant, absolute
        let elsewhere = root.path().join("target/wizard/lab.yml");
        assert_eq!(
            rebase(server, &from, &elsewhere),
            root_dir.join("target/deploy/release/shoal-node")
        );
        // and an absolute path is left alone
        assert_eq!(
            rebase(Path::new("/opt/shoal-node"), &from, &elsewhere),
            Path::new("/opt/shoal-node")
        );
    }
}
