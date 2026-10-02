//! `shoaladm bench run` with no workload named: choosing a run in a full screen form
//!
//! A run is a matrix - workloads by bundle sizes by events by runs - and each axis has names
//! that mean nothing until they are explained. The wizard walks it a page at a time, says what
//! every choice measures and what it needs, counts the arms and the least time they take, puts
//! every refusal the run would make on the page that fixes it, and starts the run or saves it as
//! a spec file that runs again without the wizard
//! ([F67](../../../docs/src/features/bench-run-wizard.md)).
//!
//! - [`form`] holds the draft and decides what every key does, and draws nothing;
//! - [`view`] draws it.

pub mod form;
pub mod view;

use color_eyre::eyre::WrapErr;
use crossterm::event::{Event, EventStream, KeyEventKind};
use futures::StreamExt;
use shoal_loadgen::spec::BenchSpec;
use std::path::Path;

use super::args::BenchRunArgs;
use form::{Outcome, Severity, Wizard};

/// The note every saved spec opens with
const HEADER: &str = "\
# Written by the `shoaladm bench run` wizard. Run it again with:
#
#   shoaladm bench run --spec <this file>
#
# Every field a flag names can be given on that line too, and wins over this file.
";

/// What the operator chose
#[derive(Debug)]
pub enum Choice {
    /// Run this spec under these flags
    Run(Box<BenchSpec>, Box<BenchRunArgs>),
    /// Leave without running
    Quit,
}

/// Write a spec as the file the wizard saves
///
/// The dataset is written as an absolute path, since a spec file's dataset is read relative to
/// the file and the wizard's is relative to where it was run.
///
/// # Arguments
///
/// * `spec` - The spec
///
/// # Errors
///
/// When the spec cannot be written as YAML.
pub fn document(spec: &BenchSpec) -> color_eyre::Result<String> {
    // the dataset where it is, from anywhere
    let mut spec = spec.clone();
    if spec.dataset.is_relative() && !spec.dataset.as_os_str().is_empty() {
        spec.dataset = std::path::absolute(&spec.dataset).unwrap_or(spec.dataset);
    }
    // the note, then the spec
    Ok(format!("{HEADER}{}", serde_yaml::to_string(&spec)?))
}

/// Write the spec next to where it goes and move it into place
///
/// # Arguments
///
/// * `path` - Where the spec goes
/// * `spec` - The spec
///
/// # Errors
///
/// When the file cannot be written or moved.
pub fn save(path: &Path, spec: &BenchSpec) -> color_eyre::Result<()> {
    // a partial file first, so an interrupted write never leaves half a spec
    let body = document(spec)?;
    let partial = path.with_extension("yml.partial");
    if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent).wrap_err_with(|| format!("failed to create {}", parent.display()))?;
    }
    std::fs::write(&partial, body).wrap_err_with(|| format!("failed to write {}", partial.display()))?;
    std::fs::rename(&partial, path).wrap_err_with(|| format!("failed to move {} into place", path.display()))?;
    Ok(())
}

/// Run the wizard until the operator starts the run or leaves
///
/// # Arguments
///
/// * `wizard` - The wizard, on the spec the flags left
///
/// # Errors
///
/// When the terminal fails.
pub async fn run(mut wizard: Wizard) -> color_eyre::Result<Choice> {
    // the terminal, restored whatever the loop comes to
    let mut terminal = ratatui::init();
    let result = event_loop(&mut terminal, &mut wizard).await;
    ratatui::restore();
    result
}

/// Draw and handle keys until the operator starts the run or leaves
///
/// # Arguments
///
/// * `terminal` - The terminal
/// * `wizard` - The wizard
async fn event_loop(terminal: &mut ratatui::DefaultTerminal, wizard: &mut Wizard) -> color_eyre::Result<Choice> {
    // only the terminal's keys move it, so nothing races them
    let mut events = EventStream::new();
    loop {
        terminal.draw(|frame| view::render(frame, wizard))?;
        let Some(event) = events.next().await else {
            return Ok(Choice::Quit);
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
            Outcome::Quit => return Ok(Choice::Quit),
            Outcome::Run => {
                // judged again, since a run is only ever of a draft with no error
                let (spec, _) = wizard.build();
                return Ok(Choice::Run(Box::new(spec), Box::new(wizard.args())));
            }
            Outcome::Save => {
                let (spec, _) = wizard.build();
                let path = wizard.draft.save_path.trim().to_string();
                wizard.message = Some(match save(Path::new(&path), &spec) {
                    Ok(()) => (Severity::Warning, format!("saved; run it again with --spec {path}")),
                    Err(error) => (Severity::Error, format!("{error:#}")),
                });
                // a later save asks again before replacing what this one wrote
                wizard.overwrite = false;
            }
        }
    }
}
