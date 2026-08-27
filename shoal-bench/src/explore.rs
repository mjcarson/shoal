//! `shoal-bench explore` - the benchmark explorer
//!
//! # What it answers that the pages cannot
//!
//! Every chart under `docs/src/performance/` is drawn from one capture. Twenty-seven captures are
//! committed, and nothing in this tool plots a metric across them, so whether a number is improving
//! or regressing has to be inferred from `compare` output two labels at a time. The explorer draws
//! any number of captures on one chart, against either a swept fact or the capture timeline.
//!
//! # The two halves
//!
//! This module reads the corpus and projects it; `shoal-top` draws the projection. The split is not
//! tidiness - `shoal-bench` links `walkdir`, which does not compile for `wasm32-unknown-unknown`,
//! so the drawing half could not live here even if it wanted to.
//!
//! # Exit codes
//!
//! 0, 1 and 2, and never 3. Three means a regression, and the explorer reaches no verdict: it draws
//! numbers and says what would make them misleading. A viewer that returned a regression code would
//! be claiming an authority it does not have.

pub mod build;
pub mod index;
pub mod serve;

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::{Context, Result};

use crate::cli::{ExploreArgs, RenderArgs};
use crate::render;
use crate::store::Store;

/// Where the index and the bundle are written unless the caller says otherwise
///
/// Under `target/` and never under `docs/`. A committed index would flip `Page::dirty` on every
/// rebuild, which would make `shoal-bench render --check` fail permanently for a reason that has
/// nothing to do with the pages.
const DEFAULT_OUT: &str = "target/explore";

/// Reads the corpus and projects it into the index the explorer draws
///
/// Separate from [`run_explore`] so that a test can project the real corpus without opening
/// anything, and because this is the half `render` would consume if it were ever reimplemented on
/// top of the explorer.
///
/// # Arguments
///
/// * `store` - The artifact tree to read
pub fn project(store: &Store) -> Result<shoal_top::index::Index> {
    // the same gather the pages are built from, so the two can never disagree about the corpus
    let page = render::gather(store, &RenderArgs::for_index())?;
    // what each capture recorded about the machine it ran on, which is what decides comparability
    let mut metas = BTreeMap::new();
    for label in store.labels()? {
        if let Some(meta) = store.read_meta(&label)? {
            metas.insert(label, meta);
        }
    }
    Ok(index::build(&page, &metas))
}

/// Opens the explorer, or serves it, or just writes the index it draws
///
/// # Arguments
///
/// * `store` - The artifact tree to read
/// * `args` - What the caller asked for
pub fn run_explore(store: &Store, args: &ExploreArgs) -> Result<i32> {
    let index = project(store)?;
    // say what was found, because an explorer that opens on an empty chart should have said why
    let measured = index
        .captures
        .iter()
        .filter(|capture| {
            capture
                .layers
                .contains(&shoal_top::index::Layer::Macro)
        })
        .count();
    println!(
        "{} captures, {} of them with a macro layer, {} workloads, {} measurements",
        index.captures.len(),
        measured,
        index.workloads.len(),
        index.macro_points.len()
    );
    // where everything this command writes goes
    let out = args
        .out
        .clone()
        .unwrap_or_else(|| store.root().join(DEFAULT_OUT));
    std::fs::create_dir_all(&out)
        .with_context(|| format!("creating {}", out.display()))?;
    let index_path = out.join("index.json");
    crate::store::write_json(&index_path, &index)?;
    println!("wrote {}", index_path.display());
    // just the index, which is what the tests and a quick look want
    if args.index_only {
        return Ok(0);
    }
    if args.serve {
        return serve::run(store, args, &out);
    }
    open_window(&index_path)
}

/// Opens the native window, or explains why this machine cannot
///
/// # Arguments
///
/// * `index_path` - The index that was just written, which the message names
fn open_window(index_path: &PathBuf) -> Result<i32> {
    // a native window needs a display, and a benchmark machine reached over SSH has none. winit
    // would panic several frames in; saying so here gives somebody the flag that does work
    let headless = std::env::var_os("DISPLAY").is_none()
        && std::env::var_os("WAYLAND_DISPLAY").is_none();
    if headless {
        eprintln!(
            "This machine has no display, so there is no window to open.\n\n\
             Serve it to a browser instead:\n\
             \x20   shoal-bench explore --serve\n\n\
             Then forward the port and open it:\n\
             \x20   ssh -L {port}:127.0.0.1:{port} <this machine>\n\
             \x20   http://127.0.0.1:{port}\n\n\
             Or open the index that was just written on a machine that has a screen:\n\
             \x20   shoal-top {index}",
            port = crate::cli::DEFAULT_PORT,
            index = index_path.display(),
        );
        // a request this machine cannot satisfy, rather than a failure of the request
        return Ok(2);
    }
    // The window itself lives in `shoal-top`, which this crate deliberately does not link the
    // graphics half of. Running it as its own program is what keeps eframe out of the runner.
    eprintln!(
        "Open the window with:\n\x20   cargo run -p shoal-top --features native -- {}",
        index_path.display()
    );
    Ok(0)
}
