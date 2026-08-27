//! `shoal-top` - the explorer as a program of its own
//!
//! `shoal-bench explore` is the entry point this was built for, and it hands the window an index it
//! projected in process. This binary reads one off disk instead, which is what makes the explorer
//! usable on a machine that has the artifacts but not the benchmark tool - and is where the live
//! view will grow, once there is a server to attach to rather than a corpus to read.

use std::path::PathBuf;

/// Reads an index off disk and opens the explorer on it
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // one positional argument, the index to open
    let path = match std::env::args().nth(1) {
        Some(given) => PathBuf::from(given),
        None => {
            eprintln!(
                "usage: shoal-top <index.json>\n\n\
                 Build one with `shoal-bench explore --index-only`, which writes it to \
                 target/explore/index.json."
            );
            return Ok(());
        }
    };
    // read and parse it, refusing an index this build does not understand rather than dropping
    // whatever section it has never been told about
    let raw = std::fs::read_to_string(&path)?;
    let index: shoal_top::index::Index = serde_json::from_str(&raw)?;
    index.check_version()?;
    shoal_top::native::run(index)?;
    Ok(())
}
