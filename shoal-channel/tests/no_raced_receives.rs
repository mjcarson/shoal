//! No bare kanal receive is raced anywhere in the workspace
//!
//! A kanal receive dropped after a sender handed it a value drops the value, so a `select!`, a
//! `timeout` or a `race` around one loses a message whenever the other side wins after the
//! hand-off ([Resolved #152](../../docs/src/appendix/resolved/kanal-receive-races.md)). This
//! reads every Rust source file in the workspace and fails on either shape of it:
//!
//! - a race (`select!`, `select_biased!`, `future::select`, `race`, `timeout`, `timeout_at`)
//!   whose body names `.recv()`;
//! - a `.recv()` future that is kept rather than awaited where it is made (pinned, fused, bound
//!   to a name, passed as an argument, or made a `select!` arm), since that is the only reason
//!   to keep one.
//!
//! Race a `shoal_channel::KeptReceiver` (or `LocalKeptReceiver`) and its `next` instead.

use std::fs;
use std::path::{Path, PathBuf};

/// The macros and calls that can drop one of their futures before it completes
const RACES: [&str; 7] = [
    "select_biased!",
    "select!",
    "future::select(",
    ".race(",
    "race(",
    "timeout_at(",
    "timeout(",
];

/// What may follow a receive that is kept rather than awaited on the spot
const KEPT: [&str; 6] = [")", ";", ",", ".fuse()", "=>", ".boxed"];

/// Every Rust source file under a directory, skipping build output and version control
///
/// # Arguments
///
/// * `dir` - The directory to walk
/// * `found` - Where to put the files
fn sources(dir: &Path, found: &mut Vec<PathBuf>) {
    // an unreadable directory has nothing this test can judge
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        // build output and version control hold no source of ours
        if name == "target" || name == ".git" || name == "node_modules" {
            continue;
        }
        if path.is_dir() {
            sources(&path, found);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            found.push(path);
        }
    }
}

/// The source with every line comment blanked, so prose about a race is not judged as one
///
/// # Arguments
///
/// * `source` - The file's text
fn without_comments(source: &str) -> String {
    source
        .lines()
        .map(|line| match line.find("//") {
            // a comment keeps its line, so the line numbers reported still match the file
            Some(at) => &line[..at],
            None => line,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The text between a bracket and the one that closes it
///
/// # Arguments
///
/// * `source` - The text
/// * `open` - The byte offset of the opening bracket
fn bracketed(source: &str, open: usize) -> &str {
    let bytes = source.as_bytes();
    let (left, right) = match bytes[open] {
        b'(' => (b'(', b')'),
        b'{' => (b'{', b'}'),
        _ => (b'[', b']'),
    };
    // count nesting until the bracket that opened this one closes
    let mut depth = 0usize;
    for (at, byte) in bytes.iter().enumerate().skip(open) {
        if *byte == left {
            depth += 1;
        } else if *byte == right {
            depth -= 1;
            if depth == 0 {
                return &source[open..=at];
            }
        }
    }
    &source[open..]
}

/// Every race over a bare receive in one file, and every receive kept rather than awaited
///
/// # Arguments
///
/// * `source` - The file's text, comments already blanked
fn offences(source: &str) -> Vec<(usize, String)> {
    let mut found = Vec::new();
    let line_of = |at: usize| source[..at].matches('\n').count() + 1;
    // a race whose body names a bare receive
    for race in RACES {
        let mut from = 0;
        while let Some(offset) = source[from..].find(race) {
            let at = from + offset;
            from = at + race.len();
            // a name that only ends in the race's (`my_timeout(`) is some other function
            let before = source[..at].chars().next_back();
            if !race.starts_with('.')
                && before.is_some_and(|c| c.is_alphanumeric() || c == '_')
            {
                continue;
            }
            // the body starts at the race's own bracket, or the first one after a macro's name
            let open = if race.ends_with('(') {
                at + race.len() - 1
            } else {
                match source[from..].find(['{', '(', '[']) {
                    Some(offset) => from + offset,
                    None => continue,
                }
            };
            if bracketed(source, open).contains(".recv()") {
                found.push((line_of(at), format!("`{race}` races a bare `.recv()`")));
            }
        }
    }
    // a receive that is kept rather than awaited where it is made
    let mut from = 0;
    while let Some(offset) = source[from..].find(".recv()") {
        let at = from + offset;
        from = at + ".recv()".len();
        let after = source[from..].trim_start();
        if KEPT.iter().any(|kept| after.starts_with(kept)) {
            found.push((line_of(at), "a `.recv()` future is kept rather than awaited".to_owned()));
        }
    }
    found
}

/// No file in the workspace races a bare kanal receive
#[test]
fn no_bare_kanal_receive_is_raced() {
    // the workspace is this crate's parent
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the crate sits in the workspace")
        .to_path_buf();
    let mut files = Vec::new();
    sources(&root, &mut files);
    assert!(files.len() > 100, "the walk found only {} files", files.len());
    let mut report = Vec::new();
    for file in files {
        // this crate is the one place a bare receive is made, and its tests race one on purpose
        if file.starts_with(root.join("shoal-channel")) {
            continue;
        }
        let Ok(source) = fs::read_to_string(&file) else {
            continue;
        };
        for (line, what) in offences(&without_comments(&source)) {
            let shown = file.strip_prefix(&root).unwrap_or(&file).display().to_string();
            report.push(format!("{shown}:{line}: {what}"));
        }
    }
    assert!(
        report.is_empty(),
        "race a shoal_channel::KeptReceiver's `next` instead:\n{}",
        report.join("\n")
    );
}

/// The scan finds each shape it is meant to, so an empty report means something
#[test]
fn the_scan_finds_each_shape() {
    let shapes = [
        "select! { x = rx.recv() => {} }",
        "tokio::time::timeout(d, rx.recv()).await",
        "glommio::timer::timeout(d, async { Ok(rx.recv().await) }).await",
        "let mut recv = Box::pin(rx.recv()).fuse();",
        "futures::future::select(rx.recv(), timer)",
    ];
    for shape in shapes {
        assert!(!offences(shape).is_empty(), "the scan missed `{shape}`");
    }
    // awaited on the spot, or a kept receiver's wait, is fine
    for fine in [
        "let x = rx.recv().await?;",
        "select! { x = kept.next() => {} }",
        "my_timeout(rx)",
    ] {
        assert!(offences(fine).is_empty(), "the scan flagged `{fine}`");
    }
}
