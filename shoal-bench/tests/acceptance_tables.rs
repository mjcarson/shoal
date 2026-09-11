//! The acceptance tables of the distributed chapter stay structurally consistent
//!
//! [C11](../../docs/src/distributed/testing.md) makes C1-C10 and C13 the source of truth for
//! the named acceptance tests and promises "a docs check that parses those tables and verifies
//! unique names, a valid milestone and a matching gate on the milestone page". This is that
//! check, plus the invariant C11 adds for a delivered milestone: every test it names has an
//! executable entry point. Line-oriented parsing, the way `css_sync.rs` reads the stylesheet.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// The repository root
fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("shoal-bench is a workspace member, so it has a parent")
        .to_path_buf()
}

/// One row of an acceptance table
#[derive(Debug)]
struct Row {
    /// The test's name
    name: String,
    /// The page it is on, by file name
    page: String,
    /// The `C` number of that page
    chapter: String,
    /// The milestone it gates
    milestone: String,
}

/// The milestones a row may name
const MILESTONES: &[&str] = &[
    "M0", "M1", "M2", "M3", "M4", "M5", "M6", "M7", "M8", "M9", "M9a", "M9b", "M9c", "M10",
];

/// Every row of every acceptance table under the distributed chapter
fn rows() -> Vec<Row> {
    let dir = repo().join("docs/src/distributed");
    let mut rows = Vec::new();
    let mut pages: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("the distributed chapter exists")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "md"))
        .collect();
    pages.sort();
    for path in pages {
        let text = std::fs::read_to_string(&path).expect("a page reads");
        let page = path.file_name().unwrap().to_string_lossy().to_string();
        // the chapter number is the page's title: `# C11. ...`
        let chapter = text
            .lines()
            .find_map(|line| line.strip_prefix("# "))
            .and_then(|title| title.split('.').next())
            .map(str::trim)
            .unwrap_or_default()
            .to_string();
        // a row is inside the table that follows the `## Acceptance tests` heading
        let mut in_table = false;
        for line in text.lines() {
            if line.starts_with("## ") {
                in_table = line.trim_start_matches("## ").starts_with("Acceptance tests");
                continue;
            }
            if !in_table || !line.starts_with('|') {
                continue;
            }
            let cells: Vec<&str> = line.trim_matches('|').split('|').map(str::trim).collect();
            if cells.len() != 3 || cells[0] == "Test" || cells[0].starts_with("---") {
                continue;
            }
            let name = cells[0].trim_matches('`').to_string();
            rows.push(Row {
                name,
                page: page.clone(),
                chapter: chapter.clone(),
                milestone: cells[2].to_string(),
            });
        }
    }
    rows
}

/// The milestone page, split into its `###` sections by milestone id
fn milestone_sections() -> BTreeMap<String, String> {
    let text = std::fs::read_to_string(repo().join("docs/src/distributed/milestones.md"))
        .expect("the milestones page reads");
    let mut sections = BTreeMap::new();
    let mut current: Option<(String, String)> = None;
    for line in text.lines() {
        if let Some(heading) = line.strip_prefix("### ") {
            if let Some((id, body)) = current.take() {
                sections.insert(id, body);
            }
            // `### M9a. Safe replica migration` names M9a; `### Before M0: ...` names nothing
            let id = heading.split(['.', ':', ' ']).next().unwrap_or_default().to_string();
            current = MILESTONES.contains(&id.as_str()).then(|| (id, String::new()));
            continue;
        }
        if let Some((_, body)) = &mut current {
            body.push_str(line);
            body.push('\n');
        }
    }
    if let Some((id, body)) = current {
        sections.insert(id, body);
    }
    sections
}

/// Every `fn` name defined in a Rust source file anywhere in the workspace
fn defined_functions() -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for entry in walkdir::WalkDir::new(repo())
        .into_iter()
        .filter_entry(|entry| {
            let name = entry.file_name().to_string_lossy();
            name != "target" && name != ".git" && name != "node_modules"
        })
        .filter_map(Result::ok)
    {
        if entry.path().extension().is_none_or(|ext| ext != "rs") {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(entry.path()) else {
            continue;
        };
        for line in text.lines() {
            // `fn name(` or `async fn name<`, at any indentation
            let trimmed = line.trim_start();
            let Some(rest) = trimmed
                .strip_prefix("pub fn ")
                .or_else(|| trimmed.strip_prefix("pub async fn "))
                .or_else(|| trimmed.strip_prefix("async fn "))
                .or_else(|| trimmed.strip_prefix("fn "))
            else {
                continue;
            };
            let name: String = rest
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if !name.is_empty() {
                names.insert(name);
            }
        }
    }
    names
}

/// Owning page tables and milestone gate references stay structurally consistent
///
/// Every named test is unique across the chapter, names a milestone that exists on the
/// milestones page, and that milestone's section names the chapter the test comes from. For a
/// milestone marked delivered, every test it gates is a function somewhere in the workspace:
/// "every named acceptance test has one milestone and an executable entry point when
/// implemented" (C11's invariants).
#[test]
fn acceptance_tables_have_unique_tests_and_valid_milestones() {
    let rows = rows();
    assert!(rows.len() > 50, "only {} acceptance rows were found", rows.len());
    // unique across pages
    let mut seen: BTreeMap<&str, &str> = BTreeMap::new();
    for row in &rows {
        if let Some(other) = seen.insert(&row.name, &row.page) {
            panic!("`{}` is named on both {} and {}", row.name, other, row.page);
        }
        assert!(
            row.name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
            "`{}` on {} is not a test name",
            row.name,
            row.page
        );
    }
    // a valid milestone, with a section on the milestones page that names the owning chapter
    let sections = milestone_sections();
    for milestone in MILESTONES {
        assert!(sections.contains_key(*milestone), "milestones.md has no `### {milestone}.` section");
    }
    for row in &rows {
        assert!(
            MILESTONES.contains(&row.milestone.as_str()),
            "`{}` on {} names milestone {:?}",
            row.name,
            row.page,
            row.milestone
        );
        let section = &sections[&row.milestone];
        // M9's substages carry M9's gates; the umbrella section defers to them
        let gate = if row.milestone == "M9" { "M9a" } else { row.milestone.as_str() };
        let gated = sections[gate].contains(&row.chapter) || section.contains(&row.chapter);
        assert!(
            gated,
            "`{}` gates {} but the {} section on milestones.md never names {}",
            row.name,
            row.milestone,
            row.milestone,
            row.chapter
        );
    }
    // a delivered milestone's tests exist
    let delivered: Vec<&str> = sections
        .iter()
        .filter(|(_, body)| body.trim_start().starts_with("**Delivered"))
        .map(|(id, _)| id.as_str())
        .collect();
    assert!(delivered.contains(&"M0"), "M0 is delivered and its section should say so first");
    let functions = defined_functions();
    for row in rows.iter().filter(|row| delivered.contains(&row.milestone.as_str())) {
        assert!(
            functions.contains(&row.name),
            "`{}` gates delivered milestone {} but no `fn {}` exists in the workspace",
            row.name,
            row.milestone,
            row.name
        );
    }
    // and no undelivered milestone claims a test that already exists under its name by accident
    // is not checked: a test may land before its milestone is complete
}
