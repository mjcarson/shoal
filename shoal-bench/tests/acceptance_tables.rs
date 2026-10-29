//! The acceptance tables of the distributed chapter and of the object storage part stay
//! structurally consistent
//!
//! [C11](../../docs/src/distributed/testing.md) makes C1-C10 and C13 the source of truth for
//! the named acceptance tests and promises "a docs check that parses those tables and verifies
//! unique names, a valid milestone and a matching gate on the milestone page". This is that
//! check, plus the invariant C11 adds for a delivered milestone: every test it names has an
//! executable entry point. Line-oriented parsing, the way `css_sync.rs` reads the stylesheet.
//!
//! Since M11 the object storage part's tables are held to the same rules
//! ([S16](../../docs/src/object-storage/testing.md#the-acceptance-index)), against its own
//! milestones page and M11 to M21, and a test is named once across both parts, since a name is
//! looked up as a function in the whole workspace.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// The repository root
fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("shoal-bench is a workspace member, so it has a parent")
        .to_path_buf()
}

/// A part of the book whose pages carry acceptance tables, each row naming a milestone
struct Chapter {
    /// Its directory under `docs/src`, which holds its pages and its `milestones.md`
    dir: &'static str,
    /// The milestones a row may name
    milestones: &'static [&'static str],
    /// Umbrella milestones whose gates are written in one of their substages
    umbrellas: &'static [(&'static str, &'static str)],
}

/// Distributed Shoal, C1 to C13, gated by M0 to M10c
const DISTRIBUTED: Chapter = Chapter {
    dir: "distributed",
    milestones: &[
        "M0", "M1", "M2", "M3", "M4", "M5", "M6", "M7", "M8", "M9", "M9a", "M9b", "M9c", "M10",
        "M10a", "M10b", "M10c",
    ],
    umbrellas: &[("M9", "M9a"), ("M10", "M10a")],
};

/// Object storage, S1 to S18, gated by M11 to M21
const OBJECT_STORAGE: Chapter = Chapter {
    dir: "object-storage",
    milestones: &[
        "M11", "M12", "M13", "M14", "M15", "M16", "M17", "M18", "M19", "M20", "M21",
    ],
    umbrellas: &[],
};

/// One row of an acceptance table
#[derive(Debug)]
struct Row {
    /// The test's name
    name: String,
    /// The page it is on, by its path under `docs/src`
    page: String,
    /// The number in that page's title: `C11`, `S16`
    chapter: String,
    /// The milestone it gates
    milestone: String,
}

/// A cell with every struck-through span removed, as a renamed test's old name is kept
///
/// # Arguments
///
/// * `cell` - The cell's text
fn unstruck(cell: &str) -> String {
    let mut kept = String::new();
    let mut rest = cell;
    // drop everything between each pair of `~~`
    while let Some(start) = rest.find("~~") {
        kept.push_str(&rest[..start]);
        match rest[start + 2..].find("~~") {
            Some(end) => rest = &rest[start + 2 + end + 2..],
            None => {
                rest = "";
            }
        }
    }
    kept.push_str(rest);
    kept.trim().to_string()
}

/// Whether some text names a page's number as a whole: `S1` is not named by `S16`
///
/// # Arguments
///
/// * `text` - The text to look in
/// * `number` - The number, such as `C1` or `S16`
fn names(text: &str, number: &str) -> bool {
    text.match_indices(number).any(|(at, _)| {
        // nothing of a longer word before it, and no further digit after it
        let before = text[..at].chars().next_back();
        let after = text[at + number.len()..].chars().next();
        !before.is_some_and(|c| c.is_alphanumeric() || c == '_')
            && !after.is_some_and(|c| c.is_ascii_digit())
    })
}

/// Every row of every acceptance table in a chapter
///
/// # Arguments
///
/// * `chapter` - The chapter to read
fn rows(chapter: &Chapter) -> Vec<Row> {
    let dir = repo().join("docs/src").join(chapter.dir);
    let mut rows = Vec::new();
    let mut pages: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("the chapter exists")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "md"))
        .collect();
    pages.sort();
    for path in pages {
        let text = std::fs::read_to_string(&path).expect("a page reads");
        let page = format!(
            "{}/{}",
            chapter.dir,
            path.file_name().unwrap().to_string_lossy()
        );
        // the chapter number is the page's title: `# C11. ...`
        let number = text
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
                in_table = line
                    .trim_start_matches("## ")
                    .starts_with("Acceptance tests");
                continue;
            }
            if !in_table || !line.starts_with('|') {
                continue;
            }
            let cells: Vec<&str> = line.trim_matches('|').split('|').map(str::trim).collect();
            if cells.len() != 3 || cells[0] == "Test" || cells[0].starts_with("---") {
                continue;
            }
            // a renamed test keeps its old name struck through beside the new one
            let name = unstruck(cells[0]).trim_matches('`').to_string();
            rows.push(Row {
                name,
                page: page.clone(),
                chapter: number.clone(),
                milestone: unstruck(cells[2]),
            });
        }
    }
    rows
}

/// A chapter's milestones page, split into its `###` sections by milestone id
///
/// # Arguments
///
/// * `chapter` - The chapter whose `milestones.md` to read
fn milestone_sections(chapter: &Chapter) -> BTreeMap<String, String> {
    let path = repo()
        .join("docs/src")
        .join(chapter.dir)
        .join("milestones.md");
    let text = std::fs::read_to_string(path).expect("the milestones page reads");
    let mut sections = BTreeMap::new();
    let mut current: Option<(String, String)> = None;
    for line in text.lines() {
        if let Some(heading) = line.strip_prefix("### ") {
            if let Some((id, body)) = current.take() {
                sections.insert(id, body);
            }
            // `### M9a. Safe replica migration` names M9a; `### Before M0: ...` names nothing
            let id = heading
                .split(['.', ':', ' '])
                .next()
                .unwrap_or_default()
                .to_string();
            current = chapter
                .milestones
                .contains(&id.as_str())
                .then(|| (id, String::new()));
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

/// Every name appears once among some rows, and is a test's name
///
/// # Arguments
///
/// * `rows` - The rows, from one chapter or several
fn assert_unique(rows: &[&Row]) {
    let mut seen: BTreeMap<&str, &str> = BTreeMap::new();
    for row in rows {
        if let Some(other) = seen.insert(&row.name, &row.page) {
            panic!("`{}` is named on both {} and {}", row.name, other, row.page);
        }
        assert!(
            row.name
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
            "`{}` on {} is not a test name",
            row.name,
            row.page
        );
    }
}

/// Hold a chapter's rows to its milestones page, and return the milestones it marks delivered
///
/// Every row names a milestone that has a section, that section (or the substage that carries
/// an umbrella's gates) names the row's page, and every test of a delivered milestone - one whose
/// section starts `**Delivered` - is a function somewhere in the workspace.
///
/// # Arguments
///
/// * `chapter` - The chapter
/// * `rows` - Its rows
fn assert_gated(chapter: &Chapter, rows: &[Row]) -> Vec<String> {
    let sections = milestone_sections(chapter);
    // a section for every milestone
    for milestone in chapter.milestones {
        assert!(
            sections.contains_key(*milestone),
            "{}/milestones.md has no `### {milestone}.` section",
            chapter.dir
        );
    }
    for row in rows {
        // a milestone of this chapter
        assert!(
            chapter.milestones.contains(&row.milestone.as_str()),
            "`{}` on {} names milestone {:?}",
            row.name,
            row.page,
            row.milestone
        );
        let section = &sections[&row.milestone];
        // an umbrella's substage carries its gates
        let gate = chapter
            .umbrellas
            .iter()
            .find(|(umbrella, _)| *umbrella == row.milestone)
            .map_or(row.milestone.as_str(), |(_, gate)| gate);
        let gated = names(&sections[gate], &row.chapter) || names(section, &row.chapter);
        assert!(
            gated,
            "`{}` gates {} but the {} section on {}/milestones.md never names {}",
            row.name, row.milestone, row.milestone, chapter.dir, row.chapter
        );
    }
    // a delivered milestone's tests exist
    let delivered: Vec<String> = sections
        .iter()
        .filter(|(_, body)| body.trim_start().starts_with("**Delivered"))
        .map(|(id, _)| id.clone())
        .collect();
    let functions = defined_functions();
    for row in rows.iter().filter(|row| delivered.contains(&row.milestone)) {
        assert!(
            functions.contains(&row.name),
            "`{}` gates delivered milestone {} but no `fn {}` exists in the workspace",
            row.name,
            row.milestone,
            row.name
        );
    }
    // a test may land before its milestone is complete, so an undelivered one is not checked
    delivered
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
    let rows = rows(&DISTRIBUTED);
    assert!(
        rows.len() > 50,
        "only {} acceptance rows were found",
        rows.len()
    );
    // unique across pages
    assert_unique(&rows.iter().collect::<Vec<_>>());
    // gated, and every delivered milestone's tests exist
    let delivered = assert_gated(&DISTRIBUTED, &rows);
    assert!(
        delivered.iter().any(|id| id == "M0"),
        "M0 is delivered and its section should say so first"
    );
}

/// Every row of every table in the object storage part names an existing function once its
/// gate is delivered, and a gate on the milestones page (S16's row)
///
/// The part's tables are held to the distributed chapter's rules: a test is named once across
/// both parts, a row names one of M11 to M21 and that gate's section names the row's page, and
/// every test a delivered gate names is a function in the workspace.
#[test]
fn object_acceptance_tables_name_real_tests_and_valid_gates() {
    let rows = rows(&OBJECT_STORAGE);
    assert!(
        rows.len() > 100,
        "only {} object acceptance rows were found",
        rows.len()
    );
    // unique across both parts, since a name is looked up in the whole workspace
    let distributed = self::rows(&DISTRIBUTED);
    assert_unique(&rows.iter().chain(&distributed).collect::<Vec<_>>());
    // gated, and every delivered gate's tests exist
    let delivered = assert_gated(&OBJECT_STORAGE, &rows);
    assert!(
        delivered.iter().any(|id| id == "M11"),
        "M11 is delivered and its section should say so first"
    );
}

/// The two helpers the tables lean on: a struck name is dropped, and a number is named whole
#[test]
fn acceptance_cells_drop_struck_names_and_match_whole_numbers() {
    // a renamed test keeps its old name struck through
    assert_eq!(
        unstruck("~~`old_name`~~ `new_name`"),
        "`new_name`".to_string()
    );
    assert_eq!(unstruck("`plain`"), "`plain`".to_string());
    assert_eq!(unstruck("a ~~b~~ c ~~d~~ e"), "a  c  e".to_string());
    // a page number is not named by a longer one
    assert!(names("S16's schedule rows", "S16"));
    assert!(!names("S16's schedule rows", "S1"));
    assert!(names("[C1](node-identity.md), C10", "C1"));
    assert!(!names("C10 and C11", "C1"));
    assert!(!names("XS2", "S2"));
}
