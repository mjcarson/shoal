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
//!
//! Since M22 the scans plan's tables are too ([F122](../../docs/src/features/scan-oracle.md)), against
//! `future-work/scans/milestones.md` and M22 to M28, a name once across all three parts. Its tables
//! put the gate second (`| Test | Gate | Proves |`) where the others put the milestone third, so a
//! table's milestone column is read from its header.
//!
//! A row whose test is a run on the lab rather than a function says so in its *Proves* cell with
//! `**Lab exit**` and links the page that records the run; once its milestone is delivered it is
//! held to that record existing, never to a function of its name
//! ([Resolved #360](../../docs/src/appendix/resolved/lab-exit-rows.md)).

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

/// Scans, W1 to W42 of the future work's scans plan, gated by M22 to M28
const SCANS: Chapter = Chapter {
    dir: "future-work/scans",
    milestones: &["M22", "M23", "M24", "M25", "M26", "M27", "M28"],
    umbrellas: &[],
};

/// The column a table names its milestone in, read from its header row
///
/// `Milestone` in the distributed and object storage parts, `Gate` in the scans plan's; a table
/// whose header names neither is read as the parts before the scans plan wrote theirs, third.
///
/// # Arguments
///
/// * `header` - The header row's cells
fn milestone_column(header: &[&str]) -> usize {
    // the first cell that names a gate
    header
        .iter()
        .position(|cell| *cell == "Milestone" || *cell == "Gate")
        .unwrap_or(2)
}

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
    /// The record a lab exit's row links, when the row is a lab exit rather than a function
    exit: Option<String>,
}

/// What a row's *Proves* cell says to mark it a run on the lab rather than a function
const EXIT_MARK: &str = "**Lab exit**";

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

/// The record a lab exit's row links, or none for a row that is not an exit
///
/// An exit that links nothing is `Some("")`, which the check refuses by name.
///
/// # Arguments
///
/// * `cell` - The row's *Proves* cell
fn exit_record(cell: &str) -> Option<String> {
    let cell = unstruck(cell);
    // only a cell that marks its row an exit
    let marked = cell.find(EXIT_MARK)?;
    let after = &cell[marked + EXIT_MARK.len()..];
    // the first link after the mark, its target up to its closing parenthesis
    let target = after
        .find("](")
        .and_then(|start| {
            let rest = &after[start + 2..];
            rest.find(')').map(|end| rest[..end].trim().to_string())
        })
        .unwrap_or_default();
    Some(target)
}

/// Whether a lab exit's row links a record that exists, beside the page the row is on
///
/// # Arguments
///
/// * `row` - The exit's row
/// * `record` - The link its *Proves* cell names
/// * `docs` - The book's `docs/src` directory
///
/// # Errors
///
/// Says why when the row links nothing, links only an anchor of its own page, or links a page
/// that is not there.
fn exit_holds(row: &Row, record: &str, docs: &Path) -> Result<(), String> {
    // the page a link names, without its anchor
    let page = record.split('#').next().unwrap_or_default();
    if page.is_empty() {
        return Err(format!(
            "`{}` on {} is a lab exit but links no page that records its run",
            row.name, row.page
        ));
    }
    // a link is relative to the page the row is on
    let dir = Path::new(&row.page).parent().unwrap_or_else(|| Path::new(""));
    let path = docs.join(dir).join(page);
    if path.is_file() {
        Ok(())
    } else {
        Err(format!(
            "`{}` on {} is a lab exit whose record {} is not there",
            row.name,
            row.page,
            path.display()
        ))
    }
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
        // and its milestone in the column its table's header names
        let mut gate = 2;
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
            if cells.len() != 3 || cells[0].starts_with("---") {
                continue;
            }
            if cells[0] == "Test" {
                gate = milestone_column(&cells);
                continue;
            }
            // a renamed test keeps its old name struck through beside the new one
            let name = unstruck(cells[0]).trim_matches('`').to_string();
            // what the row proves is whichever of the last two columns is not its gate
            let proves = if gate == 1 { 2 } else { 1 };
            rows.push(Row {
                name,
                page: page.clone(),
                chapter: number.clone(),
                milestone: unstruck(cells[gate]),
                exit: exit_record(cells[proves]),
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
/// section starts `**Delivered` - is a function somewhere in the workspace, or, for a lab exit,
/// a page that records its run ([Resolved #360](../../docs/src/appendix/resolved/lab-exit-rows.md)).
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
    let docs = repo().join("docs/src");
    for row in rows.iter().filter(|row| delivered.contains(&row.milestone)) {
        match &row.exit {
            // a lab exit is held to the record of its run
            Some(record) => {
                if let Err(why) = exit_holds(row, record, &docs) {
                    panic!("{why}, and it gates delivered milestone {}", row.milestone);
                }
            }
            // anything else to a function of its name
            None => assert!(
                functions.contains(&row.name),
                "`{}` gates delivered milestone {} but no `fn {}` exists in the workspace",
                row.name,
                row.milestone,
                row.name
            ),
        }
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

/// Every row of every table in the scans plan names an existing function once its gate is
/// delivered, and a gate on its milestones page (M22's row, F122)
///
/// The plan's tables are held to the other two parts' rules: a test is named once across all
/// three, a row names one of M22 to M28 and that gate's section names the row's page, and every
/// test a delivered gate names is a function in the workspace. A table's gate is its second column.
#[test]
fn scan_acceptance_tables_name_real_tests_and_valid_gates() {
    let rows = rows(&SCANS);
    assert!(
        rows.len() > 50,
        "only {} scan acceptance rows were found",
        rows.len()
    );
    // the gate read from the second column, never the third, which is what a row proves
    assert!(
        rows.iter().all(|row| row.milestone.starts_with('M')),
        "{:?}",
        rows.iter().find(|row| !row.milestone.starts_with('M'))
    );
    // unique across all three parts, since a name is looked up in the whole workspace
    let distributed = self::rows(&DISTRIBUTED);
    let objects = self::rows(&OBJECT_STORAGE);
    assert_unique(
        &rows
            .iter()
            .chain(&distributed)
            .chain(&objects)
            .collect::<Vec<_>>(),
    );
    // gated, and every delivered gate's tests exist
    let delivered = assert_gated(&SCANS, &rows);
    assert!(
        delivered.iter().any(|id| id == "M22"),
        "M22 is delivered and its section should say so first"
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
    // a table's milestone is whichever column its header says
    assert_eq!(milestone_column(&["Test", "Asserts", "Milestone"]), 2);
    assert_eq!(milestone_column(&["Test", "Gate", "Proves"]), 1);
    assert_eq!(milestone_column(&["Test", "Proves", "Where"]), 2);
}

/// A lab exit's row is held to the page that records its run, and only to that
/// ([Resolved #360](../../docs/src/appendix/resolved/lab-exit-rows.md))
#[test]
fn a_lab_exit_row_is_held_to_its_record() {
    // a marked cell names the first link after its mark; an unmarked one is no exit
    assert_eq!(
        exit_record("The run. **Lab exit**, recorded in [the exit](m23-exit.md#the-rounds-again)"),
        Some("m23-exit.md#the-rounds-again".to_string())
    );
    assert_eq!(exit_record("An exit, not a unit test"), None);
    assert_eq!(exit_record("**Lab exit**, recorded nowhere"), Some(String::new()));
    // a struck mark is no mark
    assert_eq!(exit_record("~~**Lab exit**~~ [old](gone.md)"), None);
    let row = |exit: &str| Row {
        name: "a_run_on_the_lab".to_string(),
        page: "future-work/scans/isolation.md".to_string(),
        chapter: "W7".to_string(),
        milestone: "M23".to_string(),
        exit: Some(exit.to_string()),
    };
    let docs = repo().join("docs/src");
    // a record beside the row's page holds
    assert_eq!(exit_holds(&row("m23-exit.md#x"), "m23-exit.md#x", &docs), Ok(()));
    // nothing linked, an anchor of its own page, or a page that is not there, each fails
    for record in ["", "#the-budget", "no-such-exit.md"] {
        let refused = exit_holds(&row(record), record, &docs);
        assert!(refused.is_err(), "{record:?} held: {refused:?}");
    }
}
