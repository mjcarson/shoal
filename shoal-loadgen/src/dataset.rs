//! A dataset folder: one file a table, named after the table it holds rows for
//!
//! The folder is judged whole before anything else happens, and every problem with it is
//! reported at once, by name, so an operator fixes a folder once rather than once per file. A
//! file is `<Table>.csv`, `<Table>.json` or `<Table>.jsonl`, where `<Table>` is exactly the name
//! `QuerySupport::table_names` reports - the row struct's name, such as `Movie`.

use serde::{Deserialize, Serialize};
use shoal::shared::dataset::DatasetSupport;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// The formats a dataset file can be in
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum Format {
    /// Comma separated, with a header row naming the fields
    Csv,
    /// One json array of row objects
    Json,
    /// One json row object a line
    Jsonl,
}

impl Format {
    /// The format a file extension names, if it names one
    ///
    /// # Arguments
    ///
    /// * `extension` - The extension, without its dot
    #[must_use]
    pub fn from_extension(extension: &str) -> Option<Self> {
        // extensions are exact, the way table names are
        match extension {
            "csv" => Some(Format::Csv),
            "json" => Some(Format::Json),
            "jsonl" => Some(Format::Jsonl),
            _ => None,
        }
    }

    /// What this format is called, which is also its extension
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        // one spelling for both
        match self {
            Format::Csv => "csv",
            Format::Json => "json",
            Format::Jsonl => "jsonl",
        }
    }
}

/// One table's file in a dataset
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetFile {
    /// The table it holds rows for
    pub table: &'static str,
    /// Where it is
    pub path: PathBuf,
    /// What format it is in
    pub format: Format,
}

/// A dataset folder that was judged loadable
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dataset {
    /// The folder
    pub root: PathBuf,
    /// One file a table, in the order the database declares its tables
    pub files: Vec<DatasetFile>,
}

/// One thing wrong with a dataset folder
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// The folder could not be read
    Unreadable {
        /// The folder
        root: PathBuf,
        /// Why
        error: String,
    },
    /// The folder holds no table file at all
    Empty {
        /// The folder
        root: PathBuf,
    },
    /// A file is named after no table in the database
    UnknownTable {
        /// The file
        file: PathBuf,
        /// The name it gave
        table: String,
        /// The table it probably meant, if one differs from it only in case
        suggestion: Option<&'static str>,
        /// Every table the database has
        known: Vec<&'static str>,
    },
    /// A file names a table that did not opt in to datasets
    NotOptedIn {
        /// The file
        file: PathBuf,
        /// The table
        table: &'static str,
    },
    /// A table has more than one file
    Duplicate {
        /// The table
        table: &'static str,
        /// Every file naming it
        files: Vec<PathBuf>,
    },
}

impl std::fmt::Display for Refusal {
    /// Say what is wrong and what to do about it
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write to
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // each names the file or folder it is about
        match self {
            Refusal::Unreadable { root, error } => {
                write!(f, "{} cannot be read: {error}", root.display())
            }
            Refusal::Empty { root } => write!(
                f,
                "{} holds no <Table>.csv, <Table>.json or <Table>.jsonl file",
                root.display()
            ),
            Refusal::UnknownTable {
                file,
                table,
                suggestion,
                known,
            } => {
                write!(f, "{}: no table is named {table:?}", file.display())?;
                if let Some(suggestion) = suggestion {
                    write!(f, " (did you mean {suggestion:?}? names are exact)")?;
                }
                write!(f, "; this database has {}", known.join(", "))
            }
            Refusal::NotOptedIn { file, table } => write!(
                f,
                "{}: table {table:?} cannot be loaded from a dataset; add `dataset` to its \
                 #[shoal_table(...)] and derive serde::Deserialize on it",
                file.display()
            ),
            Refusal::Duplicate { table, files } => {
                let files: Vec<String> =
                    files.iter().map(|file| file.display().to_string()).collect();
                write!(f, "table {table:?} has more than one file: {}", files.join(", "))
            }
        }
    }
}

/// Every problem a dataset folder has, reported together
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusals(pub Vec<Refusal>);

impl std::fmt::Display for Refusals {
    /// One problem a line
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write to
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // a heading, then each problem
        writeln!(f, "the dataset cannot be used:")?;
        for refusal in &self.0 {
            writeln!(f, "  - {refusal}")?;
        }
        Ok(())
    }
}

impl std::error::Error for Refusals {}

/// Whether a file is one a dataset folder may hold beside its table files
///
/// # Arguments
///
/// * `name` - The file's name
fn ignored(name: &str) -> bool {
    // hidden files, notes, and a bench spec kept with its data
    name.starts_with('.')
        || name.ends_with(".md")
        || name.starts_with("README")
        || name == "bench.yml"
        || name == "bench.yaml"
}

impl Dataset {
    /// Judge a dataset folder against a database's tables
    ///
    /// # Arguments
    ///
    /// * `root` - The folder
    ///
    /// # Errors
    ///
    /// Every problem the folder has, at once.
    pub fn open<S: DatasetSupport>(root: &Path) -> Result<Self, Refusals> {
        // the folder's entries, sorted so every refusal comes out in the same order
        let entries = match std::fs::read_dir(root) {
            Ok(entries) => entries,
            Err(error) => {
                return Err(Refusals(vec![Refusal::Unreadable {
                    root: root.to_path_buf(),
                    error: error.to_string(),
                }]));
            }
        };
        let mut paths: Vec<PathBuf> = entries
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.is_file())
            .collect();
        paths.sort();
        // every table the database has, with whether it opted in
        let tables = S::dataset_tables();
        let known: Vec<&'static str> = tables.iter().map(|(name, _)| *name).collect();
        let mut refusals = Vec::new();
        let mut found: BTreeMap<&'static str, Vec<(PathBuf, Format)>> = BTreeMap::new();
        for path in paths {
            // a file name that is not utf8 cannot name a table
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if ignored(name) {
                continue;
            }
            // a file whose extension is not a format is not a table file
            let Some((stem, extension)) = name.rsplit_once('.') else {
                continue;
            };
            let Some(format) = Format::from_extension(extension) else {
                continue;
            };
            // the stem must be a table's exact name
            match tables.iter().find(|(table, _)| *table == stem) {
                Some((table, true)) => found.entry(table).or_default().push((path, format)),
                Some((table, false)) => refusals.push(Refusal::NotOptedIn { file: path, table }),
                None => {
                    let suggestion = known
                        .iter()
                        .find(|table| table.eq_ignore_ascii_case(stem))
                        .copied();
                    refusals.push(Refusal::UnknownTable {
                        file: path.clone(),
                        table: stem.to_string(),
                        suggestion,
                        known: known.clone(),
                    });
                }
            }
        }
        // a table with two files is ambiguous about which to load
        for (table, files) in &found {
            if files.len() > 1 {
                refusals.push(Refusal::Duplicate {
                    table,
                    files: files.iter().map(|(path, _)| path.clone()).collect(),
                });
            }
        }
        // a folder with nothing to load is refused even if nothing else was wrong
        if found.is_empty() && refusals.is_empty() {
            refusals.push(Refusal::Empty {
                root: root.to_path_buf(),
            });
        }
        if !refusals.is_empty() {
            return Err(Refusals(refusals));
        }
        // the files in the order the database declares its tables
        let files = known
            .iter()
            .filter_map(|table| {
                found.remove(table).map(|mut files| {
                    let (path, format) = files.remove(0);
                    DatasetFile {
                        table,
                        path,
                        format,
                    }
                })
            })
            .collect();
        Ok(Dataset {
            root: root.to_path_buf(),
            files,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{Dataset, Format, Refusal};
    use crate::testing::CatalogClient;

    /// Make a folder holding empty files with these names
    ///
    /// # Arguments
    ///
    /// * `names` - The files to create
    fn folder(names: &[&str]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        for name in names {
            std::fs::write(dir.path().join(name), "").unwrap();
        }
        dir
    }

    /// A folder of table files, notes and a spec is read in the database's table order
    #[test]
    fn a_folder_is_read_in_table_order_ignoring_notes() {
        let dir = folder(&["Review.jsonl", "Item.csv", "README.md", ".hidden", "bench.yml", "x.txt"]);
        let dataset = Dataset::open::<CatalogClient>(dir.path()).unwrap();
        let tables: Vec<(&str, Format)> = dataset
            .files
            .iter()
            .map(|file| (file.table, file.format))
            .collect();
        assert_eq!(tables, vec![("Item", Format::Csv), ("Review", Format::Jsonl)]);
    }

    /// Every problem is reported at once, each by the file it is about
    #[test]
    fn every_problem_is_refused_together_by_name() {
        let dir = folder(&["Audit.csv", "item.csv", "Nope.json", "Review.csv", "Review.json"]);
        let refusals = Dataset::open::<CatalogClient>(dir.path()).unwrap_err().0;
        assert_eq!(refusals.len(), 4, "{refusals:?}");
        // the table that did not opt in
        assert!(refusals.iter().any(|refusal| matches!(
            refusal,
            Refusal::NotOptedIn { table: "Audit", .. }
        )));
        // a name that differs only in case is unknown, with the table it probably meant
        assert!(refusals.iter().any(|refusal| matches!(
            refusal,
            Refusal::UnknownTable { table, suggestion: Some("Item"), .. } if table == "item"
        )));
        // a name no table has
        assert!(refusals.iter().any(|refusal| matches!(
            refusal,
            Refusal::UnknownTable { table, suggestion: None, .. } if table == "Nope"
        )));
        // and a table with two files
        assert!(refusals.iter().any(|refusal| matches!(
            refusal,
            Refusal::Duplicate { table: "Review", files } if files.len() == 2
        )));
        // the text says what to do about the one that did not opt in
        let text = super::Refusals(refusals).to_string();
        assert!(text.contains("serde::Deserialize"), "{text}");
    }

    /// A folder with nothing to load is refused, and so is one that does not exist
    #[test]
    fn an_empty_or_missing_folder_is_refused() {
        let dir = folder(&["README.md"]);
        let refusals = Dataset::open::<CatalogClient>(dir.path()).unwrap_err().0;
        assert!(matches!(refusals[..], [Refusal::Empty { .. }]));
        let missing = dir.path().join("missing");
        let refusals = Dataset::open::<CatalogClient>(&missing).unwrap_err().0;
        assert!(matches!(refusals[..], [Refusal::Unreadable { .. }]));
    }
}
