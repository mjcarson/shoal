//! Spike X16's dataset folder: `Cell.jsonl` and `Big.jsonl` made from a seed, `Keyword.jsonl`
//! from the TMDB csv, and a README saying how
//!
//! ```sh
//! x16-data --out target/lab/x16/dataset --tmdb ~/datasets/TMDB_movie_dataset_v11.csv
//! # M23's rounds again: a cell of more rows alone, beside X16's own Big and Keyword linked in
//! x16-data --out target/lab/m23/dataset --tables cell --cell 1100000
//! ```
//!
//! The bench loads a table from `<Table>.jsonl`, preloading the rows a run asks for and inserting
//! the rest, in file order ([F66](../../../docs/src/features/dataset-benchmarks.md)). A row's bytes
//! are its seed's and id's alone, so a cell of more rows begins with the rows a smaller one holds.

use clap::{Parser, ValueEnum};
use color_eyre::eyre::{eyre, WrapErr};
use shoal_spike_scan::{body, keyword_rows, Big, Cell};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

/// A table of the spike's dataset folder
#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum Table {
    /// The foreground's rows, `Cell.jsonl`
    Cell,
    /// The rows scanned from their archives, `Big.jsonl`
    Big,
    /// The keyword rows of the TMDB csv, `Keyword.jsonl`
    Keyword,
}

/// Write the spike's dataset folder
#[derive(Parser, Debug)]
#[command(about)]
struct Args {
    /// The folder to write, made if it is not there
    #[clap(long)]
    out: PathBuf,
    /// The TMDB csv, `TMDB_movie_dataset_v11.csv`, that the keyword rows are made from; needed only
    /// when the keyword rows are written
    #[clap(long)]
    tmdb: Option<PathBuf>,
    /// The tables to write; a file of a table not named is left as it is
    #[clap(long, value_enum, value_delimiter = ',', default_values_t = [Table::Cell, Table::Big, Table::Keyword])]
    tables: Vec<Table>,
    /// How many rows the foreground's table holds in the file
    #[clap(long, default_value_t = 300_000)]
    cell: u64,
    /// How many rows the table scanned from its archives holds
    #[clap(long, default_value_t = 3_000_000)]
    big: u64,
    /// The bytes of a generated row's body
    #[clap(long, default_value_t = 1000)]
    width: usize,
    /// The seed every body is made from
    #[clap(long, default_value_t = 16)]
    seed: u64,
}

/// Write one table's rows as lines of json, and say how many and how many bytes
///
/// The rows go to a file beside the table's, renamed over it once every row is written and flushed,
/// so a write never truncates the file a table's name held: one linked from another folder keeps
/// its bytes, and a write that fails leaves the old file whole.
///
/// # Arguments
///
/// * `path` - The table's file
/// * `rows` - The rows, in the order a bench loads them
fn write_rows<T: serde::Serialize>(path: &Path, rows: impl Iterator<Item = T>) -> color_eyre::Result<(u64, u64)> {
    // a buffered file of its own beside the table's, a line a row
    let mut staged = path.as_os_str().to_owned();
    staged.push(".tmp");
    let staged = PathBuf::from(staged);
    let file = File::create(&staged).wrap_err_with(|| staged.display().to_string())?;
    let mut out = BufWriter::with_capacity(1 << 20, file);
    let (mut count, mut bytes) = (0u64, 0u64);
    for row in rows {
        let line = serde_json::to_string(&row)?;
        out.write_all(line.as_bytes())?;
        out.write_all(b"\n")?;
        count += 1;
        bytes += line.len() as u64 + 1;
    }
    // every byte on the file before it takes the table's name
    let file = out.into_inner().map_err(|error| error.into_error())?;
    file.sync_all().wrap_err_with(|| staged.display().to_string())?;
    drop(file);
    // the name moved onto the new file, leaving whatever it named before to its other links
    std::fs::rename(&staged, path).wrap_err_with(|| path.display().to_string())?;
    Ok((count, bytes))
}

/// Write the tables asked for into the folder, and its README, and return the README
///
/// # Arguments
///
/// * `args` - What to write and where
fn write_dataset(args: &Args) -> color_eyre::Result<String> {
    // the keyword rows need the csv they are made from, refused by name before anything is written
    let keyword = args.tables.contains(&Table::Keyword);
    if keyword && args.tmdb.is_none() {
        return Err(eyre!("--tables names keyword, whose rows are made from the TMDB csv: pass --tmdb"));
    }
    std::fs::create_dir_all(&args.out).wrap_err_with(|| args.out.display().to_string())?;
    let (seed, width) = (args.seed, args.width);
    // a line of the README's table for every file written
    let mut written = Vec::new();
    // the two generated tables, each its own stream of the seed
    if args.tables.contains(&Table::Cell) {
        let cells = (0..args.cell).map(|id| Cell { id, body: body(seed, id, width) });
        let (rows, bytes) = write_rows(&args.out.join("Cell.jsonl"), cells)?;
        written.push(format!("| `Cell.jsonl` | {rows} | {bytes} |"));
    }
    if args.tables.contains(&Table::Big) {
        let bigs = (0..args.big).map(|id| Big { id, body: body(seed ^ 0xb16, id, width) });
        let (rows, bytes) = write_rows(&args.out.join("Big.jsonl"), bigs)?;
        written.push(format!("| `Big.jsonl` | {rows} | {bytes} |"));
    }
    // every keyword row of every movie in the csv's order, a movie that will not read skipped
    let mut source = String::new();
    if let Some(tmdb) = args.tmdb.as_ref().filter(|_| keyword) {
        let mut reader = csv::Reader::from_path(tmdb).wrap_err_with(|| tmdb.display().to_string())?;
        let mut skipped = 0u64;
        let movies = reader.deserialize::<tmdb_dataset::Movie>().filter_map(|movie| {
            if movie.is_err() {
                skipped += 1;
            }
            movie.ok()
        });
        let keywords = movies.flat_map(|movie| keyword_rows(&movie));
        let (rows, bytes) = write_rows(&args.out.join("Keyword.jsonl"), keywords)?;
        written.push(format!("| `Keyword.jsonl` | {rows} | {bytes} |"));
        source = format!(", the keyword rows from `{}` ({skipped} movies that would not read skipped)", tmdb.display());
    }
    // and how they were made, beside them
    let readme = format!(
        "# X16's dataset\n\nWritten by `x16-data` (`shoal-spike-scan`): seed {seed}, bodies of {width} bytes{source}.\n\n\
         | File | Rows | Bytes |\n| --- | --- | --- |\n{}\n",
        written.join("\n")
    );
    std::fs::write(args.out.join("README.md"), &readme)?;
    Ok(readme)
}

/// Write the dataset folder
fn main() -> color_eyre::Result<()> {
    // report errors with their context
    color_eyre::install()?;
    let args = Args::parse();
    // the tables asked for, and what was written said on the way out
    let readme = write_dataset(&args)?;
    print!("{readme}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{write_dataset, Args, Table};
    use std::path::Path;

    /// The arguments of a small write of the tables named into a folder
    ///
    /// # Arguments
    ///
    /// * `out` - The folder
    /// * `tables` - The tables to write
    fn small(out: &Path, tables: Vec<Table>) -> Args {
        // a few rows of narrow bodies, the defaults' seed
        Args {
            out: out.to_path_buf(),
            tmdb: None,
            tables,
            cell: 7,
            big: 5,
            width: 32,
            seed: 16,
        }
    }

    /// Only the tables named are written, a cell of more rows begins with a smaller one's rows, and
    /// the keyword rows are refused without the csv they are made from
    #[test]
    fn only_the_tables_named_are_written() {
        let dir = tempfile::tempdir().expect("a scratch folder");
        let readme = write_dataset(&small(dir.path(), vec![Table::Cell])).expect("the cell writes");
        assert!(dir.path().join("Cell.jsonl").exists());
        assert!(!dir.path().join("Big.jsonl").exists());
        assert!(!dir.path().join("Cell.jsonl.tmp").exists(), "the staged file was renamed");
        assert!(readme.contains("| `Cell.jsonl` | 7 |") && !readme.contains("Big.jsonl"), "{readme}");
        // a cell of more rows, written elsewhere, begins with every line of this one
        let fewer = std::fs::read_to_string(dir.path().join("Cell.jsonl")).expect("the cell reads");
        let more_dir = tempfile::tempdir().expect("a second scratch folder");
        let mut more = small(more_dir.path(), vec![Table::Cell]);
        more.cell = 11;
        write_dataset(&more).expect("the larger cell writes");
        let larger = std::fs::read_to_string(more_dir.path().join("Cell.jsonl")).expect("the larger cell reads");
        assert_eq!(larger.lines().count(), 11);
        assert!(larger.starts_with(&fewer));
        // the keyword rows need --tmdb, and nothing is written without it
        let none_dir = tempfile::tempdir().expect("a third scratch folder");
        let refused = write_dataset(&small(none_dir.path(), vec![Table::Cell, Table::Keyword])).unwrap_err();
        assert!(refused.to_string().contains("--tmdb"), "{refused}");
        assert!(!none_dir.path().join("Cell.jsonl").exists());
    }

    /// A table written over a file that is linked from another folder leaves the other link's
    /// bytes as they were
    #[test]
    fn a_write_never_truncates_a_linked_file() {
        let dir = tempfile::tempdir().expect("a scratch folder");
        let elsewhere = dir.path().join("elsewhere.jsonl");
        std::fs::write(&elsewhere, "the other folder's rows\n").expect("the other file writes");
        let out = dir.path().join("dataset");
        std::fs::create_dir_all(&out).expect("the dataset folder");
        std::fs::hard_link(&elsewhere, out.join("Cell.jsonl")).expect("a link");
        write_dataset(&small(&out, vec![Table::Cell])).expect("the cell writes");
        assert_eq!(std::fs::read_to_string(&elsewhere).expect("the other file reads"), "the other folder's rows\n");
        assert_eq!(std::fs::read_to_string(out.join("Cell.jsonl")).expect("the cell reads").lines().count(), 7);
    }
}
