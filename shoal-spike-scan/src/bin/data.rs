//! Spike X16's dataset folder: `Cell.jsonl` and `Big.jsonl` made from a seed, `Keyword.jsonl`
//! from the TMDB csv, and a README saying how
//!
//! ```sh
//! x16-data --out target/lab/x16/dataset --tmdb ~/datasets/TMDB_movie_dataset_v11.csv
//! ```
//!
//! The bench loads a table from `<Table>.jsonl`, preloading the rows a run asks for and inserting
//! the rest, in file order ([F66](../../../docs/src/features/dataset-benchmarks.md)).

use clap::Parser;
use color_eyre::eyre::WrapErr;
use shoal_spike_scan::{body, keyword_rows, Big, Cell};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

/// Write the spike's dataset folder
#[derive(Parser, Debug)]
#[command(about)]
struct Args {
    /// The folder to write, made if it is not there
    #[clap(long)]
    out: PathBuf,
    /// The TMDB csv, `TMDB_movie_dataset_v11.csv`, that the keyword rows are made from
    #[clap(long)]
    tmdb: PathBuf,
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
/// # Arguments
///
/// * `path` - The file
/// * `rows` - The rows, in the order a bench loads them
fn write_rows<T: serde::Serialize>(path: &Path, rows: impl Iterator<Item = T>) -> color_eyre::Result<(u64, u64)> {
    // a buffered file, a line a row
    let file = File::create(path).wrap_err_with(|| path.display().to_string())?;
    let mut out = BufWriter::with_capacity(1 << 20, file);
    let (mut count, mut bytes) = (0u64, 0u64);
    for row in rows {
        let line = serde_json::to_string(&row)?;
        out.write_all(line.as_bytes())?;
        out.write_all(b"\n")?;
        count += 1;
        bytes += line.len() as u64 + 1;
    }
    out.flush()?;
    Ok((count, bytes))
}

/// Write the dataset folder
fn main() -> color_eyre::Result<()> {
    // report errors with their context
    color_eyre::install()?;
    let args = Args::parse();
    std::fs::create_dir_all(&args.out)?;
    // the two generated tables, each its own stream of the seed
    let (seed, width) = (args.seed, args.width);
    let cells = (0..args.cell).map(|id| Cell { id, body: body(seed, id, width) });
    let (cell_rows, cell_bytes) = write_rows(&args.out.join("Cell.jsonl"), cells)?;
    let bigs = (0..args.big).map(|id| Big { id, body: body(seed ^ 0xb16, id, width) });
    let (big_rows, big_bytes) = write_rows(&args.out.join("Big.jsonl"), bigs)?;
    // every keyword row of every movie in the csv's order, a movie that will not read skipped
    let mut reader = csv::Reader::from_path(&args.tmdb).wrap_err_with(|| args.tmdb.display().to_string())?;
    let mut skipped = 0u64;
    let movies = reader.deserialize::<tmdb_dataset::Movie>().filter_map(|movie| {
        if movie.is_err() {
            skipped += 1;
        }
        movie.ok()
    });
    let keywords = movies.flat_map(|movie| keyword_rows(&movie));
    let (keyword_rows_written, keyword_bytes) = write_rows(&args.out.join("Keyword.jsonl"), keywords)?;
    // and how they were made, beside them
    let readme = format!(
        "# X16's dataset\n\nWritten by `x16-data` (`shoal-spike-scan`): seed {seed}, bodies of {width} bytes, \
         the keyword rows from `{}` ({skipped} movies that would not read skipped).\n\n\
         | File | Rows | Bytes |\n| --- | --- | --- |\n\
         | `Cell.jsonl` | {cell_rows} | {cell_bytes} |\n\
         | `Big.jsonl` | {big_rows} | {big_bytes} |\n\
         | `Keyword.jsonl` | {keyword_rows_written} | {keyword_bytes} |\n",
        args.tmdb.display()
    );
    std::fs::write(args.out.join("README.md"), &readme)?;
    print!("{readme}");
    Ok(())
}
