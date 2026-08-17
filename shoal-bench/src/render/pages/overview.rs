//! The index: what has been captured, and how to read any of it
//!
//! The page a reader lands on. Its job is to point at the right one of the others and to say the
//! three things that are true of every number on all of them, before any of those numbers are read.

use anyhow::Result;

use crate::render::badges;
use crate::render::family::Surface;
use crate::render::page::Page;
use crate::render::pages::{footer, header};

/// Builds the index page
///
/// # Arguments
///
/// * `page` - Everything the page is built from
pub fn build(page: &Page) -> Result<String> {
    let mut out = header(
        Surface::Overview,
        page,
        "What Shoal costs, measured on one machine against a live server. Each page below answers \
         one question and says how to read its own answer; this one says what is true of all of \
         them.",
    );
    out.push_str(&rules());
    out.push_str(&contents());
    out.push_str(&freshness(page));
    out.push_str(&footer(Surface::Overview));
    Ok(out)
}

/// The three things that are true of every number on the site
fn rules() -> String {
    let mut out = String::new();
    out.push_str("## Before reading any of it\n\n");
    out.push_str(
        "**One machine, one configuration.** Every number here comes from the machine described in \
         [Performance Baseline](baseline.md), under the `shoal.yml` committed beside the code: \
         twelve shards, a 4 GiB memory limit, and an Intel Optane SSD whose fsync latency is \
         roughly an order of magnitude below a consumer NVMe. Anything on this site about what \
         durability costs is therefore a best case.\n\n",
    );
    out.push_str(
        "**A difference is a result only when the ranges do not overlap.** Every end to end number \
         is the median of several runs, quoted with the range of those runs beside it. This layer's \
         own spread is wider than most changes worth making - the frozen baseline moved 10.5% \
         across five identical runs of the same code - so two medians that differ while their \
         ranges overlap have not been shown to differ at all.\n\n",
    );
    out.push_str(
        "**Nothing here is a comparison against another database.** These are Shoal against Shoal, \
         over rows Shoal generated from a seed. The workload definitions borrow YCSB's - its \
         mixtures, its record size, its key distributions - so a number here is readable *next to* \
         a published YCSB figure, but it was not produced by the same harness and the two are not \
         the same measurement.\n\n",
    );
    out
}

/// What each page answers
fn contents() -> String {
    let mut out = String::new();
    out.push_str("## What each page answers\n\n");
    out.push_str("| Page | The question it answers |\n| --- | --- |\n");
    // one row per page, in the order they are registered, skipping this one
    for surface in Surface::ALL {
        if surface == Surface::Overview {
            continue;
        }
        out.push_str(&format!(
            "| [{}]({}) | {} |\n",
            surface.title(),
            surface.link(),
            question(surface)
        ));
    }
    out.push_str(
        "\nStart with [Read/write mixtures](grid.md) if the question is \"how will this handle my \
         workload\", and with [The micro layer](micro.md) if it is \"did my change make this \
         slower\". Those two are the ends of the same spectrum and they are answered by different \
         measurements.\n\n",
    );
    out
}

/// The one line question a page answers
///
/// # Arguments
///
/// * `surface` - The page to describe
fn question(surface: Surface) -> &'static str {
    match surface {
        Surface::Overview => "what is true of all of it",
        Surface::Grid => {
            "what a workload costs at a given ratio of reads to writes, on each kind of table"
        }
        Surface::RowSize => "how the cost moves as rows get wider, from 64 bytes to 4 MiB",
        Surface::TableTypes => {
            "what durability costs, and what holding rows in order costs, each measured against a \
             control that differs in one thing"
        }
        Surface::Access => {
            "what a skewed key space buys, and where the server stops keeping up with the load"
        }
        Surface::Transport => "what the client's sending mode costs, and what encryption costs",
        Surface::Fanout => "what reading many partitions in one query costs, and how that grows",
        Surface::Configuration => {
            "what each setting in `shoal.yml` is worth, and which of them this workload is actually \
             sensitive to"
        }
        Surface::Micro => {
            "whether a change made one function faster - not what the system will do with data"
        }
        Surface::Attribution => "which code the time is spent inside, from two instrumented builds",
        Surface::AllWorkloads => "every raw number, with no selection and no interpretation",
    }
}

/// The table of what has been captured and whether it can still be believed
///
/// # Arguments
///
/// * `page` - Everything the page is built from
fn freshness(page: &Page) -> String {
    let mut out = String::new();
    out.push_str("## What has been captured\n\n");
    // a one line summary, so the state of the corpus is legible before the table is read
    let stale: Vec<_> = page
        .statuses
        .iter()
        .filter(|status| status.code.iter().any(|(_, verdict)| !verdict.is_current()))
        .collect();
    out.push_str(&format!(
        "{} captures are recorded. {}\n\n",
        page.statuses.len(),
        if page.statuses.is_empty() {
            "Nothing has been captured in this tree yet.".to_string()
        } else if stale.is_empty() {
            "Every one of them still describes the current code.".to_string()
        } else {
            format!(
                "{} of them have at least one layer that no longer describes the current code, or \
                 that was taken before provenance was recorded.",
                stale.len()
            )
        }
    ));
    out.push_str(
        "One row per capture and layer. A capture is **fresh** when it was taken at this commit on \
         a clean tree, **unaffected** when the commit has moved but nothing that layer measures \
         has, **stale** when that layer's sources have changed since, and **uncommitted** when the \
         bytes it measured exist in no commit at all. **No provenance** is how a capture taken \
         before `shoal-bench` existed is reported: it is deliberately neither fresh nor stale, \
         because neither is known.\n\n",
    );
    if page.statuses.is_empty() {
        return out;
    }
    out.push_str("| Capture | Taken | Layer | Standing |\n| --- | --- | --- | --- |\n");
    // newest first, which is the order somebody looking for the current numbers reads in
    for snapshot in page.timeline.iter().rev() {
        let Some(status) = page.status(&snapshot.label) else {
            continue;
        };
        for (layer, _) in &status.code {
            out.push_str(&format!(
                "| `{}` | {} | {} | {} |\n",
                snapshot.label,
                snapshot.captured,
                layer,
                badges::for_layer(status, *layer)
            ));
        }
    }
    out.push('\n');
    out
}
