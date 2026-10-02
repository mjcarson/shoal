//! Tests for the `shoaladm bench run` wizard
//!
//! These draw the wizard to an in memory terminal, so none of it needs a real terminal, a
//! dataset or a cluster ([F67](../../docs/src/features/bench-run-wizard.md)).

use clap::Parser;
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use shoaladm::bench::wizard::form::{Page, Wizard, workload_help};
use shoaladm::bench::wizard::view;
use shoaladm::bench::{BenchCommand, BenchRunArgs};
use shoaladm::cli::{Cli, Command};
use std::path::PathBuf;

/// A wizard on the spec a `bench run` line leaves
///
/// # Arguments
///
/// * `args` - The arguments after `bench run`
fn wizard(args: &[&str]) -> Wizard {
    // the line as the command line parses it
    let line = ["shoaladm", "bench", "run"].into_iter().chain(args.iter().copied());
    let Command::Bench(BenchCommand::Run(args)) = Cli::try_parse_from(line).expect("a line").command else {
        panic!("not a run");
    };
    let args: BenchRunArgs = *args;
    let spec = args.spec().expect("a spec");
    Wizard::new(spec, args, vec!["Item".to_string()], PathBuf::from("/tmp/bench.yml"))
}

/// Everything a test terminal holds, as one string per row
///
/// # Arguments
///
/// * `terminal` - The terminal
fn screen(terminal: &Terminal<TestBackend>) -> String {
    // the buffer's cells, a row at a time
    let buffer = terminal.backend().buffer();
    let width = buffer.area.width as usize;
    buffer
        .content
        .chunks(width)
        .map(|row| row.iter().map(|cell| cell.symbol()).collect::<String>())
        .collect::<Vec<_>>()
        .join("\n")
}

/// The workloads page lists every workload ticked and explains the one in focus
#[test]
fn the_workloads_page_explains_the_one_in_focus() {
    let wizard = wizard(&["--dataset", "data"]);
    let mut terminal = Terminal::new(TestBackend::new(200, 50)).expect("a terminal");
    terminal.draw(|frame| view::render(frame, &wizard)).expect("drawn");
    let drawn = screen(&terminal);
    for name in ["read100", "insert100", "rw50", "read90"] {
        assert!(drawn.contains(&format!("[x] {name}")), "{name}: {drawn}");
    }
    // the focused one's explanation, wrapped beside the list: its opening words are on screen
    let opening: String = workload_help("read100").split_whitespace().take(4).collect::<Vec<_>>().join(" ");
    assert!(drawn.contains(&opening), "{opening}: {drawn}");
    // and the size of the run in the sidebar
    assert!(drawn.contains("36 arms"), "{drawn}");
}

/// The review counts the arms, names each one, and says what an attached run is refused for
#[test]
fn the_review_names_every_arm_and_refusal() {
    let mut wizard = wizard(&["--dataset", "data", "--attach", "--preloaded", "--bundles", "1", "--runs", "2"]);
    wizard.go(Page::Review);
    let mut terminal = Terminal::new(TestBackend::new(200, 50)).expect("a terminal");
    terminal.draw(|frame| view::render(frame, &wizard)).expect("drawn");
    let drawn = screen(&terminal);
    assert!(drawn.contains("8 arms: 4 workloads × 1 bundle size × 1 event × 2 runs"), "{drawn}");
    assert!(drawn.contains("rw50/b1/none run 1"), "{drawn}");
    assert!(drawn.contains("Workloads: a workload inserts into the attached cluster"), "{drawn}");
    assert!(drawn.contains("save spec to"), "{drawn}");
}

/// Every page draws at a wide terminal and at the smallest a terminal is commonly opened at
#[test]
fn every_page_draws_at_every_size() {
    for args in [&["--dataset", "data"][..], &["--dataset", "data", "--attach"][..]] {
        let mut wizard = wizard(args);
        for (width, height) in [(200, 50), (80, 24)] {
            for page in Page::ALL {
                wizard.go(page);
                let mut terminal = Terminal::new(TestBackend::new(width, height)).expect("a terminal");
                terminal.draw(|frame| view::render(frame, &wizard)).expect("drawn");
                assert!(screen(&terminal).contains(page.title()), "{page:?} at {width}x{height}");
            }
        }
    }
}

