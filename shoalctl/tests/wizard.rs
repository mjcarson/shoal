//! Tests for `shoalctl cluster new`, the inventory wizard
//!
//! These draw the wizard to an in memory terminal and write its file to a temp dir, so none of
//! it needs a real terminal or a host ([F53](../../docs/src/features/inventory-wizard.md)).

use ratatui::Terminal;
use ratatui::backend::TestBackend;
use shoalctl::deploy::inventory::Inventory;
use shoalctl::wizard::form::{Draft, Page, Wizard};
use shoalctl::wizard::{save, view};
use std::path::PathBuf;

/// An inventory with a group, parsed with this test binary as its server program
///
/// # Arguments
///
/// * `server` - The program the inventory names
fn grouped(server: &std::path::Path) -> Inventory {
    // two nodes share a group splitting their logs from their archives, one keeps the defaults
    let yaml = format!(
        "server: {}\nname: lab\nreplication_factor: 2\n\
         groups:\n  small:\n    resources: {{cores: 4}}\n    storage: {{latency: /mnt/nvme/shoal, throughput: /mnt/bulk/shoal}}\n\
         nodes:\n  - {{name: hyperion, address: 172.16.2.5, group: small}}\n  - {{name: titan, address: 172.16.2.4, group: small}}\n  - {{name: europa, address: 172.16.2.10}}\n",
        server.display()
    );
    serde_yaml::from_str(&yaml).expect("an inventory")
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

/// The review page shows what every node resolves to and from where, and the file it writes
#[test]
fn the_review_page_shows_what_each_node_resolves_to() {
    // a wizard on the grouped inventory, at its review
    let server = std::env::current_exe().expect("the test binary");
    let mut wizard = Wizard::new(
        Draft::from_inventory(&grouped(&server)),
        PathBuf::from("/tmp/lab.yml"),
        false,
    );
    wizard.go(Page::Review);
    // drawn on a terminal wide enough for the resolved table
    let mut terminal = Terminal::new(TestBackend::new(200, 50)).expect("a terminal");
    terminal
        .draw(|frame| view::render(frame, &wizard))
        .expect("a frame");
    let screen = screen(&terminal);
    // nothing is wrong with it
    assert!(screen.contains("Nothing wrong"), "{screen}");
    // the group's nodes take its directories and its resources, and say so
    let hyperion = screen
        .lines()
        .find(|line| line.contains("hyperion") && line.contains("cores"))
        .expect("a hyperion row");
    assert!(
        hyperion.contains("/mnt/nvme/shoal (group small)"),
        "{hyperion}"
    );
    assert!(
        hyperion.contains("/mnt/bulk/shoal (group small)"),
        "{hyperion}"
    );
    assert!(
        hyperion.contains("4 cores, 4Gi (group small)"),
        "{hyperion}"
    );
    // the node in no group takes the default directory
    let europa = screen
        .lines()
        .find(|line| line.contains("europa") && line.contains("cores"))
        .expect("a europa row");
    assert!(
        europa.contains("/opt/shoal-deploy/lab/data (default)"),
        "{europa}"
    );
    // and the file itself is on the page
    assert!(screen.contains("groups:"), "{screen}");
    // every other page draws without panicking, on a small terminal too
    for page in Page::ALL {
        wizard.go(page);
        for (width, height) in [(200, 50), (80, 24)] {
            let mut terminal = Terminal::new(TestBackend::new(width, height)).expect("a terminal");
            terminal
                .draw(|frame| view::render(frame, &wizard))
                .expect("a frame");
        }
    }
}

/// The file the wizard writes is an inventory the deployment loads, and the same one
#[test]
fn a_saved_inventory_loads() {
    // written to a temp dir, naming this test binary as its program
    let dir = tempfile::tempdir().expect("a temp dir");
    let server = std::env::current_exe().expect("the test binary");
    let inventory = grouped(&server);
    let path = dir.path().join("inventories/lab.yml");
    let wizard = Wizard::new(Draft::from_inventory(&inventory), path.clone(), false);
    let (built, issues) = wizard.build();
    assert!(issues.is_empty(), "{issues:?}");
    save(&path, &built).expect("a saved inventory");
    // no partial file is left beside it
    assert!(!path.with_extension("yml.partial").exists());
    // the deployment's own loader takes it, validation and all, and it is what was built
    let loaded = Inventory::load(&path).expect("an inventory the deployment loads");
    assert_eq!(loaded, inventory);
    assert_eq!(
        loaded.node("titan").expect("titan").storage.throughput,
        "/mnt/bulk/shoal"
    );
    // and it opens with the note saying how to build and bootstrap
    let raw = std::fs::read_to_string(&path).unwrap();
    assert!(raw.starts_with("# Written by `shoalctl cluster new`"));
}
