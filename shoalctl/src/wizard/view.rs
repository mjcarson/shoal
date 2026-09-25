//! Drawing the wizard
//!
//! The only part of the wizard that draws. Every frame is built from a [`Wizard`] and the
//! inventory it builds, so what is shown is always what would be written.

use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, Wrap},
};

use super::form::{
    Confirm, FieldId, FieldKind, Issue, Page, ProbeReport, ProbeState, Resolution, Severity, Target,
    Wizard,
};
use crate::deploy::inventory::Inventory;

/// The style of whatever has focus
fn focus_style() -> Style {
    Style::default()
        .fg(Color::Black)
        .bg(Color::Cyan)
        .add_modifier(Modifier::BOLD)
}

/// The style of text that stands for a blank field
fn placeholder_style() -> Style {
    Style::default()
        .fg(Color::DarkGray)
        .add_modifier(Modifier::ITALIC)
}

/// The style an issue of this severity is drawn in
///
/// # Arguments
///
/// * `severity` - How bad it is
fn severity_style(severity: Severity) -> Style {
    match severity {
        Severity::Error => Style::default().fg(Color::Red),
        Severity::Warning => Style::default().fg(Color::Yellow),
    }
}

/// Write a byte count the way an operator reads one
///
/// # Arguments
///
/// * `bytes` - The count
#[must_use]
pub fn human_bytes(bytes: u64) -> String {
    // the largest unit the count is at least one of
    let units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < units.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", units[unit])
    }
}

/// Say what a probe found in one line
///
/// # Arguments
///
/// * `report` - What the host said
#[must_use]
pub fn probe_line(report: &ProbeReport) -> String {
    // the host's size, then the room under each directory
    let mut parts = Vec::new();
    if let Some(cpus) = report.cpus {
        parts.push(format!("{cpus} cpus"));
    }
    if let Some(memory) = report.memory {
        parts.push(human_bytes(memory));
    }
    for (root, free) in &report.free {
        match free {
            Some(free) => parts.push(format!("{root} {} free", human_bytes(*free))),
            None => parts.push(format!("{root} ?")),
        }
    }
    // and a node already there, which bootstrap will refuse without --wipe
    for root in &report.claimed {
        parts.push(format!("{root} is already claimed"));
    }
    parts.join(" · ")
}

/// Draw the wizard
///
/// # Arguments
///
/// * `frame` - The frame to draw on
/// * `wizard` - The wizard
pub fn render(frame: &mut Frame, wizard: &Wizard) {
    // what the draft builds, which every page reads from
    let (inventory, issues) = wizard.build();
    // a title, the sidebar and page beside each other, the help and the status line
    let [title, body, help, status] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(8),
        Constraint::Length(3),
        Constraint::Length(1),
    ])
    .areas(frame.area());
    let [sidebar, page] =
        Layout::horizontal([Constraint::Length(18), Constraint::Min(20)]).areas(body);
    // the title names the file being built
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(
                " shoalctl cluster new ",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::raw(format!("→ {}", wizard.out.display())),
        ])),
        title,
    );
    render_sidebar(frame, sidebar, wizard, &issues);
    match wizard.page {
        Page::Cluster | Page::Shape | Page::Defaults => {
            render_form(frame, page, wizard, &issues);
        }
        Page::Groups => render_groups(frame, page, wizard, &issues),
        Page::Nodes => render_nodes(frame, page, wizard, &inventory, &issues),
        Page::Review => render_review(frame, page, wizard, &inventory, &issues),
    }
    render_help(frame, help, wizard);
    render_status(frame, status, wizard);
    // a question is drawn over everything
    if let Some(confirm) = wizard.confirm {
        render_confirm(frame, confirm, wizard);
    }
}

/// Draw the list of pages, each with how many errors and warnings it has
///
/// # Arguments
///
/// * `frame` - The frame
/// * `area` - Where to draw
/// * `wizard` - The wizard
/// * `issues` - Everything wrong with the draft
fn render_sidebar(frame: &mut Frame, area: Rect, wizard: &Wizard, issues: &[Issue]) {
    // one line per page, the one shown highlighted
    let lines: Vec<Line> = Page::ALL
        .iter()
        .enumerate()
        .map(|(index, page)| {
            let count = |severity| {
                issues
                    .iter()
                    .filter(|issue| issue.severity == severity && issue.target.page() == *page)
                    .count()
            };
            let (errors, warnings) = (count(Severity::Error), count(Severity::Warning));
            let mut spans = vec![Span::styled(
                format!(" {}. {:<9}", index + 1, page.title()),
                if *page == wizard.page {
                    focus_style()
                } else {
                    Style::default()
                },
            )];
            if errors > 0 {
                spans.push(Span::styled(
                    format!(" ✗{errors}"),
                    severity_style(Severity::Error),
                ));
            } else if warnings > 0 {
                spans.push(Span::styled(
                    format!(" !{warnings}"),
                    severity_style(Severity::Warning),
                ));
            }
            Line::from(spans)
        })
        .collect();
    frame.render_widget(
        Paragraph::new(lines).block(Block::default().borders(Borders::RIGHT)),
        area,
    );
}

/// The lines of a list of fields, each labelled, with its value and any issue on it
///
/// # Arguments
///
/// * `wizard` - The wizard
/// * `fields` - The fields to draw
/// * `focused` - Whether the focus is in these fields
/// * `issues` - The issues on the level these fields edit
fn field_lines<'a>(
    wizard: &Wizard,
    fields: &[FieldId],
    focused: bool,
    issues: &[&'a Issue],
) -> Vec<Line<'a>> {
    let draft = &wizard.draft;
    fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let has_focus = focused && index == wizard.focus;
            // the label, highlighted where the focus is
            let mut spans = vec![Span::styled(
                format!(" {:<20}", field.label()),
                if has_focus {
                    focus_style()
                } else {
                    Style::default().add_modifier(Modifier::BOLD)
                },
            )];
            spans.push(Span::raw(" "));
            // the value, as its kind draws it
            match field.kind() {
                FieldKind::Toggle => {
                    let on = draft.toggle(wizard.page, wizard.selected, *field);
                    spans.push(Span::raw(if on { "[x]" } else { "[ ]" }));
                }
                FieldKind::Choice => {
                    let value = draft
                        .text(wizard.page, wizard.selected, *field)
                        .unwrap_or_default()
                        .to_string();
                    spans.push(Span::raw(format!("‹ {value} ›")));
                }
                FieldKind::Text => {
                    let value = draft
                        .text(wizard.page, wizard.selected, *field)
                        .unwrap_or_default()
                        .to_string();
                    if value.is_empty() && !has_focus {
                        let placeholder = draft.placeholder(wizard.page, wizard.selected, *field);
                        spans.push(Span::styled(placeholder, placeholder_style()));
                    } else {
                        spans.push(Span::raw(value));
                    }
                    if has_focus {
                        spans.push(Span::styled("▏", Style::default().fg(Color::Cyan)));
                    }
                }
            }
            // what is wrong with it, beside it
            if let Some(issue) = issues.iter().find(|issue| issue.field == Some(*field)) {
                spans.push(Span::styled(
                    format!("  {}", issue.message),
                    severity_style(issue.severity),
                ));
            }
            Line::from(spans)
        })
        .collect()
}

/// Draw a form page
///
/// # Arguments
///
/// * `frame` - The frame
/// * `area` - Where to draw
/// * `wizard` - The wizard
/// * `issues` - Everything wrong with the draft
fn render_form(frame: &mut Frame, area: Rect, wizard: &Wizard, issues: &[Issue]) {
    // the issues on this page's fields
    let here: Vec<&Issue> = issues
        .iter()
        .filter(|issue| issue.target == Target::Field(wizard.page))
        .collect();
    let mut lines = field_lines(wizard, FieldId::page_fields(wizard.page), true, &here);
    // the defaults page says what it is the default for
    if wizard.page == Page::Defaults {
        lines.insert(
            0,
            Line::styled(
                " What every node gets unless its group or the node itself sets it.",
                placeholder_style(),
            ),
        );
        lines.insert(1, Line::raw(""));
    }
    let block = Block::bordered().title(format!(" {} ", wizard.page.title()));
    frame.render_widget(Paragraph::new(lines).block(block), area);
}

/// Draw the groups page: the list, and the selected group's edit panel
///
/// # Arguments
///
/// * `frame` - The frame
/// * `area` - Where to draw
/// * `wizard` - The wizard
/// * `issues` - Everything wrong with the draft
fn render_groups(frame: &mut Frame, area: Rect, wizard: &Wizard, issues: &[Issue]) {
    let draft = &wizard.draft;
    let [list, panel] = Layout::vertical([Constraint::Min(5), Constraint::Length(9)]).areas(area);
    // one row per group: what it sets, and who uses it
    let rows: Vec<Row> = draft
        .groups
        .iter()
        .enumerate()
        .map(|(index, group)| {
            let resources = if group.resources.is_empty() {
                "-".to_string()
            } else {
                format!(
                    "{} cores, {}",
                    Some(group.resources.cores.as_str())
                        .filter(|cores| !cores.is_empty())
                        .unwrap_or("every"),
                    Some(group.resources.memory.as_str())
                        .filter(|memory| !memory.is_empty())
                        .unwrap_or("4Gi")
                )
            };
            let members: Vec<&str> = draft
                .nodes
                .iter()
                .filter(|node| node.group == Some(group.id))
                .map(|node| node.name.as_str())
                .collect();
            let or_dash = |raw: &str| {
                if raw.is_empty() {
                    "-".to_string()
                } else {
                    raw.to_string()
                }
            };
            let bad = issues
                .iter()
                .any(|issue| issue.target == Target::Group(index));
            let row = Row::new(vec![
                Cell::from(format!("{}{}", if bad { "✗ " } else { "" }, group.name)),
                Cell::from(resources),
                Cell::from(or_dash(&group.storage.latency)),
                Cell::from(or_dash(&group.storage.throughput)),
                Cell::from(members.join(", ")),
            ]);
            if index == wizard.selected && !wizard.editing {
                row.style(focus_style())
            } else if index == wizard.selected {
                row.style(Style::default().add_modifier(Modifier::BOLD))
            } else {
                row
            }
        })
        .collect();
    let empty = draft.groups.is_empty();
    let table = Table::new(
        rows,
        [
            Constraint::Length(14),
            Constraint::Length(18),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Min(10),
        ],
    )
    .header(
        Row::new(vec!["Group", "Resources", "Latency", "Throughput", "Nodes"])
            .style(Style::default().add_modifier(Modifier::BOLD | Modifier::UNDERLINED)),
    )
    .block(Block::bordered().title(format!(
        " Groups ({}) {}",
        draft.groups.len(),
        if empty {
            "- none yet; nodes can share settings through a group. `a` adds one "
        } else {
            ""
        }
    )));
    frame.render_widget(table, list);
    // the selected group's fields
    render_panel(
        frame,
        panel,
        wizard,
        issues,
        Target::Group(wizard.selected),
        "group",
    );
}

/// Draw the nodes page: the list with what each resolves to, and the selected node's panel
///
/// # Arguments
///
/// * `frame` - The frame
/// * `area` - Where to draw
/// * `wizard` - The wizard
/// * `inventory` - What the draft builds
/// * `issues` - Everything wrong with the draft
fn render_nodes(
    frame: &mut Frame,
    area: Rect,
    wizard: &Wizard,
    inventory: &Inventory,
    issues: &[Issue],
) {
    let [list, panel] = Layout::vertical([Constraint::Min(5), Constraint::Length(13)]).areas(area);
    // one row per node, with what it resolves to and where from
    let rows: Vec<Row> = resolved_rows(wizard, inventory, issues, true)
        .into_iter()
        .enumerate()
        .map(|(index, row)| {
            if index == wizard.selected && !wizard.editing {
                row.style(focus_style())
            } else if index == wizard.selected {
                row.style(Style::default().add_modifier(Modifier::BOLD))
            } else {
                row
            }
        })
        .collect();
    let bootstrap = wizard
        .draft
        .nodes
        .iter()
        .filter(|node| node.bootstrap)
        .count();
    let table = Table::new(rows, resolved_widths(true))
        .header(resolved_header(true))
        .block(Block::bordered().title(format!(
            " Nodes ({}, {bootstrap} bootstrapping) ",
            wizard.draft.nodes.len()
        )));
    frame.render_widget(table, list);
    // the selected node's fields
    render_panel(
        frame,
        panel,
        wizard,
        issues,
        Target::Node(wizard.selected),
        "node",
    );
}

/// The header of the resolved nodes table
///
/// # Arguments
///
/// * `editing` - Whether this is the nodes page, which shows the host columns
fn resolved_header(editing: bool) -> Row<'static> {
    let mut cells = vec![
        "Node",
        "Group",
        "Boot",
        "Resources",
        "Latency",
        "Throughput",
    ];
    if editing {
        cells.insert(1, "Address");
        cells.push("Host");
    }
    Row::new(cells).style(Style::default().add_modifier(Modifier::BOLD | Modifier::UNDERLINED))
}

/// The widths of the resolved nodes table
///
/// # Arguments
///
/// * `editing` - Whether this is the nodes page, which shows the host columns
fn resolved_widths(editing: bool) -> Vec<Constraint> {
    // the resources column fits "12 cores, 16Gi (group <name>)", and the rest share what is left
    let mut widths = vec![
        Constraint::Length(12),
        Constraint::Length(10),
        Constraint::Length(4),
        Constraint::Length(32),
        Constraint::Fill(2),
        Constraint::Fill(2),
    ];
    if editing {
        widths.insert(1, Constraint::Length(15));
        widths.push(Constraint::Length(8));
    }
    widths
}

/// One row per node: its group, whether it bootstraps, and what it resolves to and from where
///
/// # Arguments
///
/// * `wizard` - The wizard
/// * `inventory` - What the draft builds
/// * `issues` - Everything wrong with the draft
/// * `editing` - Whether this is the nodes page, which shows the host columns
fn resolved_rows<'a>(
    wizard: &'a Wizard,
    inventory: &Inventory,
    issues: &[Issue],
    editing: bool,
) -> Vec<Row<'a>> {
    let bootstrap = inventory.bootstrap_names();
    inventory
        .nodes
        .iter()
        .enumerate()
        .map(|(index, spec)| {
            // what it runs with and where it keeps its data, and the level each came from
            let (resources, resources_from) = inventory.resolve_resources(spec);
            let (storage, [latency_from, throughput_from]) = inventory.resolve_storage(spec);
            let bad = issues
                .iter()
                .any(|issue| issue.target == Target::Node(index));
            let mut cells = vec![
                Cell::from(format!("{}{}", if bad { "✗ " } else { "" }, spec.name)),
                Cell::from(spec.group.clone().unwrap_or_else(|| "-".to_string())),
                Cell::from(if bootstrap.contains(&spec.name) {
                    "yes"
                } else {
                    "add"
                }),
                Cell::from(format!(
                    "{} cores, {} ({resources_from})",
                    resources
                        .cores
                        .map_or_else(|| "all".to_string(), |cores| cores.to_string()),
                    resources.memory
                )),
                Cell::from(format!("{} ({latency_from})", storage.latency)),
                Cell::from(format!("{} ({throughput_from})", storage.throughput)),
            ];
            if editing {
                // where it is reached, and what its host said when probed
                let address = match (spec.address, wizard.resolutions.get(&spec.name)) {
                    // an address given is the one used
                    (Some(address), _) => Cell::from(address.to_string()),
                    // otherwise what the name resolved to here, as bootstrap will resolve it
                    (None, Some(Resolution::Resolved(address))) => Cell::from(Span::styled(
                        format!("{address} (resolved)"),
                        placeholder_style(),
                    )),
                    (None, Some(Resolution::Loopback)) => {
                        Cell::from(Span::styled("loopback", severity_style(Severity::Error)))
                    }
                    (None, Some(Resolution::Failed(_))) => {
                        Cell::from(Span::styled("unresolved", severity_style(Severity::Warning)))
                    }
                    (None, Some(Resolution::Running) | None) => Cell::from("resolving…"),
                };
                cells.insert(1, address);
                // a word here; the whole of what it said is in the node's panel
                let host = match wizard.probes.get(&spec.name) {
                    None => Cell::from(Span::styled("-", placeholder_style())),
                    Some(ProbeState::Running) => Cell::from("probing…"),
                    Some(ProbeState::Done(report)) if report.claimed.is_empty() => {
                        Cell::from(Span::styled("ok", Style::default().fg(Color::Green)))
                    }
                    Some(ProbeState::Done(_)) => {
                        Cell::from(Span::styled("claimed", severity_style(Severity::Warning)))
                    }
                    Some(ProbeState::Failed(_)) => {
                        Cell::from(Span::styled("failed", severity_style(Severity::Error)))
                    }
                };
                cells.push(host);
            }
            Row::new(cells)
        })
        .collect()
}

/// Draw a list page's edit panel for the selected group or node
///
/// # Arguments
///
/// * `frame` - The frame
/// * `area` - Where to draw
/// * `wizard` - The wizard
/// * `issues` - Everything wrong with the draft
/// * `target` - The group or node selected
/// * `what` - What the list holds, for the title
fn render_panel(
    frame: &mut Frame,
    area: Rect,
    wizard: &Wizard,
    issues: &[Issue],
    target: Target,
    what: &str,
) {
    // nothing selected is said rather than drawn empty
    let len = match wizard.page {
        Page::Groups => wizard.draft.groups.len(),
        _ => wizard.draft.nodes.len(),
    };
    let border = if wizard.editing {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let block = Block::bordered()
        .border_style(border)
        .title(if wizard.editing {
            format!(" Editing this {what} - Esc returns to the list ")
        } else {
            format!(" This {what} - Enter edits it ")
        });
    if len == 0 {
        frame.render_widget(
            Paragraph::new(Line::styled(
                format!(" No {what}s yet. `a` adds one."),
                placeholder_style(),
            ))
            .block(block),
            area,
        );
        return;
    }
    // the issues on the selected one, beside its fields
    let here: Vec<&Issue> = issues
        .iter()
        .filter(|issue| issue.target == target)
        .collect();
    let fields = FieldId::page_fields(wizard.page);
    let lines = field_lines(wizard, fields, wizard.editing, &here);
    // two columns, so the panel stays short
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let split = lines.len().div_ceil(2);
    let mut left = lines;
    let right = left.split_off(split);
    let [columns, host_area] =
        Layout::vertical([Constraint::Length(left.len() as u16), Constraint::Min(0)]).areas(inner);
    let [left_area, right_area] =
        Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)]).areas(columns);
    frame.render_widget(Paragraph::new(left), left_area);
    frame.render_widget(Paragraph::new(right), right_area);
    // and for a node, what its host said when it was probed
    if wizard.page == Page::Nodes {
        let name = wizard
            .draft
            .nodes
            .get(wizard.selected)
            .map(|node| node.name.trim().to_string())
            .unwrap_or_default();
        let line = match wizard.probes.get(&name) {
            None => Line::styled(
                " Host: not probed - `p` asks it over ssh",
                placeholder_style(),
            ),
            Some(ProbeState::Running) => Line::raw(" Host: probing…"),
            Some(ProbeState::Done(report)) => Line::raw(format!(" Host: {}", probe_line(report))),
            Some(ProbeState::Failed(error)) => Line::styled(
                format!(" Host: the probe failed: {error}"),
                severity_style(Severity::Error),
            ),
        };
        frame.render_widget(
            Paragraph::new(vec![Line::raw(""), line]).wrap(Wrap { trim: false }),
            host_area,
        );
    }
}

/// Draw the review: what is wrong, what each node resolves to, and the file
///
/// # Arguments
///
/// * `frame` - The frame
/// * `area` - Where to draw
/// * `wizard` - The wizard
/// * `inventory` - What the draft builds
/// * `issues` - Everything wrong with the draft
fn render_review(
    frame: &mut Frame,
    area: Rect,
    wizard: &Wizard,
    inventory: &Inventory,
    issues: &[Issue],
) {
    let issue_height = (issues.len().max(1) + 2).min(8) as u16;
    let node_height = (inventory.nodes.len().max(1) + 3).min(14) as u16;
    let [issue_area, node_area, file_area] = Layout::vertical([
        Constraint::Length(issue_height),
        Constraint::Length(node_height),
        Constraint::Min(4),
    ])
    .areas(area);
    // every issue, named by its page
    let lines: Vec<Line> = if issues.is_empty() {
        vec![Line::styled(
            " Nothing wrong. `s` writes the file.",
            Style::default().fg(Color::Green),
        )]
    } else {
        issues
            .iter()
            .map(|issue| {
                Line::from(vec![
                    Span::styled(
                        match issue.severity {
                            Severity::Error => " error   ",
                            Severity::Warning => " warning ",
                        },
                        severity_style(issue.severity),
                    ),
                    Span::raw(format!(
                        "{}: {}",
                        issue.target.page().title(),
                        issue.message
                    )),
                ])
            })
            .collect()
    };
    frame.render_widget(
        Paragraph::new(lines)
            .wrap(Wrap { trim: false })
            .block(Block::bordered().title(" Issues ")),
        issue_area,
    );
    // what each node resolves to
    let table = Table::new(
        resolved_rows(wizard, inventory, issues, false),
        resolved_widths(false),
    )
    .header(resolved_header(false))
    .block(Block::bordered().title(" What each node gets "));
    frame.render_widget(table, node_area);
    // the file itself, scrolled
    let document = super::document(inventory).unwrap_or_else(|error| format!("# {error}"));
    frame.render_widget(
        Paragraph::new(document)
            .scroll((wizard.scroll, 0))
            .block(Block::bordered().title(" The inventory - ↑/↓ scroll ")),
        file_area,
    );
}

/// Draw the help for whatever has focus
///
/// # Arguments
///
/// * `frame` - The frame
/// * `area` - Where to draw
/// * `wizard` - The wizard
fn render_help(frame: &mut Frame, area: Rect, wizard: &Wizard) {
    // what the focused field is for, then the keys that work here
    let about = match wizard.focused() {
        Some(field) => field.help(wizard.page).to_string(),
        None => match wizard.page {
            Page::Groups => "A group is settings nodes share: a node naming it takes whatever it does not set itself.".to_string(),
            Page::Nodes => "Every host this cluster may place a node on. `p` asks the selected host over ssh what it has.".to_string(),
            Page::Review => format!("`s` writes {}.", wizard.out.display()),
            _ => String::new(),
        },
    };
    let keys = match wizard.page {
        Page::Groups | Page::Nodes if !wizard.editing => {
            "↑/↓ select · a add · d delete · Enter edit · ←/→ group · space bootstrap · p probe · PgDn/PgUp page · Esc quit"
        }
        Page::Review => "↑/↓ scroll · s save · PgUp back · Esc quit",
        _ => {
            "Tab/↑/↓ field · type to edit · ^U clear · space toggle · ←/→ choose · PgDn/PgUp page · Esc quit"
        }
    };
    let lines = vec![
        Line::raw(format!(" {about}")),
        Line::styled(format!(" {keys}"), Style::default().fg(Color::DarkGray)),
    ];
    frame.render_widget(
        Paragraph::new(lines)
            .wrap(Wrap { trim: false })
            .block(Block::default().borders(Borders::TOP)),
        area,
    );
}

/// Draw the one line message, if there is one
///
/// # Arguments
///
/// * `frame` - The frame
/// * `area` - Where to draw
/// * `wizard` - The wizard
fn render_status(frame: &mut Frame, area: Rect, wizard: &Wizard) {
    if let Some((severity, message)) = &wizard.message {
        frame.render_widget(
            Paragraph::new(Line::styled(
                format!(" {message}"),
                severity_style(*severity),
            )),
            area,
        );
    }
}

/// Draw a question over the page
///
/// # Arguments
///
/// * `frame` - The frame
/// * `confirm` - The question
/// * `wizard` - The wizard
fn render_confirm(frame: &mut Frame, confirm: Confirm, wizard: &Wizard) {
    // what is being asked
    let question = match confirm {
        Confirm::Quit => "Leave without writing the inventory? (y/n)".to_string(),
        Confirm::Overwrite => format!("{} already exists. Replace it? (y/n)", wizard.out.display()),
    };
    // a box in the middle of the screen, wide enough for the question
    let area = frame.area();
    let width = (question.len() as u16 + 4).min(area.width);
    let popup = Rect {
        x: area.x + (area.width.saturating_sub(width)) / 2,
        y: area.y + area.height / 2 - 1,
        width,
        height: 3,
    };
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(Line::raw(format!(" {question}")))
            .block(Block::bordered().border_style(Style::default().fg(Color::Yellow))),
        popup,
    );
}
