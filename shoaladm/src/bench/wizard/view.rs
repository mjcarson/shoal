//! Drawing the run wizard
//!
//! The only part of the wizard that draws. Every frame is built from a [`Wizard`] and the spec it
//! builds, so the arms and refusals shown are always those the run would have.

use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph, Wrap},
};
use shoal_loadgen::spec::{BenchSpec, Mode};

use super::form::{Confirm, Issue, Page, Row, RowKind, Severity, Wizard, clock, least_seconds};

/// The style of whatever has focus
fn focus_style() -> Style {
    Style::default().fg(Color::Black).bg(Color::Cyan).add_modifier(Modifier::BOLD)
}

/// The style of text that stands for a blank row
fn placeholder_style() -> Style {
    Style::default().fg(Color::DarkGray).add_modifier(Modifier::ITALIC)
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

/// What each page is for, at its top
///
/// # Arguments
///
/// * `page` - The page
fn page_intro(page: Page) -> &'static str {
    match page {
        Page::Workloads => {
            "A workload is the share of reads and inserts an arm sends. Tick every one to run; \
             each runs at every bundle size and event."
        }
        Page::Bundles => "How many queries go to the cluster in one frame. Tick every size to run.",
        Page::Events => "What is done to the cluster while an arm runs. none is the steady state.",
        Page::Timing => "How long each arm runs, how often, and how hard.",
        Page::Reads => "What is loaded before anything is measured, and how reads ask for it.",
        Page::Review => "Every arm the run will take, in the order it takes them.",
    }
}

/// Draw the wizard
///
/// # Arguments
///
/// * `frame` - The frame to draw on
/// * `wizard` - The wizard
pub fn render(frame: &mut Frame, wizard: &Wizard) {
    // what the draft builds, which every page reads from
    let (spec, issues) = wizard.build();
    // a title, the sidebar and page beside each other, the keys and the status line
    let [title, body, keys, status] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(8),
        Constraint::Length(3),
        Constraint::Length(1),
    ])
    .areas(frame.area());
    let [sidebar, page] = Layout::horizontal([Constraint::Length(18), Constraint::Min(20)]).areas(body);
    // the title names the dataset and the cluster the run drives
    let mode = match spec.mode {
        Mode::Owned => "the bench's own cluster",
        Mode::Attach => "an attached cluster",
    };
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(" shoaladm bench run ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(format!("· {} · {mode}", spec.dataset.display())),
        ])),
        title,
    );
    render_sidebar(frame, sidebar, wizard, &spec, &issues);
    match wizard.page {
        Page::Review => render_review(frame, page, wizard, &spec, &issues),
        _ => render_rows(frame, page, wizard, &issues),
    }
    render_keys(frame, keys, wizard);
    render_status(frame, status, wizard, &spec, &issues);
    // a question is drawn over everything
    if let Some(confirm) = wizard.confirm {
        render_confirm(frame, confirm, wizard);
    }
}

/// Draw the list of pages, each with how many errors and warnings it has, and the arm count
///
/// # Arguments
///
/// * `frame` - The frame
/// * `area` - Where to draw
/// * `wizard` - The wizard
/// * `spec` - The spec the draft builds
/// * `issues` - Everything standing in the way
fn render_sidebar(frame: &mut Frame, area: Rect, wizard: &Wizard, spec: &BenchSpec, issues: &[Issue]) {
    // one line per page, the one shown highlighted
    let mut lines: Vec<Line> = Page::ALL
        .iter()
        .enumerate()
        .map(|(index, page)| {
            let count = |severity| {
                issues
                    .iter()
                    .filter(|issue| issue.severity == severity && issue.page == *page)
                    .count()
            };
            let (errors, warnings) = (count(Severity::Error), count(Severity::Warning));
            let mut spans = vec![Span::styled(
                format!(" {}. {:<10}", index + 1, page.title()),
                if *page == wizard.page { focus_style() } else { Style::default() },
            )];
            if errors > 0 {
                spans.push(Span::styled(format!(" ✗{errors}"), severity_style(Severity::Error)));
            } else if warnings > 0 {
                spans.push(Span::styled(format!(" !{warnings}"), severity_style(Severity::Warning)));
            }
            Line::from(spans)
        })
        .collect();
    // the size of the run, under the pages
    lines.push(Line::raw(""));
    lines.push(Line::raw(format!(" {} arms", spec.arms().len())));
    lines.push(Line::raw(format!(" ≥ {}", clock(least_seconds(spec)))));
    frame.render_widget(Paragraph::new(lines).block(Block::default().borders(Borders::RIGHT)), area);
}

/// The line of one row: its label, its value, and the issue on it
///
/// # Arguments
///
/// * `wizard` - The wizard
/// * `row` - The row
/// * `focused` - Whether it has focus
/// * `issues` - Everything standing in the way
fn row_line<'a>(wizard: &Wizard, row: Row, focused: bool, issues: &'a [Issue]) -> Line<'a> {
    // the box or nothing, then the label
    let mut spans = Vec::new();
    match row.kind() {
        RowKind::Check => spans.push(Span::raw(if wizard.checked(row) { " [x] " } else { " [ ] " })),
        RowKind::Text | RowKind::Choice => spans.push(Span::raw("     ")),
    }
    spans.push(Span::styled(
        format!("{:<26}", row.label()),
        if focused { focus_style() } else { Style::default().add_modifier(Modifier::BOLD) },
    ));
    // the value, as its kind draws it
    match row.kind() {
        RowKind::Check => (),
        RowKind::Choice => spans.push(Span::raw(format!(" ‹ {} ›", wizard.value(row)))),
        RowKind::Text => {
            let value = wizard.value(row);
            spans.push(Span::raw(" "));
            if value.is_empty() && !focused {
                spans.push(Span::styled(wizard.placeholder(row), placeholder_style()));
            } else {
                spans.push(Span::raw(value));
            }
            if focused {
                spans.push(Span::styled("▏", Style::default().fg(Color::Cyan)));
            }
        }
    }
    // what is wrong with it, beside it
    if let Some(issue) = issues.iter().find(|issue| issue.row == Some(row)) {
        spans.push(Span::styled(format!("  {}", issue.message), severity_style(issue.severity)));
    }
    Line::from(spans)
}

/// Draw a page of rows, with what the row in focus means beside it
///
/// # Arguments
///
/// * `frame` - The frame
/// * `area` - Where to draw
/// * `wizard` - The wizard
/// * `issues` - Everything standing in the way
fn render_rows(frame: &mut Frame, area: Rect, wizard: &Wizard, issues: &[Issue]) {
    // the rows on the left, what the focused one means on the right, wide enough to read
    let [rows_area, help_area] = Layout::horizontal([Constraint::Min(40), Constraint::Percentage(40)]).areas(area);
    let mut lines = vec![
        Line::styled(format!(" {}", page_intro(wizard.page)), Style::default().fg(Color::Gray)),
        Line::raw(""),
    ];
    let rows = wizard.rows(wizard.page);
    for (index, row) in rows.iter().enumerate() {
        lines.push(row_line(wizard, *row, index == wizard.focus, issues));
    }
    // the refusals on this page that name no row, under them
    let loose: Vec<&Issue> = issues
        .iter()
        .filter(|issue| issue.page == wizard.page && issue.row.is_none())
        .collect();
    if !loose.is_empty() {
        lines.push(Line::raw(""));
        for issue in loose {
            lines.push(Line::styled(format!(" ✗ {}", issue.message), severity_style(issue.severity)));
        }
    }
    frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), rows_area);
    // the focused row's meaning
    let help = wizard.focused().map(Row::help).unwrap_or_default();
    let title = wizard.focused().map(Row::label).unwrap_or_default();
    frame.render_widget(
        Paragraph::new(help)
            .wrap(Wrap { trim: true })
            .block(Block::default().borders(Borders::LEFT).title(format!(" {title} "))),
        help_area,
    );
}

/// A count and what it counts, plural when it is not one
///
/// # Arguments
///
/// * `count` - How many
/// * `what` - What, in the singular
fn count(count: usize, what: &str) -> String {
    if count == 1 {
        format!("1 {what}")
    } else {
        format!("{count} {what}s")
    }
}

/// Draw the review: the run's size, every refusal, the save path and every arm
///
/// # Arguments
///
/// * `frame` - The frame
/// * `area` - Where to draw
/// * `wizard` - The wizard
/// * `spec` - The spec the draft builds
/// * `issues` - Everything standing in the way
fn render_review(frame: &mut Frame, area: Rect, wizard: &Wizard, spec: &BenchSpec, issues: &[Issue]) {
    let arms = spec.arms();
    // the size of it
    let mut lines = vec![
        Line::styled(
            format!(
                " {} arms: {} × {} × {} × {}",
                arms.len(),
                count(spec.workloads.len(), "workload"),
                count(spec.bundles.len(), "bundle size"),
                count(spec.events.len() * spec.overrides.len().max(1), "event"),
                count(spec.runs as usize, "run")
            ),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Line::raw(format!(
            " at least {} measured and warming up ({}s + {}s an arm), before the preload{}",
            clock(least_seconds(spec)),
            spec.warmup,
            spec.duration,
            if spec.mode == Mode::Owned {
                ", and a fresh cluster after every arm that writes or runs an event"
            } else {
                ""
            }
        )),
        Line::raw(""),
    ];
    // what stands in the way, each with its page
    let errors: Vec<&Issue> = issues.iter().filter(|issue| issue.severity == Severity::Error).collect();
    if errors.is_empty() {
        lines.push(Line::styled(" Nothing stands in the way: Enter starts the run.", Style::default().fg(Color::Green)));
    }
    for issue in issues {
        lines.push(Line::styled(
            format!(" {} {}: {}", if issue.severity == Severity::Error { "✗" } else { "!" }, issue.page.title(), issue.message),
            severity_style(issue.severity),
        ));
    }
    lines.push(Line::raw(""));
    lines.push(row_line(wizard, Row::SavePath, wizard.focus == 0, issues));
    lines.push(Line::raw(""));
    // every arm, in the order the run takes them
    for (index, arm) in arms.iter().enumerate() {
        lines.push(Line::raw(format!(" {:>4}. {} run {}", index + 1, arm.id, arm.run)));
    }
    frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }).scroll((wizard.scroll, 0)), area);
}

/// Draw the keys the page shown takes
///
/// # Arguments
///
/// * `frame` - The frame
/// * `area` - Where to draw
/// * `wizard` - The wizard
fn render_keys(frame: &mut Frame, area: Rect, wizard: &Wizard) {
    // the page's own keys, then the ones that work everywhere
    let own = match wizard.page {
        Page::Workloads => "↑↓ move · space tick · + add a custom workload · ctrl-d delete it · type to edit",
        Page::Review => "Enter run · ↑↓ scroll · type the path · ctrl-s save the spec",
        _ => "↑↓ move · space tick or cycle · ←→ cycle · type to edit · ctrl-u clear",
    };
    let lines = vec![
        Line::styled(format!(" {own}"), Style::default().fg(Color::Gray)),
        Line::styled(
            " Tab/PgDn next page · Shift-Tab/PgUp back · ctrl-r run · ctrl-s save spec · Esc leave",
            Style::default().fg(Color::DarkGray),
        ),
    ];
    frame.render_widget(Paragraph::new(lines).block(Block::default().borders(Borders::TOP)), area);
}

/// Draw the message, or what the run would refuse
///
/// # Arguments
///
/// * `frame` - The frame
/// * `area` - Where to draw
/// * `wizard` - The wizard
/// * `spec` - The spec the draft builds
/// * `issues` - Everything standing in the way
fn render_status(frame: &mut Frame, area: Rect, wizard: &Wizard, spec: &BenchSpec, issues: &[Issue]) {
    let line = match &wizard.message {
        Some((severity, message)) => Line::styled(format!(" {message}"), severity_style(*severity)),
        None => {
            let errors = issues.iter().filter(|issue| issue.severity == Severity::Error).count();
            if errors == 0 {
                Line::styled(format!(" ready: {} arms", spec.arms().len()), Style::default().fg(Color::Green))
            } else {
                Line::styled(
                    format!(" {errors} error{} before the run can start", if errors == 1 { "" } else { "s" }),
                    severity_style(Severity::Error),
                )
            }
        }
    };
    frame.render_widget(Paragraph::new(line), area);
}

/// Draw a question over the page
///
/// # Arguments
///
/// * `frame` - The frame
/// * `confirm` - The question
/// * `wizard` - The wizard
fn render_confirm(frame: &mut Frame, confirm: Confirm, wizard: &Wizard) {
    // the question, in a box in the middle
    let text = match confirm {
        Confirm::Quit => "Leave without running? y / n".to_string(),
        Confirm::Overwrite => format!("Replace {}? y / n", wizard.draft.save_path.trim()),
    };
    let area = frame.area();
    let width = (text.chars().count() as u16 + 6).min(area.width);
    let popup = Rect {
        x: area.x + area.width.saturating_sub(width) / 2,
        y: area.y + area.height / 2,
        width,
        height: 3.min(area.height),
    };
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(format!(" {text}")).block(Block::default().borders(Borders::ALL)),
        popup,
    );
}
