//! Drawing the stats view
//!
//! The only part of the stats view that draws. A frame is built from a [`Screen`]: the title
//! and each member's state, a tab per metric group, and the tab's metrics as a grid of charts,
//! each with its lines' newest, low, mean and high values under it. Space then `f` fills the
//! body with the selected chart. The open plans are at the foot, and the help page covers all
//! of it while it is open ([F64](../../../../docs/src/features/stats-tui.md)). The home tab
//! draws the cluster's totals over six charts with a line of legend each, and a table of every
//! member's figures under them ([F65](../../../../docs/src/features/query-figures-home-tab.md)).

use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    symbols::Marker,
    text::{Line, Span},
    widgets::{
        Axis, Block, Cell, Chart, Clear, Dataset, GraphType, Paragraph, Row, Table, Tabs, Wrap,
    },
};
use shoal::shared::identity::NodeId;
use shoal::shared::protocol::stats::{NodeStats, QUERY_OPS, READ_OPS, WRITE_OPS};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use super::history::Series;
use super::metrics::{
    GROUPS, METRICS, Metric, Reader, TERM_SECTIONS, TERMS, Unit, all_answers, row_bytes,
};
use super::bench::{BenchPane, Status};
use super::screen::{Screen, TABS, tab_name};
use super::{StatsModel, age, byte_rate, live, rate, short, state};
use crate::cluster::model::bytes;

/// The colors members' lines are drawn in, in the order their names sort
const PALETTE: [Color; 10] = [
    Color::Cyan,
    Color::Yellow,
    Color::Green,
    Color::Magenta,
    Color::LightBlue,
    Color::LightRed,
    Color::LightGreen,
    Color::LightMagenta,
    Color::Blue,
    Color::Red,
];

/// The narrowest a chart of the grid is drawn: its axis, a chart worth reading, and a summary
/// of a member's name and four figures
const MIN_CELL_WIDTH: u16 = 48;

/// The fewest rows a chart of the grid is given, beside its border and its summary
const MIN_CHART_HEIGHT: usize = 5;

/// How wide each figure of a chart's summary is
const FIGURE_WIDTH: u16 = 10;

/// How many open plan lines the foot shows at most
const PLAN_LINES: usize = 4;

/// How wide the help page's column of keys is
const KEY_COLUMN: usize = 24;

/// How many lines the legend under a home tab chart takes
const LEGEND_LINES: usize = 2;

/// The home tab's table of members, as its header names the columns
const HOME_COLUMNS: [&str; 13] = [
    "member",
    "get/s",
    "ins/s",
    "upd/s",
    "del/s",
    "ex/s",
    "err/s",
    "read/s",
    "write/s",
    "p50",
    "p99",
    "rows/budget",
    "resident",
];

/// The keys the foot reminds of
const KEYS: &str =
    "tab next tab  ←↑↓→ select  space f full screen  [ ] window  p freeze  ? help  q quit";

/// The keys the foot reminds of while a benchmark is drawn
const BENCH_KEYS: &str =
    "tab next tab  0 bench  ←↑↓→ select  space f full screen  [ ] window  p freeze  ? help  q stop";

/// The color a kind of query's line is drawn in, the same on every chart
///
/// # Arguments
///
/// * `kind` - The kind, one of the node figures' query kinds
fn kind_color(kind: &str) -> Color {
    match kind {
        "get" => Color::Cyan,
        "exists" => Color::LightBlue,
        "insert" => Color::Green,
        "update" => Color::Yellow,
        "delete" => Color::Magenta,
        "error" => Color::Red,
        _ => Color::Gray,
    }
}

/// The style of a heading
fn heading_style() -> Style {
    Style::default().add_modifier(Modifier::BOLD)
}

/// The style of text that explains rather than shows
fn dim_style() -> Style {
    Style::default().fg(Color::DarkGray)
}

/// The style of the selected chart's border and title
fn selected_style() -> Style {
    Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)
}

/// A span of seconds as the chart's axis writes it
///
/// # Arguments
///
/// * `secs` - The seconds
#[must_use]
pub fn span_label(secs: u64) -> String {
    // whole minutes as minutes, the rest with their seconds
    match (secs / 60, secs % 60) {
        (0, secs) => format!("{secs}s"),
        (minutes, 0) => format!("{minutes}m"),
        (minutes, secs) => format!("{minutes}m{secs:02}s"),
    }
}

/// The color each member's line is drawn in, by the order their names sort, so a member keeps
/// its color from one chart to the next
///
/// # Arguments
///
/// * `model` - The answer shown
fn colors(model: Option<&StatsModel>) -> BTreeMap<NodeId, Color> {
    // no answer yet colors nothing
    let Some(model) = model else {
        return BTreeMap::new();
    };
    // the members by name, each given the next color
    let mut named: Vec<(&String, &NodeId)> = model
        .labels
        .iter()
        .map(|(node, label)| (label, node))
        .collect();
    named.sort();
    named
        .into_iter()
        .enumerate()
        .map(|(index, (_, node))| (*node, PALETTE[index % PALETTE.len()]))
        .collect()
}

/// A line's name and color
///
/// # Arguments
///
/// * `series` - The line
/// * `model` - The answer shown, which names the members
/// * `colors` - Each member's color
fn series_style(
    series: Series,
    model: Option<&StatsModel>,
    colors: &BTreeMap<NodeId, Color>,
) -> (String, Color) {
    match series {
        Series::Cluster => ("cluster".to_string(), Color::White),
        Series::Kind(kind) => (kind.to_string(), kind_color(kind)),
        // a member the answer no longer lists is named by its id and drawn in grey
        Series::Member(node) => (
            model.map_or_else(|| short(&node.0.to_string()), |model| model.label(&node)),
            colors.get(&node).copied().unwrap_or(Color::Gray),
        ),
    }
}

/// How a tab's charts are laid out in an area
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Grid {
    /// Charts in a row
    pub columns: usize,
    /// Rows the tab's charts take
    pub rows: usize,
    /// Rows the area shows at once
    pub visible: usize,
}

/// Lay a tab's charts out in an area: as many columns as fit and no more than make the grid
/// square, and as many rows as fit at a chart's least height
///
/// # Arguments
///
/// * `count` - How many charts the tab has
/// * `width` - The area's width
/// * `height` - The area's height
/// * `summary` - How many lines each chart's summary lists
#[must_use]
pub fn grid(count: usize, width: u16, height: u16, summary: usize) -> Grid {
    let count = count.max(1);
    // the columns that fit, and the side of the smallest square holding every chart
    let fit = usize::from(width / MIN_CELL_WIDTH).max(1);
    let mut square = 1;
    while square * square < count {
        square += 1;
    }
    let columns = fit.min(square);
    let rows = count.div_ceil(columns);
    // a chart's least height: its border, a small chart, and its summary under a header
    let least = 2 + MIN_CHART_HEIGHT + 1 + summary.max(1);
    let visible = (usize::from(height) / least).clamp(1, rows);
    Grid {
        columns,
        rows,
        visible,
    }
}

/// The first row to draw so the selected chart's row is in view, moving as little as it can
///
/// # Arguments
///
/// * `first` - The first row drawn last time
/// * `selected` - The selected chart's row
/// * `grid` - The grid's layout
#[must_use]
pub fn scroll(first: usize, selected: usize, grid: Grid) -> usize {
    // the rows past the last full window are never the first
    let first = first.min(grid.rows - grid.visible);
    if selected < first {
        selected
    } else if selected >= first + grid.visible {
        selected + 1 - grid.visible
    } else {
        first
    }
}

/// Draw the stats view
///
/// The help page's scroll, the grid's columns and the grid's scroll are written back to the
/// screen here, since only the frame knows how much fits, which is why the screen is taken
/// mutably.
///
/// # Arguments
///
/// * `frame` - The frame to draw on
/// * `screen` - What to draw
/// * `now` - The time now
pub fn render(frame: &mut Frame, screen: &mut Screen, now: Instant) {
    let area = frame.area();
    // the help page covers everything while it is open
    if screen.help.is_some() {
        render_help(frame, area, screen);
        return;
    }
    // the header, the tabs, the body and the foot; a benchmark's strip under the header on home
    let mut header = header_lines(screen);
    if let Some(pane) = screen.bench.as_ref().filter(|_| screen.is_home()) {
        header.extend(pane.strip(now).into_iter().map(|line| Line::styled(line, bench_style())));
    }
    let plans = plan_lines(screen);
    let head = u16::try_from(header.len()).unwrap_or(u16::MAX);
    let foot = u16::try_from(plans.len() + 1).unwrap_or(u16::MAX);
    let [top, tabs, body, footer] = Layout::vertical([
        Constraint::Length(head),
        Constraint::Length(1),
        Constraint::Min(6),
        Constraint::Length(foot),
    ])
    .areas(area);
    frame.render_widget(Paragraph::new(header), top);
    // the grid, the home tab, or the selected chart filling the body; the grid decides its
    // scroll first, so the tab bar can say which rows are shown
    let shown = if screen.is_bench() {
        if let Some(pane) = &screen.bench {
            render_bench(frame, body, pane, now);
        }
        None
    } else if screen.fullscreen {
        render_cell(frame, body, screen, screen.metric_index(), true, true, false, now);
        None
    } else if screen.is_home() {
        Some(render_home(frame, body, screen, now))
    } else {
        Some(render_grid(frame, body, screen, false, now))
    };
    render_tabs(frame, tabs, screen, shown);
    // the open plans, then the keys, or the question a quit during a benchmark asks
    let mut lines: Vec<Line> = plans.into_iter().map(Line::from).collect();
    match screen.bench.as_ref() {
        Some(pane) if pane.confirm => lines.push(Line::styled(
            "stop the benchmark? q or y again stops it, tears its cluster down and puts the hosts back; any other key carries on",
            Style::default().fg(Color::Black).bg(Color::Red),
        )),
        Some(pane) if pane.status == Status::Stopping => lines.push(Line::styled(
            "stopping: tearing the cluster down and putting the hosts back...",
            Style::default().fg(Color::Black).bg(Color::Yellow),
        )),
        Some(pane) if pane.finished() => lines.push(Line::styled("the benchmark is over: q leaves", dim_style())),
        Some(_) => lines.push(Line::styled(BENCH_KEYS, dim_style())),
        None => lines.push(Line::styled(KEYS, dim_style())),
    }
    frame.render_widget(Paragraph::new(lines), footer);
    // the shortcut space started, over everything
    if screen.leader {
        render_leader(frame, area);
    }
}

/// The open plans' lines the foot shows
///
/// # Arguments
///
/// * `screen` - The screen
fn plan_lines(screen: &Screen) -> Vec<String> {
    // nothing read yet has no plans to show
    let Some(model) = &screen.latest else {
        return Vec::new();
    };
    // the open ones only, as many as fit, saying how many more there are
    let mut lines = model.plan_lines(0);
    if lines.len() > PLAN_LINES {
        let more = lines.len() - PLAN_LINES + 1;
        lines.truncate(PLAN_LINES - 1);
        lines.push(format!("  ... and {more} more"));
    }
    lines
}

/// The header's lines: the title, each member's state, and what is wrong if anything is
///
/// # Arguments
///
/// * `screen` - The screen
fn header_lines(screen: &Screen) -> Vec<Line<'static>> {
    // who answered, at what version, how often, and whether the picture is frozen
    let mut title = format!("shoaladm stats · {}", screen.title);
    if let Some(model) = &screen.latest {
        let view = &model.view;
        let source = if view.is_leader_view() {
            "leader"
        } else {
            "local view"
        };
        title.push_str(&format!(
            " · from {} ({source}) · version {}",
            model.label(&view.answered_by),
            view.version
        ));
        if let Some(table) = &view.table {
            title.push_str(&format!(" · table {table}"));
        }
    }
    title.push_str(&format!(" · every {}", span_label(screen.every.as_secs().max(1))));
    let mut spans = vec![Span::styled(title, heading_style())];
    if screen.paused_at.is_some() {
        spans.push(Span::styled(
            "  FROZEN (p resumes)",
            Style::default().fg(Color::Black).bg(Color::Yellow),
        ));
    }
    let mut lines = vec![Line::from(spans)];
    // every member's state, and how old its figures are when they are stale
    if let Some(model) = &screen.latest {
        lines.push(member_states(model));
    }
    // the failed read, else the note on a partial answer, else whether anything was read
    let status = match (&screen.error, &screen.latest) {
        (Some(error), _) => Some(Line::styled(
            format!("the last read failed: {error}"),
            Style::default().fg(Color::Red),
        )),
        (None, Some(model)) => match &model.note {
            Some(note) => Some(Line::styled(note.clone(), Style::default().fg(Color::Yellow))),
            None if !model.view.is_leader_view() => Some(Line::styled(
                "only the answering node's figures: the leader could not be asked",
                Style::default().fg(Color::Yellow),
            )),
            None => None,
        },
        (None, None) => Some(Line::styled("waiting for the first answer...", dim_style())),
    };
    lines.extend(status);
    lines
}

/// Every member's name in its color and its state, colored by how it stands
///
/// # Arguments
///
/// * `model` - The answer shown
fn member_states(model: &StatsModel) -> Line<'static> {
    let colors = colors(Some(model));
    // by name, the way the summaries list them
    let mut members: Vec<_> = model.view.members.iter().collect();
    members.sort_by_key(|member| model.label(&member.node));
    let mut spans = Vec::with_capacity(members.len() * 4);
    for member in members {
        if !spans.is_empty() {
            spans.push(Span::raw("   "));
        }
        let color = colors.get(&member.node).copied().unwrap_or(Color::Gray);
        spans.push(Span::styled(model.label(&member.node), Style::default().fg(color)));
        // up is green, down is red, anything between is yellow
        let standing = match member.state.as_str() {
            "up" => Color::Green,
            "down" => Color::Red,
            _ => Color::Yellow,
        };
        spans.push(Span::styled(format!(" {}", state(member)), Style::default().fg(standing)));
        // stale figures say how old they are
        if member.stale {
            spans.push(Span::styled(
                format!(" {}", age(member)),
                Style::default().fg(Color::Yellow),
            ));
        }
    }
    Line::from(spans)
}

/// Draw the tab bar, and the window and the rows shown beside it
///
/// # Arguments
///
/// * `frame` - The frame
/// * `area` - Where to draw
/// * `screen` - The screen
/// * `shown` - The grid and its first row drawn, unless one chart fills the body
fn render_tabs(frame: &mut Frame, area: Rect, screen: &Screen, shown: Option<(Grid, usize)>) {
    let [left, right] =
        Layout::horizontal([Constraint::Min(10), Constraint::Length(30)]).areas(area);
    // each tab numbered by the key that shows it, home first, a benchmark's last as 0
    let titles = (0..screen.tab_count()).map(|tab| {
        let key = if tab == TABS { 0 } else { tab + 1 };
        format!("{key} {}", tab_name(tab))
    });
    let tabs = Tabs::new(titles)
        .select(screen.tab)
        .highlight_style(Style::default().fg(Color::Black).bg(Color::Cyan))
        .divider(" ");
    frame.render_widget(tabs, left);
    // the window, and which rows are shown when the grid does not fit
    let mut beside = format!("last {}", span_label(screen.window().as_secs()));
    if let Some((grid, first)) = shown.filter(|(grid, _)| grid.visible < grid.rows) {
        beside.push_str(&format!(
            " · rows {}-{} of {}",
            first + 1,
            first + grid.visible,
            grid.rows
        ));
    }
    frame.render_widget(
        Paragraph::new(Line::styled(beside, dim_style())).right_aligned(),
        right,
    );
}

/// How many lines a metric's summary lists: a member's each, the cluster's one, or a kind's each
///
/// # Arguments
///
/// * `metric` - The metric
/// * `members` - How many members the answer holds
fn summary_len(metric: &Metric, members: usize) -> usize {
    match metric.read {
        Reader::Member(_) => members,
        Reader::Cluster(_) => 1,
        Reader::Kinds(_) => QUERY_OPS.len(),
    }
}

/// Draw the tab's charts as a grid, keeping the selected one's row in view, and return the
/// grid and its first row drawn
///
/// # Arguments
///
/// * `frame` - The frame
/// * `area` - Where to draw
/// * `screen` - The screen, which the grid's columns and scroll are written to
/// * `compact` - Whether each chart has a line of legend under it rather than its summary
/// * `now` - The time now
fn render_grid(
    frame: &mut Frame,
    area: Rect,
    screen: &mut Screen,
    compact: bool,
    now: Instant,
) -> (Grid, usize) {
    let metrics = screen.tab_metrics();
    // the tallest summary under any of the tab's charts decides how tall a row must be
    let members = screen
        .latest
        .as_ref()
        .map_or(1, |model| model.view.members.len().max(1));
    let summary = if compact {
        LEGEND_LINES
    } else {
        metrics
            .iter()
            .map(|index| summary_len(&METRICS[*index], members))
            .max()
            .unwrap_or(1)
    };
    let grid = grid(metrics.len(), area.width, area.height, summary);
    // the keys move by the columns drawn, and the selected row stays in view
    screen.columns = grid.columns;
    let selected = screen.position();
    screen.first_row = scroll(screen.first_row, selected / grid.columns, grid);
    // the rows shown share the height, and a row's charts share its width
    let rows = Layout::vertical(vec![Constraint::Fill(1); grid.visible]).split(area);
    for (offset, row) in rows.iter().enumerate() {
        let cells = Layout::horizontal(vec![Constraint::Fill(1); grid.columns]).split(*row);
        for (column, cell) in cells.iter().enumerate() {
            let position = (screen.first_row + offset) * grid.columns + column;
            let Some(metric) = metrics.get(position) else {
                break;
            };
            render_cell(frame, *cell, screen, *metric, position == selected, false, compact, now);
        }
    }
    (grid, screen.first_row)
}

/// The cluster's figures the home tab's line of totals gives, summed over the current members
#[derive(Debug, Clone, Default, PartialEq)]
pub struct HomeTotals {
    /// Queries answered per second, of every kind
    pub queries: f64,
    /// Gets and exists answered per second
    pub reads: f64,
    /// Inserts, updates and deletes answered per second
    pub writes: f64,
    /// Failures answered per second
    pub errors: f64,
    /// Answer bytes of gets and exists per second
    pub read_bytes: f64,
    /// Write intent bytes per second, once per row through its leader
    pub write_bytes: f64,
    /// The slowest member's 99th percentile, in milliseconds, if any member timed enough
    pub p99: Option<f64>,
    /// The members' resident memory together
    pub resident: u64,
    /// The rows the members hold in memory together
    pub rows: u64,
    /// The members' eviction budgets together
    pub budget: u64,
    /// The current members with no query figures at all, which run a build from before F65
    pub older: Vec<String>,
}

/// Sum the current members' figures into the home tab's totals
///
/// # Arguments
///
/// * `model` - The answer shown
#[must_use]
pub fn home_totals(model: &StatsModel) -> HomeTotals {
    let mut totals = HomeTotals::default();
    for member in &model.view.members {
        // a stale member's rates are not current, so they are not summed
        let Some(stats) = live(member) else {
            continue;
        };
        let queries = &stats.queries;
        // a member with no query figures at all runs a build that has none
        if queries.is_empty() {
            totals.older.push(model.label(&member.node));
        }
        totals.queries += all_answers(queries);
        totals.reads += queries.rate_of(&READ_OPS);
        totals.writes += queries.rate_of(&WRITE_OPS);
        totals.errors += queries.rate_of(&["error"]);
        totals.read_bytes += queries.bytes_out_of(&READ_OPS);
        totals.write_bytes += row_bytes(&stats.total.led);
        // the slowest member's tail is the cluster's
        totals.p99 = match (totals.p99, queries.p99_ms) {
            (Some(worst), Some(p99)) => Some(worst.max(p99)),
            (worst, p99) => worst.or(p99),
        };
        totals.resident = totals.resident.saturating_add(stats.resident_bytes);
        totals.rows = totals.rows.saturating_add(stats.memory_bytes);
        totals.budget = totals.budget.saturating_add(stats.memory_budget);
    }
    totals.older.sort();
    totals
}

/// The home tab's line of totals, and a note on the members that report no query figures
///
/// # Arguments
///
/// * `screen` - The screen
fn totals_lines(screen: &Screen) -> Vec<Line<'static>> {
    // nothing read yet has nothing to sum; the header says it is waiting
    let Some(model) = &screen.latest else {
        return Vec::new();
    };
    let totals = home_totals(model);
    // each figure named in grey, its value in bold
    let value = heading_style();
    let mut spans = Vec::with_capacity(20);
    let mut figure = |name: &str, text: String, style: Style| {
        if !spans.is_empty() {
            spans.push(Span::raw("   "));
        }
        spans.push(Span::styled(format!("{name} "), dim_style()));
        spans.push(Span::styled(text, style));
    };
    figure("queries", format!("{}/s", rate(totals.queries)), value);
    figure("reads", format!("{}/s", rate(totals.reads)), value);
    figure("writes", format!("{}/s", rate(totals.writes)), value);
    // a failure is worth seeing at a glance
    let errors = if totals.errors > 0.0 {
        value.fg(Color::Red)
    } else {
        value
    };
    figure("errors", format!("{}/s", rate(totals.errors)), errors);
    figure("read", byte_rate(totals.read_bytes), value);
    figure("write", byte_rate(totals.write_bytes), value);
    figure(
        "p99",
        totals
            .p99
            .map_or("-".to_string(), |p99| Unit::Millis.format(p99)),
        value,
    );
    figure("resident", bytes(totals.resident), value);
    figure(
        "rows",
        format!("{} of {}", bytes(totals.rows), bytes(totals.budget)),
        value,
    );
    let mut lines = vec![Line::from(spans)];
    // a member on an older build has no figures to sum, which the totals do not show
    if !totals.older.is_empty() {
        lines.push(Line::styled(
            format!(
                "no query figures from {}: a build from before F65",
                totals.older.join(", ")
            ),
            Style::default().fg(Color::Yellow),
        ));
    }
    lines
}

/// A member's figures as the home tab's table writes them, after its name, in the order of
/// [`HOME_COLUMNS`]
///
/// # Arguments
///
/// * `stats` - Its figures, if they are current
fn member_cells(stats: Option<&NodeStats>) -> Vec<String> {
    // a stale member's figures, or an older build's missing ones, are dashes
    let Some(stats) = stats else {
        return vec!["-".to_string(); HOME_COLUMNS.len() - 1];
    };
    let queries = &stats.queries;
    let known = !queries.is_empty();
    let per_sec = |ops: &[&str]| {
        if known {
            rate(queries.rate_of(ops))
        } else {
            "-".to_string()
        }
    };
    let millis = |millis: Option<f64>| millis.map_or("-".to_string(), |ms| format!("{ms:.2}"));
    vec![
        per_sec(&["get"]),
        per_sec(&["insert"]),
        per_sec(&["update"]),
        per_sec(&["delete"]),
        per_sec(&["exists"]),
        per_sec(&["error"]),
        if known {
            byte_rate(queries.bytes_out_of(&READ_OPS))
        } else {
            "-".to_string()
        },
        byte_rate(row_bytes(&stats.total.led)),
        millis(queries.p50_ms),
        millis(queries.p99_ms),
        format!(
            "{}/{}",
            bytes(stats.memory_bytes),
            bytes(stats.memory_budget)
        ),
        bytes(stats.resident_bytes),
    ]
}

/// The home tab's cluster figures: every current member's summed, and the slowest waits, after
/// the row's name, in the order of [`HOME_COLUMNS`]
///
/// # Arguments
///
/// * `model` - The answer shown
fn cluster_cells(model: &StatsModel) -> Vec<String> {
    let current: Vec<&NodeStats> = model.view.members.iter().filter_map(live).collect();
    // each kind's answers summed over the members
    let per_sec = |ops: &[&str]| {
        rate(
            current
                .iter()
                .fold(0.0, |sum, stats| sum + stats.queries.rate_of(ops)),
        )
    };
    // the slowest member's wait
    let slowest = |pick: fn(&NodeStats) -> Option<f64>| {
        current
            .iter()
            .filter_map(|stats| pick(stats))
            .reduce(f64::max)
            .map_or("-".to_string(), |ms| format!("{ms:.2}"))
    };
    let totals = home_totals(model);
    vec![
        per_sec(&["get"]),
        per_sec(&["insert"]),
        per_sec(&["update"]),
        per_sec(&["delete"]),
        per_sec(&["exists"]),
        per_sec(&["error"]),
        byte_rate(totals.read_bytes),
        byte_rate(totals.write_bytes),
        slowest(|stats| stats.queries.p50_ms),
        slowest(|stats| stats.queries.p99_ms),
        format!("{}/{}", bytes(totals.rows), bytes(totals.budget)),
        bytes(totals.resident),
    ]
}

/// Which of the home tab's columns fit a width, the least needed ones left out first
///
/// Each column has the width its widest figure needs and a rank: the name, the gets, the
/// speeds, the p99 and the resident memory go last, exists and deletes first.
///
/// # Arguments
///
/// * `name` - How wide the name column is
/// * `width` - The width the table has
#[must_use]
pub fn home_columns(name: u16, width: u16) -> Vec<(usize, u16)> {
    // each column's width and how late it is left out, in the order of the header
    const FIGURES: [(u16, u8); 12] = [
        (7, 9),  // get/s
        (7, 7),  // ins/s
        (7, 6),  // upd/s
        (7, 2),  // del/s
        (7, 1),  // ex/s
        (7, 7),  // err/s
        (11, 9), // read/s
        (11, 9), // write/s
        (8, 5),  // p50
        (8, 8),  // p99
        (17, 3), // rows/budget
        (9, 8),  // resident
    ];
    let mut shown: Vec<(usize, u16)> = std::iter::once((0, name))
        .chain(
            FIGURES
                .iter()
                .enumerate()
                .map(|(index, (width, _))| (index + 1, *width)),
        )
        .collect();
    // the columns and the one space between each pair
    let needed = |shown: &[(usize, u16)]| -> u16 {
        let widths: u16 = shown.iter().map(|(_, width)| *width).sum();
        widths + u16::try_from(shown.len().saturating_sub(1)).unwrap_or(u16::MAX)
    };
    // leave out the least needed column until the rest fit, never the name
    while needed(&shown) > width && shown.len() > 1 {
        let Some(least) = shown
            .iter()
            .enumerate()
            .skip(1)
            .min_by_key(|(_, (column, _))| FIGURES[column - 1].1)
            .map(|(position, _)| position)
        else {
            break;
        };
        shown.remove(least);
    }
    shown
}

/// Draw the home tab's table: a row per member by name, and the cluster's under them, with as
/// many columns as the width holds
///
/// # Arguments
///
/// * `frame` - The frame
/// * `area` - Where to draw
/// * `model` - The answer shown
fn render_members(frame: &mut Frame, area: Rect, model: &StatsModel) {
    let colors = colors(Some(model));
    // the name column fits the longest name, and the figures that fit beside it are shown
    let name = model
        .labels
        .values()
        .map(|label| label.chars().count())
        .max()
        .unwrap_or(0)
        .max("cluster".len());
    let columns = home_columns(u16::try_from(name).unwrap_or(u16::MAX), area.width);
    // a row of cells, the name first and then the shown figures
    let row = |name: Span<'static>, cells: Vec<String>| {
        Row::new(columns.iter().map(|(column, _)| match column {
            0 => Cell::from(name.clone()),
            column => Cell::from(cells[column - 1].clone()),
        }))
    };
    // by name, the way the summaries list them
    let mut members: Vec<_> = model.view.members.iter().collect();
    members.sort_by_key(|member| model.label(&member.node));
    let mut rows: Vec<Row> = members
        .into_iter()
        .map(|member| {
            let color = colors.get(&member.node).copied().unwrap_or(Color::Gray);
            let name = Span::styled(model.label(&member.node), Style::default().fg(color));
            row(name, member_cells(live(member)))
        })
        .collect();
    rows.push(row(Span::raw("cluster"), cluster_cells(model)).style(heading_style()));
    let widths = columns.iter().map(|(_, width)| Constraint::Length(*width));
    let header = Row::new(columns.iter().map(|(column, _)| HOME_COLUMNS[*column])).style(dim_style());
    frame.render_widget(Table::new(rows, widths).header(header), area);
}

/// Draw the home tab: the cluster's totals, six charts with a line of legend each, and a table
/// of every member's figures, and return the grid and its first row drawn
///
/// # Arguments
///
/// * `frame` - The frame
/// * `area` - Where to draw
/// * `screen` - The screen, which the grid's columns and scroll are written to
/// * `now` - The time now
fn render_home(frame: &mut Frame, area: Rect, screen: &mut Screen, now: Instant) -> (Grid, usize) {
    // the totals take as many lines as they wrap to
    let totals = totals_lines(screen);
    let width = usize::from(area.width.max(1));
    let strip: usize = totals
        .iter()
        .map(|line| line.width().div_ceil(width).max(1))
        .sum();
    // the table a row per member, its header and the cluster's row, once there is an answer
    let table = screen
        .latest
        .as_ref()
        .map_or(0, |model| model.view.members.len() + 2);
    let [top, charts, bottom] = Layout::vertical([
        Constraint::Length(u16::try_from(strip).unwrap_or(u16::MAX)),
        Constraint::Min(6),
        Constraint::Length(u16::try_from(table).unwrap_or(u16::MAX)),
    ])
    .areas(area);
    frame.render_widget(Paragraph::new(totals).wrap(Wrap { trim: true }), top);
    // the charts as any tab's grid, each with a line of legend rather than its summary
    let shown = render_grid(frame, charts, screen, true, now);
    if let Some(model) = &screen.latest {
        render_members(frame, bottom, model);
    }
    shown
}

/// Draw one metric's chart with its summary under it, in a border naming it
///
/// # Arguments
///
/// * `frame` - The frame
/// * `area` - Where to draw
/// * `screen` - The screen
/// * `metric` - The metric's index into [`METRICS`]
/// * `selected` - Whether it is the selected chart
/// * `full` - Whether it fills the body
/// * `compact` - Whether a line of legend goes under it rather than its summary
/// * `now` - The time now
#[allow(clippy::too_many_arguments)]
fn render_cell(
    frame: &mut Frame,
    area: Rect,
    screen: &Screen,
    metric: usize,
    selected: bool,
    full: bool,
    compact: bool,
    now: Instant,
) {
    let name = METRICS[metric].name;
    // the selected chart's border stands out, and a full screen one says how to go back
    let title = if full {
        format!(
            " {name} · last {} · space f back ",
            span_label(screen.window().as_secs())
        )
    } else {
        format!(" {name} ")
    };
    let border = if selected {
        selected_style()
    } else {
        dim_style()
    };
    let block = Block::bordered()
        .title(Span::styled(title, if selected { selected_style() } else { heading_style() }))
        .border_style(border);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    // the chart over its summary, which keeps its rows whatever the chart is left
    let rows = summary_rows(screen, metric, now);
    let height = if compact {
        LEGEND_LINES
    } else {
        rows.len() + 1
    };
    let height = u16::try_from(height).unwrap_or(u16::MAX);
    let [chart, summary] =
        Layout::vertical([Constraint::Min(2), Constraint::Length(height)]).areas(inner);
    render_chart(frame, chart, screen, metric, now);
    if compact {
        render_legend(frame, summary, METRICS[metric].unit, &rows);
    } else {
        render_summary(frame, summary, METRICS[metric].unit, rows);
    }
}

/// Draw a chart's legend: each line's name in its color and its newest value, wrapped
///
/// # Arguments
///
/// * `frame` - The frame
/// * `area` - Where to draw
/// * `unit` - How the metric's values are written
/// * `rows` - The chart's summary rows
fn render_legend(frame: &mut Frame, area: Rect, unit: Unit, rows: &[SummaryRow]) {
    // a name and a figure per line, a gap between them
    let mut spans = Vec::with_capacity(rows.len() * 3);
    for row in rows {
        if !spans.is_empty() {
            spans.push(Span::raw("  "));
        }
        spans.push(Span::styled(row.name.clone(), Style::default().fg(row.color)));
        let figure = row.now.map_or("-".to_string(), |value| unit.format(value));
        spans.push(Span::raw(format!(" {figure}")));
    }
    frame.render_widget(Paragraph::new(Line::from(spans)).wrap(Wrap { trim: true }), area);
}

/// Draw a metric's lines over the window
///
/// # Arguments
///
/// * `frame` - The frame
/// * `area` - Where to draw
/// * `screen` - The screen
/// * `metric` - The metric's index into [`METRICS`]
/// * `now` - The time now
fn render_chart(frame: &mut Frame, area: Rect, screen: &Screen, metric: usize, now: Instant) {
    let unit = METRICS[metric].unit;
    let window = screen.window();
    let model = screen.latest.as_ref();
    // every line in the window, named and colored
    let lines = screen.history.series(metric, window, screen.edge(now));
    if lines.is_empty() {
        let waiting = Paragraph::new(Line::styled("no figures in this window yet", dim_style()));
        frame.render_widget(waiting, area);
        return;
    }
    let colors = colors(model);
    let mut styled: Vec<(String, Color, Vec<(f64, f64)>)> = lines
        .into_iter()
        .map(|(series, points)| {
            let (name, color) = series_style(series, model, &colors);
            (name, color, points)
        })
        .collect();
    styled.sort_by(|a, b| a.0.cmp(&b.0));
    // the value axis from zero to a little over the highest point
    let high = styled
        .iter()
        .flat_map(|(_, _, points)| points.iter().map(|(_, value)| *value))
        .fold(0.0_f64, f64::max);
    let top = if high > 0.0 {
        high * 1.1
    } else {
        // a flat chart's axis still reads in whole units: a KiB of bytes, two of a count
        match unit {
            Unit::Bytes | Unit::BytesPerSec => 1024.0,
            Unit::Count => 2.0,
            _ => 1.0,
        }
    };
    // the summary's names in their colors are the legend, so the chart draws none
    let datasets = styled
        .iter()
        .map(|(name, color, points)| {
            Dataset::default()
                .name(name.clone())
                .marker(Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(*color))
                .data(points)
        })
        .collect();
    let span = window.as_secs_f64();
    let chart = Chart::new(datasets)
        .x_axis(
            Axis::default()
                .bounds([-span, 0.0])
                .labels([
                    format!("-{}", span_label(window.as_secs())),
                    format!("-{}", span_label(window.as_secs() / 2)),
                    "now".to_string(),
                ])
                .style(dim_style()),
        )
        .y_axis(
            Axis::default()
                .bounds([0.0, top])
                .labels([unit.format(0.0), unit.format(top / 2.0), unit.format(top)])
                .style(dim_style()),
        )
        .legend_position(None);
    frame.render_widget(chart, area);
}

/// One line of the summary under a chart
#[derive(Debug, Clone, PartialEq)]
pub struct SummaryRow {
    /// The line's name
    pub name: String,
    /// The line's color
    pub color: Color,
    /// The newest value, if the figures are current
    pub now: Option<f64>,
    /// The low, mean and high over the window, if any point is in it
    pub range: Option<(f64, f64, f64)>,
}

/// A chart's summary: the cluster for a cluster metric, else every member of the answer
///
/// # Arguments
///
/// * `screen` - The screen
/// * `metric` - The metric's index into [`METRICS`]
/// * `now` - The time now
#[must_use]
pub fn summary_rows(screen: &Screen, metric: usize, now: Instant) -> Vec<SummaryRow> {
    // nothing read yet has no rows
    let Some(model) = &screen.latest else {
        return Vec::new();
    };
    // each line's points over the window, for its low, mean and high
    let lines: BTreeMap<Series, Vec<(f64, f64)>> = screen
        .history
        .series(metric, screen.window(), screen.edge(now))
        .into_iter()
        .collect();
    let range = |series: Series| {
        lines.get(&series).map(|points| {
            let values = points.iter().map(|(_, value)| *value);
            let low = values.clone().fold(f64::INFINITY, f64::min);
            let high = values.clone().fold(f64::NEG_INFINITY, f64::max);
            let mean = values.sum::<f64>() / points.len() as f64;
            (low, mean, high)
        })
    };
    // a figure that is not a number, such as a wait nobody timed, is not known
    let known = |value: f64| Some(value).filter(|value| value.is_finite());
    match METRICS[metric].read {
        Reader::Cluster(read) => vec![SummaryRow {
            name: "cluster".to_string(),
            color: Color::White,
            now: known(read(&model.view)),
            range: range(Series::Cluster),
        }],
        // a row per kind, in the order the kinds are listed
        Reader::Kinds(read) => read(&model.view)
            .into_iter()
            .map(|(kind, value)| SummaryRow {
                name: kind.to_string(),
                color: kind_color(kind),
                now: known(value),
                range: range(Series::Kind(kind)),
            })
            .collect(),
        Reader::Member(read) => {
            let colors = colors(Some(model));
            let mut rows: Vec<SummaryRow> = model
                .view
                .members
                .iter()
                .map(|member| SummaryRow {
                    name: model.label(&member.node),
                    color: colors.get(&member.node).copied().unwrap_or(Color::Gray),
                    now: live(member).map(read).and_then(known),
                    range: range(Series::Member(member.node)),
                })
                .collect();
            rows.sort_by(|a, b| a.name.cmp(&b.name));
            rows
        }
    }
}

/// Draw a chart's summary: each line's name in its color, and its newest, low, mean and high
///
/// # Arguments
///
/// * `frame` - The frame
/// * `area` - Where to draw
/// * `unit` - How the metric's values are written
/// * `rows` - The rows
fn render_summary(frame: &mut Frame, area: Rect, unit: Unit, rows: Vec<SummaryRow>) {
    let figure = |value: Option<f64>| value.map_or("-".to_string(), |value| unit.format(value));
    // the name column fits the longest name
    let width = rows
        .iter()
        .map(|row| row.name.chars().count())
        .max()
        .unwrap_or(0)
        .max(6);
    let body = rows.into_iter().map(|row| {
        let (low, mean, high) = match row.range {
            Some((low, mean, high)) => (Some(low), Some(mean), Some(high)),
            None => (None, None, None),
        };
        Row::new(vec![
            Cell::from(Span::styled(row.name, Style::default().fg(row.color))),
            Cell::from(figure(row.now)),
            Cell::from(figure(low)),
            Cell::from(figure(mean)),
            Cell::from(figure(high)),
        ])
    });
    let widths = [
        Constraint::Length(u16::try_from(width).unwrap_or(u16::MAX)),
        Constraint::Length(FIGURE_WIDTH),
        Constraint::Length(FIGURE_WIDTH),
        Constraint::Length(FIGURE_WIDTH),
        Constraint::Length(FIGURE_WIDTH),
    ];
    let header = Row::new(["member", "now", "low", "mean", "high"]).style(dim_style());
    frame.render_widget(Table::new(body, widths).header(header), area);
}

/// Draw the shortcuts space started, in the bottom right corner
///
/// # Arguments
///
/// * `frame` - The frame
/// * `area` - The whole screen
fn render_leader(frame: &mut Frame, area: Rect) {
    // a small box clear of the foot's last line
    let width = 30.min(area.width);
    let height = 4.min(area.height);
    let popup = Rect::new(
        area.x + area.width.saturating_sub(width + 1),
        area.y + area.height.saturating_sub(height + 2),
        width,
        height,
    );
    let key = |key: &'static str, what: &'static str| {
        Line::from(vec![
            Span::styled(format!(" {key:<5}"), heading_style().fg(Color::Yellow)),
            Span::raw(what),
        ])
    };
    let lines = vec![key("f", "full screen / back"), key("esc", "cancel")];
    let block = Block::bordered()
        .title(" space ")
        .border_style(Style::default().fg(Color::Gray));
    frame.render_widget(Clear, popup);
    frame.render_widget(Paragraph::new(lines).block(block), popup);
}

/// The style a benchmark's strip is drawn in
fn bench_style() -> Style {
    Style::default().fg(Color::LightCyan)
}

/// Draw a running benchmark: its arms, the current arm's client figures as charts with its
/// event's marks, and what is worth warning about ([F66](../../../../docs/src/features/dataset-benchmarks.md))
///
/// # Arguments
///
/// * `frame` - The frame
/// * `area` - Where to draw
/// * `pane` - The benchmark
/// * `now` - The time now
fn render_bench(frame: &mut Frame, area: Rect, pane: &BenchPane, now: Instant) {
    // the strip on top, the arms beside the charts, the warnings and the log under them
    let strip: Vec<Line> = pane
        .strip(now)
        .into_iter()
        .map(|line| Line::styled(line, bench_style()))
        .collect();
    let [top, middle, bottom] = Layout::vertical([
        Constraint::Length(strip.len() as u16),
        Constraint::Min(8),
        Constraint::Length(6),
    ])
    .areas(area);
    frame.render_widget(Paragraph::new(strip), top);
    let [arms, charts] =
        Layout::horizontal([Constraint::Percentage(38), Constraint::Percentage(62)]).areas(middle);
    render_bench_arms(frame, arms, pane);
    let [rates, waits] = Layout::vertical([Constraint::Percentage(50), Constraint::Percentage(50)]).areas(charts);
    // operations a second by kind, then the client's latency per query and per bundle
    let series = |read: fn(&shoal_loadgen::window::WindowSummary) -> f64| -> Vec<(f64, f64)> {
        pane.seconds.iter().map(|sample| (sample.at as f64, read(&sample.summary))).collect()
    };
    render_bench_chart(
        frame,
        rates,
        pane,
        "client ops/s (from send)",
        Unit::PerSec,
        vec![
            ("read".to_string(), kind_color("get"), series(|summary| summary.read.per_sec)),
            ("insert".to_string(), kind_color("insert"), series(|summary| summary.insert.per_sec)),
        ],
    );
    render_bench_chart(
        frame,
        waits,
        pane,
        "client p99 ms (from send; a node's own is from its frame's arrival)",
        Unit::Millis,
        vec![
            ("read".to_string(), kind_color("get"), series(|summary| summary.read.latency.p99_ms)),
            ("insert".to_string(), kind_color("insert"), series(|summary| summary.insert.latency.p99_ms)),
            ("bundle".to_string(), Color::Magenta, series(|summary| summary.bundle.p99_ms)),
        ],
    );
    // what is worth warning about, then the newest lines
    let mut lines: Vec<Line> = pane
        .warnings()
        .into_iter()
        .map(|warning| Line::styled(format!("! {warning}"), Style::default().fg(Color::Yellow)))
        .collect();
    let room = usize::from(bottom.height).saturating_sub(lines.len());
    lines.extend(
        pane.log
            .iter()
            .rev()
            .take(room)
            .rev()
            .map(|line| Line::styled(line.clone(), dim_style())),
    );
    frame.render_widget(Paragraph::new(lines), bottom);
}

/// Draw the run's arms, done, running and to come, keeping the running one in view
///
/// # Arguments
///
/// * `frame` - The frame
/// * `area` - Where to draw
/// * `pane` - The benchmark
fn render_bench_arms(frame: &mut Frame, area: Rect, pane: &BenchPane) {
    let visible = usize::from(area.height.saturating_sub(1)).max(1);
    let current = pane.current.unwrap_or(0);
    let first = current.saturating_sub(visible / 2).min(pane.arms.len().saturating_sub(visible));
    let rows = pane.arms.iter().enumerate().skip(first).take(visible).map(|(index, row)| {
        // done, running or to come, and what a done one measured
        let (marker, style) = match (&row.summary, pane.current == Some(index)) {
            (Some(_), _) => ("✓", Style::default()),
            (None, true) => ("▶", selected_style()),
            (None, false) => ("·", dim_style()),
        };
        let measured = row.summary.as_ref().map_or(String::new(), |summary| {
            let p99 = summary.read.latency.p99_ms.max(summary.insert.latency.p99_ms);
            let mut text = format!("{:.0}/s p99 {:.1}ms", summary.ops_per_sec(), p99);
            if summary.read.failed() + summary.insert.failed() > 0 {
                text.push_str(&format!(" {} failed", summary.read.failed() + summary.insert.failed()));
            }
            if row.ended_early.is_some() {
                text.push_str(" early");
            }
            text
        });
        Row::new(vec![
            Cell::from(marker),
            Cell::from(row.arm.id.0.clone()),
            Cell::from(format!("{}", row.arm.run)),
            Cell::from(measured),
        ])
        .style(style)
    });
    let table = Table::new(
        rows,
        [Constraint::Length(1), Constraint::Min(18), Constraint::Length(3), Constraint::Min(16)],
    )
    .header(Row::new(vec!["", "arm", "run", "measured"]).style(heading_style()));
    frame.render_widget(table, area);
}

/// Draw one of a benchmark's charts over the current arm, with its event's marks as rules
///
/// # Arguments
///
/// * `frame` - The frame
/// * `area` - Where to draw
/// * `pane` - The benchmark
/// * `title` - What the chart shows
/// * `unit` - Its values' unit
/// * `lines` - Each line's name, color and points
fn render_bench_chart(
    frame: &mut Frame,
    area: Rect,
    pane: &BenchPane,
    title: &str,
    unit: Unit,
    lines: Vec<(String, Color, Vec<(f64, f64)>)>,
) {
    let [head, body] = Layout::vertical([Constraint::Length(1), Constraint::Min(3)]).areas(area);
    // the title with each line's name in its color
    let mut spans = vec![Span::styled(format!("{title}  "), heading_style())];
    for (name, color, _) in &lines {
        spans.push(Span::styled(format!("{name} "), Style::default().fg(*color)));
    }
    frame.render_widget(Paragraph::new(Line::from(spans)), head);
    if pane.seconds.is_empty() {
        frame.render_widget(Paragraph::new(Line::styled("waiting for the arm's first second", dim_style())), body);
        return;
    }
    // the axes: the arm's seconds so far, and zero to a little over the highest point
    let right = pane.seconds.last().map_or(1.0, |sample| sample.at as f64).max(1.0);
    let high = lines
        .iter()
        .flat_map(|(_, _, points)| points.iter().map(|(_, value)| *value))
        .fold(0.0_f64, f64::max);
    let top = if high > 0.0 { high * 1.1 } else { 1.0 };
    // each mark as a rule from the floor to the top
    let rules: Vec<Vec<(f64, f64)>> = pane
        .marks
        .iter()
        .map(|mark| {
            let at = mark.at_ms as f64 / 1000.0;
            vec![(at, 0.0), (at, top)]
        })
        .collect();
    let mut datasets: Vec<Dataset> = lines
        .iter()
        .map(|(name, color, points)| {
            Dataset::default()
                .name(name.clone())
                .marker(Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(*color))
                .data(points)
        })
        .collect();
    datasets.extend(rules.iter().map(|rule| {
        Dataset::default()
            .marker(Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::default().fg(Color::Yellow))
            .data(rule)
    }));
    let chart = Chart::new(datasets)
        .x_axis(
            Axis::default()
                .bounds([0.0, right])
                .labels(["0s".to_string(), format!("{:.0}s", right / 2.0), format!("{right:.0}s")])
                .style(dim_style()),
        )
        .y_axis(
            Axis::default()
                .bounds([0.0, top])
                .labels([unit.format(0.0), unit.format(top / 2.0), unit.format(top)])
                .style(dim_style()),
        )
        .legend_position(None);
    frame.render_widget(chart, body);
}

/// Wrap text to a width at its spaces
///
/// # Arguments
///
/// * `text` - The text
/// * `width` - The widest a line may be
#[must_use]
pub fn wrap(text: &str, width: usize) -> Vec<String> {
    let width = width.max(10);
    let mut lines = Vec::new();
    let mut line = String::new();
    for word in text.split_whitespace() {
        // a word that does not fit starts the next line
        if !line.is_empty() && line.chars().count() + 1 + word.chars().count() > width {
            lines.push(std::mem::take(&mut line));
        }
        if !line.is_empty() {
            line.push(' ');
        }
        line.push_str(word);
    }
    if !line.is_empty() {
        lines.push(line);
    }
    lines
}

/// A name and what it means as help lines: the name, then its text indented under it
///
/// # Arguments
///
/// * `name` - The name, and its unit if it has one
/// * `text` - What it means
/// * `width` - The widest a line may be
fn entry(name: String, text: &str, width: usize) -> Vec<Line<'static>> {
    let mut lines = vec![Line::from(vec![Span::raw("  "), Span::styled(name, heading_style())])];
    lines.extend(
        wrap(text, width.saturating_sub(6))
            .into_iter()
            .map(|line| Line::raw(format!("      {line}"))),
    );
    lines
}

/// Every line of the help page, wrapped to a width
///
/// # Arguments
///
/// * `width` - The widest a line may be
/// * `every` - How often the figures are read
#[must_use]
pub fn help_lines(width: usize, every: Duration) -> Vec<Line<'static>> {
    // a heading wraps like any other text, so a narrow terminal loses none of it
    let heading = |text: &str| {
        wrap(text, width)
            .into_iter()
            .map(|line| Line::styled(line, heading_style().fg(Color::Cyan)))
            .collect::<Vec<_>>()
    };
    let para = |text: &str| {
        wrap(text, width.saturating_sub(2))
            .into_iter()
            .map(|line| Line::raw(format!("  {line}")))
            .collect::<Vec<_>>()
    };
    let mut lines = heading("Reading the view");
    lines.push(Line::default());
    lines.extend(para(&format!(
        "The figures are read from the control leader every {}, which is about how often each \
         member sends them. Each tab holds one group of metrics, and each metric is charted on \
         its own: a line per member, or one line for the cluster, over the window shown beside \
         the tabs. Under each chart, each line's name in its color gives its newest value and \
         its low, mean and high over the window. Space then f shows the selected chart on its \
         own, and again brings the grid back. The line under the title gives each member's \
         state, with the age of its figures when they are stale.",
        span_label(every.as_secs().max(1))
    )));
    lines.push(Line::default());
    lines.extend(para(
        "The home tab, which the view opens on, gives the cluster's totals on one line, six \
         charts each with a line of legend giving every line's newest value, and a table of \
         every member's answers by kind, read and write speed, waits and memory. Queries are \
         counted by the member their client connected to, once each.",
    ));
    lines.push(Line::default());
    lines.extend(para(
        "Rates are drawn from their ten second windows. The view keeps its own history of what \
         it read, for half an hour, and forgets it when it exits. A member whose figures are \
         stale adds no points, so a line that stops is a member that stopped reporting.",
    ));
    lines.push(Line::default());
    lines.extend(para(
        "shoaladm stats --basic prints the same figures as tables, once or with --watch, and \
         is what a pipe or a script gets. --json prints the leader's answer as it came.",
    ));
    lines.push(Line::default());
    lines.extend(para(
        "shoaladm bench run draws this view with a benchmark in it: a strip on the home tab \
         with the arm running and what the driver measures, and a bench tab (0 or b) charting \
         the arm's seconds with its event's marks as rules. The driver's latency is from each \
         query's send, per query and per bundle; the members' own p99 here is from when a \
         bundle's frame arrived, so a large bundle's queries wait in it before that clock \
         starts. q stops a benchmark only when pressed twice, and the view stays up while it \
         tears its cluster down and puts the hosts back.",
    ));
    // every metric, under its group
    for (group, about) in GROUPS {
        lines.push(Line::default());
        lines.extend(heading(&format!("{group}: {about}")));
        for metric in METRICS.iter().filter(|metric| metric.group == group) {
            lines.extend(metric_entry(metric, width));
        }
    }
    // every other word, under its section
    for (section, title) in TERM_SECTIONS {
        lines.push(Line::default());
        lines.extend(heading(title));
        for term in TERMS.iter().filter(|term| term.section == section) {
            lines.extend(entry(term.name.to_string(), term.help, width));
        }
    }
    // and the keys
    lines.push(Line::default());
    lines.extend(heading("Keys"));
    for (keys, what) in [
        ("tab, shift-tab, 1-8", "show the next, the previous or a numbered tab; 1 is home"),
        ("← ↑ ↓ → or h k j l", "select a chart; with one chart shown, show the previous or next"),
        ("space f", "show the selected chart on its own, and the grid again"),
        ("[ ] or - +", "show a shorter or a longer window: 1m, 5m, 15m, 30m"),
        ("p", "freeze the picture; the figures keep being read underneath"),
        ("? or F1", "open and close this page"),
        ("PgUp PgDn Home End", "scroll this page"),
        ("q, Esc or ctrl-c", "leave; Esc closes this page or a chart shown on its own first"),
    ] {
        // the keys in a column of their own, and what they do wrapped beside them
        let text = wrap(what, width.saturating_sub(KEY_COLUMN));
        for (index, line) in text.into_iter().enumerate() {
            let keys = if index == 0 { keys } else { "" };
            lines.push(Line::from(vec![
                Span::styled(format!("  {keys:<width$}", width = KEY_COLUMN - 2), heading_style()),
                Span::raw(line),
            ]));
        }
    }
    lines
}

/// A metric's help lines: its name and unit, then what it means
///
/// # Arguments
///
/// * `metric` - The metric
/// * `width` - The widest a line may be
fn metric_entry(metric: &Metric, width: usize) -> Vec<Line<'static>> {
    // the unit is said beside the name, except for a count, which needs none
    let name = match metric.unit {
        Unit::Count => metric.name.to_string(),
        unit => format!("{} ({})", metric.name, unit.describe()),
    };
    entry(name, metric.help, width)
}

/// Draw the help page over the whole screen, clamping its scroll to its length
///
/// # Arguments
///
/// * `frame` - The frame
/// * `area` - Where to draw
/// * `screen` - The screen, whose scroll is clamped
fn render_help(frame: &mut Frame, area: Rect, screen: &mut Screen) {
    let block = Block::bordered()
        .title(" shoaladm stats: what the figures mean ")
        .title_bottom(" ↑↓ PgUp PgDn scroll · Esc or ? closes · q quits ");
    let inner = block.inner(area);
    let lines = help_lines(usize::from(inner.width), screen.every);
    // the page never scrolls past its last line
    let most = u16::try_from(lines.len().saturating_sub(usize::from(inner.height)))
        .unwrap_or(u16::MAX);
    let scroll = screen.help.unwrap_or(0).min(most);
    screen.help = Some(scroll);
    frame.render_widget(Clear, area);
    frame.render_widget(Paragraph::new(lines).block(block).scroll((scroll, 0)), area);
}


#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::{KeyCode, KeyEvent};
    use ratatui::{Terminal, backend::TestBackend};
    use shoal::serde_json::json;

    /// An answer from two members on two hosts reporting at `at_ms`, and a third down with stale
    /// figures
    ///
    /// # Arguments
    ///
    /// * `at_ms` - When the live members derived their figures
    /// * `inserts` - Their applied inserts per second
    fn answer(at_ms: u64, inserts: f64) -> StatsModel {
        let a = "aaaaaaaa-1111-1111-1111-111111111111";
        let b = "bbbbbbbb-2222-2222-2222-222222222222";
        let c = "cccccccc-3333-3333-3333-333333333333";
        let member = |node: &str, hostname: &str| {
            json!({ "node": node, "state": "up", "report_age_ms": 500, "stats": {
                "node": node, "at_ms": at_ms, "hostname": hostname,
                "total": { "applied": { "inserts": { "r10s": inserts } } }
            } })
        };
        let view = shoal::serde_json::from_value(json!({
            "source": "leader", "answered_by": a, "leader": a, "version": 31,
            "members": [
                member(a, "hyperion"),
                member(b, "titan"),
                { "node": c, "state": "down", "stale": true, "report_age_ms": 14000, "stats": {
                    "node": c, "at_ms": 1, "hostname": "europa"
                } }
            ],
            "plans": [{ "op": "33333333-3333-3333-3333-333333333333", "kind": "rebalance",
                        "phase": "running", "steps_total": 4, "moved": 2 }]
        }))
        .expect("an answer decodes");
        StatsModel::new(view, None)
    }

    /// Everything a test backend's buffer holds, row by row
    ///
    /// # Arguments
    ///
    /// * `terminal` - The terminal drawn on
    fn text(terminal: &Terminal<TestBackend>) -> String {
        let buffer = terminal.backend().buffer();
        let width = usize::from(buffer.area.width);
        buffer
            .content()
            .chunks(width)
            .map(|row| row.iter().map(|cell| cell.symbol()).collect::<String>())
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// A tab draws a chart per metric with a summary under each and no state or age in it, the
    /// header gives each member's state, space f fills the body with the selected chart, space
    /// shows its shortcut, and the help page explains every metric and term within its width
    #[test]
    fn the_view_draws_a_tab_of_charts() {
        let start = Instant::now();
        let mut screen = Screen::new("lab", Duration::from_secs(2));
        // two answers, so every chart has two points per live member
        screen.observe(Ok(answer(1000, 100.0)), start);
        let now = start + Duration::from_secs(2);
        screen.observe(Ok(answer(3000, 300.0)), now);
        let press = |screen: &mut Screen, code: KeyCode| screen.handle_key(KeyEvent::from(code), now);
        press(&mut screen, KeyCode::Char('4'));
        let mut terminal = Terminal::new(TestBackend::new(160, 44)).expect("a terminal");
        terminal
            .draw(|frame| render(frame, &mut screen, now))
            .expect("the view draws");
        let drawn = text(&terminal);
        // the title, and each member's state with the stale one's age
        assert!(drawn.contains("shoaladm stats · lab · from hyperion (leader) · version 31"), "{drawn}");
        assert!(drawn.contains("europa down 14s!   hyperion up   titan up"), "{drawn}");
        // the tabs, the window beside them, and every chart of the writes tab by name
        assert!(drawn.contains("1 home   2 queries   3 cluster   4 writes   5 streams"), "{drawn}");
        assert!(drawn.contains("last 5m"), "{drawn}");
        for metric in screen.tab_metrics() {
            assert!(drawn.contains(METRICS[metric].name), "{} is not drawn: {drawn}", METRICS[metric].name);
        }
        // seven charts in rows of three, all of which fit
        assert_eq!(screen.columns, 3);
        assert!(!drawn.contains("rows 1-"), "{drawn}");
        // a summary under each chart, with no state or age column
        assert!(drawn.contains("member   now"), "{drawn}");
        assert!(drawn.contains("1.0KiB/s"), "a flat byte chart reads in whole units: {drawn}");
        assert!(!drawn.contains("state"), "{drawn}");
        assert!(drawn.contains("300"), "{drawn}");
        // the open plan and the keys at the foot
        assert!(drawn.contains("rebalance running 2/4 moved"), "{drawn}");
        assert!(drawn.contains("space f full screen"), "{drawn}");
        // a summary's figures: the stale member's newest is unknown, the others' ranges known
        let inserts = crate::cluster::stats::metrics::index_of("inserts").expect("inserts");
        let rows = summary_rows(&screen, inserts, now);
        let names: Vec<&str> = rows.iter().map(|row| row.name.as_str()).collect();
        assert_eq!(names, vec!["europa", "hyperion", "titan"]);
        assert_eq!(rows[0].now, None);
        assert_eq!(rows[1].now, Some(300.0));
        assert_eq!(rows[1].range, Some((100.0, 200.0, 300.0)));
        // space shows its shortcut, and f fills the body with the selected chart
        press(&mut screen, KeyCode::Right);
        press(&mut screen, KeyCode::Char(' '));
        terminal.draw(|frame| render(frame, &mut screen, now)).expect("the shortcut draws");
        assert!(text(&terminal).contains("full screen / back"));
        press(&mut screen, KeyCode::Char('f'));
        terminal.draw(|frame| render(frame, &mut screen, now)).expect("one chart draws");
        let full = text(&terminal);
        assert!(full.contains("inserts/s · last 5m · space f back"), "{full}");
        assert!(!full.contains("misses/s"), "{full}");
        assert!(!full.contains("full screen / back"), "{full}");
        // the help page opens over everything and scrolls no further than its end
        press(&mut screen, KeyCode::Char('?'));
        press(&mut screen, KeyCode::End);
        terminal.draw(|frame| render(frame, &mut screen, now)).expect("the help page draws");
        let most = help_lines(158, screen.every).len() - 42;
        assert_eq!(screen.help, Some(u16::try_from(most).unwrap()));
        let helped = text(&terminal);
        assert!(helped.contains("what the figures mean"), "{helped}");
        assert!(helped.contains("space f"), "{helped}");
        // every metric and every term is on the page, with its unit where it has one
        let page = help_lines(100, screen.every)
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        for metric in METRICS {
            assert!(page.contains(metric.name), "{} is not explained", metric.name);
        }
        for term in TERMS {
            assert!(page.contains(term.name), "{} is not explained", term.name);
        }
        assert!(page.contains("inserts/s (per second)"), "{page}");
        // and no line is wider than the page
        assert!(help_lines(60, screen.every).iter().all(|line| line.width() <= 60));
    }

    /// An answer for the home tab: hyperion answering gets, inserts and a few failures with its
    /// waits timed, titan on a build with no query figures, and europa down and stale
    ///
    /// # Arguments
    ///
    /// * `at_ms` - When the live members derived their figures
    fn home_answer(at_ms: u64) -> StatsModel {
        let a = "aaaaaaaa-1111-1111-1111-111111111111";
        let b = "bbbbbbbb-2222-2222-2222-222222222222";
        let c = "cccccccc-3333-3333-3333-333333333333";
        let gib = 1024u64 * 1024 * 1024;
        let view = shoal::serde_json::from_value(json!({
            "source": "leader", "answered_by": a, "leader": a, "version": 40,
            "members": [
                { "node": a, "state": "up", "report_age_ms": 500, "stats": {
                    "node": a, "at_ms": at_ms, "hostname": "hyperion",
                    "total": { "led": { "insert_bytes": { "r10s": 10240.0 } } },
                    "memory_bytes": gib, "memory_budget": 4 * gib, "resident_bytes": 2 * gib,
                    "queries": {
                        "sampled_every": 1, "p50_ms": 0.4, "p99_ms": 3.5,
                        "bytes_in": { "r10s": 4096.0 },
                        "ops": [
                            { "op": "get", "rate": { "r10s": 300.0 },
                              "bytes_out": { "r10s": 30720.0 }, "p50_ms": 0.3, "p99_ms": 2.0 },
                            { "op": "insert", "rate": { "r10s": 100.0 },
                              "p50_ms": 1.0, "p99_ms": 3.5 },
                            { "op": "error", "rate": { "r10s": 2.0 } }
                        ]
                    }
                } },
                { "node": b, "state": "up", "report_age_ms": 500, "stats": {
                    "node": b, "at_ms": at_ms, "hostname": "titan",
                    "memory_bytes": gib, "memory_budget": 4 * gib, "resident_bytes": gib
                } },
                { "node": c, "state": "down", "stale": true, "report_age_ms": 14000, "stats": {
                    "node": c, "at_ms": 1, "hostname": "europa"
                } }
            ]
        }))
        .expect("an answer decodes");
        StatsModel::new(view, None)
    }

    /// The home tab sums the current members into one line of totals, charts six figures with a
    /// line of legend each, lists every member's answers by kind, speeds, waits and memory with
    /// the cluster's under them, names a member on an older build, and space f shows one of its
    /// charts with the full summary
    #[test]
    fn the_view_draws_the_home_tab() {
        let start = Instant::now();
        let mut screen = Screen::new("lab", Duration::from_secs(2));
        screen.observe(Ok(home_answer(1000)), start);
        let now = start + Duration::from_secs(2);
        screen.observe(Ok(home_answer(3000)), now);
        // the totals: only the current members, the stale one's figures left out
        let totals = home_totals(screen.latest.as_ref().expect("an answer"));
        assert!((totals.queries - 402.0).abs() < 1e-9, "{totals:?}");
        assert!((totals.reads - 300.0).abs() < 1e-9);
        assert!((totals.writes - 100.0).abs() < 1e-9);
        assert!((totals.errors - 2.0).abs() < 1e-9);
        assert!((totals.read_bytes - 30720.0).abs() < 1e-9);
        assert!((totals.write_bytes - 10240.0).abs() < 1e-9);
        assert_eq!(totals.p99, Some(3.5));
        assert_eq!(totals.resident, 3 * 1024 * 1024 * 1024);
        assert_eq!(totals.older, vec!["titan".to_string()]);
        // the home tab is what the view opens on
        let mut terminal = Terminal::new(TestBackend::new(160, 44)).expect("a terminal");
        terminal
            .draw(|frame| render(frame, &mut screen, now))
            .expect("the home tab draws");
        let drawn = text(&terminal);
        assert!(drawn.contains("1 home   2 queries   3 cluster"), "{drawn}");
        // the line of totals, and the member with no query figures named under it
        assert!(drawn.contains("queries 402/s   reads 300/s   writes 100/s   errors 2.0/s"), "{drawn}");
        assert!(drawn.contains("read 30.0KiB/s   write 10.0KiB/s   p99 3.50ms"), "{drawn}");
        assert!(drawn.contains("rows 2.0GiB of 8.0GiB"), "{drawn}");
        assert!(drawn.contains("no query figures from titan: a build from before F65"), "{drawn}");
        // the six charts, each with a line of legend giving its lines' newest values
        for key in crate::cluster::stats::metrics::HOME {
            let index = crate::cluster::stats::metrics::index_of(key).expect("a home metric");
            assert!(drawn.contains(METRICS[index].name), "{key} is not drawn: {drawn}");
        }
        assert!(drawn.contains("get 300"), "{drawn}");
        assert!(drawn.contains("insert 100"), "{drawn}");
        assert_eq!(screen.columns, 3);
        // the table: every member by name, a dash for what is not known, and the cluster's row
        assert!(drawn.contains("get/s   ins/s   upd/s   del/s   ex/s    err/s"), "{drawn}");
        assert!(drawn.contains("rows/budget"), "{drawn}");
        let row = |name: &str| {
            drawn
                .lines()
                .find(|line| line.trim_start().starts_with(name) && line.contains('/'))
                .unwrap_or_else(|| panic!("no row for {name}: {drawn}"))
                .split_whitespace()
                .collect::<Vec<_>>()
        };
        let hyperion = row("hyperion");
        assert_eq!(&hyperion[1..7], ["300", "100", "0", "0", "0", "2.0"], "{hyperion:?}");
        assert_eq!(&hyperion[7..11], ["30.0KiB/s", "10.0KiB/s", "0.40", "3.50"], "{hyperion:?}");
        assert_eq!(&hyperion[11..], ["1.0GiB/4.0GiB", "2.0GiB"], "{hyperion:?}");
        let titan = row("titan");
        assert_eq!(&titan[1..8], ["-", "-", "-", "-", "-", "-", "-"], "{titan:?}");
        assert_eq!(titan[8], "0B/s", "{titan:?}");
        let cluster = row("cluster");
        assert_eq!(&cluster[1..3], ["300", "100"], "{cluster:?}");
        assert_eq!(&cluster[9..11], ["0.40", "3.50"], "{cluster:?}");
        // space f shows the selected chart with its full summary, a row per kind
        let press = |screen: &mut Screen, code: KeyCode| screen.handle_key(KeyEvent::from(code), now);
        press(&mut screen, KeyCode::Char(' '));
        press(&mut screen, KeyCode::Char('f'));
        terminal.draw(|frame| render(frame, &mut screen, now)).expect("one chart draws");
        let full = text(&terminal);
        assert!(full.contains("ops/s by kind · last 5m · space f back"), "{full}");
        let ops = crate::cluster::stats::metrics::index_of("ops_by_kind").expect("the kinds metric");
        let kinds: Vec<String> = summary_rows(&screen, ops, now).into_iter().map(|row| row.name).collect();
        assert_eq!(kinds, ["get", "exists", "insert", "update", "delete", "error"]);
        assert!(full.contains("exists"), "{full}");
        // and a wait nobody timed is not known rather than a number
        let p99 = crate::cluster::stats::metrics::index_of("p99").expect("p99");
        let waits = summary_rows(&screen, p99, now);
        assert_eq!(waits.iter().find(|row| row.name == "titan").and_then(|row| row.now), None);
        // a short, narrow terminal still draws it, scrolling the charts
        press(&mut screen, KeyCode::Esc);
        let mut small = Terminal::new(TestBackend::new(100, 30)).expect("a terminal");
        small.draw(|frame| render(frame, &mut screen, now)).expect("a small home tab draws");
        let drawn = text(&small);
        assert!(drawn.contains("rows 1-"), "{drawn}");
        // its table leaves out the least needed columns rather than cutting a name short
        let header = drawn
            .lines()
            .find(|line| line.trim_start().starts_with("member"))
            .unwrap_or_else(|| panic!("no table header: {drawn}"));
        assert!(header.contains("resident") && header.contains("p99"), "{header}");
        assert!(!header.contains("ex/s") && !header.contains("rows/budget"), "{header}");
        assert!(drawn.lines().any(|line| line.starts_with("hyperion ")), "{drawn}");
        // every column fits a wide table, the least needed go first, and the name never does
        assert_eq!(home_columns(8, 160).len(), HOME_COLUMNS.len());
        let narrow: Vec<&str> = home_columns(8, 100)
            .iter()
            .map(|(column, _)| HOME_COLUMNS[*column])
            .collect();
        assert_eq!(
            narrow,
            ["member", "get/s", "ins/s", "upd/s", "err/s", "read/s", "write/s", "p50", "p99", "resident"]
        );
        assert_eq!(home_columns(8, 4), vec![(0, 8)]);
    }

    /// A tab's charts take as many columns as fit and no more than a square needs, and a grid
    /// taller than its area scrolls to keep the selected row in view
    #[test]
    fn the_grid_fits_its_area() {
        // columns by count on a wide screen: one, a pair, two by two, three by three
        assert_eq!(grid(1, 160, 40, 3).columns, 1);
        assert_eq!(grid(2, 160, 40, 3), Grid { columns: 2, rows: 1, visible: 1 });
        assert_eq!(grid(4, 160, 40, 3), Grid { columns: 2, rows: 2, visible: 2 });
        assert_eq!(grid(7, 160, 40, 3), Grid { columns: 3, rows: 3, visible: 3 });
        assert_eq!(grid(9, 160, 40, 3), Grid { columns: 3, rows: 3, visible: 3 });
        // a narrow screen holds fewer columns, and a short one fewer rows, but always one
        assert_eq!(grid(9, 100, 40, 3).columns, 2);
        assert_eq!(grid(9, 30, 40, 3).columns, 1);
        assert_eq!(grid(9, 100, 25, 3), Grid { columns: 2, rows: 5, visible: 2 });
        assert_eq!(grid(9, 100, 5, 3).visible, 1);
        // more members under each chart make a row taller
        assert_eq!(grid(9, 160, 40, 10).visible, 2);
        // the scroll follows the selection down and back up, moving as little as it can
        let tall = Grid { columns: 2, rows: 5, visible: 2 };
        assert_eq!(scroll(0, 1, tall), 0);
        assert_eq!(scroll(0, 2, tall), 1);
        assert_eq!(scroll(1, 4, tall), 3);
        assert_eq!(scroll(3, 3, tall), 3);
        assert_eq!(scroll(3, 0, tall), 0);
        // and never starts past the last full window
        assert_eq!(scroll(9, 4, tall), 3);
    }

    /// Axis spans and wrapping read the way the view uses them
    #[test]
    fn spans_and_wrapping() {
        assert_eq!(span_label(30), "30s");
        assert_eq!(span_label(300), "5m");
        assert_eq!(span_label(150), "2m30s");
        assert_eq!(wrap("one two three four", 10), vec!["one two", "three four"]);
        assert!(wrap("", 10).is_empty());
    }

    /// A benchmark draws its strip on home, its tab with its arms and charts and its marks, and
    /// asks before it stops ([F66](../../../../docs/src/features/dataset-benchmarks.md))
    #[test]
    fn a_benchmark_draws_its_strip_and_tab() {
        use shoal_loadgen::events::Mark;
        use shoal_loadgen::progress::{BenchEvent, Phase};
        use shoal_loadgen::results::{SecondPhase, SecondSample};
        use shoal_loadgen::spec::BenchSpec;
        let start = Instant::now();
        let mut screen = Screen::with_bench("lab", Duration::from_secs(2), start);
        screen.observe(Ok(answer(1000, 100.0)), start);
        let spec = BenchSpec {
            dataset: "d".into(),
            runs: 1,
            bundles: vec![16],
            ..BenchSpec::default()
        };
        let arms = spec.arms();
        let pane = screen.bench.as_mut().unwrap();
        pane.apply(BenchEvent::Planned { label: "nightly".to_string(), arms: arms.clone() }, start);
        pane.apply(BenchEvent::ArmStarted { index: 2, arm: arms[2].clone(), warmup: 0, duration: 30 }, start);
        pane.apply(BenchEvent::Phase(Phase::Measure), start);
        for at in 0..5u64 {
            let mut sample = SecondSample {
                at,
                phase: SecondPhase::Measure,
                summary: Default::default(),
                driver_cpu_pct: 40.0,
            };
            sample.summary.read.per_sec = 1000.0 + at as f64;
            sample.summary.read.latency.p99_ms = 2.5;
            pane.apply(BenchEvent::Second(sample), start);
        }
        pane.apply(BenchEvent::Mark(Mark { kind: "kill".to_string(), at_ms: 2000, note: Some("titan".to_string()) }), start);
        let now = start + Duration::from_secs(5);
        let mut terminal = Terminal::new(TestBackend::new(170, 48)).expect("a terminal");
        // the home tab has the strip over the cluster's figures, and the bench tab is listed
        terminal.draw(|frame| render(frame, &mut screen, now)).expect("drawn");
        let home = text(&terminal);
        assert!(home.contains("bench nightly · arm 3/4 rw50/b16/none run 0 · measure"), "{home}");
        assert!(home.contains("client (from send): read 1004/s"), "{home}");
        assert!(home.contains("0 bench"), "{home}");
        assert!(home.contains("q stop"), "{home}");
        // the bench tab lists the arms and charts the arm with its mark in the log
        screen.handle_key(KeyEvent::from(KeyCode::Char('b')), now);
        terminal.draw(|frame| render(frame, &mut screen, now)).expect("drawn");
        let tab = text(&terminal);
        assert!(tab.contains("read100/b16/none"), "{tab}");
        assert!(tab.contains("client ops/s (from send)"), "{tab}");
        assert!(tab.contains("a node's own is from its frame's arrival"), "{tab}");
        assert!(tab.contains("kill at 2.0s: titan"), "{tab}");
        // a quit asks first
        screen.handle_key(KeyEvent::from(KeyCode::Char('q')), now);
        terminal.draw(|frame| render(frame, &mut screen, now)).expect("drawn");
        assert!(text(&terminal).contains("stop the benchmark?"));
    }

    /// Figures from one live member with every number the view reads set above zero
    ///
    /// Built from the wire form the way a node's answer is, so a field the frame leaves out
    /// when zero is set here by name; a metric whose reader reads a field this leaves at zero
    /// fails [`every_metric_reaches_the_view`], which names it.
    ///
    /// # Arguments
    ///
    /// * `at_ms` - When the member derived its figures
    /// * `scale` - What every figure is multiplied by, so two answers differ
    fn every_figure(at_ms: u64, scale: f64) -> StatsModel {
        use shoal::shared::protocol::stats::{NodeStats, OpStats, QUERY_OPS, Rates, WriteRates};
        let node = "aaaaaaaa-1111-1111-1111-111111111111";
        let rates = |value: f64| Rates {
            r10s: value * scale,
            r1m: value * scale,
            r5m: value * scale,
        };
        // every count and gauge the frame always carries, through the wire form
        let mut stats: NodeStats = shoal::serde_json::from_value(json!({
            "node": node, "at_ms": at_ms, "hostname": "hyperion", "shards": 2,
            "total": {
                "table": "", "groups": 4, "groups_led": 3, "tablets": 4, "tablets_led": 3,
                "partitions": 900, "partitions_led": 800, "chained": 5, "bytes": 1 << 30,
                "bytes_led": 1 << 29
            },
            "free_bytes": 1u64 << 40, "volatile_bytes": 1 << 20, "memory_bytes": 1 << 30,
            "memory_budget": 1u64 << 32, "archive_map_bytes": 1 << 22,
            "table_index_bytes": 1 << 23, "wal_index_bytes": 1 << 21, "lru_bytes": 1 << 24,
            "resident_bytes": 1u64 << 31, "wal_segments": 6, "compacting_segments": 1,
            "apply_lag": 12, "pending_bytes": 4096
        }))
        .expect("figures decode");
        // the rates, which the frame leaves out at zero
        let writes = WriteRates {
            inserts: rates(100.0),
            updates: rates(20.0),
            deletes: rates(10.0),
            insert_bytes: rates(10_240.0),
            update_bytes: rates(2_048.0),
            delete_bytes: rates(512.0),
            misses: rates(1.0),
        };
        stats.total.applied = writes.clone();
        stats.total.led = writes;
        stats.stream_sent = rates(4096.0);
        stats.stream_received = rates(2048.0);
        stats.wal_syncs_per_sec = 50.0 * scale;
        stats.wal_bytes_per_sec = 65_536.0 * scale;
        stats.wal_sync_ms = 0.8;
        stats.wal_appends_per_sync = 4.0;
        stats.shard_writes_per_sec = vec![60.0 * scale, 40.0 * scale];
        // every kind of answer, timed
        stats.queries.sampled_every = 1;
        stats.queries.p50_ms = Some(0.4);
        stats.queries.p99_ms = Some(3.5);
        stats.queries.bytes_in = rates(4096.0);
        stats.queries.ops = QUERY_OPS
            .iter()
            .map(|op| OpStats {
                op: (*op).to_string(),
                rate: rates(100.0),
                bytes_out: rates(1024.0),
                p50_ms: Some(0.3),
                p99_ms: Some(2.0),
                answers_total: 1000,
                bytes_out_total: 1 << 20,
            })
            .collect();
        let view = shoal::serde_json::from_value(json!({
            "source": "leader", "answered_by": node, "leader": node, "version": 7,
            "members": [{ "node": node, "state": "up", "report_age_ms": 500,
                          "stats": shoal::serde_json::to_value(&stats).expect("figures encode") }]
        }))
        .expect("an answer decodes");
        StatsModel::new(view, None)
    }

    /// Every metric of the catalog reaches the view from a member's figures: each one reads
    /// above zero, has a line in the history with a point above zero (a line per kind for a
    /// per kind metric) and a current value in its summary, and the home tab draws every one of
    /// its figures with no dash and no wait
    #[test]
    fn every_metric_reaches_the_view() {
        use crate::cluster::stats::metrics::values;
        use shoal::shared::protocol::stats::QUERY_OPS;
        let start = Instant::now();
        let mut screen = Screen::new("lab", Duration::from_secs(2));
        screen.observe(Ok(every_figure(1000, 1.0)), start);
        let now = start + Duration::from_secs(2);
        screen.observe(Ok(every_figure(3000, 2.0)), now);
        let latest = screen.latest.clone().expect("an answer");
        let mut flat = Vec::new();
        for (index, metric) in METRICS.iter().enumerate() {
            // the reader finds a figure above zero
            let read = values(metric, &latest.view);
            if read.is_empty() || read.iter().any(|(_, value)| !(*value > 0.0)) {
                flat.push(format!("{} reads {read:?}", metric.key));
            }
            // the history holds a line to draw, every point of it a number
            let lines = screen.history.series(index, Duration::from_secs(300), now);
            let expected = match metric.read {
                Reader::Kinds(_) => QUERY_OPS.len(),
                Reader::Member(_) | Reader::Cluster(_) => 1,
            };
            if lines.len() != expected
                || lines.iter().any(|(_, points)| points.iter().all(|(_, value)| *value <= 0.0))
            {
                flat.push(format!("{} has the lines {lines:?}", metric.key));
            }
        }
        assert!(flat.is_empty(), "metrics that never reach the view: {flat:#?}");
        // the home tab: every chart's legend has a value, and no member cell is a dash
        let mut terminal = Terminal::new(TestBackend::new(160, 44)).expect("a terminal");
        terminal
            .draw(|frame| render(frame, &mut screen, now))
            .expect("the home tab draws");
        let drawn = text(&terminal);
        assert!(!drawn.contains("waiting"), "{drawn}");
        assert!(!drawn.contains("no query figures"), "{drawn}");
        let row = drawn
            .lines()
            .find(|line| line.trim_start().starts_with("hyperion") && line.contains('/'))
            .unwrap_or_else(|| panic!("no row for hyperion: {drawn}"));
        assert!(!row.split_whitespace().any(|cell| cell == "-"), "{row}");
        // and every other tab draws each of its charts with no wait
        for tab in 1..=GROUPS.len() {
            screen.tab = tab;
            terminal.draw(|frame| render(frame, &mut screen, now)).expect("a tab draws");
            let drawn = text(&terminal);
            assert!(!drawn.contains("waiting"), "tab {tab}: {drawn}");
        }
    }
}
