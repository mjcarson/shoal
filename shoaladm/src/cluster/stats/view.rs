//! Drawing the stats view
//!
//! The only part of the stats view that draws. A frame is built from a [`Screen`]: the title
//! and each member's state, a tab per metric group, and the tab's metrics as a grid of charts,
//! each with its lines' newest, low, mean and high values under it. Space then `f` fills the
//! body with the selected chart. The open plans are at the foot, and the help page covers all
//! of it while it is open ([F64](../../../../docs/src/features/stats-tui.md)).

use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    symbols::Marker,
    text::{Line, Span},
    widgets::{
        Axis, Block, Cell, Chart, Clear, Dataset, GraphType, Paragraph, Row, Table, Tabs,
    },
};
use shoal::shared::identity::NodeId;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use super::history::Series;
use super::metrics::{GROUPS, METRICS, Metric, Reader, TERM_SECTIONS, TERMS, Unit};
use super::screen::Screen;
use super::{StatsModel, age, live, short, state};

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

/// The keys the foot reminds of
const KEYS: &str =
    "tab group  ←↑↓→ select  space f full screen  [ ] window  p freeze  ? help  q quit";

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
    // the header, the tabs, the body and the foot
    let header = header_lines(screen);
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
    // the grid, or the selected chart filling the body; the grid decides its scroll first, so
    // the tab bar can say which rows are shown
    let shown = if screen.fullscreen {
        render_cell(frame, body, screen, screen.metric_index(), true, true, now);
        None
    } else {
        Some(render_grid(frame, body, screen, now))
    };
    render_tabs(frame, tabs, screen, shown);
    // the open plans, then the keys
    let mut lines: Vec<Line> = plans.into_iter().map(Line::from).collect();
    lines.push(Line::styled(KEYS, dim_style()));
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
    // each group numbered by the key that shows it
    let titles = GROUPS
        .iter()
        .enumerate()
        .map(|(index, (group, _))| format!("{} {group}", index + 1));
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

/// Draw the tab's charts as a grid, keeping the selected one's row in view, and return the
/// grid and its first row drawn
///
/// # Arguments
///
/// * `frame` - The frame
/// * `area` - Where to draw
/// * `screen` - The screen, which the grid's columns and scroll are written to
/// * `now` - The time now
fn render_grid(frame: &mut Frame, area: Rect, screen: &mut Screen, now: Instant) -> (Grid, usize) {
    let metrics = screen.tab_metrics();
    // a tab of member metrics lists every member under each chart, the cluster's one line
    let members = screen
        .latest
        .as_ref()
        .map_or(1, |model| model.view.members.len().max(1));
    let summary = if metrics
        .iter()
        .any(|index| matches!(METRICS[*index].read, Reader::Member(_)))
    {
        members
    } else {
        1
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
            render_cell(frame, *cell, screen, *metric, position == selected, false, now);
        }
    }
    (grid, screen.first_row)
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
/// * `now` - The time now
fn render_cell(
    frame: &mut Frame,
    area: Rect,
    screen: &Screen,
    metric: usize,
    selected: bool,
    full: bool,
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
    let height = u16::try_from(rows.len() + 1).unwrap_or(u16::MAX);
    let [chart, summary] =
        Layout::vertical([Constraint::Min(2), Constraint::Length(height)]).areas(inner);
    render_chart(frame, chart, screen, metric, now);
    render_summary(frame, summary, METRICS[metric].unit, rows);
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
    match METRICS[metric].read {
        Reader::Cluster(read) => vec![SummaryRow {
            name: "cluster".to_string(),
            color: Color::White,
            now: Some(read(&model.view)),
            range: range(Series::Cluster),
        }],
        Reader::Member(read) => {
            let colors = colors(Some(model));
            let mut rows: Vec<SummaryRow> = model
                .view
                .members
                .iter()
                .map(|member| SummaryRow {
                    name: model.label(&member.node),
                    color: colors.get(&member.node).copied().unwrap_or(Color::Gray),
                    now: live(member).map(read),
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
        "Rates are drawn from their ten second windows. The view keeps its own history of what \
         it read, for half an hour, and forgets it when it exits. A member whose figures are \
         stale adds no points, so a line that stops is a member that stopped reporting.",
    ));
    lines.push(Line::default());
    lines.extend(para(
        "shoaladm stats --basic prints the same figures as tables, once or with --watch, and \
         is what a pipe or a script gets. --json prints the leader's answer as it came.",
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
        ("tab, shift-tab, 1-6", "show the next, the previous or a numbered group"),
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
        press(&mut screen, KeyCode::Char('2'));
        let mut terminal = Terminal::new(TestBackend::new(160, 44)).expect("a terminal");
        terminal
            .draw(|frame| render(frame, &mut screen, now))
            .expect("the view draws");
        let drawn = text(&terminal);
        // the title, and each member's state with the stale one's age
        assert!(drawn.contains("shoaladm stats · lab · from hyperion (leader) · version 31"), "{drawn}");
        assert!(drawn.contains("europa down 14s!   hyperion up   titan up"), "{drawn}");
        // the tabs, the window beside them, and every chart of the writes tab by name
        assert!(drawn.contains("1 cluster   2 writes   3 streams"), "{drawn}");
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
}
