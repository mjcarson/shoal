//! Drawing the stats view
//!
//! The only part of the stats view that draws. A frame is built from a [`Screen`]: the metric
//! list on the left, the chosen metric's lines over the window on the right with each line's
//! newest, low, mean and high value under them, the open plans at the foot, and the help page
//! over all of it while it is open ([F64](../../../../docs/src/features/stats-tui.md)).

use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    symbols::Marker,
    text::{Line, Span},
    widgets::{
        Axis, Block, Cell, Chart, Clear, Dataset, GraphType, LegendPosition, List, ListItem,
        ListState, Paragraph, Row, Table,
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

/// How wide the metric list is drawn
const LIST_WIDTH: u16 = 26;

/// How many open plan lines the foot shows at most
const PLAN_LINES: usize = 4;

/// How wide the help page's column of keys is
const KEY_COLUMN: usize = 22;

/// The keys the foot reminds of
const KEYS: &str = "↑↓ metric  ←→ window  p freeze  ? help  q quit";

/// The style of a heading
fn heading_style() -> Style {
    Style::default().add_modifier(Modifier::BOLD)
}

/// The style of text that explains rather than shows
fn dim_style() -> Style {
    Style::default().fg(Color::DarkGray)
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
/// its color from one metric to the next
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

/// Draw the stats view
///
/// The help page's scroll is clamped to its length here, since only the frame knows how many
/// lines fit, which is why the screen is taken mutably.
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
    // the header, the body and the foot
    let plans = plan_lines(screen);
    let foot = u16::try_from(plans.len() + 1).unwrap_or(u16::MAX);
    let [header, body, footer] = Layout::vertical([
        Constraint::Length(2),
        Constraint::Min(8),
        Constraint::Length(foot),
    ])
    .areas(area);
    render_header(frame, header, screen);
    // the list beside the chart, and the chart over its table
    let [list, right] =
        Layout::horizontal([Constraint::Length(LIST_WIDTH), Constraint::Min(20)]).areas(body);
    render_list(frame, list, screen);
    let rows = table_rows(screen, now);
    let table_height = u16::try_from(rows.len() + 3).unwrap_or(u16::MAX).min(14);
    let [chart, table] =
        Layout::vertical([Constraint::Min(6), Constraint::Length(table_height)]).areas(right);
    render_chart(frame, chart, screen, now);
    render_table(frame, table, screen, rows);
    // the open plans, then the keys
    let mut lines: Vec<Line> = plans.into_iter().map(Line::from).collect();
    lines.push(Line::styled(KEYS, dim_style()));
    frame.render_widget(Paragraph::new(lines), footer);
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

/// Draw the title line and the line under it saying what is wrong, if anything
///
/// # Arguments
///
/// * `frame` - The frame
/// * `area` - Where to draw
/// * `screen` - The screen
fn render_header(frame: &mut Frame, area: Rect, screen: &Screen) {
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
    // the failed read, else the note on a partial answer, else whether anything was read
    let status = match (&screen.error, &screen.latest) {
        (Some(error), _) => Line::styled(
            format!("the last read failed: {error}"),
            Style::default().fg(Color::Red),
        ),
        (None, Some(model)) => match &model.note {
            Some(note) => Line::styled(note.clone(), Style::default().fg(Color::Yellow)),
            None if !model.view.is_leader_view() => Line::styled(
                "only the answering node's figures: the leader could not be asked",
                Style::default().fg(Color::Yellow),
            ),
            None => Line::default(),
        },
        (None, None) => Line::styled("waiting for the first answer...", dim_style()),
    };
    frame.render_widget(Paragraph::new(vec![Line::from(spans), status]), area);
}

/// Draw the metric list, grouped, with the charted one selected
///
/// # Arguments
///
/// * `frame` - The frame
/// * `area` - Where to draw
/// * `screen` - The screen
fn render_list(frame: &mut Frame, area: Rect, screen: &Screen) {
    // each group's heading, then its metrics, noting which row the selected one is
    let mut items = Vec::with_capacity(METRICS.len() + GROUPS.len());
    let mut selected = 0;
    for (group, _) in GROUPS {
        items.push(ListItem::new(Line::styled(group, heading_style().fg(Color::DarkGray))));
        for (index, metric) in METRICS.iter().enumerate().filter(|(_, m)| m.group == group) {
            if index == screen.selected {
                selected = items.len();
            }
            items.push(ListItem::new(format!(" {}", metric.name)));
        }
    }
    // the list scrolls itself to keep the selection in view
    let mut state = ListState::default().with_selected(Some(selected));
    let list = List::new(items)
        .block(Block::bordered().title(" metrics "))
        .highlight_style(
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        );
    frame.render_stateful_widget(list, area, &mut state);
}

/// Draw the charted metric's lines over the window
///
/// # Arguments
///
/// * `frame` - The frame
/// * `area` - Where to draw
/// * `screen` - The screen
/// * `now` - The time now
fn render_chart(frame: &mut Frame, area: Rect, screen: &Screen, now: Instant) {
    let metric = screen.metric();
    let window = screen.window();
    let model = screen.latest.as_ref();
    let block = Block::bordered().title(format!(
        " {} · last {} ",
        metric.name,
        span_label(window.as_secs())
    ));
    // every line in the window, named and colored
    let lines = screen
        .history
        .series(screen.selected, window, screen.edge(now));
    if lines.is_empty() {
        let waiting = Paragraph::new(Line::styled(
            "no figures in this window yet: a member sends them about every two seconds",
            dim_style(),
        ))
        .block(block);
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
    let top = if high > 0.0 { high * 1.1 } else { 1.0 };
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
        .block(block)
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
                .labels([
                    metric.unit.format(0.0),
                    metric.unit.format(top / 2.0),
                    metric.unit.format(top),
                ])
                .style(dim_style()),
        )
        .legend_position(Some(LegendPosition::TopLeft))
        .hidden_legend_constraints((Constraint::Ratio(1, 2), Constraint::Ratio(2, 3)));
    frame.render_widget(chart, area);
}

/// One row of the table under the chart
#[derive(Debug, Clone, PartialEq)]
pub struct ValueRow {
    /// The line's name
    pub name: String,
    /// The line's color
    pub color: Color,
    /// The member's state, or nothing for the cluster
    pub state: String,
    /// How old the member's figures are, or nothing for the cluster
    pub age: String,
    /// The newest value, if the figures are current
    pub now: Option<f64>,
    /// The low, mean and high over the window, if any point is in it
    pub range: Option<(f64, f64, f64)>,
}

/// The table's rows: the cluster for a cluster metric, else every member of the answer
///
/// # Arguments
///
/// * `screen` - The screen
/// * `now` - The time now
#[must_use]
pub fn table_rows(screen: &Screen, now: Instant) -> Vec<ValueRow> {
    // nothing read yet has no rows
    let Some(model) = &screen.latest else {
        return Vec::new();
    };
    let metric = screen.metric();
    // each line's points over the window, for its low, mean and high
    let lines: BTreeMap<Series, Vec<(f64, f64)>> = screen
        .history
        .series(screen.selected, screen.window(), screen.edge(now))
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
    match metric.read {
        Reader::Cluster(read) => vec![ValueRow {
            name: "cluster".to_string(),
            color: Color::White,
            state: String::new(),
            age: String::new(),
            now: Some(read(&model.view)),
            range: range(Series::Cluster),
        }],
        Reader::Member(read) => {
            let colors = colors(Some(model));
            let mut rows: Vec<ValueRow> = model
                .view
                .members
                .iter()
                .map(|member| ValueRow {
                    name: model.label(&member.node),
                    color: colors.get(&member.node).copied().unwrap_or(Color::Gray),
                    state: state(member),
                    age: age(member),
                    now: live(member).map(read),
                    range: range(Series::Member(member.node)),
                })
                .collect();
            rows.sort_by(|a, b| a.name.cmp(&b.name));
            rows
        }
    }
}

/// Draw the table of each line's newest, low, mean and high values
///
/// # Arguments
///
/// * `frame` - The frame
/// * `area` - Where to draw
/// * `screen` - The screen
/// * `rows` - The rows
fn render_table(frame: &mut Frame, area: Rect, screen: &Screen, rows: Vec<ValueRow>) {
    let unit = screen.metric().unit;
    let figure = |value: Option<f64>| value.map_or("-".to_string(), |value| unit.format(value));
    // the member column fits the longest name
    let width = rows
        .iter()
        .map(|row| row.name.chars().count())
        .max()
        .unwrap_or(0)
        .max(8);
    let body = rows.into_iter().map(|row| {
        let (low, mean, high) = match row.range {
            Some((low, mean, high)) => (Some(low), Some(mean), Some(high)),
            None => (None, None, None),
        };
        Row::new(vec![
            Cell::from(Span::styled(row.name, Style::default().fg(row.color))),
            Cell::from(row.state),
            Cell::from(row.age),
            Cell::from(figure(row.now)),
            Cell::from(figure(low)),
            Cell::from(figure(mean)),
            Cell::from(figure(high)),
        ])
    });
    let widths = [
        Constraint::Length(u16::try_from(width).unwrap_or(u16::MAX)),
        Constraint::Length(9),
        Constraint::Length(6),
        Constraint::Length(11),
        Constraint::Length(11),
        Constraint::Length(11),
        Constraint::Length(11),
    ];
    let header = Row::new(["member", "state", "age", "now", "low", "mean", "high"])
        .style(heading_style());
    let table = Table::new(body, widths).header(header).block(
        Block::bordered().title(format!(
            " {} ({}) over the last {} ",
            screen.metric().name,
            unit.describe(),
            span_label(screen.window().as_secs())
        )),
    );
    frame.render_widget(table, area);
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
         member sends them. The list on the left chooses what the chart draws: one line per \
         member, or one line for the cluster, over the window shown. The table under the chart \
         gives each line's newest value and its low, mean and high over the window.",
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
        ("↑ ↓ or k j", "choose the metric to chart"),
        ("← → or - +", "show a shorter or a longer window: 1m, 5m, 15m, 30m"),
        ("p or space", "freeze the picture; the figures keep being read underneath"),
        ("? h or F1", "open and close this page"),
        ("PgUp PgDn Home End", "scroll this page"),
        ("q, Esc or ctrl-c", "leave; Esc closes this page first"),
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

    /// An answer from two members on two hosts, reporting at `at_ms`
    ///
    /// # Arguments
    ///
    /// * `at_ms` - When the members derived their figures
    /// * `inserts` - Their applied inserts per second
    fn answer(at_ms: u64, inserts: f64) -> StatsModel {
        let a = "aaaaaaaa-1111-1111-1111-111111111111";
        let b = "bbbbbbbb-2222-2222-2222-222222222222";
        let member = |node: &str, hostname: &str| {
            json!({ "node": node, "state": "up", "report_age_ms": 500, "stats": {
                "node": node, "at_ms": at_ms, "hostname": hostname,
                "total": { "applied": { "inserts": { "r10s": inserts } } }
            } })
        };
        let view = shoal::serde_json::from_value(json!({
            "source": "leader", "answered_by": a, "leader": a, "version": 31,
            "members": [member(a, "hyperion"), member(b, "titan")],
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

    /// The view draws each member by its hostname, the charted metric and its values, the open
    /// plan and the keys, and the help page explains every metric and term
    #[test]
    fn the_view_draws_hostnames_and_help() {
        let start = Instant::now();
        let mut screen = Screen::new("lab", Duration::from_secs(2));
        // two answers, so the chart has two points per member
        screen.observe(Ok(answer(1000, 100.0)), start);
        let now = start + Duration::from_secs(2);
        screen.observe(Ok(answer(3000, 300.0)), now);
        screen.selected = super::super::metrics::index_of("inserts").expect("inserts");
        let mut terminal = Terminal::new(TestBackend::new(160, 44)).expect("a terminal");
        terminal
            .draw(|frame| render(frame, &mut screen, now))
            .expect("the view draws");
        let drawn = text(&terminal);
        // the title names the cluster and the answering member by its hostname
        assert!(drawn.contains("shoaladm stats · lab · from hyperion (leader) · version 31"), "{drawn}");
        // the chart and its table, with both members by name and the newest value
        assert!(drawn.contains("inserts/s · last 5m"), "{drawn}");
        assert!(drawn.contains("hyperion"), "{drawn}");
        assert!(drawn.contains("titan"), "{drawn}");
        assert!(drawn.contains("300"), "{drawn}");
        assert!(drawn.contains("now"), "{drawn}");
        // the open plan and the keys at the foot
        assert!(drawn.contains("rebalance running 2/4 moved"), "{drawn}");
        assert!(drawn.contains("? help"), "{drawn}");
        // the table's range over the window
        let rows = table_rows(&screen, now);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].name, "hyperion");
        assert_eq!(rows[0].now, Some(300.0));
        assert_eq!(rows[0].range, Some((100.0, 200.0, 300.0)));
        // the help page opens over everything and scrolls no further than its end
        screen.handle_key(KeyEvent::from(KeyCode::Char('?')), now);
        screen.handle_key(KeyEvent::from(KeyCode::End), now);
        terminal
            .draw(|frame| render(frame, &mut screen, now))
            .expect("the help page draws");
        let most = help_lines(158, screen.every).len() - 42;
        assert_eq!(screen.help, Some(u16::try_from(most).unwrap()));
        let helped = text(&terminal);
        assert!(helped.contains("what the figures mean"), "{helped}");
        assert!(helped.contains("Keys"), "{helped}");
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
