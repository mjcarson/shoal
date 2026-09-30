//! What the stats view shows, and what each key does to it
//!
//! The view has a tab per metric group, and each tab draws every metric of its group as a chart
//! of its own. The arrows select a chart, space then `f` shows the selected one full screen, and
//! Esc brings the grid back ([F64](../../../../docs/src/features/stats-tui.md)).
//!
//! Nothing here draws: [`super::view`] draws a [`Screen`], so every key and every sample can be
//! tested without a terminal. The grid's shape depends on the terminal, so the view writes the
//! columns it drew and the row it scrolled to back here, and the keys move by them.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use std::time::{Duration, Instant};

use super::StatsModel;
use super::history::History;
use super::metrics::{GROUPS, METRICS, Metric, in_group};

/// The windows a chart can show, shortest first
pub const WINDOWS: [Duration; 4] = [
    Duration::from_secs(60),
    Duration::from_secs(5 * 60),
    Duration::from_secs(15 * 60),
    Duration::from_secs(30 * 60),
];

/// The window a chart starts at: five minutes, the longest of the figures' own windows
pub const DEFAULT_WINDOW: usize = 1;

/// How many lines a page key scrolls the help page by
const HELP_PAGE: u16 = 10;

/// What the loop should do after a key
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Keep drawing
    Continue,
    /// Restore the terminal and leave
    Quit,
}

/// Everything the stats view shows
#[derive(Debug)]
pub struct Screen {
    /// The cluster's name, for the title
    pub title: String,
    /// How often the figures are read
    pub every: Duration,
    /// The newest answer shown
    pub latest: Option<StatsModel>,
    /// Why the last read failed, if it did
    pub error: Option<String>,
    /// Every sample taken
    pub history: History,
    /// The tab shown, as an index into [`GROUPS`]
    pub tab: usize,
    /// Each tab's selected chart, as a position among its group's metrics
    pub selected: [usize; GROUPS.len()],
    /// How many charts a row of the grid holds, as the last frame drew it
    pub columns: usize,
    /// The first row of the grid the last frame drew, when the grid does not fit
    pub first_row: usize,
    /// Whether the selected chart fills the screen
    pub fullscreen: bool,
    /// Whether space was pressed and the next key is a shortcut
    pub leader: bool,
    /// The window charted, as an index into [`WINDOWS`]
    pub window: usize,
    /// How far the help page is scrolled, while it is open
    pub help: Option<u16>,
    /// The moment the picture was frozen at, while it is
    pub paused_at: Option<Instant>,
}

impl Screen {
    /// A screen with nothing read yet, on the first tab
    ///
    /// # Arguments
    ///
    /// * `title` - The cluster's name
    /// * `every` - How often the figures are read
    #[must_use]
    pub fn new(title: impl Into<String>, every: Duration) -> Self {
        Screen {
            title: title.into(),
            every,
            latest: None,
            error: None,
            history: History::default(),
            tab: 0,
            selected: [0; GROUPS.len()],
            columns: 1,
            first_row: 0,
            fullscreen: false,
            leader: false,
            window: DEFAULT_WINDOW,
            help: None,
            paused_at: None,
        }
    }

    /// Take a read's result: sample an answer, or say why there is none
    ///
    /// A frozen picture keeps its answer, but the samples keep being taken, so the lines
    /// that arrived meanwhile are there when it is unfrozen.
    ///
    /// # Arguments
    ///
    /// * `answer` - The read's answer, or why it failed
    /// * `now` - When it was read
    pub fn observe(&mut self, answer: Result<StatsModel, String>, now: Instant) {
        match answer {
            Ok(model) => {
                // every answer is sampled, frozen or not
                self.history.record(&model, now);
                self.error = None;
                // and shown unless the picture is frozen
                if self.paused_at.is_none() {
                    self.latest = Some(model);
                }
            }
            // the last answer stays on screen, under the reason the next one did not come
            Err(error) => self.error = Some(error),
        }
    }

    /// The indexes into [`METRICS`] of the tab's metrics, in the order the grid draws them
    #[must_use]
    pub fn tab_metrics(&self) -> Vec<usize> {
        in_group(GROUPS[self.tab.min(GROUPS.len() - 1)].0)
    }

    /// The selected chart's position among the tab's metrics
    #[must_use]
    pub fn position(&self) -> usize {
        let count = self.tab_metrics().len();
        self.selected[self.tab].min(count.saturating_sub(1))
    }

    /// The selected metric's index into [`METRICS`], which the history keys its lines by
    #[must_use]
    pub fn metric_index(&self) -> usize {
        self.tab_metrics()[self.position()]
    }

    /// The selected metric
    #[must_use]
    pub fn metric(&self) -> &'static Metric {
        &METRICS[self.metric_index()]
    }

    /// The window charted
    #[must_use]
    pub fn window(&self) -> Duration {
        WINDOWS[self.window.min(WINDOWS.len() - 1)]
    }

    /// The moment the chart's right edge stands for: now, or when the picture was frozen
    ///
    /// # Arguments
    ///
    /// * `now` - The time now
    #[must_use]
    pub fn edge(&self, now: Instant) -> Instant {
        self.paused_at.unwrap_or(now)
    }

    /// Show another tab, drawn from its top with the chart it had selected
    ///
    /// # Arguments
    ///
    /// * `tab` - The tab, as an index into [`GROUPS`]
    fn show_tab(&mut self, tab: usize) {
        self.tab = tab % GROUPS.len();
        self.first_row = 0;
    }

    /// Move the selection: through the grid by its rows and columns, or through the tab's
    /// metrics in order while one fills the screen
    ///
    /// # Arguments
    ///
    /// * `key` - The arrow pressed
    fn step(&mut self, key: KeyCode) {
        let count = self.tab_metrics().len();
        let at = self.position();
        let columns = self.columns.max(1);
        let next = if self.fullscreen {
            // one chart at a time, wrapping at both ends
            match key {
                KeyCode::Left | KeyCode::Up => (at + count - 1) % count,
                _ => (at + 1) % count,
            }
        } else {
            match key {
                // along a row, stopping at the first and the last chart
                KeyCode::Left => at.saturating_sub(1),
                KeyCode::Right => (at + 1).min(count - 1),
                // a row up, if there is one
                KeyCode::Up if at >= columns => at - columns,
                // a row down, or the last chart when the row below it is short
                KeyCode::Down if at + columns < count => at + columns,
                KeyCode::Down if at / columns < (count - 1) / columns => count - 1,
                _ => at,
            }
        };
        self.selected[self.tab] = next;
    }

    /// Handle one key press
    ///
    /// # Arguments
    ///
    /// * `key` - The key
    /// * `now` - When it was pressed, which a freeze holds the picture at
    pub fn handle_key(&mut self, key: KeyEvent, now: Instant) -> Outcome {
        // ctrl-c leaves from anywhere
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            return Outcome::Quit;
        }
        // the help page takes the keys while it is open
        if let Some(scroll) = self.help {
            self.help = match key.code {
                KeyCode::Char('q') => return Outcome::Quit,
                KeyCode::Esc | KeyCode::Char('?') | KeyCode::F(1) => None,
                KeyCode::Up | KeyCode::Char('k') => Some(scroll.saturating_sub(1)),
                KeyCode::Down | KeyCode::Char('j') => Some(scroll.saturating_add(1)),
                KeyCode::PageUp => Some(scroll.saturating_sub(HELP_PAGE)),
                KeyCode::PageDown | KeyCode::Char(' ') => Some(scroll.saturating_add(HELP_PAGE)),
                KeyCode::Home => Some(0),
                KeyCode::End => Some(u16::MAX),
                _ => Some(scroll),
            };
            return Outcome::Continue;
        }
        // after space, the next key is a shortcut and nothing else, whatever it is
        if self.leader {
            self.leader = false;
            if key.code == KeyCode::Char('f') {
                self.fullscreen = !self.fullscreen;
            }
            return Outcome::Continue;
        }
        match key.code {
            KeyCode::Char('q') => return Outcome::Quit,
            // Esc brings the grid back before it leaves
            KeyCode::Esc if self.fullscreen => self.fullscreen = false,
            KeyCode::Esc => return Outcome::Quit,
            KeyCode::Char(' ') => self.leader = true,
            // the tabs wrap both ways, and a number jumps to one
            KeyCode::Tab => self.show_tab(self.tab + 1),
            KeyCode::BackTab => self.show_tab(self.tab + GROUPS.len() - 1),
            KeyCode::Char(digit @ '1'..='9') => {
                let tab = digit as usize - '1' as usize;
                if tab < GROUPS.len() {
                    self.show_tab(tab);
                }
            }
            // the arrows and their vi keys move the selection
            KeyCode::Left | KeyCode::Char('h') => self.step(KeyCode::Left),
            KeyCode::Right | KeyCode::Char('l') => self.step(KeyCode::Right),
            KeyCode::Up | KeyCode::Char('k') => self.step(KeyCode::Up),
            KeyCode::Down | KeyCode::Char('j') => self.step(KeyCode::Down),
            // the windows stop at both ends
            KeyCode::Char('[' | '-') => self.window = self.window.saturating_sub(1),
            KeyCode::Char(']' | '+' | '=') => {
                self.window = (self.window + 1).min(WINDOWS.len() - 1);
            }
            KeyCode::Char('?') | KeyCode::F(1) => self.help = Some(0),
            // a freeze holds the picture at the moment it was pressed
            KeyCode::Char('p') => {
                self.paused_at = match self.paused_at {
                    Some(_) => None,
                    None => Some(now),
                };
            }
            _ => (),
        }
        Outcome::Continue
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::{KeyEventKind, KeyEventState};
    use shoal::serde_json::json;

    /// A key press with no modifiers
    ///
    /// # Arguments
    ///
    /// * `code` - The key
    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent {
            code,
            modifiers: KeyModifiers::NONE,
            kind: KeyEventKind::Press,
            state: KeyEventState::NONE,
        }
    }

    /// An answer whose one member reported at `at_ms`
    ///
    /// # Arguments
    ///
    /// * `at_ms` - When the member derived its figures
    fn answer(at_ms: u64) -> StatsModel {
        let a = "aaaaaaaa-1111-1111-1111-111111111111";
        let view = shoal::serde_json::from_value(json!({
            "source": "leader", "answered_by": a, "version": at_ms,
            "members": [{ "node": a, "state": "up", "stats": { "node": a, "at_ms": at_ms } }]
        }))
        .expect("an answer decodes");
        StatsModel::new(view, None)
    }

    /// The tabs wrap and jump, each keeps its selection, and the arrows move through the grid
    /// by its columns
    #[test]
    fn keys_move_the_screen() {
        let now = Instant::now();
        let mut screen = Screen::new("lab", Duration::from_secs(2));
        let press = |screen: &mut Screen, code: KeyCode| screen.handle_key(key(code), now);
        // the tabs wrap both ways, and a number jumps to one while one past the last is ignored
        press(&mut screen, KeyCode::Tab);
        assert_eq!(screen.tab, 1);
        press(&mut screen, KeyCode::BackTab);
        press(&mut screen, KeyCode::BackTab);
        assert_eq!(screen.tab, GROUPS.len() - 1);
        press(&mut screen, KeyCode::Char('3'));
        assert_eq!(screen.tab, 2);
        press(&mut screen, KeyCode::Char('9'));
        assert_eq!(screen.tab, 2);
        // the writes tab's seven charts in rows of three
        press(&mut screen, KeyCode::Char('2'));
        assert_eq!(GROUPS[screen.tab].0, "writes");
        assert_eq!(screen.tab_metrics().len(), 7);
        screen.columns = 3;
        // along the row, then down a row
        press(&mut screen, KeyCode::Right);
        press(&mut screen, KeyCode::Char('l'));
        assert_eq!(screen.position(), 2);
        press(&mut screen, KeyCode::Down);
        assert_eq!(screen.position(), 5);
        // the row below is short, so down lands on its one chart, and stays there
        press(&mut screen, KeyCode::Down);
        assert_eq!(screen.position(), 6);
        press(&mut screen, KeyCode::Char('j'));
        assert_eq!(screen.position(), 6);
        assert_eq!(screen.metric().name, "misses/s");
        // up a row, then left to the first chart and no further
        press(&mut screen, KeyCode::Up);
        assert_eq!(screen.position(), 3);
        assert_eq!(screen.metric().name, "deletes/s");
        for _ in 0..5 {
            press(&mut screen, KeyCode::Char('h'));
        }
        assert_eq!(screen.position(), 0);
        press(&mut screen, KeyCode::Up);
        assert_eq!(screen.position(), 0);
        // each tab keeps its own selection
        press(&mut screen, KeyCode::Right);
        press(&mut screen, KeyCode::Tab);
        assert_eq!(screen.position(), 0);
        press(&mut screen, KeyCode::BackTab);
        assert_eq!(screen.position(), 1);
        // the windows stop at both ends
        assert_eq!(screen.window(), WINDOWS[DEFAULT_WINDOW]);
        for _ in 0..10 {
            press(&mut screen, KeyCode::Char(']'));
        }
        assert_eq!(screen.window(), *WINDOWS.last().unwrap());
        for _ in 0..10 {
            press(&mut screen, KeyCode::Char('['));
        }
        assert_eq!(screen.window(), WINDOWS[0]);
    }

    /// Space then f fills the screen with the selected chart and back, space then anything else
    /// does nothing, the arrows step through the tab while one chart is shown, and Esc brings
    /// the grid back before it leaves
    #[test]
    fn space_f_fills_the_screen() {
        let now = Instant::now();
        let mut screen = Screen::new("lab", Duration::from_secs(2));
        let press = |screen: &mut Screen, code: KeyCode| screen.handle_key(key(code), now);
        press(&mut screen, KeyCode::Char('2'));
        // space then another key is only a cancelled shortcut
        press(&mut screen, KeyCode::Char(' '));
        assert!(screen.leader);
        press(&mut screen, KeyCode::Right);
        assert!(!screen.leader && !screen.fullscreen);
        assert_eq!(screen.position(), 0);
        // space then f fills the screen
        press(&mut screen, KeyCode::Char(' '));
        press(&mut screen, KeyCode::Char('f'));
        assert!(screen.fullscreen && !screen.leader);
        // one chart at a time, wrapping at both ends
        press(&mut screen, KeyCode::Left);
        assert_eq!(screen.position(), 6);
        press(&mut screen, KeyCode::Down);
        assert_eq!(screen.position(), 0);
        press(&mut screen, KeyCode::Right);
        assert_eq!(screen.metric().name, "inserts/s");
        // space then f again brings the grid back
        press(&mut screen, KeyCode::Char(' '));
        press(&mut screen, KeyCode::Char('f'));
        assert!(!screen.fullscreen);
        // Esc leaves the full screen chart before it leaves the view
        press(&mut screen, KeyCode::Char(' '));
        press(&mut screen, KeyCode::Char('f'));
        assert_eq!(press(&mut screen, KeyCode::Esc), Outcome::Continue);
        assert!(!screen.fullscreen);
        assert_eq!(press(&mut screen, KeyCode::Esc), Outcome::Quit);
    }

    /// The help page opens, scrolls, takes the arrows and closes, and q and ctrl-c leave
    #[test]
    fn the_help_page_takes_the_keys() {
        let now = Instant::now();
        let mut screen = Screen::new("lab", Duration::from_secs(2));
        let press = |screen: &mut Screen, code: KeyCode| screen.handle_key(key(code), now);
        // h moves left now, and ? opens the page
        press(&mut screen, KeyCode::Char('h'));
        assert_eq!(screen.help, None);
        press(&mut screen, KeyCode::Char('?'));
        assert_eq!(screen.help, Some(0));
        // it scrolls, and the arrows scroll it rather than move the selection
        press(&mut screen, KeyCode::PageDown);
        press(&mut screen, KeyCode::Down);
        assert_eq!(screen.help, Some(HELP_PAGE + 1));
        assert_eq!(screen.position(), 0);
        press(&mut screen, KeyCode::Home);
        assert_eq!(screen.help, Some(0));
        // Esc closes it rather than leaving
        assert_eq!(press(&mut screen, KeyCode::Esc), Outcome::Continue);
        assert_eq!(screen.help, None);
        // q leaves even from the page, and so does ctrl-c
        press(&mut screen, KeyCode::F(1));
        assert_eq!(press(&mut screen, KeyCode::Char('q')), Outcome::Quit);
        let ctrl_c = KeyEvent {
            modifiers: KeyModifiers::CONTROL,
            ..key(KeyCode::Char('c'))
        };
        assert_eq!(screen.handle_key(ctrl_c, now), Outcome::Quit);
    }

    /// A frozen picture keeps its answer and its edge while samples keep arriving underneath
    #[test]
    fn a_frozen_picture_keeps_sampling() {
        let start = Instant::now();
        let mut screen = Screen::new("lab", Duration::from_secs(2));
        screen.observe(Ok(answer(1000)), start);
        // frozen at the second second
        let frozen = start + Duration::from_secs(2);
        screen.handle_key(key(KeyCode::Char('p')), frozen);
        assert_eq!(screen.edge(start + Duration::from_secs(9)), frozen);
        // a later answer is sampled but not shown
        screen.observe(Ok(answer(5000)), start + Duration::from_secs(4));
        assert_eq!(screen.latest.as_ref().map(|model| model.view.version), Some(1000));
        let metric = crate::cluster::stats::metrics::index_of("inserts").unwrap();
        let lines = screen.history.series(metric, WINDOWS[3], start + Duration::from_secs(4));
        assert_eq!(lines[0].1.len(), 2);
        // a failed read says so and keeps the answer on screen
        screen.observe(Err("the leader could not be reached".to_string()), start);
        assert!(screen.error.is_some());
        assert!(screen.latest.is_some());
        // unfrozen, the edge is now again and the next answer is shown
        screen.handle_key(key(KeyCode::Char('p')), start);
        let later = start + Duration::from_secs(9);
        assert_eq!(screen.edge(later), later);
        screen.observe(Ok(answer(7000)), later);
        assert_eq!(screen.latest.as_ref().map(|model| model.view.version), Some(7000));
        assert!(screen.error.is_none());
    }
}
