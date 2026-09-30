//! What the stats view shows, and what each key does to it
//!
//! Nothing here draws: [`super::view`] draws a [`Screen`], so every key and every sample can be
//! tested without a terminal ([F64](../../../../docs/src/features/stats-tui.md)).

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use std::time::{Duration, Instant};

use super::StatsModel;
use super::history::History;
use super::metrics::{METRICS, Metric};

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
    /// The metric charted, as an index into [`METRICS`]
    pub selected: usize,
    /// The window charted, as an index into [`WINDOWS`]
    pub window: usize,
    /// How far the help page is scrolled, while it is open
    pub help: Option<u16>,
    /// The moment the picture was frozen at, while it is
    pub paused_at: Option<Instant>,
}

impl Screen {
    /// A screen with nothing read yet
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
            selected: 0,
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

    /// The metric charted
    #[must_use]
    pub fn metric(&self) -> &'static Metric {
        &METRICS[self.selected.min(METRICS.len() - 1)]
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
                KeyCode::Esc | KeyCode::Char('?' | 'h') | KeyCode::F(1) => None,
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
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => return Outcome::Quit,
            // the metric list wraps at both ends
            KeyCode::Up | KeyCode::Char('k') => {
                self.selected = (self.selected + METRICS.len() - 1) % METRICS.len();
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.selected = (self.selected + 1) % METRICS.len();
            }
            // the windows stop at both ends
            KeyCode::Left | KeyCode::Char('-') => self.window = self.window.saturating_sub(1),
            KeyCode::Right | KeyCode::Char('+' | '=') => {
                self.window = (self.window + 1).min(WINDOWS.len() - 1);
            }
            KeyCode::Char('?' | 'h') | KeyCode::F(1) => self.help = Some(0),
            // a freeze holds the picture at the moment it was pressed
            KeyCode::Char('p' | ' ') => {
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

    /// The keys choose a metric and a window, open and scroll the help page, freeze the
    /// picture and leave, and Esc closes the help page before it leaves
    #[test]
    fn keys_move_the_screen() {
        let now = Instant::now();
        let mut screen = Screen::new("lab", Duration::from_secs(2));
        // the metric list wraps both ways
        assert_eq!(screen.handle_key(key(KeyCode::Up), now), Outcome::Continue);
        assert_eq!(screen.selected, METRICS.len() - 1);
        screen.handle_key(key(KeyCode::Down), now);
        screen.handle_key(key(KeyCode::Char('j')), now);
        assert_eq!(screen.selected, 1);
        // the windows stop at both ends
        assert_eq!(screen.window(), WINDOWS[DEFAULT_WINDOW]);
        for _ in 0..10 {
            screen.handle_key(key(KeyCode::Right), now);
        }
        assert_eq!(screen.window(), *WINDOWS.last().unwrap());
        for _ in 0..10 {
            screen.handle_key(key(KeyCode::Left), now);
        }
        assert_eq!(screen.window(), WINDOWS[0]);
        // the help page opens, scrolls, and takes the arrows from the metric list
        screen.handle_key(key(KeyCode::Char('?')), now);
        assert_eq!(screen.help, Some(0));
        screen.handle_key(key(KeyCode::PageDown), now);
        screen.handle_key(key(KeyCode::Down), now);
        assert_eq!(screen.help, Some(HELP_PAGE + 1));
        assert_eq!(screen.selected, 1);
        screen.handle_key(key(KeyCode::Home), now);
        assert_eq!(screen.help, Some(0));
        // Esc closes it rather than leaving, and a second Esc leaves
        assert_eq!(screen.handle_key(key(KeyCode::Esc), now), Outcome::Continue);
        assert_eq!(screen.help, None);
        assert_eq!(screen.handle_key(key(KeyCode::Esc), now), Outcome::Quit);
        // q leaves even from the help page, and so does ctrl-c
        screen.handle_key(key(KeyCode::Char('h')), now);
        assert_eq!(screen.handle_key(key(KeyCode::Char('q')), now), Outcome::Quit);
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
