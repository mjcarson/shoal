//! The completion menu for the query box
//!
//! This is modelled on helix's completion menu: it opens on its own as soon as there is a word
//! to complete, preselects the best match, wraps as you move through it, and closes the moment
//! nothing matches. Everything it offers comes from the client's own schema, so it never talks
//! to a server.

use ratatui::{
    Frame,
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Clear, Paragraph},
};
use shoal::traits::QuerySupport;
use shoal::shared::queries::parser::{Completions, Suggestion};
use unicode_width::UnicodeWidthStr;

use super::Tab;

/// The most rows the menu will ever show at once before it starts scrolling
const MAX_ROWS: usize = 10;

/// The state of the completion menu for a single tab
#[derive(Debug, Clone, Default)]
pub struct CompletionState {
    /// The suggestions on offer, best match first
    items: Vec<Suggestion>,
    /// The index of the selected suggestion
    cursor: Option<usize>,
    /// The index of the first suggestion that is visible
    scroll: usize,
    /// The byte offset accepting a suggestion should splice it in at
    word_start: usize,
    /// The byte offset accepting a suggestion should splice it in up to
    word_end: usize,
    /// Whether the user has closed this menu for the word they are on
    dismissed: bool,
}

impl CompletionState {
    /// Take a new set of suggestions, replacing whatever was on offer before
    ///
    /// Anything worth offering is offered, even with nothing typed yet — every position in the
    /// grammar that allows something unbounded, like a number or the body of a string, hands
    /// back no suggestions at all, so an empty list is the only gate the menu needs.
    ///
    /// # Arguments
    ///
    /// * `completions` - The suggestions to offer
    /// * `forced` - Whether the user asked for these explicitly rather than just typing
    pub fn refresh(&mut self, completions: Completions, forced: bool) {
        // asking for the menu again is a fresh start, so forget any earlier dismissal
        if forced {
            self.dismissed = false;
        }
        self.word_start = completions.word_start;
        self.word_end = completions.word_end;
        self.items = completions.items;
        // preselect the best match so enter always has something to accept
        self.cursor = if self.items.is_empty() { None } else { Some(0) };
        self.scroll = 0;
    }

    /// Forget every suggestion without marking this menu as dismissed
    pub fn clear(&mut self) {
        self.items.clear();
        self.cursor = None;
        self.scroll = 0;
    }

    /// Close this menu until the user moves on to another word
    pub fn close(&mut self) {
        self.clear();
        self.dismissed = true;
    }

    /// Let this menu open again, which typing another character always does
    pub fn undismiss(&mut self) {
        self.dismissed = false;
    }

    /// Whether this menu is currently on screen
    pub fn is_open(&self) -> bool {
        !self.dismissed && !self.items.is_empty()
    }

    /// Get every suggestion on offer, best match first
    pub fn items(&self) -> &[Suggestion] {
        &self.items
    }

    /// Get the suggestion that is currently selected
    pub fn selected(&self) -> Option<&Suggestion> {
        // a closed menu has nothing selected no matter what it is holding
        if !self.is_open() {
            return None;
        }
        self.cursor.and_then(|cursor| self.items.get(cursor))
    }

    /// Move to the next suggestion, wrapping around at the end
    pub fn move_down(&mut self) {
        // there is nothing to move through in a closed menu
        if !self.is_open() {
            return;
        }
        // step forward, wrapping back around to the top
        self.cursor = Some(match self.cursor {
            Some(cursor) => (cursor + 1) % self.items.len(),
            None => 0,
        });
        self.scroll_to_selection();
    }

    /// Move to the previous suggestion, wrapping around at the start
    pub fn move_up(&mut self) {
        // there is nothing to move through in a closed menu
        if !self.is_open() {
            return;
        }
        // step backwards, wrapping around to the bottom
        self.cursor = Some(match self.cursor {
            Some(0) | None => self.items.len() - 1,
            Some(cursor) => cursor - 1,
        });
        self.scroll_to_selection();
    }

    /// Scroll the menu so the selected suggestion is visible
    fn scroll_to_selection(&mut self) {
        // we only scroll for a real selection
        let Some(cursor) = self.cursor else {
            return;
        };
        // scroll up if the selection has moved above the visible rows
        if cursor < self.scroll {
            self.scroll = cursor;
        } else if cursor >= self.scroll + MAX_ROWS {
            // scroll down if it has moved below them
            self.scroll = cursor + 1 - MAX_ROWS;
        }
    }

    /// Get the part of the selected suggestion that has not been typed yet
    ///
    /// This is only the tail of a suggestion that the typed word is a real prefix of — a fuzzy
    /// match like `mvk` for `MovieByKeyword` has no sensible tail to show inline.
    ///
    /// # Arguments
    ///
    /// * `query` - The query the menu was built for
    pub fn ghost<'a>(&'a self, query: &str) -> Option<&'a str> {
        // grab whatever is selected
        let selected = self.selected()?;
        // pull out the word this suggestion would replace
        let word = query.get(self.word_start..self.word_end)?;
        // only show a tail when the user is typing this suggestion out directly
        selected.text.strip_prefix(word)
    }

    /// Get the span of the query accepting a suggestion replaces
    pub fn word_span(&self) -> (usize, usize) {
        (self.word_start, self.word_end)
    }
}

/// The completion menu component
///
/// Renders the suggestions for the active tab as a floating menu next to the word being typed.
pub struct CompletionMenu;

impl CompletionMenu {
    /// Create a new completion menu component
    ///
    /// # Returns
    ///
    /// A new CompletionMenu instance
    pub fn new() -> Self {
        Self
    }

    /// Render the completion menu to the frame
    ///
    /// The menu is placed directly below the cursor when there is room for it and flips above
    /// when there is not.
    ///
    /// # Arguments
    ///
    /// * `frame` - The frame to render to
    /// * `cursor` - Where the query box's cursor was drawn on screen
    /// * `tab` - The tab whose menu we are rendering
    pub fn render<S: QuerySupport + Send + Sync>(
        &self,
        frame: &mut Frame,
        cursor: (u16, u16),
        tab: &Tab<S>,
    ) {
        // there is nothing to draw for a closed menu
        if !tab.completion.is_open() {
            return;
        }
        // work out how many rows we can show at once
        let total = tab.completion.items.len();
        let rows = total.min(MAX_ROWS);
        // measure the widest text and annotation so the two columns line up
        let text_width = tab
            .completion
            .items
            .iter()
            .map(|item| item.text.width())
            .max()
            .unwrap_or(0);
        let detail_width = tab
            .completion
            .items
            .iter()
            .map(|item| item.detail.width())
            .max()
            .unwrap_or(0);
        // a scrollbar only costs a column when there is more to scroll to
        let scrollbar = total > rows;
        // pad the columns apart and leave a space on either side of the menu
        let width = 1 + text_width + 2 + detail_width + 1 + usize::from(scrollbar);
        let full = frame.area();
        let width = (width as u16).min(full.width);
        // anchor the menu so its text starts under the cursor, which sits one column in from the
        // menu's own left padding, and shift it left if that would run off the right hand edge
        let (column, row) = cursor;
        let x = column
            .saturating_sub(1)
            .min(full.width.saturating_sub(width));
        // prefer to sit on the row below the cursor, and flip above it when there is no room
        let height = rows as u16;
        let y = if row + 1 + height <= full.height {
            row + 1
        } else {
            row.saturating_sub(height)
        };
        let area = Rect::new(x, y, width, height);
        // the menu's own colors, which are close enough to helix's defaults to feel familiar
        let menu = Style::default().bg(Color::Rgb(60, 60, 60)).fg(Color::White);
        let selected = Style::default()
            .bg(Color::Rgb(90, 90, 120))
            .fg(Color::White)
            .add_modifier(Modifier::BOLD);
        let detail_style = Style::default()
            .bg(Color::Rgb(60, 60, 60))
            .fg(Color::DarkGray);
        // build a line for each visible suggestion
        let mut lines = Vec::with_capacity(rows);
        for offset in 0..rows {
            // find the suggestion this row is showing
            let index = tab.completion.scroll + offset;
            let Some(item) = tab.completion.items.get(index) else {
                break;
            };
            // highlight the row the cursor is on
            let row_style = if Some(index) == tab.completion.cursor {
                selected
            } else {
                menu
            };
            // pad by display width rather than character count so wide glyphs still line up
            let text = format!(
                " {}{}  ",
                item.text,
                " ".repeat(text_width - item.text.width())
            );
            let detail = format!(
                "{}{} ",
                " ".repeat(detail_width - item.detail.width()),
                item.detail
            );
            // the annotation is dimmed unless this is the row the cursor is on
            let detail_style = if Some(index) == tab.completion.cursor {
                row_style
            } else {
                detail_style
            };
            let mut spans = vec![
                Span::styled(text, row_style),
                Span::styled(detail, detail_style),
            ];
            // draw the scrollbar in the last column when there is more than one screenful
            if scrollbar {
                spans.push(Span::styled(
                    if Self::in_scrollbar(offset, rows, total, tab.completion.scroll) {
                        "▐"
                    } else {
                        " "
                    },
                    Style::default().bg(Color::Rgb(60, 60, 60)).fg(Color::Gray),
                ));
            }
            lines.push(Line::from(spans));
        }
        // clear the area first, since this floats over whatever is behind it
        frame.render_widget(Clear, area);
        frame.render_widget(Paragraph::new(lines).style(menu), area);
    }

    /// Whether a row falls inside the scrollbar's thumb
    ///
    /// The thumb is sized in proportion to how much of the list is visible, the same way helix
    /// sizes the one in its menus.
    ///
    /// # Arguments
    ///
    /// * `row` - The row being drawn, counted from the top of the menu
    /// * `rows` - The number of rows the menu is showing
    /// * `total` - The total number of suggestions on offer
    /// * `scroll` - The index of the first visible suggestion
    fn in_scrollbar(row: usize, rows: usize, total: usize, scroll: usize) -> bool {
        // size the thumb by how much of the list we can see
        let thumb = (rows * rows / total).max(1).min(rows);
        // slide it down in proportion to how far we have scrolled
        let travel = total.saturating_sub(rows).max(1);
        let top = (rows - thumb) * scroll / travel;
        row >= top && row < top + thumb
    }
}
