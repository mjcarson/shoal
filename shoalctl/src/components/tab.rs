//! The tab bar component for shoalctl
//!
//! This module provides a clickable tab bar that allows users to switch
//! between different views in the application.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use kanal::AsyncSender;
use ratatui::{
    Frame,
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
};
use shoal::{client::Shoal, traits::QuerySupport};
use shoal_core::shared::queries::parser;
use std::marker::PhantomData;
use std::sync::Arc;
use unicode_width::UnicodeWidthStr;
use uuid::Uuid;

use crate::AppEvent;

mod completion;
mod content;
mod error;
mod query_bar;

pub use completion::{CompletionMenu, CompletionState};
pub use content::TabContent;
pub use error::{ErrorBar, QueryError};
pub use query_bar::{QueryLayout, QueryRow, TabQueryBar, layout_query};

/// A single tab in the application
#[derive(Debug, Clone)]
pub struct Tab<S: QuerySupport> {
    /// The unique identifier for this tab
    pub id: Uuid,
    /// The display label for this tab
    pub label: String,
    /// The content to display (query results or messages)
    pub content: String,
    /// The error to display under the query, which is cleared by any edit to it
    pub error: Option<QueryError>,
    /// This query string for this tab
    pub query: String,
    /// The cursor position within the query text
    pub query_cursor: usize,
    /// The completions on offer for the query as it currently stands
    pub completion: CompletionState,
    /// Vertical scroll offset for the content area
    pub scroll_y: u16,
    /// Horizontal scroll offset for the content area
    pub scroll_x: u16,
    /// The type of queries this Tab can handle
    phantom: PhantomData<S>,
}

impl<S: QuerySupport + Sync + Send> Tab<S>
where
    S::TableNames: Send,
    for<'a> <<S as QuerySupport>::ResponseKinds as rkyv::Archive>::Archived:
        rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    <S::ResponseKinds as rkyv::Archive>::Archived: rkyv::Deserialize<
            S::ResponseKinds,
            rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
        >,
    <S::ResponseKinds as rkyv::Archive>::Archived: std::marker::Send,
{
    /// Create a new tab with a generated UUID and the given label
    ///
    /// # Arguments
    ///
    /// * `label` - The display label for this tab
    pub fn new<L: Into<String>>(label: L) -> Self {
        Self {
            id: Uuid::new_v4(),
            label: label.into(),
            content: String::new(),
            error: None,
            query: String::new(),
            query_cursor: 0,
            completion: CompletionState::default(),
            scroll_y: 0,
            scroll_x: 0,
            phantom: PhantomData,
        }
    }

    /// Get the byte offset of the character before the cursor
    ///
    /// Table and field names can be any valid rust identifier, so the query is stepped through
    /// a character at a time rather than a byte at a time.
    fn prev_boundary(&self) -> Option<usize> {
        self.query[..self.query_cursor]
            .char_indices()
            .next_back()
            .map(|(index, _)| index)
    }

    /// Get the byte offset just past the character at the cursor
    fn next_boundary(&self) -> Option<usize> {
        self.query[self.query_cursor..]
            .chars()
            .next()
            .map(|c| self.query_cursor + c.len_utf8())
    }

    /// Forget the error shown under the query
    ///
    /// This is called by every edit to the query and by none of the cursor moves, because an
    /// error describes the query it was parsed from. Moving the cursor leaves that query alone,
    /// so the error is still true; changing a character of it means the error is about a query
    /// that no longer exists, and the span it carries no longer points at what it named.
    fn clear_error(&mut self) {
        self.error = None;
    }

    /// Insert a character at the current cursor position
    ///
    /// # Arguments
    ///
    /// * `c` - The character to insert
    pub fn insert_char(&mut self, c: char) {
        self.query.insert(self.query_cursor, c);
        self.query_cursor += c.len_utf8();
        // the query this error was about has just been edited
        self.clear_error();
    }

    /// Delete the character before the cursor (backspace)
    pub fn delete_char_before(&mut self) {
        // find the character behind the cursor, if there is one
        if let Some(start) = self.prev_boundary() {
            self.query.remove(start);
            self.query_cursor = start;
            // the query this error was about has just been edited
            self.clear_error();
        }
    }

    /// Delete the character at the cursor (delete)
    pub fn delete_char_at(&mut self) {
        // there is only a character to delete if the cursor isn't at the end
        if self.query_cursor < self.query.len() {
            self.query.remove(self.query_cursor);
            // the query this error was about has just been edited
            self.clear_error();
        }
    }

    /// Move the cursor left
    pub fn move_cursor_left(&mut self) {
        // step back to the start of the character behind the cursor
        if let Some(start) = self.prev_boundary() {
            self.query_cursor = start;
        }
    }

    /// Move the cursor right
    pub fn move_cursor_right(&mut self) {
        // step forward past the character at the cursor
        if let Some(end) = self.next_boundary() {
            self.query_cursor = end;
        }
    }

    /// Rebuild the completions on offer for the query as it currently stands
    ///
    /// # Arguments
    ///
    /// * `forced` - Whether the user asked for completions rather than just typing
    pub fn refresh_completions(&mut self, forced: bool) {
        // ask the client what could be typed at our cursor
        let completions = parser::suggest::<S>(&self.query, self.query_cursor);
        // hand those to the menu, which decides whether they are worth showing
        self.completion.refresh(completions, forced);
    }

    /// Handle a key aimed at the completion menu
    ///
    /// These are the keys helix binds its own completion menu to. Enter is only taken when the
    /// menu is open, which leaves it free to submit the query the rest of the time.
    ///
    /// # Arguments
    ///
    /// * `key` - The key that was pressed
    ///
    /// # Returns
    ///
    /// Whether the menu consumed this key
    pub fn handle_completion_key(&mut self, key: KeyEvent) -> bool {
        // whether this key was pressed with control held down
        let control = key.modifiers.contains(KeyModifiers::CONTROL);
        match key.code {
            // ask for completions even when there is no word to trigger on
            KeyCode::Char(' ') if control => self.refresh_completions(true),
            // move down the menu
            KeyCode::Tab | KeyCode::Down => self.completion.move_down(),
            KeyCode::Char('n') if control => self.completion.move_down(),
            // move up the menu
            KeyCode::BackTab | KeyCode::Up => self.completion.move_up(),
            KeyCode::Char('p') if control => self.completion.move_up(),
            // close the menu
            KeyCode::Char('c') if control => self.completion.close(),
            // accept whatever is selected, but only while the menu is open
            KeyCode::Enter if self.completion.is_open() => self.accept_completion(),
            // this key isn't ours
            _ => return false,
        }
        true
    }

    /// Accept the selected completion, splicing it into the query
    pub fn accept_completion(&mut self) {
        // there is nothing to accept unless the menu has a selection
        let Some(text) = self
            .completion
            .selected()
            .map(|suggestion| suggestion.insert_text())
        else {
            return;
        };
        // replace the word the menu was built for with the completed one
        let (start, end) = self.completion.word_span();
        self.query.replace_range(start..end, &text);
        self.query_cursor = start + text.len();
        // the query this error was about has just been edited
        self.clear_error();
        // the word we were completing is gone, so build the menu for whatever comes next
        self.refresh_completions(false);
    }

    /// Try to submit the current query
    pub async fn submit_query(
        &mut self,
        shoal: &Arc<Shoal<S>>,
        app_tx: &mut AsyncSender<AppEvent<S>>,
    ) where
        S: 'static,
        S::QueryKinds: Send,
        S::ResponseKinds: Send,
    {
        // Don't submit empty queries
        if self.query.trim().is_empty() {
            return;
        }
        // try to parse our query
        let query = match S::parse(&self.query) {
            Ok(q) => q,
            Err(e) => {
                // show the parse error in the UI and leave the query for the user to fix, with
                // the span kept apart from the message so the query box can underline it
                self.error = Some(QueryError::parse(&e));
                return;
            }
        };
        // Get the table name from the query
        let table_name = S::query_table_name(&query);
        // Get the current tab ID
        let tab = self.id;
        // clone our shoal client
        let shoal = shoal.clone();
        // clone our event sender channel
        let app_tx = app_tx.clone();
        // start a task to execute this query
        tokio::task::spawn(async move {
            query_bar::run::<S>(shoal, tab, table_name, query, app_tx).await
        })
        .await
        .unwrap();
    }
}

/// The tab bar component
///
/// Renders a horizontal tab bar and tracks tab positions for click detection.
pub struct TabSelector {
    /// The clickable regions for each tab as (start_x, end_x, index)
    tab_positions: Vec<(u16, u16, usize)>,
    /// The clickable region for the add button as (start_x, end_x)
    add_button_position: Option<(u16, u16)>,
    /// The area where the tabs were last rendered
    area: Rect,
}

impl TabSelector {
    /// Create a new tabs component
    ///
    /// # Returns
    ///
    /// A new Tabs instance with no tracked positions
    pub fn new() -> Self {
        Self {
            tab_positions: Vec::new(),
            add_button_position: None,
            area: Rect::default(),
        }
    }

    /// Check if a screen position corresponds to a tab
    ///
    /// # Arguments
    ///
    /// * `x` - The column position to check
    /// * `y` - The row position to check
    ///
    /// # Returns
    ///
    /// * `Some(index)` - The index of the tab at the given position
    /// * `None` - No tab at the given position
    pub fn tab_index_at_position(&self, x: u16, y: u16) -> Option<usize> {
        // tabs are rendered on the row inside the border
        let tab_y = self.area.y + 1;
        // check if the click is on the correct row
        if y != tab_y {
            return None;
        }
        // check each tab's horizontal range
        for &(start, end, index) in &self.tab_positions {
            if x >= start && x < end {
                return Some(index);
            }
        }
        None
    }

    /// Check if a screen position corresponds to the add button
    ///
    /// # Arguments
    ///
    /// * `x` - The column position to check
    /// * `y` - The row position to check
    ///
    /// # Returns
    ///
    /// * `true` - The click is on the add button
    /// * `false` - The click is not on the add button
    pub fn is_add_button_clicked(&self, x: u16, y: u16) -> bool {
        // add button is rendered on the row inside the border
        let tab_y = self.area.y + 1;
        // check if the click is on the correct row
        if y != tab_y {
            return false;
        }
        // check if the click is within the add button region
        if let Some((start, end)) = self.add_button_position {
            return x >= start && x < end;
        }
        false
    }

    /// Render the tab bar to the frame
    ///
    /// # Arguments
    ///
    /// * `frame` - The frame to render to
    /// * `area` - The area to render the tabs in
    /// * `tabs` - The list of tabs to render
    /// * `selected_index` - The index of the currently selected tab
    pub fn render<S: QuerySupport + Send + Sync>(
        &mut self,
        frame: &mut Frame,
        area: Rect,
        tabs: &TabState<S>,
    ) {
        // store the area for click detection
        self.area = area;
        // clear previous positions
        self.tab_positions.clear();
        self.add_button_position = None;
        // build spans for each tab and track positions
        let mut spans: Vec<Span> = Vec::new();
        // start position after the left border
        let mut x = area.x + 1;
        // build each tab span
        for (index, tab) in tabs.tabs.iter().enumerate() {
            // add padding around the label
            let label = format!(" {} ", tab.label);
            let label_len = label.width() as u16;
            // track this tab's position
            self.tab_positions.push((x, x + label_len, index));
            // style based on selection
            let style = if index == tabs.active {
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Blue)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::White).bg(Color::DarkGray)
            };
            spans.push(Span::styled(label, style));
            // update position
            x += label_len;
            // add a space between tabs
            spans.push(Span::raw(" "));
            x += 1;
        }
        // add the "+" button
        let add_label = " + ";
        let add_len = add_label.len() as u16;
        self.add_button_position = Some((x, x + add_len));
        spans.push(Span::styled(
            add_label,
            Style::default()
                .fg(Color::Black)
                .bg(Color::Green)
                .add_modifier(Modifier::BOLD),
        ));
        // build the line from spans
        let line = Line::from(spans);
        // render as a paragraph inside a block
        let paragraph =
            Paragraph::new(line).block(Block::default().borders(Borders::ALL).title("Tabs"));
        frame.render_widget(paragraph, area);
    }
}

/// The current state of tabs
pub struct TabState<S: QuerySupport + Send + Sync> {
    /// All open tabs in shoalctl
    pub tabs: Vec<Tab<S>>,
    /// The currently active tab
    pub active: usize,
    /// The visible height of the content viewport (set during render)
    pub viewport_height: u16,
    /// The visible width of the content viewport (set during render)
    pub viewport_width: u16,
    /// The type of queries this Tab can handle
    phantom: PhantomData<S>,
}

impl<S: QuerySupport + Send + Sync> Default for TabState<S>
where
    S::TableNames: Send,
    for<'a> <<S as QuerySupport>::ResponseKinds as rkyv::Archive>::Archived:
        rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    <S::ResponseKinds as rkyv::Archive>::Archived: rkyv::Deserialize<
            S::ResponseKinds,
            rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
        >,
    <S::ResponseKinds as rkyv::Archive>::Archived: std::marker::Send,
{
    /// Create a default initial tab state
    fn default() -> Self {
        // create a default Tab state
        TabState {
            tabs: vec![Tab::new("¯\\_(ツ)_/¯")],
            active: 0,
            viewport_height: 0,
            viewport_width: 0,
            phantom: PhantomData,
        }
    }
}

impl<S: QuerySupport + Sync + Send> TabState<S>
where
    S::TableNames: Send,
    for<'a> <<S as QuerySupport>::ResponseKinds as rkyv::Archive>::Archived:
        rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    <S::ResponseKinds as rkyv::Archive>::Archived: rkyv::Deserialize<
            S::ResponseKinds,
            rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
        >,
    <S::ResponseKinds as rkyv::Archive>::Archived: std::marker::Send,
{
    /// Get the currently selected tab
    pub fn get_active(&self) -> Option<&Tab<S>> {
        self.tabs.get(self.active)
    }

    /// Get the currently selected tab
    pub fn get_active_mut(&mut self) -> Option<&mut Tab<S>> {
        self.tabs.get_mut(self.active)
    }

    /// Change our active tab
    ///
    /// # Arguments
    ///
    /// * `new` - The index of the new tab to set as active
    pub fn set_active(&mut self, new: usize) {
        // only change our tab if its valid tab
        if new < self.tabs.len() {
            self.active = new;
        }
    }

    /// Add a new tab and make it active
    pub fn add_tab(&mut self) {
        // add a new generic tab
        self.tabs.push(Tab::new("New Tab"));
        // set this new tab as active
        self.active = self.tabs.len() - 1;
    }

    /// Close our currently active tab
    pub fn close_active_tab(&mut self) -> bool {
        // remove our current tab
        self.tabs.remove(self.active);
        // if we have no more tabs left then exit
        if self.tabs.is_empty() {
            // we have no more tabs so flag that we should exit
            return true;
        }
        // get the largest valid tab index
        let largest = self.tabs.len() - 1;
        // find the available tab to make active
        if self.active < largest {
            // set the tab after us as the new active tab
            self.active += 1
        } else {
            // set the tab before us as the new active tab
            self.active -= 1
        }
        // we still have tabs so no need to exit
        false
    }

    /// Move to the next tab in the tab order
    ///
    /// Wraps around from the last tab to the first.
    fn next(&mut self) {
        if self.tabs.is_empty() {
            return;
        }
        self.active = (self.active + 1) % self.tabs.len();
    }

    /// Move to the previous tab in the tab order
    ///
    /// Wraps around from the first tab to the last.
    fn prev(&mut self) {
        if self.tabs.is_empty() {
            return;
        }
        self.active = if self.active == 0 {
            self.tabs.len() - 1
        } else {
            self.active - 1
        };
    }

    /// Handle key input when the query box is focused
    ///
    /// The completion menu takes the keys helix binds it to — tab, the arrows, and control n
    /// and p move through it, and enter accepts whatever is selected. Enter only submits the
    /// query when the menu is closed.
    ///
    /// # Arguments
    ///
    /// * `key` - The key that was pressed
    /// * `shoal` - A client to shoal to submit queries with
    /// * `app_tx` - The channel to send query results back over
    pub async fn handle_query_input(
        &mut self,
        key: KeyEvent,
        shoal: &Arc<Shoal<S>>,
        app_tx: &mut AsyncSender<AppEvent<S>>,
    ) where
        S: 'static,
        S::QueryKinds: Send,
        S::ResponseKinds: Send,
    {
        // get the currently active tab
        let Some(active_tab) = self.get_active_mut() else {
            return;
        };
        // let the completion menu take the keys it is bound to first
        if active_tab.handle_completion_key(key) {
            return;
        }
        match key.code {
            // type characters, which always gives the menu another chance to open
            KeyCode::Char(c) => {
                active_tab.insert_char(c);
                active_tab.completion.undismiss();
                active_tab.refresh_completions(false);
            }
            // delete character before cursor
            KeyCode::Backspace => {
                active_tab.delete_char_before();
                active_tab.refresh_completions(false);
            }
            // delete character at cursor
            KeyCode::Delete => {
                active_tab.delete_char_at();
                active_tab.refresh_completions(false);
            }
            // move cursor left
            KeyCode::Left => {
                active_tab.move_cursor_left();
                active_tab.refresh_completions(false);
            }
            // move cursor right
            KeyCode::Right => {
                active_tab.move_cursor_right();
                active_tab.refresh_completions(false);
            }
            // move cursor to start
            KeyCode::Home => {
                active_tab.query_cursor = 0;
                active_tab.refresh_completions(false);
            }
            // move cursor to end
            KeyCode::End => {
                active_tab.query_cursor = active_tab.query.len();
                active_tab.refresh_completions(false);
            }
            // submit this query, since an open menu would have taken this key already
            KeyCode::Enter => active_tab.submit_query(shoal, app_tx).await,
            _ => {}
        }
    }

    /// Rebuild the completions on offer for the active tab's query
    ///
    /// This is what puts the menu up when the query box is focused rather than typed in.
    pub fn refresh_completions(&mut self) {
        // get our currently active tab
        if let Some(active_tab) = self.get_active_mut() {
            // show whatever could be typed at its cursor
            active_tab.refresh_completions(false);
        }
    }

    /// Scroll the active tab's content, clamped to content bounds
    pub fn scroll(&mut self, dy: i16, dx: i16) {
        let vh = self.viewport_height;
        let vw = self.viewport_width;
        if let Some(tab) = self.get_active_mut() {
            if tab.content.is_empty() {
                return;
            }
            // compute content dimensions
            let line_count = tab.content.lines().count() as u16;
            let max_line_width = tab
                .content
                .lines()
                .map(|l| l.width() as u16)
                .max()
                .unwrap_or(0);
            // max scroll is content size minus viewport, floored at 0
            let max_y = line_count.saturating_sub(vh);
            let max_x = max_line_width.saturating_sub(vw);
            // apply delta and clamp
            tab.scroll_y = tab.scroll_y.saturating_add_signed(dy).min(max_y);
            tab.scroll_x = tab.scroll_x.saturating_add_signed(dx).min(max_x);
        }
    }

    /// Clear the query on our active tab
    pub fn clear_query(&mut self) {
        // get our currently active tab
        if let Some(active_tab) = self.get_active_mut() {
            // clear our query string
            active_tab.query.clear();
            // reset our cursor to 0
            active_tab.query_cursor = 0;
            // there is nothing left to complete
            active_tab.completion.clear();
            // and nothing left for an error to be about
            active_tab.clear_error();
        }
    }
}
