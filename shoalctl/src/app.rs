//! The main application state and event handling for shoalctl
//!
//! This module contains the core application struct that manages tabs,
//! handles user input, and coordinates rendering of all components.

use crossterm::event::{Event, KeyCode, KeyEventKind, MouseButton, MouseEventKind};
use ratatui::Frame;
use uuid::Uuid;

use crate::components::{QueryInput, StatusBar, TabContent, Tabs};

/// The current mode of the application
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Normal mode for navigation and commands
    Normal,
    /// Insert mode for text input
    Insert,
}

/// A single tab in the application
#[derive(Debug, Clone)]
pub struct Tab {
    /// The unique identifier for this tab
    pub id: Uuid,
    /// The display label for this tab
    pub label: String,
}

impl Tab {
    /// Create a new tab with a generated UUID and the given label
    ///
    /// # Arguments
    ///
    /// * `label` - The display label for this tab
    ///
    /// # Returns
    ///
    /// A new Tab instance with a unique UUID
    pub fn new(label: String) -> Self {
        Self {
            id: Uuid::new_v4(),
            label,
        }
    }
}

/// The main application state
pub struct App {
    /// The current mode of the application
    pub mode: Mode,
    /// The list of all tabs
    pub tabs: Vec<Tab>,
    /// The index of the currently selected tab
    pub selected_index: usize,
    /// Whether the application should quit on the next loop iteration
    pub should_quit: bool,
    /// The current query text in the input box
    pub query: String,
    /// The cursor position within the query text
    pub query_cursor: usize,
    /// Counter for generating tab labels
    tab_counter: usize,
    /// The tabs component for rendering the tab bar
    tabs_component: Tabs,
    /// The content component for rendering the tab content area
    content_component: TabContent,
    /// The status bar component for rendering the mode indicator
    status_bar: StatusBar,
    /// The query input component for entering queries
    query_input: QueryInput,
}

impl App {
    /// Create a new application instance
    ///
    /// Initializes the app with the default tabs and all components
    /// in their default state. Starts in Normal mode.
    ///
    /// # Returns
    ///
    /// A new App instance ready to run
    pub fn new() -> Self {
        // create the initial tabs
        let tabs = vec![
            Tab::new("one".to_string()),
            Tab::new("two".to_string()),
            Tab::new("woot".to_string()),
        ];

        Self {
            mode: Mode::Normal,
            tabs,
            selected_index: 0,
            should_quit: false,
            query: String::new(),
            query_cursor: 0,
            tab_counter: 3,
            tabs_component: Tabs::new(),
            content_component: TabContent::new(),
            status_bar: StatusBar::new(),
            query_input: QueryInput::new(),
        }
    }

    /// Get the currently selected tab
    ///
    /// # Returns
    ///
    /// A reference to the currently selected tab, or None if no tabs exist
    pub fn selected_tab(&self) -> Option<&Tab> {
        self.tabs.get(self.selected_index)
    }

    /// Handle an incoming terminal event
    ///
    /// Dispatches key presses and mouse clicks to the appropriate handlers.
    ///
    /// # Arguments
    ///
    /// * `event` - The terminal event to process
    pub fn handle_event(&mut self, event: Event) {
        match event {
            // handle key press events
            Event::Key(key) if key.kind == KeyEventKind::Press => {
                self.handle_key(key.code);
            }
            // handle mouse click events (work in any mode)
            Event::Mouse(mouse) => {
                // only respond to left mouse button down events
                if mouse.kind == MouseEventKind::Down(MouseButton::Left) {
                    self.handle_click(mouse.column, mouse.row);
                }
            }
            // ignore all other events
            _ => {}
        }
    }

    /// Handle a key press event
    ///
    /// Key behavior depends on the current mode. Mode switching keys
    /// work in any mode, but shortcuts only work in Normal mode.
    ///
    /// # Arguments
    ///
    /// * `code` - The key code that was pressed
    fn handle_key(&mut self, code: KeyCode) {
        match self.mode {
            Mode::Normal => self.handle_normal_mode_key(code),
            Mode::Insert => self.handle_insert_mode_key(code),
        }
    }

    /// Handle a key press in Normal mode
    ///
    /// # Arguments
    ///
    /// * `code` - The key code that was pressed
    fn handle_normal_mode_key(&mut self, code: KeyCode) {
        match code {
            // switch to insert mode
            KeyCode::Char('i') => self.mode = Mode::Insert,
            // quit the application
            KeyCode::Char('q') => self.should_quit = true,
            // move to the next tab
            KeyCode::Tab => self.next_tab(),
            // move to the previous tab
            KeyCode::BackTab => self.prev_tab(),
            // add a new tab
            KeyCode::Char('+') | KeyCode::Char('=') | KeyCode::Char('n') => self.add_tab(),
            // close the current tab
            KeyCode::Char('-') => self.close_current_tab(),
            // query input: type characters
            KeyCode::Char(c) => self.insert_char(c),
            // query input: delete character before cursor
            KeyCode::Backspace => self.delete_char_before(),
            // query input: delete character at cursor
            KeyCode::Delete => self.delete_char_at(),
            // query input: move cursor left
            KeyCode::Left => self.move_cursor_left(),
            // query input: move cursor right
            KeyCode::Right => self.move_cursor_right(),
            // query input: move cursor to start
            KeyCode::Home => self.query_cursor = 0,
            // query input: move cursor to end
            KeyCode::End => self.query_cursor = self.query.len(),
            // submit query with Enter
            KeyCode::Enter => self.submit_query(),
            // ignore all other keys
            _ => {}
        }
    }

    /// Handle a key press in Insert mode
    ///
    /// # Arguments
    ///
    /// * `code` - The key code that was pressed
    fn handle_insert_mode_key(&mut self, code: KeyCode) {
        match code {
            // switch back to normal mode
            KeyCode::Esc => self.mode = Mode::Normal,
            // query input: type characters
            KeyCode::Char(c) => self.insert_char(c),
            // query input: delete character before cursor
            KeyCode::Backspace => self.delete_char_before(),
            // query input: delete character at cursor
            KeyCode::Delete => self.delete_char_at(),
            // query input: move cursor left
            KeyCode::Left => self.move_cursor_left(),
            // query input: move cursor right
            KeyCode::Right => self.move_cursor_right(),
            // query input: move cursor to start
            KeyCode::Home => self.query_cursor = 0,
            // query input: move cursor to end
            KeyCode::End => self.query_cursor = self.query.len(),
            // submit query with Enter
            KeyCode::Enter => self.submit_query(),
            // ignore all other keys
            _ => {}
        }
    }

    /// Insert a character at the current cursor position
    fn insert_char(&mut self, c: char) {
        self.query.insert(self.query_cursor, c);
        self.query_cursor += 1;
    }

    /// Delete the character before the cursor (backspace)
    fn delete_char_before(&mut self) {
        if self.query_cursor > 0 {
            self.query_cursor -= 1;
            self.query.remove(self.query_cursor);
        }
    }

    /// Delete the character at the cursor (delete)
    fn delete_char_at(&mut self) {
        if self.query_cursor < self.query.len() {
            self.query.remove(self.query_cursor);
        }
    }

    /// Move the cursor left
    fn move_cursor_left(&mut self) {
        if self.query_cursor > 0 {
            self.query_cursor -= 1;
        }
    }

    /// Move the cursor right
    fn move_cursor_right(&mut self) {
        if self.query_cursor < self.query.len() {
            self.query_cursor += 1;
        }
    }

    /// Submit the current query
    fn submit_query(&mut self) {
        // TODO: Actually submit the query to the server
        // For now, just clear the input
        self.query.clear();
        self.query_cursor = 0;
    }

    /// Handle a mouse click event
    ///
    /// Checks if the click is on a tab or the add button and handles accordingly.
    /// Mouse clicks work in any mode.
    ///
    /// # Arguments
    ///
    /// * `x` - The column position of the click
    /// * `y` - The row position of the click
    fn handle_click(&mut self, x: u16, y: u16) {
        // check if the click was on the add button
        if self.tabs_component.is_add_button_clicked(x, y) {
            self.add_tab();
            return;
        }
        // check if the click was on a tab
        if let Some(index) = self.tabs_component.tab_index_at_position(x, y) {
            if index < self.tabs.len() {
                self.selected_index = index;
            }
        }
    }

    /// Add a new tab to the application
    fn add_tab(&mut self) {
        // increment counter and generate a label for the new tab
        self.tab_counter += 1;
        let label = format!("tab {}", self.tab_counter);
        let tab = Tab::new(label);
        self.tabs.push(tab);
        // select the newly created tab
        self.selected_index = self.tabs.len() - 1;
    }

    /// Close the currently selected tab
    fn close_current_tab(&mut self) {
        // don't close if there's only one tab
        if self.tabs.len() <= 1 {
            return;
        }
        // remove the current tab
        self.tabs.remove(self.selected_index);
        // adjust the selected index if needed
        if self.selected_index >= self.tabs.len() {
            self.selected_index = self.tabs.len() - 1;
        }
    }

    /// Move to the next tab in the tab order
    ///
    /// Wraps around from the last tab to the first.
    fn next_tab(&mut self) {
        if self.tabs.is_empty() {
            return;
        }
        self.selected_index = (self.selected_index + 1) % self.tabs.len();
    }

    /// Move to the previous tab in the tab order
    ///
    /// Wraps around from the first tab to the last.
    fn prev_tab(&mut self) {
        if self.tabs.is_empty() {
            return;
        }
        self.selected_index = if self.selected_index == 0 {
            self.tabs.len() - 1
        } else {
            self.selected_index - 1
        };
    }

    /// Render the application to the terminal
    ///
    /// Draws the tab bar at the top, the content area in the middle,
    /// the query input below that, and the status bar at the bottom.
    ///
    /// # Arguments
    ///
    /// * `frame` - The frame to render widgets to
    pub fn render(&mut self, frame: &mut Frame) {
        use ratatui::layout::{Constraint, Layout};

        // split the frame into four vertical chunks: tabs, content, query input, and status bar
        let chunks = Layout::vertical([
            Constraint::Length(3),  // tabs
            Constraint::Min(0),     // content
            Constraint::Length(3),  // query input
            Constraint::Length(2),  // status bar
        ])
        .split(frame.area());

        // render the tab bar in the top chunk
        self.tabs_component
            .render(frame, chunks[0], &self.tabs, self.selected_index);
        // render the content area in the middle chunk
        self.content_component
            .render(frame, chunks[1], self.selected_tab());
        // render the query input (always focused since it accepts input in both modes)
        self.query_input
            .render(frame, chunks[2], &self.query, self.query_cursor, true);
        // render the status bar in the bottom chunk
        self.status_bar.render(frame, chunks[3], self.mode);
    }
}
