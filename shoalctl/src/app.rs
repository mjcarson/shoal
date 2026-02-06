//! The main application state and event handling for shoalctl
//!
//! This module contains the core application struct that manages tabs,
//! handles user input, and coordinates rendering of all components.

use crossterm::event::{Event, EventStream, KeyCode, KeyEventKind, MouseButton, MouseEventKind};
use futures::StreamExt;
use kanal::{AsyncReceiver, AsyncSender};
use ratatui::{DefaultTerminal, Frame};
use shoal::client::{Errors, Shoal, ShoalResponse};
use shoal::traits::QuerySupport;
use std::sync::Arc;
use uuid::Uuid;

use crate::AppEvent;
use crate::components::{HelpOverlay, StatusBar, TabContent, TabQueryBar, TabSelector, TabState};

/// A request to execute a query
pub struct QueryRequest<S: QuerySupport> {
    /// The tab ID to send results to
    pub tab_id: Uuid,
    /// The parsed query to execute
    pub query: S::QueryKinds,
}

/// The result of a query execution
pub enum QueryResult<S: QuerySupport> {
    /// A response from a query
    Response(ShoalResponse<S>),
    /// Query failed with an error
    Error(Errors),
}

/// The current mode of the application
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Normal mode for navigation and commands
    Normal,
    /// Insert mode for text input
    Insert,
}

/// Background task that forwards terminal events to the central event channel
async fn terminal_event_forwarder<S: QuerySupport>(event_tx: AsyncSender<AppEvent<S>>) {
    let mut event_stream = EventStream::new();
    while let Some(event_result) = event_stream.next().await {
        match event_result {
            Ok(event) => {
                if event_tx.send(AppEvent::Terminal(event)).await.is_err() {
                    // Channel closed, stop forwarding
                    break;
                }
            }
            Err(_) => {
                // Terminal event error, continue trying
                continue;
            }
        }
    }
}

/// The main application state, generic over the database client type
pub struct App<Q: QuerySupport + Send + Sync> {
    /// A client to shoal
    shoal: Arc<Shoal<Q>>,
    /// A channel to send App events over
    app_tx: AsyncSender<AppEvent<Q>>,
    /// A channel to receive App events over
    app_rx: AsyncReceiver<AppEvent<Q>>,
    /// The current mode of the application
    pub mode: Mode,
    /// The state for all tabs
    pub tabs: TabState<Q>,
    /// The area of the query input for click detection
    query_area: Option<ratatui::layout::Rect>,
    /// Whether the query input box is focused
    pub query_focused: bool,
    /// Counter for generating tab labels
    tab_counter: usize,
    /// The tabs component for rendering the tab bar
    tabs_component: TabSelector,
    /// The content component for rendering the tab content area
    content_component: TabContent,
    /// The status bar component for rendering the mode indicator
    status_bar: StatusBar,
    /// The query input component for entering queries
    query_input: TabQueryBar,
    /// The help overlay component for displaying shortcuts
    help_overlay: HelpOverlay,
    /// Whether shortcut mode is active (triggered by spacebar)
    shortcut_mode_active: bool,
    /// Whether the user has asked shoalctl to exit
    should_quit: bool,
}

impl<Q: QuerySupport + Sync + Send> App<Q>
where
    for<'a> <<Q as QuerySupport>::ResponseKinds as rkyv::Archive>::Archived:
        rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'a>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    <<Q as QuerySupport>::ResponseKinds as rkyv::Archive>::Archived: rkyv::Deserialize<
            <Q as QuerySupport>::ResponseKinds,
            rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
        >,
    <<Q as QuerySupport>::ResponseKinds as rkyv::Archive>::Archived: std::marker::Send,
{
    /// Create a new application instance
    ///
    /// Initializes the app with the default tabs and all components
    /// in their default state. Starts in Normal mode.
    ///
    /// # Arguments
    ///
    /// * `query_tx` - Channel to send query requests to the background executor
    pub fn new(shoal: Arc<Shoal<Q>>) -> Self {
        // create the channel for app events we need to handle
        let (app_tx, app_rx) = kanal::unbounded_async::<AppEvent<Q>>();
        // create our app
        Self {
            shoal,
            app_tx,
            app_rx,
            mode: Mode::Normal,
            tabs: TabState::default(),
            query_area: None,
            query_focused: true,
            tab_counter: 1,
            tabs_component: TabSelector::new(),
            content_component: TabContent::new(),
            status_bar: StatusBar::new(),
            query_input: TabQueryBar::new(),
            help_overlay: HelpOverlay::new(),
            shortcut_mode_active: false,
            should_quit: false,
        }
    }

    /// Handle an incoming terminal event
    ///
    /// Dispatches key presses and mouse clicks to the appropriate handlers.
    ///
    /// # Arguments
    ///
    /// * `event` - The terminal event to process
    pub async fn handle_event(&mut self, event: Event) {
        match event {
            // handle key press events
            Event::Key(key) if key.kind == KeyEventKind::Press => {
                self.handle_key(key.code).await;
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
    /// When the query box is focused, all typing goes to the query.
    /// When unfocused, key behavior depends on the current mode.
    /// Escape always switches from Insert to Normal mode or exits shortcut mode.
    ///
    /// # Arguments
    ///
    /// * `code` - The key code that was pressed
    async fn handle_key(&mut self, code: KeyCode) {
        // if the escape key was hit then revert to normal mode or minimize the help window
        if code == KeyCode::Esc {
            // check if we need to minimize the shortcut help window
            if self.shortcut_mode_active {
                // minimize the shortcut help window
                self.shortcut_mode_active = false;
                // we only exit one thing at a time
                return;
            }
            // check if we need to revert back to normal mode
            if self.mode == Mode::Insert {
                // revert back to normal mode
                self.mode = Mode::Normal;
                // we only exit one thing at a time
                return;
            }
        }
        // ff shortcut mode is active, handle shortcut keys
        if self.shortcut_mode_active {
            // try to process this shortcut
            self.handle_shortcut_mode_key(code);
            // if were in shortcut mode we don't also want to type or do other things
            return;
        }
        // If the query box is focused, handle input for the query
        if self.query_focused {
            self.tabs
                .handle_query_input(code, &self.shoal, &mut self.app_tx)
                .await;
        } else {
            // Query not focused, use mode-based handling
            match self.mode {
                Mode::Normal => self.handle_normal_mode_key(code),
                Mode::Insert => self.handle_insert_mode_key(code),
            }
        }
    }

    /// Handle a key press in Normal mode (when query is not focused)
    ///
    /// # Arguments
    ///
    /// * `code` - The key code that was pressed
    fn handle_normal_mode_key(&mut self, code: KeyCode) {
        match code {
            // activate shortcut mode
            KeyCode::Char(' ') => self.shortcut_mode_active = true,
            // switch to insert mode
            KeyCode::Char('i') => self.mode = Mode::Insert,
            // quit the application
            KeyCode::Char('q') => self.should_quit = true,
            //// move to the next tab
            //KeyCode::Tab => self.next_tab(),
            //// move to the previous tab
            //KeyCode::BackTab => self.prev_tab(),
            // add a new tab
            KeyCode::Char('+') | KeyCode::Char('=') | KeyCode::Char('n') => self.tabs.add_tab(),
            // close the current tab
            KeyCode::Char('-') => {
                // close our currently active tab
                if self.tabs.close_active_tab() {
                    // we have no more tabs so exit
                    self.should_quit = true;
                }
            }
            // ignore all other keys
            _ => {}
        }
    }

    /// Handle a key press in shortcut mode
    ///
    /// Shortcut mode is activated by pressing spacebar in normal mode.
    /// Available shortcuts:
    /// - q: Focus the query box
    /// - w: Clear the query box
    /// - t: Create a new tab
    /// - p: Close the current tab
    ///
    /// # Arguments
    ///
    /// * `code` - The key code that was pressed
    fn handle_shortcut_mode_key(&mut self, code: KeyCode) {
        // Always exit shortcut mode after handling a key
        self.shortcut_mode_active = false;

        match code {
            // focus the query box
            KeyCode::Char('q') => self.query_focused = true,
            // clear the query box
            KeyCode::Char('w') => self.tabs.clear_query(),
            // create a new tab
            //KeyCode::Char('t') => self.add_tab(),
            // close the current tab
            KeyCode::Char('p') => {
                // close our currently active tab
                if self.tabs.close_active_tab() {
                    // we have no more tabs so exit
                    self.should_quit = true;
                }
            }
            // any other key just exits shortcut mode (already done above)
            _ => {}
        }
    }

    /// Handle a key press in Insert mode (when query is not focused)
    ///
    /// In Insert mode without query focus, this will be used for editing
    /// table rows in the future.
    ///
    /// # Arguments
    ///
    /// * `code` - The key code that was pressed
    fn handle_insert_mode_key(&mut self, code: KeyCode) {
        match code {
            // TODO: Handle insert mode for editing rows
            // For now, just handle tab switching
            //KeyCode::Tab => self.next_tab(),
            //KeyCode::BackTab => self.prev_tab(),
            // ignore all other keys for now
            _ => {}
        }
    }

    /// Handle a single query result
    ///
    /// Called by the main event loop when a query result is received.
    pub fn handle_result(&mut self, tab_id: Uuid, result: QueryResult<Q>) {
        //match result {
        //    QueryResult::Response(response) => {
        //        if let Some(tab) = self.tabs.iter_mut().find(|t| t.id == tab_id) {
        //            // Format the response and append to content
        //            let row_str = format!("{:?}", response);
        //            // update our content
        //            tab.content = row_str;
        //        }
        //    }
        //    QueryResult::Error(error) => {
        //        if let Some(tab) = self.tabs.iter_mut().find(|t| t.id == tab_id) {
        //            tab.content = format!("Error: {:#?}", error);
        //        }
        //    }
        //}
    }

    /// Handle a mouse click event
    ///
    /// Checks if the click is on a tab, the add button, or the query box.
    /// Clicking on the query box focuses it, clicking elsewhere unfocuses it.
    /// Mouse clicks work in any mode.
    ///
    /// # Arguments
    ///
    /// * `x` - The column position of the click
    /// * `y` - The row position of the click
    fn handle_click(&mut self, x: u16, y: u16) {
        // Check if the click was on the query input area
        if let Some(query_area) = self.query_area {
            if x >= query_area.x
                && x < query_area.x + query_area.width
                && y >= query_area.y
                && y < query_area.y + query_area.height
            {
                self.query_focused = true;
                return;
            }
        }
        // Click was outside the query box, unfocus it
        self.query_focused = false;

        //// check if the click was on the add button
        //if self.tabs_component.is_add_button_clicked(x, y) {
        //    self.add_tab();
        //    return;
        //}
        // check if the click was on a tab
        if let Some(new) = self.tabs_component.tab_index_at_position(x, y) {
            self.tabs.set_active(new);
        }
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
            Constraint::Length(3), // tabs
            Constraint::Min(0),    // content
            Constraint::Length(3), // query input
            Constraint::Length(2), // status bar
        ])
        .split(frame.area());
        // Store the query area for click detection
        self.query_area = Some(chunks[2]);
        // render the tab bar in the top chunk
        self.tabs_component.render(frame, chunks[0], &self.tabs);
        // get our currently active tab
        let active_tab = self.tabs.get_active();
        // render the content area in the middle chunk
        self.content_component.render(frame, chunks[1], active_tab);
        // render the query input (focused state determines border color)
        self.query_input
            .render(frame, chunks[2], active_tab, self.query_focused);
        // render the status bar in the bottom chunk
        self.status_bar.render(frame, chunks[3], self.mode);

        // render the help overlay if shortcut mode is active
        if self.shortcut_mode_active {
            self.help_overlay.render(frame, frame.area());
        }
    }

    /// Start handling events and updating our tui
    pub async fn start(&mut self, terminal: &mut DefaultTerminal) -> std::io::Result<()> {
        // Spawn the terminal event forwarder (sends terminal events to event channel)
        tokio::spawn(terminal_event_forwarder(self.app_tx.clone()));
        // draw an initial frame until we get an event to handle
        terminal.draw(|frame| self.render(frame))?;
        // keep handling events until we get an exit event
        while !self.should_quit {
            match self.app_rx.recv().await {
                Ok(event) => {
                    match event {
                        AppEvent::Terminal(terminal_event) => {
                            self.handle_event(terminal_event).await;
                        }
                        AppEvent::QueryResult { tab_id, result } => {
                            self.handle_result(tab_id, result);
                        }
                    }
                    // Redraw after handling any event
                    terminal.draw(|frame| self.render(frame))?;
                }
                Err(_) => {
                    // Channel closed, exit
                    break;
                }
            }
        }
        Ok(())
    }
}
