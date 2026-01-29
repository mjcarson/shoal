//! The status bar component for shoalctl
//!
//! This module provides a status bar at the bottom of the screen that
//! displays the current mode and other status information.

use ratatui::{
    Frame,
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
};

use crate::app::Mode;

/// The status bar component
///
/// Displays the current mode and other status information at the bottom
/// of the screen.
pub struct StatusBar;

impl StatusBar {
    /// Create a new status bar component
    ///
    /// # Returns
    ///
    /// A new StatusBar instance
    pub fn new() -> Self {
        Self
    }

    /// Render the status bar to the frame
    ///
    /// Displays the current mode in the bottom left corner.
    ///
    /// # Arguments
    ///
    /// * `frame` - The frame to render to
    /// * `area` - The area to render the status bar in
    /// * `mode` - The current application mode
    pub fn render(&self, frame: &mut Frame, area: Rect, mode: Mode) {
        // determine the mode text and color
        let (mode_text, mode_color) = match mode {
            Mode::Normal => ("NORMAL", Color::Blue),
            Mode::Insert => ("INSERT", Color::Green),
        };
        // build the mode indicator span with styling
        let mode_span = Span::styled(
            format!(" {} ", mode_text),
            Style::default()
                .fg(Color::Black)
                .bg(mode_color)
                .add_modifier(Modifier::BOLD),
        );
        // build the status line
        let status_line = Line::from(vec![mode_span]);
        // build the paragraph widget
        let status = Paragraph::new(status_line).block(
            Block::default()
                .borders(Borders::TOP)
                .border_style(Style::default().fg(Color::DarkGray)),
        );
        // render the status bar
        frame.render_widget(status, area);
    }
}
