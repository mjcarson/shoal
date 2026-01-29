//! The query input component for shoalctl
//!
//! This module provides a text input box at the bottom of the screen
//! for entering database queries.

use ratatui::{
    Frame,
    layout::Rect,
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Paragraph},
};

/// The query input component
///
/// Displays a text input box for entering queries.
pub struct QueryInput;

impl QueryInput {
    /// Create a new query input component
    ///
    /// # Returns
    ///
    /// A new QueryInput instance
    pub fn new() -> Self {
        Self
    }

    /// Render the query input to the frame
    ///
    /// Displays the current query text with a cursor.
    ///
    /// # Arguments
    ///
    /// * `frame` - The frame to render to
    /// * `area` - The area to render the input in
    /// * `query` - The current query text
    /// * `cursor_position` - The current cursor position in the query
    /// * `focused` - Whether the input is currently focused
    pub fn render(
        &self,
        frame: &mut Frame,
        area: Rect,
        query: &str,
        cursor_position: usize,
        focused: bool,
    ) {
        // determine border color based on focus state
        let border_color = if focused {
            Color::Cyan
        } else {
            Color::DarkGray
        };
        // build the input widget
        let input = Paragraph::new(query)
            .style(Style::default().fg(Color::White))
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(border_color))
                    .title(" Query ")
                    .title_style(
                        Style::default()
                            .fg(border_color)
                            .add_modifier(Modifier::BOLD),
                    ),
            );
        // render the input
        frame.render_widget(input, area);
        // show cursor when focused
        if focused {
            // calculate cursor position within the input area
            // account for the border (1 char) on the left
            let cursor_x = area.x + 1 + cursor_position as u16;
            let cursor_y = area.y + 1;
            // only show cursor if it's within the visible area
            if cursor_x < area.x + area.width - 1 {
                frame.set_cursor_position((cursor_x, cursor_y));
            }
        }
    }
}
