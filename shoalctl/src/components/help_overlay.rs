//! The help overlay component for shoalctl
//!
//! This module provides a floating help window that displays available
//! keyboard shortcuts when the user activates shortcut mode with spacebar.

use ratatui::{
    Frame,
    layout::{Alignment, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph},
};

/// The help overlay component
///
/// Displays a floating window in the bottom right corner showing
/// available keyboard shortcuts when shortcut mode is active.
pub struct HelpOverlay;

impl HelpOverlay {
    /// Create a new help overlay component
    ///
    /// # Returns
    ///
    /// A new HelpOverlay instance
    pub fn new() -> Self {
        Self
    }

    /// Render the help overlay to the frame
    ///
    /// Displays a floating window in the bottom right corner with
    /// available keyboard shortcuts.
    ///
    /// # Arguments
    ///
    /// * `frame` - The frame to render to
    /// * `area` - The full terminal area (overlay positions itself)
    pub fn render(&self, frame: &mut Frame, area: Rect) {
        // Define the overlay size
        let overlay_width = 24u16;
        let overlay_height = 8u16;

        // Calculate position in bottom right corner with some padding
        let x = area.width.saturating_sub(overlay_width + 2);
        let y = area.height.saturating_sub(overlay_height + 2);

        let overlay_area = Rect::new(x, y, overlay_width, overlay_height);

        // Build the shortcut lines
        let shortcuts = vec![
            Line::from(vec![
                Span::styled("q", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
                Span::raw(" - focus query box"),
            ]),
            Line::from(vec![
                Span::styled("w", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
                Span::raw(" - clear query box"),
            ]),
            Line::from(vec![
                Span::styled("t", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
                Span::raw(" - new tab"),
            ]),
            Line::from(vec![
                Span::styled("p", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
                Span::raw(" - close tab"),
            ]),
            Line::from(""),
            Line::from(Span::styled(
                "esc - cancel",
                Style::default().fg(Color::DarkGray),
            )),
        ];

        // Create the overlay block with medium dark gray background
        let block = Block::default()
            .title(" Shortcuts ")
            .title_alignment(Alignment::Center)
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Gray))
            .style(Style::default().bg(Color::Rgb(60, 60, 60)));

        let paragraph = Paragraph::new(shortcuts)
            .block(block)
            .style(Style::default().fg(Color::White).bg(Color::Rgb(60, 60, 60)));

        // Clear the area first (important for overlays), then render the widget
        frame.render_widget(Clear, overlay_area);
        frame.render_widget(paragraph, overlay_area);
    }
}
