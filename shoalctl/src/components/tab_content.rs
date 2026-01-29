//! The tab content component for shoalctl
//!
//! This module provides the content area that displays information
//! based on the currently selected tab.

use ratatui::{
    Frame,
    layout::{Alignment, Rect},
    style::{Color, Style},
    widgets::{Block, Borders, Paragraph},
};

use crate::app::Tab;

/// The content area component
///
/// Displays content based on the currently selected tab.
pub struct TabContent;

impl TabContent {
    /// Create a new tab content component
    ///
    /// # Returns
    ///
    /// A new TabContent instance
    pub fn new() -> Self {
        Self
    }

    /// Render the content area to the frame
    ///
    /// Displays the label of the currently selected tab centered
    /// in a bordered box.
    ///
    /// # Arguments
    ///
    /// * `frame` - The frame to render to
    /// * `area` - The area to render the content in
    /// * `selected` - The currently selected tab, or None if no tabs exist
    pub fn render(&self, frame: &mut Frame, area: Rect, selected: Option<&Tab>) {
        // get the label to display, or a placeholder if no tab is selected
        let label = selected.map(|t| t.label.as_str()).unwrap_or("No tab selected");
        // build a paragraph with the selected tab's label
        let content = Paragraph::new(label)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Content")
                    .border_style(Style::default().fg(Color::Cyan)),
            )
            .alignment(Alignment::Center)
            .style(Style::default().fg(Color::White));
        // render the content widget
        frame.render_widget(content, area);
    }
}
