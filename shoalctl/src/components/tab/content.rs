//! The tab content component for shoalctl
//!
//! This module provides the content area that displays information
//! based on the currently selected tab.

use ratatui::{
    Frame,
    layout::Rect,
    style::{Color, Style},
    widgets::{Block, Borders, Paragraph},
};
use shoal::traits::QuerySupport;

use super::Tab;

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
    /// Displays the content of the currently selected tab.
    /// Shows a placeholder message if no tab is selected or content is empty.
    ///
    /// # Arguments
    ///
    /// * `frame` - The frame to render to
    /// * `area` - The area to render the content in
    /// * `selected` - The currently selected tab, or None if no tabs exist
    pub fn render<S: QuerySupport + Send + Sync>(
        &self,
        frame: &mut Frame,
        area: Rect,
        selected: Option<&Tab<S>>,
    ) {
        // get the content and scroll offsets
        let (text, scroll_y, scroll_x) = match selected {
            Some(tab) if !tab.content.is_empty() => {
                (tab.content.as_str(), tab.scroll_y, tab.scroll_x)
            }
            Some(_) => ("Enter a query and press Enter to see results", 0, 0),
            None => ("No tab selected", 0, 0),
        };
        // build a paragraph with the tab's content
        let content = Paragraph::new(text)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Results")
                    .border_style(Style::default().fg(Color::Cyan)),
            )
            .style(Style::default().fg(Color::White))
            .scroll((scroll_y, scroll_x));
        // render the content widget
        frame.render_widget(content, area);
    }
}
