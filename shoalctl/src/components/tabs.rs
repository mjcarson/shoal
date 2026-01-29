//! The tab bar component for shoalctl
//!
//! This module provides a clickable tab bar that allows users to switch
//! between different views in the application.

use ratatui::{
    Frame,
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
};

use crate::app::Tab;

/// The tab bar component
///
/// Renders a horizontal tab bar and tracks tab positions for click detection.
pub struct Tabs {
    /// The clickable regions for each tab as (start_x, end_x, index)
    tab_positions: Vec<(u16, u16, usize)>,
    /// The clickable region for the add button as (start_x, end_x)
    add_button_position: Option<(u16, u16)>,
    /// The area where the tabs were last rendered
    area: Rect,
}

impl Tabs {
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
    pub fn render(&mut self, frame: &mut Frame, area: Rect, tabs: &[Tab], selected_index: usize) {
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
        for (index, tab) in tabs.iter().enumerate() {
            // add padding around the label
            let label = format!(" {} ", tab.label);
            let label_len = label.len() as u16;
            // track this tab's position
            self.tab_positions.push((x, x + label_len, index));
            // style based on selection
            let style = if index == selected_index {
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
        let paragraph = Paragraph::new(line)
            .block(Block::default().borders(Borders::ALL).title("Tabs"));
        frame.render_widget(paragraph, area);
    }
}
