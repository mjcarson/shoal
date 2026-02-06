//! The query input component for shoalctl
//!
//! This module provides a text input box at the bottom of the screen
//! for entering database queries.

use kanal::AsyncSender;
use ratatui::{
    Frame,
    layout::Rect,
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Paragraph},
};
use rkyv::Archive;
use shoal::client::{Errors, Shoal};
use shoal::traits::QuerySupport;
use std::sync::Arc;
use uuid::Uuid;

use super::Tab;
use crate::{AppEvent, app::QueryResult};

/// Execute queries for shoalctl
pub async fn run<S: QuerySupport>(
    shoal: Arc<Shoal<S>>,
    tab_id: Uuid,
    query: S::QueryKinds,
    app_tx: AsyncSender<AppEvent<S>>,
) where
    S: QuerySupport + Send + Sync,
    S::QueryKinds: Send,
    S::ResponseKinds: Send,
    <S::ResponseKinds as Archive>::Archived: Send
        + rkyv::Deserialize<
            S::ResponseKinds,
            rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
        >,
    for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    // execute this query and wrap the result
    let result = match shoal.send_one(query).await {
        Ok(response) => QueryResult::Response(response),
        Err(error) => QueryResult::Error(error),
    };
    // send this query result to the app to be rendered
    app_tx
        .send(AppEvent::QueryResult { tab_id, result })
        .await
        .unwrap();
}
/// The query input component
///
/// Displays a text input box for entering queries.
pub struct TabQueryBar;

impl TabQueryBar {
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
    pub fn render<Q: QuerySupport + Send + Sync>(
        &self,
        frame: &mut Frame,
        area: Rect,
        tab: Option<&Tab<Q>>,
        focused: bool,
    ) {
        // only render our query if we have a tab
        if let Some(tab) = &tab {
            // determine border color based on focus state
            let border_color = if focused {
                Color::Cyan
            } else {
                Color::DarkGray
            };
            // build the input widget
            let input = Paragraph::new(tab.query.clone())
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
                let cursor_x = area.x + 1 + tab.query_cursor as u16;
                let cursor_y = area.y + 1;
                // only show cursor if it's within the visible area
                if cursor_x < area.x + area.width - 1 {
                    frame.set_cursor_position((cursor_x, cursor_y));
                }
            }
        }
    }
}
