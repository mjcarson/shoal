//! The query input component for shoalctl
//!
//! This module provides a text input box below the tab bar for entering database queries. The
//! box is only as tall as the query needs it to be — a single row until the query is long
//! enough to wrap.

use kanal::AsyncSender;
use ratatui::{
    Frame,
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
};
use rkyv::Archive;
use shoal::client::Shoal;
use shoal::traits::QuerySupport;
use std::sync::Arc;
use unicode_width::UnicodeWidthChar;
use uuid::Uuid;

use super::Tab;
use crate::{AppEvent, app::QueryResult};

/// The most rows of text the query box will grow to before it starts scrolling
const MAX_QUERY_ROWS: usize = 5;

/// Execute queries for shoalctl
pub async fn run<S: QuerySupport>(
    shoal: Arc<Shoal<S>>,
    tab_id: Uuid,
    table_name: S::TableNames,
    query: S::QueryKinds,
    app_tx: AsyncSender<AppEvent<S>>,
) where
    S: QuerySupport + Send + Sync + 'static,
    S::TableNames: Send,
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
        .send(AppEvent::QueryResult { tab_id, table_name, result })
        .await
        .unwrap();
}
/// One wrapped row of a query
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryRow {
    /// The query text drawn on this row
    pub text: String,
    /// The rest of the selected completion, where it trails this row
    pub hint: String,
    /// The byte offset in the query that this row's text starts at
    ///
    /// Wrapping is the only place a query stops being a single string, so this is what lets a
    /// span measured against the whole query — a parse error's, say — be found again on the row
    /// it was drawn on.
    pub start: usize,
}

/// A query broken into the rows it will be drawn on
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryLayout {
    /// The wrapped rows, each holding the text typed on it and the hint trailing it
    pub rows: Vec<QueryRow>,
    /// The row the cursor sits on, counted from the first row of the query
    pub cursor_row: usize,
    /// The column the cursor sits at within that row
    pub cursor_col: u16,
}

/// A query being wrapped a character at a time
///
/// Queries are laid out by hand rather than handed to [`Paragraph`]'s own wrapping because the
/// cursor has to be tracked through the same walk. Anything else means guessing where the widget
/// decided to break, which is a guess that goes wrong the moment a name is not ascii.
struct Wrapper {
    /// The rows built so far
    rows: Vec<QueryRow>,
    /// The columns used up on the row currently being built
    used: usize,
    /// The width every row is wrapped at
    width: usize,
}

impl Wrapper {
    /// Create a wrapper for rows of a given width
    ///
    /// # Arguments
    ///
    /// * `width` - The number of columns each row has room for
    fn new(width: usize) -> Self {
        Wrapper {
            rows: vec![QueryRow {
                text: String::new(),
                hint: String::new(),
                start: 0,
            }],
            used: 0,
            width,
        }
    }

    /// Start a new row
    ///
    /// # Arguments
    ///
    /// * `start` - The byte offset in the query the new row starts at
    fn wrap(&mut self, start: usize) {
        self.rows.push(QueryRow {
            text: String::new(),
            hint: String::new(),
            start,
        });
        self.used = 0;
    }

    /// Add a character to the row being built, wrapping when it no longer fits
    ///
    /// A wide character that would straddle the edge is moved down whole rather than split.
    ///
    /// # Arguments
    ///
    /// * `index` - The byte offset of this character in the query
    /// * `character` - The character to add
    /// * `hint` - Whether this character is part of the trailing hint rather than the query
    fn push(&mut self, index: usize, character: char, hint: bool) {
        // work out how many columns this character takes up
        let width = character.width().unwrap_or(0);
        // move down a row when this character no longer fits on the current one
        if self.used + width > self.width {
            self.wrap(index);
        }
        // add it to whichever half of this row it belongs to
        let row = self.rows.last_mut().expect("a wrapper always has a row");
        if hint {
            row.hint.push(character);
        } else {
            row.text.push(character);
        }
        self.used += width;
    }

    /// Get the row and column the next character would be placed at
    ///
    /// # Arguments
    ///
    /// * `index` - The byte offset in the query of the character that would be placed there
    fn position(&mut self, index: usize) -> (usize, u16) {
        // a full row has no room left for a cursor, so it moves down to the next one
        if self.used >= self.width {
            self.wrap(index);
        }
        (self.rows.len() - 1, self.used as u16)
    }
}

/// Wrap a query to a width, tracking where the cursor ends up
///
/// The query is a single logical line — enter submits rather than inserting a newline — so this
/// is purely soft wrapping by display width.
///
/// # Arguments
///
/// * `query` - The query being typed
/// * `hint` - The rest of the selected completion, which trails the query
/// * `cursor` - The byte offset of the cursor in the query
/// * `width` - The number of columns each row has room for
///
/// # Examples
///
/// ```
/// use shoalctl::components::layout_query;
///
/// let layout = layout_query("SELECT * FROM Movie", "", 19, 10);
///
/// assert_eq!(layout.rows.len(), 2);
/// assert_eq!(layout.rows[1].start, 10);
/// assert_eq!(layout.cursor_row, 1);
/// assert_eq!(layout.cursor_col, 9);
/// ```
pub fn layout_query(query: &str, hint: &str, cursor: usize, width: u16) -> QueryLayout {
    // a box with no room in it still needs a column for the cursor to sit in
    let mut wrapper = Wrapper::new(usize::from(width.max(1)));
    // clamp the cursor so a stale cursor can never fall outside the query
    let cursor = cursor.min(query.len());
    // where the cursor ended up, which stays unset until we walk past it
    let mut position = None;
    // lay the query out a character at a time
    for (index, character) in query.char_indices() {
        // this is where the cursor sits if we have just reached it
        if index == cursor {
            position = Some(wrapper.position(index));
        }
        wrapper.push(index, character, false);
    }
    // a cursor at the end of the query sits just past the last character
    let (cursor_row, cursor_col) = match position {
        Some(position) => position,
        None => wrapper.position(query.len()),
    };
    // lay the hint out behind it, kept apart so it can be dimmed
    //
    // the hint is not part of the query, so a row it wraps onto holds none of it and starts
    // where the query ended
    for character in hint.chars() {
        wrapper.push(query.len(), character, true);
    }
    QueryLayout {
        rows: wrapper.rows,
        cursor_row,
        cursor_col,
    }
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

    /// Get the number of columns a query box has for text between its borders
    ///
    /// # Arguments
    ///
    /// * `width` - The full width of the query box
    fn inner_width(width: u16) -> u16 {
        width.saturating_sub(2)
    }

    /// Get the part of the selected completion to show trailing the query
    ///
    /// The hint is drawn after the whole query rather than at the cursor, so it is only shown
    /// when those are the same place.
    ///
    /// # Arguments
    ///
    /// * `tab` - The tab whose query is being drawn
    fn hint<S: QuerySupport + Send + Sync>(tab: &Tab<S>) -> &str {
        // a hint behind the cursor would just be text in the wrong place
        if tab.query_cursor != tab.query.len() {
            return "";
        }
        tab.completion.ghost(&tab.query).unwrap_or("")
    }

    /// Get the height a query box needs to show its query
    ///
    /// This is a single row of text between the borders until the query is long enough to wrap,
    /// after which it grows a row at a time up to a cap.
    ///
    /// # Arguments
    ///
    /// * `tab` - The tab whose query is being drawn, if there is one
    /// * `width` - The full width the query box will be drawn at
    pub fn height<S: QuerySupport + Send + Sync>(tab: Option<&Tab<S>>, width: u16) -> u16 {
        // an empty box is still a row of text between two borders
        let Some(tab) = tab else {
            return 3;
        };
        // work out how many rows this query wraps onto
        let layout = layout_query(
            &tab.query,
            Self::hint(tab),
            tab.query_cursor,
            Self::inner_width(width),
        );
        // cap the box so a pasted monster of a query cannot take over the screen
        layout.rows.len().clamp(1, MAX_QUERY_ROWS) as u16 + 2
    }

    /// Get the part of a row an error span covers, in offsets into that row's own text
    ///
    /// A span is measured against the whole query while a row holds a slice of it, so the two
    /// have to be intersected before either can be drawn. A cut that does not land on a
    /// character boundary is refused rather than clamped — the span came from a string that is
    /// no longer this one, and half a character is worse than no underline.
    ///
    /// # Arguments
    ///
    /// * `row` - The row being drawn
    /// * `highlight` - The byte range of the query the error covers
    fn row_span(row: &QueryRow, highlight: Option<(usize, usize)>) -> Option<(usize, usize)> {
        // there is nothing to draw unless an error is pointing somewhere
        let (start, end) = highlight?;
        // the range of the query this row holds
        let row_end = row.start + row.text.len();
        // clip the error to this row, which leaves nothing when it lands on another one
        let from = start.max(row.start).min(row_end);
        let to = end.min(row_end).max(from);
        if from >= to {
            return None;
        }
        // move it into this row's own text
        let (from, to) = (from - row.start, to - row.start);
        // never cut a row anywhere but on a character boundary
        if !row.text.is_char_boundary(from) || !row.text.is_char_boundary(to) {
            return None;
        }
        Some((from, to))
    }

    /// Build the line a row is drawn as, underlining the part of it an error covers
    ///
    /// The underline is a style on the cells the query is already drawn into rather than
    /// anything added to the text, so a query drawn under an error is the same characters in
    /// the same columns as one drawn without it.
    ///
    /// # Arguments
    ///
    /// * `row` - The row being drawn
    /// * `highlight` - The byte range of the query the error covers
    fn row_line(row: &QueryRow, highlight: Option<(usize, usize)>) -> Line<'static> {
        // the hint always trails the row dimmed, whatever the query in front of it is doing
        let hint = Span::styled(row.hint.clone(), Style::default().fg(Color::DarkGray));
        // the style a query is drawn in when nothing is wrong with it
        let plain = Style::default().fg(Color::White);
        // draw the row in one piece when no error lands on it
        let Some((from, to)) = Self::row_span(row, highlight) else {
            return Line::from(vec![Span::styled(row.text.clone(), plain), hint]);
        };
        // the style the offending part of a query is drawn in
        let marked = Style::default()
            .fg(Color::Red)
            .add_modifier(Modifier::UNDERLINED);
        // split the row around the part the error covers
        Line::from(vec![
            Span::styled(row.text[..from].to_string(), plain),
            Span::styled(row.text[from..to].to_string(), marked),
            Span::styled(row.text[to..].to_string(), plain),
            hint,
        ])
    }

    /// Render the query input to the frame
    ///
    /// Displays the current query text with a cursor, wrapped to the width of the box. Where an
    /// error points at part of the query, that part is drawn red and underlined.
    ///
    /// # Arguments
    ///
    /// * `frame` - The frame to render to
    /// * `area` - The area to render the input in
    /// * `tab` - The tab whose query is being drawn, if there is one
    /// * `focused` - Whether the input is currently focused
    ///
    /// # Returns
    ///
    /// Where the cursor landed on screen, which is what the completion menu anchors to
    pub fn render<S: QuerySupport + Send + Sync>(
        &self,
        frame: &mut Frame,
        area: Rect,
        tab: Option<&Tab<S>>,
        focused: bool,
    ) -> Option<(u16, u16)> {
        // only render our query if we have a tab
        let tab = tab?;
        // determine border color based on focus state
        let border_color = if focused {
            Color::Cyan
        } else {
            Color::DarkGray
        };
        // wrap the query to the room we have between the borders
        let layout = layout_query(
            &tab.query,
            Self::hint(tab),
            tab.query_cursor,
            Self::inner_width(area.width),
        );
        // scroll the box so the row the cursor is on is always in view
        let visible = usize::from(area.height.saturating_sub(2)).max(1);
        let scroll = (layout.cursor_row + 1).saturating_sub(visible);
        // work out which part of the query an error is pointing at, if one is
        let highlight = tab
            .error
            .as_ref()
            .and_then(|error| error.highlight_span(&tab.query));
        // build a line for each visible row, dimming the part that has not been typed yet
        let lines: Vec<Line> = layout
            .rows
            .iter()
            .skip(scroll)
            .take(visible)
            .map(|row| Self::row_line(row, highlight))
            .collect();
        // build the input widget
        let input = Paragraph::new(lines)
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
        // work out where the cursor ended up, accounting for the border and any scrolling
        let cursor = (
            area.x + 1 + layout.cursor_col,
            area.y + 1 + (layout.cursor_row - scroll) as u16,
        );
        // show cursor when focused
        if focused {
            frame.set_cursor_position(cursor);
        }
        Some(cursor)
    }
}
