//! The error box for shoalctl
//!
//! A query that does not parse is answered with a box under the query box holding the message,
//! and with the offending part of the query underlined where it was typed. The box is only
//! there while there is something to say — an error is cleared the moment the query it came
//! from is edited, so the box appears and disappears with it.
//!
//! Nothing in here ever touches the query text. The underline is a style carried on the cells
//! the query is already drawn into rather than a row of drawn carets, and the message lives in
//! its own box. A decoration made of characters could not be aligned against a wide character
//! or a wrapped query, and worse, it could be read back as part of the query it describes.

use ratatui::{
    Frame,
    layout::Rect,
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Paragraph},
};
use shoal::traits::QuerySupport;
use shoal::client::ShqlParseError;
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use super::Tab;

/// The most rows of text the error box will grow to before its message is cut short
const MAX_ERROR_ROWS: usize = 3;

/// The most columns of a query the error box will quote when it cannot underline it instead
const MAX_QUOTED_COLUMNS: usize = 24;

/// Flatten a message into something that can be drawn on a row of a box
///
/// Messages arrive from two places that do not agree on shape: a parse error is a single
/// sentence, while a server error arrives as pretty printed debug output, which is a wall of
/// newlines and indentation. Both have to end up as one line, because the box works out how
/// tall it is from this text and a newline that survived would push what follows it out of a
/// box already sized without it.
///
/// # Arguments
///
/// * `message` - The message to flatten
fn sanitize(message: &str) -> String {
    // a control character has no width of its own, so anything drawn after it lands short
    let flattened: String = message
        .chars()
        .map(|character| if character.is_control() { ' ' } else { character })
        .collect();
    // collapse the runs of whitespace pretty printing leaves behind
    flattened.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Cut a message down to a number of columns, marking that it was cut
///
/// # Arguments
///
/// * `message` - The message to cut down
/// * `columns` - The most columns the message may take up
fn truncate(message: &str, columns: usize) -> String {
    // a message that already fits is left exactly as it is
    if message.width() <= columns {
        return message.to_string();
    }
    // build it back up until there is only room left for the mark saying it was cut
    let mut kept = String::new();
    let mut used = 0;
    for character in message.chars() {
        let width = character.width().unwrap_or(0);
        // stop while there is still a column for the ellipsis
        if used + width > columns.saturating_sub(1) {
            break;
        }
        kept.push(character);
        used += width;
    }
    kept.push('…');
    kept
}

/// An error to show under the query that produced it
///
/// The span is kept apart from the message rather than formatted into it, because it is what
/// the query box underlines. `ShqlParseError`'s own `Display` folds the two together across two
/// lines, which is the wrong shape for both jobs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryError {
    /// The single line message describing what went wrong
    pub message: String,
    /// The byte range of the query the message points at, if it points at one
    pub span: Option<(usize, usize)>,
}

impl QueryError {
    /// Build an error from a query that did not parse
    ///
    /// # Arguments
    ///
    /// * `error` - The parse error to show
    pub fn parse(error: &ShqlParseError) -> Self {
        QueryError {
            message: sanitize(&error.message),
            span: Some((error.start, error.end)),
        }
    }

    /// Build an error with nothing to point at
    ///
    /// # Arguments
    ///
    /// * `message` - The message to show
    pub fn plain<M: AsRef<str>>(message: M) -> Self {
        QueryError {
            message: sanitize(message.as_ref()),
            span: None,
        }
    }

    /// Get the part of a query this error can be underlined under, if any of it can be
    ///
    /// An underline is only worth drawing when it points somewhere, and it is only safe to draw
    /// when the span it came from still describes the query on screen. Everything this rejects
    /// falls back to naming the position in the message instead, which is always safe because
    /// the message lives in its own box.
    ///
    /// # Arguments
    ///
    /// * `query` - The query the error came from
    pub fn highlight_span(&self, query: &str) -> Option<(usize, usize)> {
        // an error with no span has nothing to point at
        let (start, end) = self.span?;
        // a span can only ever reach as far as the query it was measured against
        let end = end.min(query.len());
        // a span with nothing in it cannot be underlined
        if start >= end {
            return None;
        }
        // a span that does not land on character boundaries was measured against a different
        // string, so drawing it would cut a character in half
        if !query.is_char_boundary(start) || !query.is_char_boundary(end) {
            return None;
        }
        // several errors span the whole query — a failed SELECT, a missing partition key — and
        // underlining every character of it says nothing about which one is wrong
        if start == 0 && end == query.len() {
            return None;
        }
        // a control character does not take up a cell of its own, so an underline drawn over
        // one lands on the wrong columns
        if query[start..end].chars().any(char::is_control) {
            return None;
        }
        Some((start, end))
    }

    /// Get the line of text this error shows for a query
    ///
    /// Where the query box can underline the problem the message says only what is wrong, since
    /// the underline is already saying where. Where it cannot, the message has to carry the
    /// position itself, and quotes the offending text so a query too long to fit on screen can
    /// still be searched by eye.
    ///
    /// # Arguments
    ///
    /// * `query` - The query the error came from
    pub fn line(&self, query: &str) -> String {
        // the underline is already pointing at the problem, so the message need not
        if self.highlight_span(query).is_some() {
            return self.message.clone();
        }
        // an error with no span has no position to add
        let Some((start, end)) = self.span else {
            return self.message.clone();
        };
        // an error covering the whole query is about the query, not about a part of it
        if start == 0 && end >= query.len() {
            return self.message.clone();
        }
        // quote the offending text when there is text there to quote
        match query.get(start..end.min(query.len())) {
            Some(text) if !text.is_empty() => format!(
                "{} - at position {}: '{}'",
                self.message,
                start,
                truncate(&sanitize(text), MAX_QUOTED_COLUMNS)
            ),
            _ => format!("{} - at position {}", self.message, start),
        }
    }
}

/// Wrap a message to a width, breaking between words where it can
///
/// This is used to work out how tall the box is and again to draw it, so both answers come from
/// the same walk. Handing the message to [`Paragraph`]'s own wrapping for one of those and
/// guessing at it for the other is a guess that goes wrong the moment a message is not the
/// length it was expected to be.
///
/// # Arguments
///
/// * `message` - The message being wrapped
/// * `width` - The number of columns each row has room for
fn wrap_message(message: &str, width: u16) -> Vec<String> {
    // a box with no room in it still has a row
    let width = usize::from(width.max(1));
    // the rows built so far, and how many columns the last of them has used
    let mut rows = vec![String::new()];
    let mut used = 0;
    // place one word at a time so a message breaks where it reads rather than mid word
    for word in message.split_whitespace() {
        // the space this word needs after one already on the row
        let spacer = usize::from(used > 0);
        // move down a row when this word will not fit beside what is already there
        if used > 0 && used + spacer + word.width() > width {
            rows.push(String::new());
            used = 0;
        } else if spacer > 0 {
            rows.last_mut().expect("a wrap always has a row").push(' ');
            used += spacer;
        }
        // place the word a character at a time, since a word longer than the box has to break
        for character in word.chars() {
            let columns = character.width().unwrap_or(0);
            // a character that no longer fits moves down whole rather than being split
            if used + columns > width {
                rows.push(String::new());
                used = 0;
            }
            rows.last_mut()
                .expect("a wrap always has a row")
                .push(character);
            used += columns;
        }
    }
    rows
}

/// Wrap an error's message to a width, capped at the tallest the box is allowed to be
///
/// # Arguments
///
/// * `error` - The error being shown
/// * `query` - The query the error came from
/// * `width` - The number of columns each row has room for
fn message_rows(error: &QueryError, query: &str, width: u16) -> Vec<String> {
    // wrap the whole message first, since it is what decides how many rows there would be
    let mut rows = wrap_message(&error.line(query), width);
    // cut it down to a box that cannot take over the screen, marking the row that was cut
    if rows.len() > MAX_ERROR_ROWS {
        rows.truncate(MAX_ERROR_ROWS);
        if let Some(last) = rows.last_mut() {
            // adding the mark first means it survives whether or not the row had room for it
            *last = truncate(&format!("{last}…"), usize::from(width.max(1)));
        }
    }
    rows
}

/// The error box component
///
/// Sits between the query box and the results, and takes up no room at all until there is an
/// error to show.
pub struct ErrorBar;

impl ErrorBar {
    /// Create a new error box component
    pub fn new() -> Self {
        Self
    }

    /// Get the number of columns an error box has for text between its borders
    ///
    /// # Arguments
    ///
    /// * `width` - The full width of the error box
    fn inner_width(width: u16) -> u16 {
        width.saturating_sub(2)
    }

    /// Get the height an error box needs to show its message
    ///
    /// This is zero whenever there is no error, so the layout on the ordinary path is exactly
    /// what it was before the box existed.
    ///
    /// # Arguments
    ///
    /// * `tab` - The tab whose error is being drawn, if there is one
    /// * `width` - The full width the error box will be drawn at
    pub fn height<S: QuerySupport + Send + Sync>(tab: Option<&Tab<S>>, width: u16) -> u16 {
        // a tab with no error on it has nothing to show
        let Some(tab) = tab else {
            return 0;
        };
        let Some(error) = tab.error.as_ref() else {
            return 0;
        };
        // work out how many rows this message wraps onto between two borders
        let rows = message_rows(error, &tab.query, Self::inner_width(width));
        rows.len() as u16 + 2
    }

    /// Render the error box to the frame
    ///
    /// # Arguments
    ///
    /// * `frame` - The frame to render to
    /// * `area` - The area to render the box in
    /// * `tab` - The tab whose error is being drawn, if there is one
    pub fn render<S: QuerySupport + Send + Sync>(
        &self,
        frame: &mut Frame,
        area: Rect,
        tab: Option<&Tab<S>>,
    ) {
        // only render an error if we have a tab with one on it
        let Some(tab) = tab else {
            return;
        };
        let Some(error) = tab.error.as_ref() else {
            return;
        };
        // a box with no room for its own borders cannot be drawn at all
        if area.height < 3 || area.width < 2 {
            return;
        }
        // wrap the message the same way the height was worked out
        let rows = message_rows(error, &tab.query, Self::inner_width(area.width));
        // build the box, which is the one piece of chrome that is always red
        let message = Paragraph::new(rows.join("\n"))
            .style(Style::default().fg(Color::Red))
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(Color::Red))
                    .title(" Error ")
                    .title_style(Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)),
            );
        // render the box
        frame.render_widget(message, area);
    }
}
