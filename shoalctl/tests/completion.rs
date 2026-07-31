//! Tests for the query box's completion menu
//!
//! These drive a tab the same way the key handler does and render the menu to an in memory
//! terminal, so none of it needs a server or a real terminal.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use deepsize2::DeepSizeOf;
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use ratatui::layout::Rect;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::storage::FileSystem;
use shoal::tables::{PersistentSortedTable, PersistentUnsortedTable};
use shoal::{ShoalSortedTable, ShoalUnsortedTable, db};
use shoalctl::components::{CompletionMenu, Tab, TabQueryBar, layout_query};

/// An unsorted table with a partition key and a couple of filters
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "TestDb")]
pub struct Movie {
    /// The partition key for this movie
    #[shoal(partition)]
    pub id: u64,
    /// The title of this movie, which can be filtered on
    #[shoal(filter)]
    pub title: String,
    /// Whether this movie has been watched, which can be filtered on
    #[shoal(filter)]
    pub watched: bool,
    /// A payload that cannot be used in a where clause
    #[shoal(update)]
    pub data: String,
}

/// A sorted table sharing a prefix with the unsorted one so fuzzy matching has work to do
#[derive(Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, DeepSizeOf)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "TestDb")]
pub struct MovieByKeyword {
    /// The keyword this movie is filed under
    #[shoal(partition)]
    pub keyword: String,
    /// The title of this movie
    #[shoal(sort)]
    pub title: String,
}

/// A table with more fields than the menu can show at once
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "TestDb")]
pub struct Wide {
    /// The partition key for this row
    #[shoal(partition)]
    pub id: u64,
    /// The first filterable field
    #[shoal(filter)]
    pub field_a: String,
    /// The second filterable field
    #[shoal(filter)]
    pub field_b: String,
    /// The third filterable field
    #[shoal(filter)]
    pub field_c: String,
    /// The fourth filterable field
    #[shoal(filter)]
    pub field_d: String,
    /// The fifth filterable field
    #[shoal(filter)]
    pub field_e: String,
    /// The sixth filterable field
    #[shoal(filter)]
    pub field_f: String,
    /// The seventh filterable field
    #[shoal(filter)]
    pub field_g: String,
    /// The eighth filterable field
    #[shoal(filter)]
    pub field_h: String,
    /// The ninth filterable field
    #[shoal(filter)]
    pub field_i: String,
    /// The tenth filterable field
    #[shoal(filter)]
    pub field_j: String,
    /// The eleventh filterable field
    #[shoal(filter)]
    pub field_k: String,
}

/// The database the query box is completing against
#[db]
pub struct TestDb {
    /// The unsorted movie table
    pub movie: PersistentUnsortedTable<Movie, FileSystem>,
    /// The sorted movie by keyword table
    pub movie_by_keyword: PersistentSortedTable<MovieByKeyword, FileSystem>,
    /// A table with more fields than fit in the menu at once
    pub wide: PersistentUnsortedTable<Wide, FileSystem>,
}

/// Build a tab whose query box already holds a query, as if it had been typed in
///
/// # Arguments
///
/// * `query` - The query to type into the tab
fn typed(query: &str) -> Tab<TestDbClient> {
    // start from an empty tab
    let mut tab = Tab::<TestDbClient>::new("test");
    // type the query out a character at a time so completions refresh the way they really do
    for c in query.chars() {
        tab.insert_char(c);
        tab.completion.undismiss();
        tab.refresh_completions(false);
    }
    tab
}

/// Build a key press with no modifiers held
///
/// # Arguments
///
/// * `code` - The key that was pressed
fn key(code: KeyCode) -> KeyEvent {
    KeyEvent::new(code, KeyModifiers::NONE)
}

/// Build a key press with control held down
///
/// # Arguments
///
/// * `code` - The key that was pressed
fn control(code: KeyCode) -> KeyEvent {
    KeyEvent::new(code, KeyModifiers::CONTROL)
}

/// Pull the cells drawn into a terminal back out as lines of text
///
/// # Arguments
///
/// * `terminal` - The terminal that was drawn into
/// * `size` - The width and height of that terminal
fn drawn(terminal: &Terminal<TestBackend>, size: (u16, u16)) -> Vec<String> {
    let (width, height) = size;
    let buffer = terminal.backend().buffer().clone();
    (0..height)
        .map(|y| {
            (0..width)
                .map(|x| buffer[(x, y)].symbol().to_string())
                .collect::<String>()
                .trim_end()
                .to_string()
        })
        .collect()
}

/// Render a tab's completion menu and hand back what was drawn, a line at a time
///
/// # Arguments
///
/// * `tab` - The tab whose menu should be drawn
/// * `size` - The width and height of the terminal to draw into
/// * `cursor` - Where the query box drew its cursor
fn render(tab: &Tab<TestDbClient>, size: (u16, u16), cursor: (u16, u16)) -> Vec<String> {
    // draw the menu into an in memory terminal
    let (width, height) = size;
    let mut terminal =
        Terminal::new(TestBackend::new(width, height)).expect("failed to build a terminal");
    terminal
        .draw(|frame| CompletionMenu::new().render(frame, cursor, tab))
        .expect("failed to draw the menu");
    drawn(&terminal, size)
}

/// Render a tab's query box and hand back what was drawn and where the cursor landed
///
/// # Arguments
///
/// * `tab` - The tab whose query box should be drawn
/// * `size` - The width and height of the terminal to draw into
/// * `area` - The area to draw the query box in
fn render_query(
    tab: &Tab<TestDbClient>,
    size: (u16, u16),
    area: Rect,
) -> (Vec<String>, Option<(u16, u16)>) {
    // draw the query box into an in memory terminal
    let (width, height) = size;
    let mut terminal =
        Terminal::new(TestBackend::new(width, height)).expect("failed to build a terminal");
    // hang on to where the box put its cursor, since that is what the menu anchors to
    let mut cursor = None;
    terminal
        .draw(|frame| cursor = TabQueryBar::new().render(frame, area, Some(tab), true))
        .expect("failed to draw the query box");
    (drawn(&terminal, size), cursor)
}

/// Get the text of every suggestion a tab is offering
///
/// # Arguments
///
/// * `tab` - The tab to read suggestions from
fn offered(tab: &Tab<TestDbClient>) -> Vec<String> {
    tab.completion
        .items()
        .iter()
        .map(|item| item.text.clone())
        .collect()
}

#[test]
/// Typing a table name opens the menu with the tables that match it
fn typing_opens_the_menu() {
    let tab = typed("SELECT * FROM Mov");
    assert!(tab.completion.is_open());
    assert_eq!(offered(&tab), vec!["Movie", "MovieByKeyword"]);
}

#[test]
/// The menu opens with nothing typed wherever the grammar leaves a known set of options
fn opens_when_the_options_are_known() {
    // an empty query box already knows the only word a query can start with
    let mut tab = Tab::<TestDbClient>::new("test");
    tab.refresh_completions(false);
    assert_eq!(offered(&tab), vec!["SELECT"]);
    // and after FROM it knows every table it could read
    let tab = typed("SELECT * FROM ");
    assert!(tab.completion.is_open());
    assert_eq!(offered(&tab), vec!["Movie", "MovieByKeyword", "Wide"]);
    // a string field cannot be guessed at, but its opening quote can
    let tab = typed("SELECT * FROM Movie WHERE title = ");
    assert_eq!(offered(&tab), vec!["'"]);
}

#[test]
/// The menu stays shut wherever anything at all could be typed
fn stays_shut_when_anything_could_be_typed() {
    // a u64 is not a set of options, so there is nothing to put on offer
    let tab = typed("SELECT * FROM Movie WHERE id = ");
    assert!(!tab.completion.is_open());
    // and neither is the count for a limit
    let tab = typed("SELECT * FROM Movie WHERE id = 5 LIMIT ");
    assert!(!tab.completion.is_open());
}

#[test]
/// The menu closes as soon as nothing matches what has been typed
fn closes_when_nothing_matches() {
    let tab = typed("SELECT * FROM zzz");
    assert!(!tab.completion.is_open());
}

#[test]
/// The best match is preselected so accepting never needs a keystroke first
fn preselects_the_best_match() {
    let tab = typed("SELECT * FROM Mov");
    assert_eq!(
        tab.completion.selected().map(|item| item.text.as_str()),
        Some("Movie")
    );
}

#[test]
/// Moving through the menu wraps around at both ends, the way helix's does
fn moving_through_the_menu_wraps() {
    let mut tab = typed("SELECT * FROM Mov");
    // step down to the second entry
    tab.completion.move_down();
    assert_eq!(
        tab.completion.selected().map(|item| item.text.as_str()),
        Some("MovieByKeyword")
    );
    // stepping down again wraps back to the top
    tab.completion.move_down();
    assert_eq!(
        tab.completion.selected().map(|item| item.text.as_str()),
        Some("Movie")
    );
    // and stepping up from the top wraps to the bottom
    tab.completion.move_up();
    assert_eq!(
        tab.completion.selected().map(|item| item.text.as_str()),
        Some("MovieByKeyword")
    );
}

#[test]
/// Accepting a suggestion replaces the word under the cursor and moves on
fn accepting_splices_in_the_selection() {
    let mut tab = typed("SELECT * FROM Mov");
    tab.completion.move_down();
    tab.accept_completion();
    assert_eq!(tab.query, "SELECT * FROM MovieByKeyword ");
    assert_eq!(tab.query_cursor, tab.query.len());
    // and the menu moves straight on to whatever comes next
    assert_eq!(offered(&tab), vec!["WHERE"]);
}

#[test]
/// A completed table name is enough to start completing that table's fields
fn completing_a_table_unlocks_its_fields() {
    // accept the table name and carry on typing
    let mut tab = typed("SELECT * FROM Mov");
    tab.accept_completion();
    for c in "WHERE ".chars() {
        tab.insert_char(c);
        tab.completion.undismiss();
        tab.refresh_completions(false);
    }
    // the fields of the table we just named are on offer without asking
    assert_eq!(offered(&tab), vec!["id", "title", "watched"]);
    // the payload field has no role, so it is never on offer
    assert!(!offered(&tab).contains(&"data".to_string()));
}

#[test]
/// Closing the menu keeps it shut until another character is typed
fn closing_the_menu_keeps_it_shut() {
    let mut tab = typed("SELECT * FROM Mov");
    tab.completion.close();
    assert!(!tab.completion.is_open());
    // a cursor move on its own does not bring it back
    tab.move_cursor_left();
    tab.refresh_completions(false);
    assert!(!tab.completion.is_open());
    // typing does
    tab.insert_char('i');
    tab.completion.undismiss();
    tab.refresh_completions(false);
    assert!(tab.completion.is_open());
}

#[test]
/// The rest of the selected suggestion is shown inline after the cursor
fn ghosts_the_rest_of_the_selection() {
    let tab = typed("SELECT * FROM Mov");
    assert_eq!(tab.completion.ghost(&tab.query), Some("ie"));
    // a fuzzy match that isn't a prefix has no sensible tail to show
    let tab = typed("SELECT * FROM mvk");
    assert_eq!(offered(&tab), vec!["MovieByKeyword"]);
    assert_eq!(tab.completion.ghost(&tab.query), None);
}

#[test]
/// Names that are not ascii are completed and spliced without splitting a character
fn handles_non_ascii_names() {
    let mut tab = Tab::<TestDbClient>::new("test");
    for c in "SELECT * FROM Movie WHERE title = 'café".chars() {
        tab.insert_char(c);
        tab.refresh_completions(false);
    }
    // the cursor sits inside a string literal, so there is nothing to complete
    assert!(!tab.completion.is_open());
    assert_eq!(tab.query_cursor, tab.query.len());
    // stepping back over a multibyte character lands on a boundary rather than panicking
    tab.move_cursor_left();
    assert_eq!(&tab.query[tab.query_cursor..], "é");
    tab.delete_char_before();
    assert_eq!(tab.query, "SELECT * FROM Movie WHERE title = 'caé");
}

#[test]
/// The menu drops onto the row directly below the cursor
fn renders_below_the_cursor() {
    // draw into a terminal with plenty of room under the cursor
    let tab = typed("SELECT * FROM Mov");
    let rendered = render(&tab, (60, 20), (18, 4));
    // the suggestions start in the column the cursor was in
    assert_eq!(
        rendered[5].find("Movie"),
        Some(18),
        "unexpected render: {:#?}",
        rendered
    );
    assert!(
        rendered[6].contains("MovieByKeyword"),
        "unexpected render: {:#?}",
        rendered
    );
}

#[test]
/// The menu flips above the cursor when there is no room below it
fn renders_above_the_cursor() {
    // draw into a terminal where the cursor is on the last row there is
    let tab = typed("SELECT * FROM Mov");
    let rendered = render(&tab, (60, 12), (18, 11));
    // the two suggestions sit on the rows just above it
    assert_eq!(
        rendered[9].find("Movie"),
        Some(18),
        "unexpected render: {:#?}",
        rendered
    );
    assert!(
        rendered[10].contains("MovieByKeyword"),
        "unexpected render: {:#?}",
        rendered
    );
}

#[test]
/// The menu is shifted left when it would otherwise run off the right hand edge
fn stays_inside_the_terminal() {
    // put the cursor close enough to the edge that a 23 column menu cannot start there
    let tab = typed("SELECT * FROM Mov");
    let rendered = render(&tab, (30, 20), (28, 4));
    assert_eq!(
        rendered[5].find("Movie"),
        Some(8),
        "unexpected render: {:#?}",
        rendered
    );
}

#[test]
/// A table with more fields than fit shows a scrollbar and scrolls to follow the selection
fn scrolls_through_a_long_list() {
    // every field of the wide table is on offer, which is more than the menu can show
    let mut tab = typed("SELECT * FROM Wide WHERE ");
    assert_eq!(tab.completion.items().len(), 12);
    // only ten of them are drawn, on the ten rows below the cursor
    let rendered = render(&tab, (60, 24), (26, 4));
    let drawn: Vec<&String> = rendered[5..15]
        .iter()
        .filter(|line| !line.is_empty())
        .collect();
    assert_eq!(drawn.len(), 10, "unexpected render: {:#?}", rendered);
    assert!(
        rendered[15].is_empty(),
        "expected only ten rows: {:#?}",
        rendered
    );
    // the list is longer than that, so it is drawn with a scrollbar alongside
    assert!(
        rendered[5].ends_with('▐'),
        "expected a scrollbar: {:#?}",
        rendered
    );
    // the first row is the partition key, since a query cannot be built without one
    assert!(
        rendered[5].contains("id"),
        "unexpected render: {:#?}",
        rendered
    );
    // moving past the bottom scrolls the list rather than running off the end
    for _ in 0..10 {
        tab.completion.move_down();
    }
    assert_eq!(
        tab.completion.selected().map(|item| item.text.as_str()),
        Some("field_j")
    );
    let rendered = render(&tab, (60, 24), (26, 4));
    // the top of the list has scrolled out of view and the selection is on the last row
    assert!(
        !rendered[5].contains("id"),
        "expected the list to have scrolled: {:#?}",
        rendered
    );
    assert!(
        rendered[14].contains("field_j"),
        "unexpected render: {:#?}",
        rendered
    );
    // and the scrollbar's thumb has slid down in proportion to how far we scrolled
    assert!(
        !rendered[5].ends_with('▐') && rendered[6].ends_with('▐'),
        "expected the scrollbar to have moved: {:#?}",
        rendered
    );
}

#[test]
/// The menu takes the keys helix binds it to and leaves everything else alone
fn takes_the_keys_helix_binds() {
    let mut tab = typed("SELECT * FROM Mov");
    // moving down the menu is bound three ways
    for down in [key(KeyCode::Tab), key(KeyCode::Down), control(KeyCode::Char('n'))] {
        assert!(tab.handle_completion_key(down), "{:?} should be taken", down);
    }
    // three keys down from the top of a two entry menu wraps back to the second
    assert_eq!(
        tab.completion.selected().map(|item| item.text.as_str()),
        Some("MovieByKeyword")
    );
    // and moving back up is bound three ways too
    for up in [
        key(KeyCode::BackTab),
        key(KeyCode::Up),
        control(KeyCode::Char('p')),
    ] {
        assert!(tab.handle_completion_key(up), "{:?} should be taken", up);
    }
    assert_eq!(
        tab.completion.selected().map(|item| item.text.as_str()),
        Some("Movie")
    );
    // typing is never the menu's business
    assert!(!tab.handle_completion_key(key(KeyCode::Char('i'))));
    assert!(!tab.handle_completion_key(key(KeyCode::Backspace)));
    assert!(!tab.handle_completion_key(key(KeyCode::Left)));
}

#[test]
/// Enter accepts while the menu is open and is left for the query the rest of the time
fn enter_is_only_taken_while_the_menu_is_open() {
    let mut tab = typed("SELECT * FROM Mov");
    // with the menu up, enter accepts the selection
    assert!(tab.handle_completion_key(key(KeyCode::Enter)));
    assert_eq!(tab.query, "SELECT * FROM Movie ");
    // a value that could be anything leaves nothing on offer, so enter falls through
    let mut tab = typed("SELECT * FROM Movie WHERE id = 5");
    assert!(!tab.completion.is_open());
    assert!(!tab.handle_completion_key(key(KeyCode::Enter)));
    // closing a menu that is up frees enter the same way
    let mut tab = typed("SELECT * FROM Movie WHERE id = 5 ");
    assert!(tab.completion.is_open());
    tab.completion.close();
    assert!(!tab.handle_completion_key(key(KeyCode::Enter)));
}

#[test]
/// A query wraps at the width of the box it is drawn in, and the cursor follows it
fn wraps_a_query_at_its_width() {
    // a query that fits stays on one row with the cursor just past its end
    let layout = layout_query("SELECT *", "", 8, 20);
    assert_eq!(layout.rows.len(), 1);
    assert_eq!((layout.cursor_row, layout.cursor_col), (0, 8));
    // a longer one breaks at exactly the width it was given
    let layout = layout_query("SELECT * FROM Movie", "", 19, 10);
    assert_eq!(layout.rows[0].0, "SELECT * F");
    assert_eq!(layout.rows[1].0, "ROM Movie");
    assert_eq!((layout.cursor_row, layout.cursor_col), (1, 9));
    // a cursor at the end of a full row moves down to the start of the next one
    let layout = layout_query("SELECT * F", "", 10, 10);
    assert_eq!(layout.rows.len(), 2);
    assert_eq!((layout.cursor_row, layout.cursor_col), (1, 0));
    // the hint trailing the query is kept apart from it so it can be dimmed
    let layout = layout_query("Mov", "ie", 3, 20);
    assert_eq!(layout.rows[0], ("Mov".to_string(), "ie".to_string()));
    assert_eq!((layout.cursor_row, layout.cursor_col), (0, 3));
}

#[test]
/// A character too wide to fit moves down whole rather than being split down the middle
fn keeps_wide_characters_whole() {
    // seven columns are used before the last character, which needs two more than are left
    let layout = layout_query("FROM 映画", "", 11, 8);
    assert_eq!(layout.rows[0].0, "FROM 映");
    assert_eq!(layout.rows[1].0, "画");
    assert_eq!((layout.cursor_row, layout.cursor_col), (1, 2));
}

#[test]
/// The query box is a single row until the query is too long to fit on one
fn the_query_box_grows_with_its_query() {
    // a short query leaves the box a row of text between its two borders
    let tab = typed("SELECT * FROM Mov");
    assert_eq!(TabQueryBar::height(Some(&tab), 60), 3);
    // a query too long for the box grows it a row at a time
    let tab = typed("SELECT * FROM Movie WHERE id = 5");
    assert_eq!(tab.query.len(), 32);
    assert_eq!(TabQueryBar::height(Some(&tab), 22), 4);
    assert_eq!(TabQueryBar::height(Some(&tab), 12), 6);
    // but only so far, so a pasted monster cannot take over the screen
    assert_eq!(TabQueryBar::height(Some(&tab), 8), 7);
}

#[test]
/// A wrapped query is drawn across its rows with the cursor on the row it belongs to
fn draws_a_wrapped_query() {
    let tab = typed("SELECT * FROM Movie WHERE id = 5");
    // draw the box at the height it asked for
    let height = TabQueryBar::height(Some(&tab), 22);
    let (rendered, cursor) = render_query(&tab, (22, 10), Rect::new(0, 0, 22, height));
    // the query is split across the two rows between the borders
    assert_eq!(
        rendered[1],
        "│SELECT * FROM Movie │",
        "unexpected render: {:#?}",
        rendered
    );
    assert_eq!(
        rendered[2],
        "│WHERE id = 5        │",
        "unexpected render: {:#?}",
        rendered
    );
    // and the cursor sits on the second of them, just past the text
    assert_eq!(cursor, Some((13, 2)));
}

#[test]
/// Control c closes the menu and control space opens it back up
fn control_c_closes_and_control_space_opens() {
    let mut tab = typed("SELECT * FROM Mov");
    // control c puts the menu away
    assert!(tab.handle_completion_key(control(KeyCode::Char('c'))));
    assert!(!tab.completion.is_open());
    // enter is free again now that the menu is closed
    assert!(!tab.handle_completion_key(key(KeyCode::Enter)));
    // and control space brings it back
    assert!(tab.handle_completion_key(control(KeyCode::Char(' '))));
    assert!(tab.completion.is_open());
    assert_eq!(offered(&tab), vec!["Movie", "MovieByKeyword"]);
}
