# 48. A query that does not parse was answered with silence

## Symptom

Pressing enter on a query that did not parse changed nothing on screen. No message, no mark on
the query, no change to the rows already in the results pane — which were left showing the
answer to whichever query last succeeded. A typo and an empty result set looked identical, and
the only way to find out which one you had was to change something and try again.

## Cause

`Tab::submit_query` recorded the error and returned:

```rust
Err(e) => {
    // show the parse error in the UI and leave the query for the user to fix
    self.error = Some(format!("Parse error: {}", e));
    return;
}
```

Nothing read that field. `grep -rn "\.error" shoalctl/src` gave three writes —
`shoalctl/src/components/tab.rs:241`, `shoalctl/src/app.rs:478`, `:481` — and no reads.
`TabContent::render` drew `tab.content` and nothing else, `StatusBar::render` drew the mode, and
`App::render` laid out four chunks with nowhere for an error to go. The comment above the field
said "The error to display above the query", which described a renderer that was never written.

This is the unfinished half of [item 24](shoalctl-panic.md). That page says the error is
"recorded on the tab and rendered"; only the recording was ever real, and the claim has been
struck through there.

Two things about the stored value made it unusable even once something wanted to draw it. The
span was thrown away — `ShqlParseError` carries `start` and `end`, and `format!("{}", e)` folds
them into prose and adds a newline while doing it. And `App::handle_result` stored server errors
as `format!("Error: {:#?}", error)`, which is pretty-printed debug output: a wall of newlines and
indentation, in a field a one-line renderer would have to draw.

## Evidence

**Reproduced.** `a_bad_query_is_not_answered_with_silence` was written against the unfixed tree.
It types a query naming a field `Movie` does not have, parses it the way `submit_query` does,
stores the error the way `submit_query` did, and draws both the query box and the results pane:

```rust
tab.error = Some(format!("Parse error: {}", error));
let mut rendered = render_query(&tab, (60, 6), Rect::new(0, 0, 60, 3)).0;
rendered.extend(render_content(&tab, (60, 6), Rect::new(0, 0, 60, 6)));
assert!(rendered.iter().any(|line| line.contains("Unknown field")));
```

It failed on everything that had been drawn:

```
---- a_bad_query_is_not_answered_with_silence stdout ----
the parse error was never drawn: [
    "┌ Query ───────────────────────────────────────────────────┐",
    "│SELECT * FROM Movie WHERE bogus = 1                       │",
    "└──────────────────────────────────────────────────────────┘",
    "",
    "",
    "",
    "┌Results───────────────────────────────────────────────────┐",
    "│Enter a query and press Enter to see results              │",
```

The error the parser produced for that query is `Unknown field 'bogus'. Valid fields are: ["id",
"title", "watched", "data"]`, spanning bytes 26 to 31 — exactly the five characters of `bogus`.
All of that was known and none of it reached the screen.

## The fix

Three parts: a structured error, a box to draw it in, and an underline under the part of the
query it blames.

**The error keeps its span.** `Option<String>` on `Tab` became `Option<QueryError>`
(`shoalctl/src/components/tab/error.rs`):

```rust
pub struct QueryError {
    /// The single line message describing what went wrong
    pub message: String,
    /// The byte range of the query the message points at, if it points at one
    pub span: Option<(usize, usize)>,
}
```

`QueryError::parse` reads `message`, `start` and `end` off `ShqlParseError` rather than going
through its `Display`, and `QueryError::plain` covers the server-error path, which has nothing in
the query to point at. Both run the message through `sanitize`, which replaces control characters
with spaces and collapses the runs of whitespace pretty-printing leaves behind. A newline that
survived would push text out of a box whose height was worked out without it.

**The box.** `ErrorBar` draws a red bordered box titled `Error` between the query box and the
results, and `ErrorBar::height` returns **zero** whenever there is no error, so the layout on the
ordinary path is exactly what it was before the box existed. `App::render` gained a fifth
constraint for it:

```rust
Constraint::Length(3),            // tab bar
Constraint::Length(query_height), // query bar
Constraint::Length(error_height), // error box, zero rows when there is no error
Constraint::Min(0),               // hypertile-managed area
Constraint::Length(2),            // status bar
```

**The underline.** `QueryLayout::rows` was `Vec<(String, String)>` and lost the byte offsets when
it wrapped, so a span measured against the whole query could not be found again on the row it was
drawn on. Rows became `QueryRow { text, hint, start }`, `Wrapper` records the offset each row
starts at, and `TabQueryBar::row_line` splits a row into up to four spans — text, the part the
error covers in red and underlined, the rest, and the dimmed completion hint.

**The safety gate.** `QueryError::highlight_span` refuses to underline, falling back to a message
that names the position instead, when:

| Condition | Why |
| --- | --- |
| there is no span | nothing to point at |
| the span is empty after clamping to the query | nothing to underline; `SELECT * FROM Movie` reports `19..19` |
| either offset is not a character boundary | the span was measured against a different string |
| the span is the whole query | `Unknown table`, `Missing partition key` and a failed `SELECT` all report `0..len`, and underlining every character says nothing about which one is wrong |
| the spanned text holds a control character | a control character takes up no column of its own, so the underline would land somewhere other than under the text |

**Stale rows.** Rows are left where they are when a query fails — the query being fixed is
usually the one that produced them, and comparing the two is the point — but the results pane is
titled `Results (stale)` while an error is set, so they stop claiming to answer the query in the
box.

**Clearing.** `Tab::clear_error` is called by `insert_char`, `delete_char_before`,
`delete_char_at`, `accept_completion` and `TabState::clear_query`, and by none of the cursor
moves. Moving the cursor does not change the query, so the error is still true; changing one
character of it means the error describes a query that no longer exists and the span it carries
no longer points at what it named.

## Alternatives rejected

**A row of drawn carets under the query.** The obvious way to underline in a terminal is to draw
`^^^^^` on the row below, and it is wrong here for three separate reasons. It cannot be aligned
under a wide character, because a caret is one column and the character above it is two. It
cannot follow a query that wrapped, because the row below the first row of a wrapped query is the
second row of the query. And it puts characters that look like query text on the query box, which
is the failure mode worth being most afraid of: shoalctl's whole job is to hold a string the user
is going to press enter on, and a decoration made of characters is one careless edit away from
being read back as part of it. The underline is a *style* carried on the cells the query is
already drawn into, so the query under an error is byte-for-byte the same characters in the same
columns as the query without one. `an_error_never_reaches_the_query_text` asserts exactly that,
by rendering the same query with and without an error and comparing what was drawn.

**Growing the query box by a row and putting the message inside it.** Cheaper — no new layout
constraint — but it puts prose inside the border that holds the query, which is the same
confusion in a quieter form. A separate box that is either there or not is unambiguous about
what is query and what is commentary.

**Parsing on every keystroke and showing errors live.** `refresh_completions` already runs on
every keystroke, so the parse would have been nearly free. It was rejected because a query
halfway through being typed is almost always invalid: the box would have flickered an error on
for most of the time anyone spent typing, and a warning that is on by default is not a warning.
Errors appear on enter, which is when the user asserted the query was finished.

**Clearing the results on an error.** Considered and rejected in favour of the stale title. The
rows from the last query are usually what you are checking the new one against, and throwing them
away to make room for a message is a poor trade.

**Formatting the position into the message always.** `ShqlParseError`'s `Display` already does
this, across two lines. Where the underline can be drawn, the message repeating "at positions
26-31" is noise pointing at what is already marked. `QueryError::line` adds the position only
when the underline was refused — the two are alternatives, never both.

## Invariants to uphold

- **Error text is render state and never query state.** Nothing that draws an error may write to
  `Tab::query`, and no decoration may be made of characters placed among the query's own. The
  only mutators of `query` are `insert_char`, `delete_char_before`, `delete_char_at`,
  `accept_completion` and `clear_query`, and every one of them clears the error rather than
  reading it.
- **Any edit to a query invalidates the error on it.** A span is a pair of byte offsets into one
  particular string. The moment that string changes the offsets point somewhere else, and an
  underline drawn from them is worse than none — it accuses the wrong text. Adding a new way to
  edit the query means adding a `clear_error` call to it.
- **`height` and `render` must wrap identically.** Both `ErrorBar` and `TabQueryBar` work out
  their own height and then draw into an area laid out from that answer. Both call one wrapping
  function for both jobs, for this reason. Handing one of them to `Paragraph`'s own wrapping and
  keeping the hand-rolled walk for the other reintroduces the guess the walk exists to avoid.
- **Refusing to draw an underline is always available.** Every reason `highlight_span` returns
  `None` is a case where the mark would be misleading rather than merely absent. New reasons may
  be added freely; the fallback path is a message that cannot be wrong.
- **A message reaching the box is one line.** `sanitize` is what makes that true, and it runs in
  the constructors rather than at the draw, so there is no way to build a `QueryError` that
  bypasses it.

## Still open

- The spans on the most common errors are too coarse to underline. `Unknown table`,
  `Missing partition key in WHERE clause` and a failed `SELECT ... FROM` all report `0..len`
  (`shoal-core/src/shared/queries/parser.rs`, `shoal-derive/src/structs/client.rs`), so the
  underline is refused and only the message is shown. Filed as
  [item 49](../known-issues.md#49-the-coarsest-parse-errors-report-a-span-covering-the-whole-query).
- The query box does not scroll to bring an underlined span into view. A query longer than five
  wrapped rows can carry its error on a row that is not on screen, and only the message is seen.
- The remainder of [item 24](shoalctl-panic.md) is still open: `TabState::next`/`prev` are dead
  code and `submit_query` spawns a task only to await it immediately.

## Tests

`shoalctl/tests/completion.rs`.

| Test | What breaks without the fix |
| --- | --- |
| `a_bad_query_shows_an_error_box` | The box is never drawn and the message is never seen — this is the reproduction |
| `the_error_box_takes_no_room_when_there_is_no_error` | The box would take rows off the results on every ordinary query |
| `the_offending_part_of_a_query_is_underlined` | The mark lands on the wrong columns, or is not drawn |
| `an_underline_follows_a_wrapped_query_onto_its_next_row` | `QueryRow::start` is wrong, so a span on a wrapped query is drawn on one row or none |
| `a_whole_query_span_is_not_underlined` | The whole query goes red for an error that is about all of it |
| `a_span_over_control_characters_is_not_underlined` | An underline is drawn in columns the text it names does not occupy |
| `an_error_never_reaches_the_query_text` | A decoration has become part of the query |
| `editing_the_query_clears_the_error` | A stale span underlines text it was never measured against |
| `a_long_error_message_is_capped` | A long message grows the box past its cap, or the box closes somewhere other than where `height` said |
| `a_multi_line_server_error_stays_one_box` | A pretty-printed server error draws past its own border |
| `stale_rows_say_so` | Rows from an older query go on claiming to answer the one in the box |
| `wraps_a_query_at_its_width`, `keeps_wide_characters_whole` | Row offsets drift from the text they belong to |

## Related

- [shoalctl](../../operations/shoalctl.md)
- [SHQL](../../api/shql.md)
- [24. A bad query could leave the terminal in raw mode](shoalctl-panic.md)
