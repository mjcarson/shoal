# 27. SHQL could not express a string containing a single quote

## Symptom

A SHQL string literal could not contain a `'`. There was no escape of any kind: not doubling
(`''`) and not a backslash. `WHERE title = 'it''s'` was refused as trailing input after `'it'`,
and no other spelling worked.

This was more than a missing convenience. The partition key is the one condition every query
has to name, so a row whose string partition key held an apostrophe could not be reached from
SHQL at all. That covers any title, name or free-text key with one in it. `shoalctl` could
not read such a row, even though the typed client could write it.

## Cause

`string_literal` was
`delimited("'", take_till(0.., |c| c == '\''), "'")`. Nothing between the quotes could be a
quote, so the first quote inside a literal closed it. The completion tokenizer in
`parser/complete.rs` made the same assumption: it consumed a literal up to the first quote.

## Evidence

**Reproduced against the unfixed tree.** Each test below was written first and failed:

```text
test shared::queries::parser::tests::parses_doubled_quotes_in_string_literals ... FAILED
test shared::queries::parser::tests::tracks_positions_across_doubled_quotes ... FAILED
test shared::queries::parser::tests::rejects_a_string_ending_in_a_doubled_quote ... FAILED
test shared::queries::parser::complete::tests::a_doubled_quote_does_not_split_a_string_literal ... FAILED

Failed to parse 'SELECT * FROM Movie WHERE title = 'it''s'': SHQL parse error at positions 38-41:
  Unexpected trailing input: ''s''
```

The end-to-end binding test failed the same way, on a sorted table's string partition key:

```text
test binds_a_partition_key_holding_a_quote ... FAILED
Failed to parse 'SELECT * FROM Review WHERE movie = 'ocean''s eleven'': SHQL parse error at
  positions 42-52: Unexpected trailing input: ''s eleven''
```

`rejects_a_string_ending_in_a_doubled_quote` also shows that the old parser read `'it''` as the
literal `it` followed by a stray `'`, rather than as a string that was never closed.

## The fix

**A quote inside a literal is written twice, as SQL writes it.** `'it''s'` is the value `it's`,
`''''` is `'`, and `''` is still the empty string.

`string_literal` is now built from `string_run`, which reads the characters up to a quote and
then that quote:

1. It takes the opening quote and the first run. If the next character is not a quote, the
   literal is closed and the run becomes the value. This is the common case, and it costs the
   single `to_string` it always did.
2. Only when a quote follows the closing quote does it start building a `String`. It adds one
   `'` for each pair and then the run after it, until a closing quote is not followed by another.

The span the caller records is unchanged. `where_value` measures it from the input position
before and after the literal, so it still covers the literal as written, both of its quotes
included. For a literal with a doubled quote, that span is now longer than the decoded value,
which is what error rendering needs: it slices the query, not the value.

In the completion tokenizer, a quote followed by another quote no longer ends a literal.
`in_string_literal` needed no change: its parity count already holds, because a doubled quote
adds two.

## Alternatives rejected

- **Backslash escapes (`'it\'s'`).** They would work, but they diverge from SQL, which is the
  language every SHQL user already knows. They would also turn every backslash in a literal
  into something that has to be escaped itself. A backslash is an ordinary character in SHQL.
- **Always building the value into a `String` char by char.** This is simpler to write, but it
  would cost every literal a push per character to serve the rare literal that holds a quote.
  The fast path keeps unescaped literals exactly as cheap as before.
- **Recording the decoded value's length as the span.** The span exists to slice the query in
  an error message, so it has to describe the query.

## Invariants to uphold

- **A literal's span is its raw text.** Anything that derives a position from the decoded value
  (`Value::String(s).len()`) is wrong for any literal with a doubled quote.
- **The parser and the completion tokenizer agree on where a literal ends.** A change to one
  escape rule without the other makes completion suggest fields in the middle of a string.
- **`''` stays the empty string.** A doubled quote only means "one quote" *inside* a literal.
  The two quotes at the start of `''` are the opening and closing quotes.

## Still open

Nothing of this item. SHQL still cannot express a composite partition or sort key
([Known Issues #41, #42](../known-issues.md#41-shql-cannot-express-a-composite-partition-key)).

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `parses_doubled_quotes_in_string_literals` (`shoal-proto/src/shared/queries/parser/tests.rs`) | `'it''s'`, `''''` and an escaped literal inside an `IN` list do not parse. |
| `tracks_positions_across_doubled_quotes` | The query does not parse. A fix that records the decoded length slices the wrong text. |
| `rejects_a_string_ending_in_a_doubled_quote` | `'it''` is read as `it` plus trailing input, not as an unterminated literal. |
| `a_doubled_quote_does_not_split_a_string_literal` (`parser/complete/tests.rs`) | The tokenizer reads `'it''s'` as two literals and suggests nothing after it. |
| `binds_a_partition_key_holding_a_quote` (`shoal/tests/shql.rs`) | A partition key holding an apostrophe cannot be named, so its row cannot be reached. |

## Related

- [SHQL](../../api/shql.md), which documents the literal grammar.
- [Resolved #48](query-error-display.md), the error rendering the spans feed.
