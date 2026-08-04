# 24. A bad query could leave the terminal in raw mode

The fixed half of item 24. The remaining shoalctl warnings — dead `TabState::next`/`prev`, and
`submit_query` spawning a task only to immediately await it — stay in
[Known Issues](../known-issues.md#24-shoalctl-warnings).

## Symptom

Typing a query that did not parse killed shoalctl and left the terminal unusable: no echo, no
line discipline, requiring a blind `reset`.

## Cause

The parse-error branch of `Tab::submit_query` carried an unconditional `panic!` where an error
should have been rendered, followed by an unreachable `return`.

The panic itself would have been survivable. What made it hostile is that shoalctl puts the
terminal into raw mode on startup and restores it with `ratatui::restore()` on the way out — and
a panic unwinds straight past that call. The user is left in a terminal that no longer echoes,
for a typo.

## The fix

The error is recorded on the tab ~~and rendered~~:

```rust
let query = match S::parse(&self.query) {
    Ok(q) => q,
    Err(e) => {
        // show the parse error in the UI and leave the query for the user to fix
        self.error = Some(format!("Parse error: {}", e));
        return;
    }
};
```

`shoalctl/src/components/tab.rs:237-244`

**"And rendered" was not true.** Nothing read `Tab::error` — the field was written here, written
again for server errors, and never drawn anywhere. Pressing enter on a bad query stopped
panicking and started doing nothing at all, which is what this page should have said. That was
filed as item 48 and is now [fixed](query-error-display.md): the error carries its span rather
than a formatted string, an `Error` box under the query shows the message, and the part of the
query the parser blamed is drawn red and underlined.

Leaving the query text in the input rather than clearing it matters as much as not panicking: a
parse error is usually a typo, and retyping the whole query to fix one character is its own
kind of hostile.

## Invariants to uphold

- **Nothing on shoalctl's input path may panic.** Raw mode is restored by a normal return, and
  a panic skips it. Any new code between "read a key" and "render a frame" has to handle its
  own errors, including the ones it thinks are impossible.
- **User input errors are UI state, not control flow.** Parse failures belong in `self.error`,
  where the renderer can show them; they are not exceptional. Which requires a renderer that
  reads the field — see [item 48](query-error-display.md), where writing it was mistaken for
  showing it for as long as this page went unchallenged.

## Related

- [48. A query that does not parse was answered with silence](query-error-display.md)
- [shoalctl](../../operations/shoalctl.md)
- [SHQL](../../api/shql.md)
