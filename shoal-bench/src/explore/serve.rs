//! Serves the explorer over loopback
//!
//! # Why this is hand rolled
//!
//! Four static files over `GET`, on loopback, for one reader. That is a hundred lines of
//! `std::net`, against a dependency tree this crate's manifest is explicit about not growing -
//! `src/clock.rs` exists for exactly the same reason, doing one civil date conversion rather than
//! taking a date library.
//!
//! # What it will not do
//!
//! It binds loopback by default, answers `GET` alone, refuses any path it did not expect, and
//! serves from a fixed table rather than from whatever is on disk. There is no authentication and
//! there is not meant to be: a port forward reaches loopback, and nothing else should.
//!
//! # The one detail that will cost an afternoon
//!
//! `.wasm` must be served as `application/wasm`. Anything else and
//! `WebAssembly.instantiateStreaming` refuses it, and the page is blank with a single console line
//! that does not mention the content type.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;

use anyhow::{Context, Result};

use crate::cli::ExploreArgs;
use crate::store::Store;

/// The largest request line this will read before giving up
///
/// A request that never sends a newline would otherwise hold a thread forever.
const MAX_REQUEST_BYTES: usize = 8 * 1024;

/// What each served file is, by extension
///
/// A fixed table rather than a guess. `application/wasm` in particular is not optional - see the
/// module docs.
const TYPES: [(&str, &str); 4] = [
    ("html", "text/html; charset=utf-8"),
    ("js", "text/javascript; charset=utf-8"),
    ("wasm", "application/wasm"),
    ("json", "application/json"),
];

/// Builds the bundle if it is needed, then serves it until interrupted
///
/// # Arguments
///
/// * `store` - The artifact tree, which is also the repository to build in
/// * `args` - What the caller asked for
/// * `out` - The directory holding the index and the bundle
pub fn run(store: &Store, args: &ExploreArgs, out: &Path) -> Result<i32> {
    // a bundle older than the sources it was built from would serve a stale explorer against a
    // fresh index, which looks exactly like the explorer being wrong
    if args.build || stale(store.root(), out)? {
        super::build::wasm(store.root(), out)?;
    }
    let address = format!("{}:{}", args.addr, args.port);
    let listener = TcpListener::bind(&address)
        .with_context(|| format!("binding {address}"))?;
    // print the forward as well as the address, because the machine that can serve this usually is
    // not the machine with the browser on it
    println!(
        "\nserving the explorer on http://{address}\n\n\
         from another machine:\n\
         \x20   ssh -L {port}:127.0.0.1:{port} {host}\n\
         \x20   open http://127.0.0.1:{port}\n\n\
         ctrl-c to stop",
        port = args.port,
        host = hostname(),
    );
    for stream in listener.incoming() {
        // one bad connection is not a reason to stop serving
        let Ok(stream) = stream else {
            continue;
        };
        if let Err(why) = answer(stream, out) {
            eprintln!("connection failed: {why}");
        }
    }
    Ok(0)
}

/// Answers one request
///
/// # Arguments
///
/// * `stream` - The connection to answer on
/// * `out` - The directory holding the index and the bundle
fn answer(mut stream: TcpStream, out: &Path) -> Result<()> {
    // read the request line, and only the request line: nothing here reads a body or a header
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut line = String::new();
    let read = reader
        .by_ref()
        .take(MAX_REQUEST_BYTES as u64)
        .read_line(&mut line)?;
    if read == 0 {
        return Ok(());
    }
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let target = parts.next().unwrap_or_default();
    // GET alone. there is nothing here to POST to
    if method != "GET" {
        return reply(&mut stream, 405, "text/plain; charset=utf-8", b"GET only");
    }
    // strip the query, which nothing here reads
    let target = target.split('?').next().unwrap_or_default();
    let Some(name) = resolve(target) else {
        return reply(&mut stream, 404, "text/plain; charset=utf-8", b"not found");
    };
    let path = out.join(name);
    let Ok(body) = std::fs::read(&path) else {
        // the commonest cause by far is a bundle that was never built, so say which file
        return reply(
            &mut stream,
            404,
            "text/plain; charset=utf-8",
            format!("{name} has not been built").as_bytes(),
        );
    };
    reply(&mut stream, 200, content_type(name), &body)
}

/// The file a request path maps to, or nothing when it maps to none
///
/// A fixed set rather than a join against the request. Nothing here ever concatenates a
/// caller-supplied path onto a directory, which is what makes traversal impossible rather than
/// merely checked for.
///
/// # Arguments
///
/// * `target` - The path that was requested
fn resolve(target: &str) -> Option<&'static str> {
    match target {
        "/" | "/index.html" => Some("index.html"),
        "/shoal_top.js" => Some("shoal_top.js"),
        "/shoal_top_bg.wasm" => Some("shoal_top_bg.wasm"),
        "/index.json" => Some("index.json"),
        _ => None,
    }
}

/// What one file should be served as
///
/// # Arguments
///
/// * `name` - The file being served
fn content_type(name: &str) -> &'static str {
    // by extension, out of the fixed table. an unknown extension is served as bytes rather than
    // guessed at
    let extension = name.rsplit_once('.').map(|(_, tail)| tail).unwrap_or("");
    TYPES
        .iter()
        .find(|(candidate, _)| *candidate == extension)
        .map(|(_, mime)| *mime)
        .unwrap_or("application/octet-stream")
}

/// Writes one response
///
/// # Arguments
///
/// * `stream` - The connection to write to
/// * `status` - The status code to send
/// * `mime` - What the body is
/// * `body` - The body itself
fn reply(stream: &mut TcpStream, status: u16, mime: &str, body: &[u8]) -> Result<()> {
    // the reason phrase is not read by anything, but a response without one is malformed
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "Error",
    };
    // an explicit length on every response, so the browser does not wait for a close to know the
    // body ended
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {mime}\r\nContent-Length: {}\r\n\
         Cache-Control: no-store\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()?;
    Ok(())
}

/// Whether the bundle is missing or older than the sources it was built from
///
/// # Arguments
///
/// * `root` - The repository the explorer is built from
/// * `out` - The directory holding the bundle
fn stale(root: &Path, out: &Path) -> Result<bool> {
    // no bundle at all is the commonest case, and the cheapest to check
    let bundle = out.join("shoal_top_bg.wasm");
    let Ok(built) = std::fs::metadata(&bundle).and_then(|meta| meta.modified()) else {
        return Ok(true);
    };
    // any source newer than the bundle means the bundle no longer describes the explorer. mtimes
    // are the right comparison here and would be the wrong one for a capture, because this is a
    // build artifact in `target/` rather than something git has to reproduce
    for entry in walkdir::WalkDir::new(root.join("shoal-top")).into_iter().flatten() {
        if !entry.file_type().is_file() {
            continue;
        }
        // walkdir's metadata error is its own type, so the two failures are unwrapped separately
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        let Ok(modified) = meta.modified() else {
            continue;
        };
        if modified > built {
            return Ok(true);
        }
    }
    Ok(false)
}

/// This machine's name, for the port forward line
fn hostname() -> String {
    // read rather than shelled out for, and with an honest placeholder when it cannot be read: the
    // line is a hint to copy, not something anything parses
    std::fs::read_to_string("/etc/hostname")
        .map(|found| found.trim().to_string())
        .ok()
        .filter(|found| !found.is_empty())
        .unwrap_or_else(|| "<this machine>".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_four_files_resolve() {
        // the bundle, the page, the glue and the index. everything else is a 404 rather than a
        // path joined onto a directory
        assert_eq!(resolve("/"), Some("index.html"));
        assert_eq!(resolve("/index.html"), Some("index.html"));
        assert_eq!(resolve("/shoal_top.js"), Some("shoal_top.js"));
        assert_eq!(resolve("/shoal_top_bg.wasm"), Some("shoal_top_bg.wasm"));
        assert_eq!(resolve("/index.json"), Some("index.json"));
    }

    #[test]
    fn traversal_does_not_resolve() {
        // there is nothing to escape from, because nothing is ever joined - but a future edit that
        // introduced a join would have to delete this test to pass
        for attempt in [
            "/../Cargo.toml",
            "/../../etc/passwd",
            "//etc/passwd",
            "/index.json/../../../etc/shadow",
            "/shoal_top_bg.wasm/../../secret",
            "",
        ] {
            assert_eq!(resolve(attempt), None, "{attempt} resolved to something");
        }
    }

    #[test]
    fn the_bundle_is_served_as_wasm() {
        // `WebAssembly.instantiateStreaming` refuses anything else, and says so in one console line
        // that does not mention the content type
        assert_eq!(content_type("shoal_top_bg.wasm"), "application/wasm");
        assert_eq!(
            content_type("index.html"),
            "text/html; charset=utf-8"
        );
        assert_eq!(content_type("index.json"), "application/json");
        assert_eq!(
            content_type("shoal_top.js"),
            "text/javascript; charset=utf-8"
        );
        // an extension the table does not know is served as bytes rather than guessed at
        assert_eq!(content_type("something.bin"), "application/octet-stream");
    }
}
