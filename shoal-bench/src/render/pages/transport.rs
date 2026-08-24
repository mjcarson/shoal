//! What the client's transport modes and encryption cost
//!
//! Two families that are both about the wire rather than about the engine: how the client chooses
//! to send, and whether the kernel encrypts what it sends.

use anyhow::Result;

use crate::registry::Layer;
use crate::render::arms;
use crate::render::chart::encryption;
use crate::render::family::Surface;
use crate::render::page::Page;
use crate::render::pages::{caption, footer, header, nothing_measured};
use crate::render::tables;

/// Builds the transport and encryption page
///
/// # Arguments
///
/// * `page` - Everything the page is built from
pub fn build(page: &Page) -> Result<String> {
    let mut out = header(
        Surface::Transport,
        page,
        "The client's four ways of sending, each at a narrow row and at a MiB one, and the \
         encryption sweeps that say how the cost of TLS behaves across row width, load depth and \
         client count rather than merely whether it exists.",
    );
    let Some(capture) = page.current_for(Layer::Macro).and_then(|current| current.macro_layer.as_ref()) else {
        out.push_str(&nothing_measured("the transport modes or encryption"));
        out.push_str(&footer(Surface::Transport));
        return Ok(out);
    };
    // the transport modes
    let modes = arms::with_prefix(capture, "macro/transport/");
    out.push_str("## The four sending modes\n\n");
    if modes.is_empty() {
        out.push_str(&nothing_measured("the transport modes"));
    } else {
        out.push_str(&tables::transport(&modes));
        out.push('\n');
    }
    // the encryption sweeps, which were the one section of the old page that already grouped its
    // arms properly, and which are moved here unchanged
    let depth = encryption::pairs(capture, encryption::DEPTH_SWEEP);
    let clients = encryption::pairs(capture, encryption::CLIENT_SWEEP);
    out.push_str("## What encryption costs\n\n");
    if depth.is_empty() && clients.is_empty() {
        out.push_str(&nothing_measured("the encryption sweeps"));
        out.push_str(&footer(Surface::Transport));
        return Ok(out);
    }
    out.push_str(
        "Every point below is a pair: one workload over a plaintext wire and one over a wire the \
         kernel encrypts, differing in the wire and in nothing else - same seed, same rows, same \
         row width, same query count, same load. The gap between them is therefore what encryption \
         cost, rather than what else happened to move.\n\n\
         **A hollow marker is not a result.** The macro layer's rule is that a difference counts \
         only when the two sides' observed intervals are disjoint, and a pair whose runs overlapped \
         has not been shown to differ however far apart its medians sit.\n\n",
    );
    if !depth.is_empty() {
        out.push_str("### Against row width\n\n");
        out.push_str(&encryption::draw_by_row(capture)?);
        out.push('\n');
        out.push_str(&caption(
            "What TLS added, in nanoseconds, against how wide a row is. One curve per load depth. \
             Zero is drawn, so a curve that hugs it is visibly hugging it, and a curve below it is \
             a pair where the encrypted arm came out faster.",
        ));
        out.push_str("### Against load depth\n\n");
        out.push_str(&encryption::draw_by_depth(capture)?);
        out.push('\n');
        out.push_str(&caption(
            "The same pairs read the other way: what TLS added against how many queries were \
             outstanding at once on one client. One curve per row width.",
        ));
        out.push_str("### What it was added to\n\n");
        out.push_str(&encryption::draw_absolute(capture)?);
        out.push('\n');
        out.push_str(&caption(
            "The absolute p50 of one get on each wire, at every load depth the sweep covers. This \
             is what the gap above was added to: the same number of microseconds is a different \
             finding on a query that takes forty of them and on one that takes four thousand.",
        ));
    }
    if !clients.is_empty() {
        out.push_str("### Against client count\n\n");
        out.push_str(&encryption::draw_by_clients(capture)?);
        out.push('\n');
        out.push_str(&caption(
            "What TLS added against how many independent clients produced the load, each one query \
             deep and each with its own connection pool and its own handshakes.",
        ));
    }
    out.push_str(&tables::encryption(&depth, &clients));
    out.push('\n');
    out.push_str(&footer(Surface::Transport));
    Ok(out)
}
