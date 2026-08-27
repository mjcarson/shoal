//! The browser entry point
//!
//! # Why the browser is the primary path
//!
//! The machine this runs against is reached over SSH and has no display at all - no `DISPLAY`, no
//! Wayland socket, no `libGL`, no Vulkan. `shoal-bench explore --serve` builds this crate to
//! WebAssembly, writes the index beside it, and serves both from a loopback port that a port
//! forward reaches. That is how the explorer is actually opened.
//!
//! # What this does not do
//!
//! It does not read the corpus. Thirteen megabytes of captures stay on the machine that has them,
//! and the server hands over the projected index instead - about half a megabyte, and one request.

use wasm_bindgen::prelude::*;

use crate::app::Explorer;
use crate::index::Index;

/// Starts the explorer on a canvas, fetching the index the server built
///
/// # Arguments
///
/// * `canvas_id` - The id of the canvas element to draw into
#[wasm_bindgen]
pub async fn start(canvas_id: String) -> Result<(), JsValue> {
    // a panic in the browser is otherwise a silent stop with nothing in the console
    console_error_panic_hook::set_once();
    // the index is fetched rather than embedded, so rebuilding the corpus does not mean rebuilding
    // the wasm - and so a stale bundle can be told apart from a stale index
    let index = fetch_index().await?;
    // refuse an index this bundle does not understand, rather than dropping whatever section it has
    // never been told about
    index
        .check_version()
        .map_err(|why| JsValue::from_str(&why))?;
    let document = web_sys::window()
        .ok_or_else(|| JsValue::from_str("no window"))?
        .document()
        .ok_or_else(|| JsValue::from_str("no document"))?;
    let canvas = document
        .get_element_by_id(&canvas_id)
        .ok_or_else(|| JsValue::from_str("no canvas with that id"))?
        .dyn_into::<web_sys::HtmlCanvasElement>()?;
    // hand the app the index it will draw, exactly as the native path does
    eframe::WebRunner::new()
        .start(
            canvas,
            eframe::WebOptions::default(),
            Box::new(move |cc| {
                // installed here rather than per frame, exactly as the window does it. this is also
                // what stops the browser's `prefers-color-scheme` deciding the theme, which is the
                // one thing the shell page around this canvas cannot follow
                crate::theme::install(&cc.egui_ctx);
                Ok(Box::new(Explorer::new(index)))
            }),
        )
        .await
}

/// Fetches and parses the index the server wrote beside this bundle
async fn fetch_index() -> Result<Index, JsValue> {
    // one request, to a path the server always serves from the same directory as the bundle
    let window = web_sys::window().ok_or_else(|| JsValue::from_str("no window"))?;
    let response = wasm_bindgen_futures::JsFuture::from(window.fetch_with_str("index.json")).await?;
    let response: web_sys::Response = response.dyn_into()?;
    // a non `2xx` here is almost always the server having been started without an index built, so
    // it is worth saying which of the two went wrong
    if !response.ok() {
        return Err(JsValue::from_str(&format!(
            "the server answered {} for index.json",
            response.status()
        )));
    }
    let text = wasm_bindgen_futures::JsFuture::from(response.text()?).await?;
    let text = text
        .as_string()
        .ok_or_else(|| JsValue::from_str("index.json was not text"))?;
    serde_json::from_str(&text).map_err(|why| JsValue::from_str(&format!("{why}")))
}
