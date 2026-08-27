//! The desktop window
//!
//! # Why this is not the main path
//!
//! It needs a display, and the machine this was written on is a headless SSH session with no
//! `DISPLAY`, no Wayland socket, no `libGL` and no Vulkan. `shoal-bench explore --serve` builds the
//! same application to WebAssembly and serves it over localhost, which is what reaches a browser
//! through a port forward. This path exists for a workstation that has a screen, and for the live
//! view this crate is meant to grow into.

use crate::app::Explorer;
use crate::index::Index;

/// Opens the explorer in a window and runs until it is closed
///
/// # Arguments
///
/// * `index` - The corpus to draw from
pub fn run(index: Index) -> Result<(), eframe::Error> {
    // a window large enough for the picker and a chart side by side, which is the layout
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([1400.0, 900.0]),
        ..Default::default()
    };
    // the index is moved into the app rather than reloaded, so the window shows exactly the corpus
    // the caller projected
    eframe::run_native(
        "shoal-top",
        options,
        Box::new(move |cc| {
            // the creation context is the only place a `Context` is reachable before the first
            // frame - `eframe::App::ui` is handed a `Ui`, not a context, so a theme installed there
            // would be re-installed every frame and would fight the switch in the toolbar
            crate::theme::install(&cc.egui_ctx);
            Ok(Box::new(Explorer::new(index)))
        }),
    )
}
