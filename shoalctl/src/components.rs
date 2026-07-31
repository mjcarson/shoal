//! UI components for shoalctl
//!
//! This module contains reusable UI components that can be rendered
//! to the terminal frame.

mod help_overlay;
mod status_bar;
mod tab;

pub use help_overlay::HelpOverlay;
pub use status_bar::StatusBar;
pub use tab::{
    CompletionMenu, CompletionState, QueryLayout, Tab, TabContent, TabQueryBar, TabSelector,
    TabState, layout_query,
};
