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
    ClusterState, CompletionMenu, CompletionState, ErrorBar, QueryError, QueryLayout, QueryRow,
    Tab, TabContent, TabKind, TabQueryBar, TabSelector, TabState, follow_once, layout_query,
};
