//! UI components for shoalctl
//!
//! This module contains reusable UI components that can be rendered
//! to the terminal frame.

mod query_input;
mod status_bar;
mod tab_content;
mod tabs;

pub use query_input::QueryInput;
pub use status_bar::StatusBar;
pub use tab_content::TabContent;
pub use tabs::Tabs;
