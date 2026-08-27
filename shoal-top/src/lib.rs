//! `top` for Shoal - an interactive view of what the database is doing, and of what it has done
//!
//! # What this is for
//!
//! Every chart in the book's performance chapter is a static SVG drawn from **one** capture, so a
//! metric moving across captures is not visible anywhere. This crate draws the same numbers with
//! the axes, the metric, the workloads and the set of captures all chosen at runtime, which is what
//! makes a regression something you can see rather than something you infer from two numbers in a
//! terminal.
//!
//! The benchmark corpus is the first thing it reads. A live trace from a running server is meant to
//! be the second, which is why every chart is written against [`index::Source`] rather than against
//! [`index::Index`] - a live feed is another implementor, not a rewrite.
//!
//! # The shape of the crate
//!
//! [`index`], [`fmt`] and [`preset`] are always compiled and depend on nothing but `serde`.
//! `shoal-bench` enters the crate that way, with `default-features = false`, so that the half of it
//! which judges a change never grows a graphics dependency. Everything else is behind `ui`, and
//! has to compile for `wasm32-unknown-unknown`.
//!
//! # Who fills the index
//!
//! Nothing here reads `docs/perf/`. `shoal_bench::explore::index` projects the corpus into
//! [`index::Index`], and this crate draws whatever it is handed. That split is not tidiness: it is
//! the only arrangement that compiles, because `shoal-bench` cannot be built for wasm at all.

pub mod fmt;
pub mod index;
pub mod preset;

// The drawing half. Behind a feature so that `shoal-bench` can link the index types without
// linking egui, which is what keeps `cargo tree -p shoal-bench --no-default-features` free of a
// graphics tree.
#[cfg(feature = "ui")]
pub mod app;
#[cfg(feature = "ui")]
pub mod picker;
#[cfg(feature = "ui")]
pub mod plot;
#[cfg(feature = "ui")]
pub mod prose;
#[cfg(feature = "ui")]
pub mod readout;
#[cfg(feature = "ui")]
pub mod theme;

// The native window. `shoal-bench explore` calls this; the `shoal-top` binary is the same thing
// with its own argument parsing.
#[cfg(feature = "native")]
pub mod native;

// The browser entry point, which fetches the index the server built rather than reading a file.
#[cfg(all(feature = "ui", target_arch = "wasm32"))]
pub mod web;

#[cfg(feature = "ui")]
pub use app::Explorer;
