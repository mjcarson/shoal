//! Turning what a phase produced into a durable artifact
//!
//! The four layers arrive in four different shapes. Criterion scatters a directory tree under
//! `target/`; the macro workload writes one file per run and they have to be folded; the hotpath
//! profile is the last line a run printed; and the stage report is written by the run itself
//! because only the instrumented build can join the client and server halves of a record.
//!
//! What they have in common is that none of them is durable where it lands. `target/` does not
//! survive a `cargo clean`, and a scratch file is thrown away at the end of the capture. This is
//! where each becomes a file in `docs/perf/runs/` that will still be readable in a year.

pub mod hotpath;
pub mod macro_layer;
pub mod micro;
pub mod stages;
