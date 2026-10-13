//! A pure, deterministic model of the object store's stripe write protocol
//!
//! The second model in this crate, beside the tablet model, and held to its rules: no clock, no
//! file read at runtime, no random source but the seeded [`SplitMix64`](crate::rng::SplitMix64),
//! and no shoal crate in its graph. It is the executable form of the object contract, P7-P13 and
//! P15-P17 in `docs/src/object-storage/contract.md`, checked against the preferred direction of
//! S7: holders stage, one conditional commit of the stripe's row decides, holders apply
//! (`docs/src/object-storage/write-path.md`). It is spike X1, whose code is the one spike's code
//! that is kept: its schedules are the first gate's acceptance test.
//!
//! What it models is one object: its `ObjectMeta` entry and two or three stripes, each with its
//! row, its placement group's generation and positions, its holders and its readers, at three
//! layouts. The tablet groups that hold the entry and the rows are atomic objects that apply
//! commands in one order and let a lagging replica answer a read with a committed prefix: P1-P6
//! are what they are held to, by the tablet model, and they are not modelled again.
//!
//! - [`policy`] is the protocol's rules, one knob each, the first variant the contract and every
//!   other one a violation S16 says the model must reject.
//! - [`world`] and [`actors`] are the protocol: stagers, readers, rebuilders, movers, the
//!   reclaimer and the leader's timer acting on [`group`]s and [`holder`]s.
//! - [`check`] judges every event against the clauses from durable facts alone, [`oracle`] keeps
//!   the client history it judges reads by, and [`progress`] judges what readers and stagers took
//!   once the faults stop.
//! - [`schedule`], [`scenarios`] and [`minimize`] make, save, replay and shrink schedules.

pub mod actors;
pub mod check;
pub mod content;
pub mod event;
pub mod group;
pub mod holder;
pub mod ids;
pub mod layout;
pub mod minimize;
pub mod oracle;
pub mod policy;
pub mod progress;
pub mod scenarios;
pub mod schedule;
pub mod world;

pub use check::{StripeChecker, StripeCoverage};
pub use event::StripeEvent;
pub use layout::Layout;
pub use policy::StripePolicy;
pub use progress::ProgressFailure;
pub use schedule::{generate, StripeBuilder, StripeParams, StripeSchedule};
pub use world::{StripeOutcomeOfRun, StripeWorld};
