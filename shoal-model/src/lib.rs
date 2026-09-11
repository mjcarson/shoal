//! A pure, deterministic model of Shoal's replication protocol
//!
//! This crate is the executable form of the protocol contract that gates distributed Shoal:
//! the six properties P1-P6 in `docs/src/distributed/protocol.md`. It models one replicated
//! tablet group the way the contract describes it - a Raft-shaped protocol with stable storage
//! kept apart from volatile state - and drives it through explicit events, so that a schedule is
//! a list a test can save, replay, and minimize.
//!
//! Three things live here, and each is judged by the others:
//!
//! - [`raft`] is the protocol. Its [`policy::Policy`] has a safe setting, which is the contract,
//!   and six unsafe settings, each of which is one of the violations the contract table says the
//!   model must reject. The unsafe settings exist so that the checks below can be shown to fire.
//! - [`invariants`] checks every transition against the `P` numbers, computing what is committed
//!   from durable facts alone rather than trusting any node's own opinion.
//! - [`oracle`] judges the history a schedule produced against a sequential state machine, with
//!   successful, rejected and unknown outcomes held to their different contracts.
//!
//! Nothing here reads a clock, a file at runtime, or a random source other than the seeded
//! [`rng::SplitMix64`], and nothing depends on a shoal crate. That is what makes a saved schedule
//! reproduce the same violation forever, and what lets this run while the engine does not build.
//! The chosen consensus library (C13 Q1, still open) is not modeled: this is the contract the
//! adapter around it will be held to, not the library.

pub mod event;
pub mod ids;
pub mod invariants;
pub mod minimize;
pub mod observer;
pub mod oracle;
pub mod policy;
pub mod raft;
pub mod rng;
pub mod schedule;
pub mod storage;
pub mod world;

pub use event::{Actor, Body, ClientOp, Effect, Event, Message, MutationOp, OpResult};
pub use ids::{Attempt, Key, LogIndex, NodeId, OpId, TableId, TabletId, Term, Value};
pub use invariants::{Coverage, Property, Violation};
pub use oracle::{Ledger, OracleError, Outcome};
pub use policy::Policy;
pub use schedule::{Builder, Schedule, ScheduleParams};
pub use world::{RunOutcome, World};
