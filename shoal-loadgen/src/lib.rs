//! Benchmark any Shoal schema against a dataset folder ([F66](../../docs/src/features/dataset-benchmarks.md))
//!
//! Nothing here names a schema. The driver is generic over the client type `#[shoal::db]`
//! emits, and everything it needs per table - parse a row, insert it, read it back - comes from
//! the table derives through `shoal::shared::dataset`. `shoaladm bench` is built on this; a
//! database writes no benchmark code of its own.

pub mod compare;
pub mod dataset;
pub mod driver;
pub mod events;
pub mod feed;
pub mod keys;
pub mod pick;
pub mod progress;
pub mod read;
pub mod results;
pub mod seed;
pub mod spec;
pub mod window;

#[cfg(test)]
mod testing;
