//! The shapes of every artifact this tool reads and writes
//!
//! Three of these mirror types that live in the `shoal` crate. They are mirrors rather than
//! reuses because `shoal-bench` deliberately depends on nothing else in this workspace - a tool
//! that judges a change must not rebuild as part of that change. The cost is that a mirror can
//! drift from what it mirrors, which is why [`macro_layer::MacroCapture`] carries a catch-all for
//! keys it does not name and a test asserts that catch-all comes back empty over every committed
//! artifact.

pub mod hotpath;
pub mod macro_layer;
pub mod meta;
pub mod micro;
pub mod stages;
