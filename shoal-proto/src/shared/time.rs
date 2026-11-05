//! Times a row can hold: chrono's types, archived so that they order as they do
//!
//! A row is archived with rkyv 0.8, and chrono, even at its newest, archives only with rkyv 0.7.
//! Neither Shoal nor a schema may implement rkyv's traits for chrono's types, since neither crate
//! owns either, so a field of `chrono::DateTime<Utc>` cannot be in a row at all. These four types
//! wrap chrono's and are owned here, so they can be: each derefs to the chrono value it holds and
//! converts to and from it, reads and writes as chrono does through serde (so a SHQL literal is
//! the string chrono prints), and archives as integers in the order chrono gives its values.
//! That last is what lets any of them be a sort key: a partition read from disk is sought by its
//! keys' archived order, which has to be their own
//! ([F131](../../../docs/src/features/time-types.md)).
//!
//! | Type | Wraps | Archived as |
//! | --- | --- | --- |
//! | [`UtcDateTime`] | `DateTime<Utc>` | seconds since the Unix epoch (`i64`), then nanoseconds past them (`u32`) |
//! | [`NaiveDateTime`] | `chrono::NaiveDateTime` | the same, read as if it were UTC |
//! | [`NaiveDate`] | `chrono::NaiveDate` | days since the first day of the common era (`i32`) |
//! | [`NaiveTime`] | `chrono::NaiveTime` | seconds since midnight (`u32`), then nanoseconds past them (`u32`) |
//!
//! A leap second's nanoseconds run past a billion, as chrono holds them, so it orders after the
//! second before it and before the next one, as chrono orders it. A time with an offset is not
//! here: chrono orders and compares it by the instant alone, so its offset would be neither
//! ordered nor kept. Convert it to UTC first.
//!
//! The whole module is behind the `chrono` feature, off by default, so no client links chrono
//! unless it asks to; without it this module is empty.

#[cfg(feature = "chrono")]
mod types;

#[cfg(feature = "chrono")]
pub use types::{
    ArchivedNaiveDate, ArchivedNaiveDateTime, ArchivedNaiveTime, ArchivedUtcDateTime, NaiveDate,
    NaiveDateParts, NaiveDateTime, NaiveDateTimeParts, NaiveTime, NaiveTimeParts,
    UnrepresentableTime, UtcDateTime, UtcDateTimeParts,
};

/// The chrono these types wrap, so a schema names the exact version they were built against
#[cfg(feature = "chrono")]
pub use chrono;
