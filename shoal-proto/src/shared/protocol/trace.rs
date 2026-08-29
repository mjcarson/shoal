//! The trace context a request frame carries when its peer is tracing
//!
//! A client and a server are two processes, so the span a caller was in when it sent a query and
//! the span the shard opens answering it are joined by nothing unless something on the wire says
//! they belong together. This is that something: the W3C trace context, in the binary form
//! described below, written between a request frame's header and its payload when
//! [`Flags::TRACE_CONTEXT`](super::Flags::TRACE_CONTEXT) is set.
//!
//! ```text
//!  ┌─────────┬──────────────────┬────────────┬─────────┐
//!  │ version │     trace id     │  span id   │  flags  │
//!  │  (1 B)  │      (16 B)      │   (8 B)    │  (1 B)  │
//!  └─────────┴──────────────────┴────────────┴─────────┘
//! ```
//!
//! # Invariants
//!
//! **This module knows nothing about OpenTelemetry.** It is three byte arrays and a codec, which
//! is what lets it sit in the crate that links no async runtime. Both peers turn these bytes into
//! whatever their tracing stack wants: the server builds a `SpanContext` out of them in
//! `shoal-core`, and the client takes them off one there.
//!
//! **A context that cannot name a parent is never built.** W3C calls an all-zero trace id or span
//! id invalid, and so does [`TraceContext::new`], which returns `None` for either. That refusal is
//! the load bearing part of this module rather than a tidiness check: `tracing` turns a parent it
//! cannot resolve into `Attributes::new_root`, so a zero parent does not produce an orphan
//! somebody would notice - it silently starts a **new trace**, which is the exact failure a trace
//! context on the wire exists to stop.

use super::ProtocolError;

/// The size of a trace context in bytes
pub const TRACE_CONTEXT_LEN: usize = 26;

/// The version byte this build writes ahead of a trace context
///
/// The frame header already carries a protocol version, so this is not the mechanism by which the
/// wire evolves. It is here so that a decoder can refuse bytes that are not a trace context at
/// all, which the flag bit alone cannot tell it.
pub const TRACE_CONTEXT_VERSION: u8 = 0;

/// The bit W3C sets on a trace context whose trace was sampled
const SAMPLED: u8 = 1 << 0;

/// The offset the trace id starts at
const TRACE_ID_AT: usize = 1;

/// The offset the span id starts at
const SPAN_ID_AT: usize = TRACE_ID_AT + 16;

/// The offset the trace flags byte sits at
const FLAGS_AT: usize = SPAN_ID_AT + 8;

/// The trace a request belongs to, and the span within it that sent the request
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TraceContext {
    /// The trace this request is part of
    trace_id: [u8; 16],
    /// The span that sent this request, which the receiver's root span hangs off
    span_id: [u8; 8],
    /// The W3C trace flags, of which only the sampled bit is defined
    flags: u8,
}

impl TraceContext {
    /// Creates a trace context, if the ids in it can name a parent
    ///
    /// Returns `None` when either id is all zeroes. That is what an application with no
    /// OpenTelemetry layer installed produces, so this is the ordinary case rather than an error
    /// case - a caller that is not tracing gets `None` here and writes no trace context at all.
    ///
    /// # Arguments
    ///
    /// * `trace_id` - The trace this request is part of
    /// * `span_id` - The span that sent this request
    /// * `flags` - The W3C trace flags to carry
    ///
    /// # Examples
    ///
    /// ```
    /// use shoal_proto::shared::protocol::trace::TraceContext;
    ///
    /// // an id of all zeroes cannot name a parent, so no context is built from one
    /// assert!(TraceContext::new([0; 16], [1; 8], 1).is_none());
    /// assert!(TraceContext::new([1; 16], [1; 8], 1).is_some());
    /// ```
    #[inline]
    #[must_use]
    pub const fn new(trace_id: [u8; 16], span_id: [u8; 8], flags: u8) -> Option<Self> {
        // a zero trace id is invalid, and a parent that cannot be resolved is a new trace
        if u128::from_be_bytes(trace_id) == 0 {
            return None;
        }
        // and so is a zero span id, for the same reason
        if u64::from_be_bytes(span_id) == 0 {
            return None;
        }
        Some(TraceContext {
            trace_id,
            span_id,
            flags,
        })
    }

    /// Get the trace this request is part of
    #[inline]
    #[must_use]
    pub const fn trace_id(&self) -> [u8; 16] {
        self.trace_id
    }

    /// Get the span that sent this request
    #[inline]
    #[must_use]
    pub const fn span_id(&self) -> [u8; 8] {
        self.span_id
    }

    /// Get the W3C trace flags this context carries
    #[inline]
    #[must_use]
    pub const fn flags(&self) -> u8 {
        self.flags
    }

    /// Check whether the sender sampled this trace
    ///
    /// A receiver whose sampler is parent based takes this as the decision, which is what makes
    /// one trace whole rather than half exported.
    #[inline]
    #[must_use]
    pub const fn is_sampled(&self) -> bool {
        self.flags & SAMPLED == SAMPLED
    }

    /// Write this trace context out as the bytes that go on the wire
    #[inline]
    #[must_use]
    pub const fn encode(&self) -> [u8; TRACE_CONTEXT_LEN] {
        // lay the version down first so a decoder can refuse bytes that are not one of these
        let mut raw = [0u8; TRACE_CONTEXT_LEN];
        raw[0] = TRACE_CONTEXT_VERSION;
        // `copy_from_slice` is not const, so both ids are copied a byte at a time
        let mut index = 0;
        while index < 16 {
            raw[TRACE_ID_AT + index] = self.trace_id[index];
            index += 1;
        }
        let mut index = 0;
        while index < 8 {
            raw[SPAN_ID_AT + index] = self.span_id[index];
            index += 1;
        }
        // and the flags go last, where a future field would be appended after them
        raw[FLAGS_AT] = self.flags;
        raw
    }

    /// Read a trace context off the wire
    ///
    /// # Arguments
    ///
    /// * `raw` - The trace context bytes to read
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::UnknownTraceContextVersion`] for a version this build does not
    /// write, and [`ProtocolError::InvalidTraceContext`] when either id is all zeroes - which is a
    /// peer that set the flag and then wrote a context it should never have built.
    #[inline]
    pub const fn decode(raw: &[u8; TRACE_CONTEXT_LEN]) -> Result<Self, ProtocolError> {
        // refuse a version we do not know before making sense of anything after it
        if raw[0] != TRACE_CONTEXT_VERSION {
            return Err(ProtocolError::UnknownTraceContextVersion(raw[0]));
        }
        // pull both ids out, a byte at a time so this stays const
        let mut trace_id = [0u8; 16];
        let mut index = 0;
        while index < 16 {
            trace_id[index] = raw[TRACE_ID_AT + index];
            index += 1;
        }
        let mut span_id = [0u8; 8];
        let mut index = 0;
        while index < 8 {
            span_id[index] = raw[SPAN_ID_AT + index];
            index += 1;
        }
        // and refuse a context whose ids cannot name a parent, the same way `new` does
        match TraceContext::new(trace_id, span_id, raw[FLAGS_AT]) {
            Some(context) => Ok(context),
            None => Err(ProtocolError::InvalidTraceContext),
        }
    }
}
