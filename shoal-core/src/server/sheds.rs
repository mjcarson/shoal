//! The writes a shard sheds, counted by what shed each and warned of
//!
//! A shed write is turned away before anything of it is applied, so its client may send it again,
//! and under a load a cluster cannot keep up with it is the answer a client should expect. But a
//! bench's preload of four million rows met a cluster that shed one write for longer than the
//! preload sent it again, and no node said anything above DEBUG, so what shed it was never known
//! ([item 357](../../../docs/src/appendix/resolved/preload-sheds-waited-out.md)).
//!
//! Each place that sheds a write calls [`note`] where it decides to, with its cause and the message
//! its client is answered with. The count is kept on the executor's own thread: a shard is one
//! executor, and the tables and the tasks it spawns to propose a write run on it, so every place
//! reaches the count without a handle threaded through the schema's tables. A shard reads it into
//! its replication report ([`figures`]), and a node sums its shards'. The warning is one a cause a
//! node, at most every [`WARN_EVERY`], the sheds between counted into the next: the first shed after
//! a quiet spell is always a WARN, and a cluster shedding thousands a second logs a line every ten
//! seconds a cause.

use std::cell::Cell;
use std::time::Duration;

use tracing::{event, Level};

use crate::server::objects::inflight::{Throttle, WARN_EVERY};
use crate::shared::protocol::stats::{WriteShedCause, WriteSheds};

/// How many causes a write is shed for, one throttle each
const CAUSES: usize = 8;

thread_local! {
    /// The writes this executor's shard shed since it started, by cause
    static SHED: Cell<WriteSheds> = Cell::new(WriteSheds::default());
}

/// The warnings of each cause, shared by every shard of the node
static WARNINGS: [Throttle; CAUSES] = [const { Throttle::new() }; CAUSES];

/// Where a cause's throttle sits among the warnings
///
/// # Arguments
///
/// * `cause` - What shed the write
fn slot(cause: WriteShedCause) -> usize {
    // one throttle a cause, in the order the causes are declared
    match cause {
        WriteShedCause::Gate => 0,
        WriteShedCause::PendingBytes => 1,
        WriteShedCause::VolatileLog => 2,
        WriteShedCause::ForgottenIdentity => 3,
        WriteShedCause::ForwardQueue => 4,
        WriteShedCause::Assembly => 5,
        WriteShedCause::PendingWrites => 6,
        WriteShedCause::Parked => 7,
    }
}

/// Count a write this shard shed, and warn of it unless one of its cause was warned of lately
///
/// # Arguments
///
/// * `cause` - What shed it
/// * `why` - What its client is told
pub fn note(cause: WriteShedCause, why: &str) {
    note_on(&WARNINGS, WARN_EVERY, cause, why);
}

/// Count a write this shard shed, and warn of it through a set of throttles
///
/// # Arguments
///
/// * `warnings` - The throttle of each cause
/// * `every` - How long after a cause's last warning its next is let through
/// * `cause` - What shed it
/// * `why` - What its client is told
fn note_on(warnings: &[Throttle; CAUSES], every: Duration, cause: WriteShedCause, why: &str) {
    // counted on this shard's own thread, whatever is logged
    SHED.with(|shed| {
        let mut counts = shed.get();
        counts.count(cause);
        shed.set(counts);
    });
    // the first of its cause in a while is a warning, saying how many were shed since the last
    match warnings[slot(cause)].pass(every) {
        Some(besides) => event!(
            Level::WARN,
            msg = "this node shed a write, which its client may send again",
            cause = cause.name(),
            why,
            shed_besides = besides,
        ),
        None => event!(
            Level::DEBUG,
            msg = "this node shed a write, which its client may send again",
            cause = cause.name(),
            why,
        ),
    }
}

/// The writes this shard shed since it started, by cause
///
/// Read on the shard's own thread, which is where every one of its sheds was counted.
#[must_use]
pub fn figures() -> WriteSheds {
    SHED.with(Cell::get)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tracing::field::{Field, Visit};
    use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

    /// One shed as it was logged: its level, its cause, and how many it said were shed besides
    type Logged = (Level, String, Option<u64>);

    /// Held by each test of this module for its whole length: a test that logs a shed from a thread
    /// with no subscriber while another's subscriber is being registered can leave the second's
    /// interest in this module's events cached as none, and its sheds unlogged
    static SERIAL: Mutex<()> = Mutex::new(());

    /// Take this module's turn, whichever test before it panicked
    fn serial() -> std::sync::MutexGuard<'static, ()> {
        SERIAL.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The fields of one event this module logs
    #[derive(Default)]
    struct Fields {
        /// Its message
        msg: Option<String>,
        /// Its cause
        cause: Option<String>,
        /// How many it said were shed besides
        besides: Option<u64>,
    }

    impl Visit for Fields {
        /// Keep the message and the cause, which are recorded as strings
        ///
        /// # Arguments
        ///
        /// * `field` - The field being recorded
        /// * `value` - Its value
        fn record_str(&mut self, field: &Field, value: &str) {
            // the two strings that say what was logged
            match field.name() {
                "msg" => self.msg = Some(value.to_owned()),
                "cause" => self.cause = Some(value.to_owned()),
                _ => {}
            }
        }

        /// Keep how many were shed besides
        ///
        /// # Arguments
        ///
        /// * `field` - The field being recorded
        /// * `value` - Its value
        fn record_u64(&mut self, field: &Field, value: u64) {
            // the one count a warning carries
            if field.name() == "shed_besides" {
                self.besides = Some(value);
            }
        }

        /// Ignore every other field
        ///
        /// # Arguments
        ///
        /// * `_field` - The field being recorded
        /// * `_value` - Its value
        fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}
    }

    /// A layer keeping every shed this module logs
    struct Sheds(Arc<Mutex<Vec<Logged>>>);

    impl<S: tracing::Subscriber> Layer<S> for Sheds {
        /// Keep this event if it is a shed
        ///
        /// # Arguments
        ///
        /// * `event` - The event being logged
        /// * `_ctx` - The context of the subscriber this layer is part of
        fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
            // read what the event says
            let mut fields = Fields::default();
            event.record(&mut fields);
            if fields.msg.as_deref().is_some_and(|msg| msg.starts_with("this node shed a write")) {
                self.0.lock().unwrap().push((
                    *event.metadata().level(),
                    fields.cause.unwrap_or_default(),
                    fields.besides,
                ));
            }
        }
    }

    /// A run of sheds is warned of once, each cause apart, and the next warning of a cause after its
    /// interval says how many were shed since; every shed is counted on the shard's thread whatever
    /// is logged (item 357)
    #[test]
    fn a_shed_run_is_warned_of_once_then_every_interval() {
        let _turn = serial();
        // throttles of this test's own, so no other test's sheds are counted into them
        let warnings: [Throttle; CAUSES] = [const { Throttle::new() }; CAUSES];
        let every = Duration::from_millis(200);
        let logged = Arc::new(Mutex::new(Vec::new()));
        let before = figures();
        tracing::subscriber::with_default(
            tracing_subscriber::registry().with(Sheds(logged.clone())),
            || {
                // every event's interest judged again with this subscriber among them, whatever a
                // test before it cached while it had none
                tracing::callsite::rebuild_interest_cache();
                // five sheds of a gate at once, and one of a table's pending bound among them
                for _ in 0..5 {
                    note_on(&warnings, every, WriteShedCause::Gate, "a gate");
                }
                note_on(&warnings, every, WriteShedCause::PendingWrites, "a table");
                // the gate sheds again once its interval has passed
                std::thread::sleep(every + Duration::from_millis(50));
                note_on(&warnings, every, WriteShedCause::Gate, "a gate");
            },
        );
        // the gate's first is a warning, its four after it are not, and its next says they were shed
        let warned: Vec<(String, Option<u64>)> = logged
            .lock()
            .unwrap()
            .iter()
            .filter(|(level, _, _)| *level == Level::WARN)
            .map(|(_, cause, besides)| (cause.clone(), *besides))
            .collect();
        assert_eq!(
            warned,
            vec![
                ("gate".to_owned(), Some(0)),
                ("pending_writes".to_owned(), Some(0)),
                ("gate".to_owned(), Some(4)),
            ]
        );
        assert_eq!(logged.lock().unwrap().len(), 7, "every shed is logged at some level");
        // and every one is counted on this thread, under its cause
        let after = figures();
        assert_eq!(after.gate - before.gate, 6);
        assert_eq!(after.pending_writes - before.pending_writes, 1);
        assert_eq!(after.total() - before.total(), 7);
    }

    /// A shard's count is its own thread's: another thread's sheds are never counted in it
    #[test]
    fn a_shards_count_is_its_own_threads() {
        let _turn = serial();
        let before = figures();
        // a shed on another thread, as on another shard's executor
        std::thread::spawn(|| note(WriteShedCause::ForwardQueue, "another shard"))
            .join()
            .expect("the other thread sheds");
        assert_eq!(figures(), before);
        // and one on this thread is this thread's
        note(WriteShedCause::ForwardQueue, "this shard");
        assert_eq!(figures().forward_queue, before.forward_queue + 1);
    }
}
