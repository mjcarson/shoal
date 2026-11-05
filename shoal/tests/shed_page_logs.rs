//! Integration tests for what a driver's walk logs when its pages are shed
//!
//! A shard reads at most two of its drivers' walk pages at once and sheds the next one, which is
//! the admission working: since [Resolved #345](../../docs/src/appendix/resolved/backfill-shed-page.md)
//! every driver's walk asks a shed page again shortly, up to its tries. `walk_page` was instrumented
//! with `err(Debug)`, so every shed it handed back was logged at ERROR, and on M23's exit's drivers
//! rounds a device event's moves logged about a thousand of them in a capture, dozens in one
//! millisecond ([Resolved #349](../../docs/src/appendix/resolved/shed-pages-logged-quietly.md)).
//!
//! The walk here runs over a loop of the test's own that sheds every page as a shard's admission
//! does, since only a driver's `TabletWalk` reaches `walk_page`: a client's walk is answered by the
//! shard directly.
//!
//! **This binary holds one test.** The counters and the subscriber that feeds them are process
//! wide, and a second test running beside this one would add its own events to the same numbers.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::glommio::LocalExecutorBuilder;
use shoal::kanal;
use shoal::server::messages::ServerMsg;
use shoal::server::shard::walk::{ShedCause, TabletWalk, WalkAsk, WalkRefusal};
use shoal::shared::identity::TableId;
use shoal::shared::protocol::error::ErrorCode;
use shoal::storage::FileSystem;
use shoal::tables::PersistentUnsortedTable;
use shoal_derive::{db, ShoalUnsortedTable};
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::{Event, Level};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;

/// The module a walk's events are logged from
const WALK_TARGET: &str = "shoal_core::server::shard::walk";

/// How many events the walk's module logged at ERROR
static ERRORS: AtomicUsize = AtomicUsize::new(0);

/// How many events the walk's module logged at WARN
static WARNINGS: AtomicUsize = AtomicUsize::new(0);

/// A tracing layer that counts the walk module's errors and warnings this process logs
struct CountWalkEvents;

impl<S: tracing::Subscriber> Layer<S> for CountWalkEvents {
    /// Count this event if the walk's module logged it at ERROR or WARN
    ///
    /// # Arguments
    ///
    /// * `event` - The event being logged
    /// * `_ctx` - The context of the subscriber this layer is part of
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        // only the walk's own module
        let metadata = event.metadata();
        if metadata.target() != WALK_TARGET {
            return;
        }
        // counted by level
        match *metadata.level() {
            Level::ERROR => {
                ERRORS.fetch_add(1, Ordering::SeqCst);
            }
            Level::WARN => {
                WARNINGS.fetch_add(1, Ordering::SeqCst);
            }
            _ => (),
        }
    }
}

/// An unsorted row with a storage engine behind it
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "ShedDb")]
pub struct ShedRow {
    /// The partition key
    #[shoal(partition)]
    pub key: u64,
    /// Some data payload
    #[shoal(update)]
    pub data: String,
}

/// A schema whose loop's messages the walk is sent over
#[db]
pub struct ShedDb {
    /// The one table
    pub rows: PersistentUnsortedTable<ShedRow, FileSystem>,
}

/// A driver's walk whose every page is shed logs no error, and says once that it gave up
/// ([Resolved #349](../../docs/src/appendix/resolved/shed-pages-logged-quietly.md))
#[test]
fn a_drivers_shed_page_logs_no_error() {
    // every event this process logs, counted by the walk's module and level
    tracing_subscriber::registry()
        .with(CountWalkEvents.with_filter(tracing_subscriber::filter::LevelFilter::TRACE))
        .init();
    let refused = LocalExecutorBuilder::default()
        .name("shed_page_logs")
        .spawn(|| async move {
            // a loop that sheds every page as a shard reading two of its drivers' pages does
            let (loop_tx, loop_rx) = kanal::unbounded_async::<ServerMsg<ShedDb>>();
            let shard = shoal::glommio::spawn_local(async move {
                while let Ok(msg) = loop_rx.recv().await {
                    if let ServerMsg::Walk(call) = msg {
                        let _ = call.reply.send(Err(WalkRefusal::shed(
                            ErrorCode::Shedding,
                            "this shard is reading 2 driver pages already",
                            ShedCause::Full,
                        )));
                    }
                }
            });
            // a driver's walk of one tablet, asking a shed page again five times
            let ask = WalkAsk::tablet(TableId::of("ShedRow"), 0);
            let mut walk = TabletWalk::<ShedDb>::new(loop_tx.clone(), ask).shed_tries(5);
            let answer = walk.next().await;
            // the loop ends once nothing can send to it
            drop(walk);
            drop(loop_tx);
            shard.await;
            answer.err()
        })
        .expect("failed to spawn the walk's executor")
        .join()
        .expect("the walk's executor panicked");
    // the walk gave the shed page up past its tries
    let refused = refused.expect("a page shed every time was answered");
    assert_eq!(refused.code, ErrorCode::Shedding, "{refused:?}");
    // six pages shed and none of them logged as an error
    assert_eq!(
        ERRORS.load(Ordering::SeqCst),
        0,
        "a shed page, the admission working, was logged at ERROR"
    );
    // and the give-up said once
    assert_eq!(
        WARNINGS.load(Ordering::SeqCst),
        1,
        "the walk did not say once that it gave a shed page up"
    );
}
