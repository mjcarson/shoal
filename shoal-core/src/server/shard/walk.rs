//! A walk of one tablet's rows on the shard hosting it, a page at a time
//!
//! A driver on the shard that leads a tablet's group - M16's backfill, M17's light scrub, M20's
//! reclamation - asks for a tablet's rows by tablet and by key range, a page at a time, with the
//! last key of the page before as its cursor ([F87](../../../../docs/src/features/tablet-walk.md)).
//! An admin read asks the same of the copy a node holds. Either way the page is a message to the
//! shard's loop: the loop routes it to the executor hosting the tablet, checks the copy against
//! its group, has the table capture the page and reads it on a task of its own, and checks the
//! copy again before it answers.
//!
//! # Invariants
//!
//! **A page is never read from a copy that is not whole.** A copy that is starting, down,
//! installing a snapshot or quarantined is refused by name before the page is captured, and a
//! page whose copy began an install or was quarantined while it read is refused when it lands,
//! since its reads may have met the install's files.
//!
//! **A page is never older than the walk's floor.** Each page reports the applied index its copy
//! had reached when it began, and a page asked `at_least` an index its copy has not reached is
//! refused with where the copy is, so a walk resumed on another copy or after a restart never
//! steps back past what an earlier page saw.
//!
//! **The loop never waits on a page.** The capture is synchronous and bounded by the page; the
//! reads run off the loop and post their outcome back: a driver's in the shard's medium queue, a
//! scan's in its scan queue in 64 KiB steps ([F117](../../../../docs/src/features/scan-class.md)).
//!
//! **No buffer of a scan's page is allocated past its node's scan budget**: its bytes are drawn
//! before it reaches the loop, or where it is admitted, its capture's excess before its reads, and
//! each archived record before it is read; the permits travel with the page and drop with its
//! bytes ([F118](../../../../docs/src/features/scan-budget.md)).
//!
//! **A scan's page is captured only when the loop is idle**, from the loop's second inbox, and
//! everything its page reports - the copy's group, installs, boundary and leadership - is taken at
//! that capture, its copy checked again there; admission only refuses early.
//!
//! **A scan never takes a driver's admission.** A page is admitted by its class's count: a
//! driver's or an operator's by [`PAGES_IN_FLIGHT`], a scan's by `scans.pages_in_flight`, so a
//! client's scan of a table never sheds the backfill that brings a stale copy current
//! ([F116](../../../../docs/src/features/scan-admission.md)).

use glommio::TaskQueueHandle;
use kanal::AsyncSender;
use openraft_rt::WatchReceiver as _;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{event, instrument, Level};
use uuid::Uuid;

use super::idle::IdleStep;
use super::{control_reply, Shard, ADMIN_TIMEOUT};
use crate::server::database::ShoalDatabase;
use crate::server::errors::ShoalError;
use crate::server::messages::{Reply, ReplyKind, ServerMsg};
use crate::server::shard::ShardContact;
use crate::server::tables::walk::{PageDraws, WalkBounds, WalkRows, WalkedPartition};
use crate::server::ServerError;
use crate::shared::identity::{GroupId, NodeId, TableId};
use crate::shared::placement::{tablet_range, TABLET_COUNT};
use crate::server::budget::{Permit, Shed, DRAW_WAIT};
use crate::server::conf::Scans;
use crate::server::faults;
use crate::server::steps::Steps;
use crate::shared::protocol::admin::{
    AdminError, AdminKind, AdminOutcome, AdminRequest, AdminResponse, WalkAnswer, WalkClass, WalkedStripe,
    SCAN_MAX_BYTES, WALK_DEFAULT_BYTES, WALK_DEFAULT_LIMIT, WALK_MAX_BYTES, WALK_MAX_LIMIT,
};
use crate::shared::protocol::error::ErrorCode;
use crate::shared::stripes::StripeRow;
use shoal_channel::KeptReceiver;

/// How many of its drivers' and operators' pages one shard reads at once before it sheds another
///
/// A scan's pages are counted apart, by `scans.pages_in_flight`
/// ([F116](../../../../docs/src/features/scan-admission.md)).
pub const PAGES_IN_FLIGHT: usize = 2;

/// How long a driver waits on a page before it calls the shard silent
const PAGE_TIMEOUT: Duration = Duration::from_secs(30);

/// What one page of a walk asks for
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalkAsk {
    /// The table, by its stable identity
    pub table: TableId,
    /// The tablet
    pub tablet: u16,
    /// The first key of the range walked
    pub low: u64,
    /// The last key of the range walked
    pub high: u64,
    /// The last key the page before handed out, or none for the first page
    pub after: Option<u64>,
    /// The most partitions the page holds
    pub limit: u32,
    /// The bytes past which the page holds no more partitions
    pub max_bytes: u64,
    /// The lowest applied index the page may be read at
    pub at_least: Option<u64>,
    /// Whose count the page is admitted by: a driver's, unless it is a scan's
    /// ([F116](../../../../docs/src/features/scan-admission.md))
    pub class: WalkClass,
}

impl WalkAsk {
    /// The first page of a walk of a whole tablet, at the default bounds
    ///
    /// # Arguments
    ///
    /// * `table` - The table, by its stable identity
    /// * `tablet` - The tablet
    #[must_use]
    pub fn tablet(table: TableId, tablet: u16) -> Self {
        // the tablet's whole range
        let (low, high) = tablet_range(tablet);
        WalkAsk {
            table,
            tablet,
            low,
            high,
            after: None,
            limit: WALK_DEFAULT_LIMIT,
            max_bytes: WALK_DEFAULT_BYTES,
            at_least: None,
            class: WalkClass::Driver,
        }
    }

    /// The same page, asked as a scan's: admitted by a scan's count, never a driver's
    #[must_use]
    pub fn as_scan(self) -> Self {
        WalkAsk {
            class: WalkClass::Scan,
            ..self
        }
    }

    /// The first page of a walk of one key range of a tablet, such as a placement group's
    ///
    /// # Arguments
    ///
    /// * `table` - The table, by its stable identity
    /// * `tablet` - The tablet
    /// * `range` - The range's first and last keys, inside the tablet
    #[must_use]
    pub fn range(table: TableId, tablet: u16, range: (u64, u64)) -> Self {
        // the tablet's defaults, narrowed to the range
        WalkAsk {
            low: range.0,
            high: range.1,
            ..Self::tablet(table, tablet)
        }
    }

    /// The ask an admin request names, filled in with the defaults it leaves out
    ///
    /// # Arguments
    ///
    /// * `kind` - The admin request, a `Walk`
    #[must_use]
    pub fn of_admin(kind: &AdminKind) -> Option<Self> {
        // only a walk names one
        let AdminKind::Walk {
            table,
            tablet,
            low,
            high,
            after,
            limit,
            bytes,
            at_least,
            class,
        } = kind
        else {
            return None;
        };
        // the tablet's range unless the request narrows it
        let (tablet_low, tablet_high) = tablet_range(*tablet);
        Some(WalkAsk {
            table: TableId::of(table),
            tablet: *tablet,
            low: low.unwrap_or(tablet_low),
            high: high.unwrap_or(tablet_high),
            after: *after,
            limit: limit.unwrap_or(WALK_DEFAULT_LIMIT),
            max_bytes: bytes.unwrap_or(WALK_DEFAULT_BYTES),
            at_least: *at_least,
            // a request that names none is a driver's or an operator's, as every one was before
            class: class.unwrap_or_default(),
        })
    }
}

/// One page of a walk, as the shard hosting the tablet read it
#[derive(Debug)]
pub struct WalkPage {
    /// The table walked
    pub table: TableId,
    /// The tablet walked
    pub tablet: u16,
    /// What the page was asked for
    pub ask: WalkAsk,
    /// The partitions, in ascending key order
    pub partitions: Vec<WalkedPartition>,
    /// Their archived bytes
    pub bytes: u64,
    /// The applied index the copy had reached when the page began, or none on a standalone node
    pub boundary: Option<u64>,
    /// Whether the copy led its group when the page began, or none on a standalone node
    pub led: Option<bool>,
    /// The key the next page starts after, or none once the range is walked
    pub next: Option<u64>,
    /// The group the copy belongs to, or none on a standalone node
    pub group: Option<GroupId>,
    /// How many partitions came from memory
    pub resident: u32,
    /// How many came from an archive
    pub archived: u32,
    /// How many archived records had moved under the page
    pub reprobed: u32,
    /// How long the capture held the shard's loop
    pub held: Duration,
    /// How long the reads took off it
    pub read: Duration,
    /// How long a scan's page waited from its admission to its capture, for the loop to be idle
    /// ([F117](../../../../docs/src/features/scan-class.md)); zero for a driver's
    pub queued: Duration,
    /// What a scan's page drew from its node's scan budget, given back when the page and its
    /// bytes are dropped ([F118](../../../../docs/src/features/scan-budget.md)); none for a driver's
    pub drawn: Vec<Permit>,
}

impl WalkPage {
    /// Every partition of a page of a bucket's `StripeMeta` table, as its key and stripe row
    ///
    /// # Errors
    ///
    /// Fails if the table is not a bucket's stripe table, or a partition does not read back.
    pub fn stripes<D: ShoalDatabase>(&self) -> Result<Vec<(u64, StripeRow)>, ServerError> {
        // the table, as this schema names it
        let Some(table) = D::table_of_id(self.table) else {
            return Err(ServerError::GlommioGeneric(format!(
                "no table of this schema has the identity {}",
                self.table
            )));
        };
        // each partition's row, by the conversion an answer reads it with
        self.partitions
            .iter()
            .map(|partition| match D::walked_stripe(table, &partition.bytes) {
                Some(row) => row.map(|row| (partition.key, row)),
                None => Err(ServerError::GlommioGeneric(format!(
                    "{table} is not a bucket's stripe table"
                ))),
            })
            .collect()
    }

    /// Every partition of a page of a bucket's `ObjectMeta` table, as its key and row (M20)
    ///
    /// # Errors
    ///
    /// Fails if the table is not a bucket's object table, or a partition does not read back.
    pub fn objects<D: ShoalDatabase>(&self) -> Result<Vec<(u64, crate::shared::objects::ObjectRow)>, ServerError> {
        // the table, as this schema names it
        let Some(table) = D::table_of_id(self.table) else {
            return Err(ServerError::GlommioGeneric(format!(
                "no table of this schema has the identity {}",
                self.table
            )));
        };
        // each partition's row, by the conversion a walk of the table reads it with
        self.partitions
            .iter()
            .map(|partition| match D::walked_object(table, &partition.bytes) {
                Some(row) => row.map(|row| (partition.key, row)),
                None => Err(ServerError::GlommioGeneric(format!(
                    "{table} is not a bucket's object table"
                ))),
            })
            .collect()
    }

    /// The page as an admin read answers it: the keys and what each holds, and no rows
    #[must_use]
    pub fn answer<D: ShoalDatabase>(&self) -> WalkAnswer {
        // the table by the name the schema spells it, and the stripes if it is a stripe table
        let table = D::table_of_id(self.table);
        let stripes = table.and_then(|table| {
            let rows: Option<Vec<WalkedStripe>> = self
                .partitions
                .iter()
                .map(|partition| {
                    D::walked_stripe(table, &partition.bytes)
                        .and_then(Result::ok)
                        .map(|row| WalkedStripe {
                            consumer: row.consumer,
                            object: format!("{:032x}", row.object),
                            stripe: row.stripe,
                            sequence: row.sequence,
                        })
                })
                .collect();
            rows
        });
        WalkAnswer {
            table: table.map_or_else(|| self.table.to_string(), |table| table.to_string()),
            tablet: self.tablet,
            low: self.ask.low,
            high: self.ask.high,
            after: self.ask.after,
            keys: self.partitions.iter().map(|partition| partition.key).collect(),
            rows: self.partitions.iter().map(|partition| partition.rows).collect(),
            bytes: self.bytes,
            boundary: self.boundary,
            led: self.led,
            next: self.next,
            group: self.group.map(|group| group.to_string()),
            resident: self.resident,
            archived: self.archived,
            reprobed: self.reprobed,
            held_us: u64::try_from(self.held.as_micros()).unwrap_or(u64::MAX),
            read_us: u64::try_from(self.read.as_micros()).unwrap_or(u64::MAX),
            queued_us: u64::try_from(self.queued.as_micros()).unwrap_or(u64::MAX),
            stripes: (!self.partitions.is_empty()).then_some(stripes).flatten(),
        }
    }
}

/// Why a page of a walk was not read
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalkRefusal {
    /// What class of refusal this is
    pub code: ErrorCode,
    /// What the shard said about it, for a person
    pub msg: String,
    /// The applied index the copy had reached, when the refusal is about where it is
    pub applied: Option<u64>,
}

impl WalkRefusal {
    /// A refusal
    ///
    /// # Arguments
    ///
    /// * `code` - What class of refusal this is
    /// * `msg` - What to say about it
    pub fn new<M: Into<String>>(code: ErrorCode, msg: M) -> Self {
        WalkRefusal {
            code,
            msg: msg.into(),
            applied: None,
        }
    }

    /// Whether asking the same page again later can be answered
    #[must_use]
    pub fn is_transient(&self) -> bool {
        // a copy that is busy, behind or changing settles; anything else is the ask's
        matches!(
            self.code,
            ErrorCode::Unavailable | ErrorCode::Shedding | ErrorCode::Timeout
        )
    }
}

impl From<WalkRefusal> for AdminError {
    /// The refusal as an admin read reports it
    fn from(refusal: WalkRefusal) -> Self {
        // where the copy is, when the refusal is about that
        let msg = match refusal.applied {
            Some(applied) => format!("{} (applied {applied})", refusal.msg),
            None => refusal.msg,
        };
        AdminError::new(refusal.code, msg)
    }
}

/// A page asked of a shard, and where its answer goes
pub struct WalkCall {
    /// What the page asks for
    pub ask: WalkAsk,
    /// Whether another executor of this node already routed it here
    pub routed: bool,
    /// Where the page or the refusal goes
    pub reply: kanal::Sender<Result<WalkPage, WalkRefusal>>,
    /// A scan's page's bytes, drawn from its node's scan budget before the page reached a loop;
    /// none for a driver's, or a scan's asked in process, which draws where it is admitted (F118)
    pub permit: Option<Permit>,
}

impl std::fmt::Debug for WalkCall {
    /// The ask and whether it was routed, since a channel has nothing worth printing
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalkCall")
            .field("ask", &self.ask)
            .field("routed", &self.routed)
            .finish()
    }
}

/// A page captured on this shard and still being read
struct InFlight {
    /// What the page asked for, its class among it
    ask: WalkAsk,
    /// Where its answer goes
    reply: kanal::Sender<Result<WalkPage, WalkRefusal>>,
    /// The copy's group, and how many installs it had begun, when the page began
    group: Option<(GroupId, u64)>,
    /// The applied index the copy had reached when the page began
    boundary: Option<u64>,
    /// Whether the copy led its group when the page began
    led: Option<bool>,
    /// How long the capture held the loop
    held: Duration,
    /// How long the page waited for its capture after it was admitted
    queued: Duration,
    /// What a scan's page drew from the scan budget at its admission and its capture (F118)
    drawn: Vec<Permit>,
}

/// A scan's page admitted and waiting in the loop's second inbox for its capture
/// ([F117](../../../../docs/src/features/scan-class.md))
#[derive(Debug)]
pub struct QueuedCapture {
    /// What the page asks for
    ask: WalkAsk,
    /// Where its answer goes
    reply: kanal::Sender<Result<WalkPage, WalkRefusal>>,
    /// When it was admitted
    admitted: Instant,
    /// Its bytes, drawn from the node's scan budget at or before its admission (F118)
    permit: Permit,
}

/// A refusal of a scan's page that found no room in its node's scan budget
///
/// Shed, to be asked again after a while, unless it needed more than the whole budget holds,
/// which no wait answers ([F118](../../../../docs/src/features/scan-budget.md)).
///
/// # Arguments
///
/// * `shed` - What the budget shed
fn scan_refusal(shed: Shed) -> WalkRefusal {
    if shed.never_fits() {
        WalkRefusal::new(
            ErrorCode::ResponseTooLarge,
            format!(
                "a scan's page needs {} bytes, past the whole scan budget of {}; raise resources.scan_memory",
                shed.needed, shed.limit
            ),
        )
    } else {
        WalkRefusal::new(
            ErrorCode::Shedding,
            format!("the scan budget is spent: {} bytes found no room in their wait", shed.needed),
        )
    }
}

/// The pages this shard is reading
#[derive(Default)]
pub(super) struct Walks {
    /// The number the next page is known by
    next: u64,
    /// The pages being read, by number
    flight: HashMap<u64, InFlight>,
    /// What this node does for a scan's pages: whether it answers them, and how many a shard reads
    scans: Scans,
    /// How many scan pages are admitted and waiting for their capture, which count against the
    /// scan class from their admission
    queued: usize,
}

impl Walks {
    /// No page being read yet, under this node's settings for scans
    ///
    /// # Arguments
    ///
    /// * `scans` - What this node does for a scan's pages
    pub(super) fn new(scans: &Scans) -> Self {
        Walks {
            next: 0,
            flight: HashMap::new(),
            scans: scans.clone(),
            queued: 0,
        }
    }

    /// How many pages of a class may be read at once on this shard
    ///
    /// # Arguments
    ///
    /// * `class` - The class
    fn admits(&self, class: WalkClass) -> usize {
        match class {
            WalkClass::Driver => PAGES_IN_FLIGHT,
            WalkClass::Scan => self.scans.pages_in_flight,
        }
    }

    /// How many pages of a class this shard is reading
    ///
    /// # Arguments
    ///
    /// * `class` - The class
    fn reading(&self, class: WalkClass) -> usize {
        // a few pages at most, so counting them is cheaper than keeping a count right; a scan's
        // waiting for its capture count from their admission (F117)
        let waiting = match class {
            WalkClass::Driver => 0,
            WalkClass::Scan => self.queued,
        };
        waiting + self.flight.values().filter(|flight| flight.ask.class == class).count()
    }
}

/// The bytes past which a page holds no more partitions: what it asked, a scan's held to
/// [`SCAN_MAX_BYTES`], a few seconds of a busy device's trickle
/// ([F116](../../../../docs/src/features/scan-admission.md))
///
/// # Arguments
///
/// * `ask` - What the page asks for
fn page_bytes(ask: &WalkAsk) -> u64 {
    match ask.class {
        WalkClass::Driver => ask.max_bytes,
        WalkClass::Scan => ask.max_bytes.min(SCAN_MAX_BYTES),
    }
}

/// Ask a shard's loop for one page of a walk and wait for it
///
/// What a driver on the shard that leads a tablet's group calls: the loop routes the page to the
/// executor hosting the tablet and answers once it is read, never waiting on it itself.
///
/// # Arguments
///
/// * `loop_tx` - The shard's own loop
/// * `ask` - What the page asks for
///
/// # Errors
///
/// Refused by name when the copy cannot answer, and `Timeout` when the loop does not.
#[instrument(name = "walk::walk_page", skip_all, fields(tablet = ask.tablet), err(Debug))]
pub async fn walk_page<D: ShoalDatabase>(
    loop_tx: &AsyncSender<ServerMsg<D>>,
    ask: WalkAsk,
) -> Result<WalkPage, WalkRefusal> {
    // the answer comes back on a channel of its own
    let (reply, rx) = kanal::bounded(1);
    let call = WalkCall {
        ask,
        routed: false,
        reply,
        permit: None,
    };
    if loop_tx.send(ServerMsg::Walk(Box::new(call))).await.is_err() {
        return Err(WalkRefusal::new(
            ErrorCode::Unavailable,
            "the shard's loop is gone",
        ));
    }
    // the receive is kept, never raced bare: a timer that wins after the page was handed over
    // would drop it (Resolved #152)
    let mut answer = KeptReceiver::new(rx.to_async());
    match glommio::timer::timeout(PAGE_TIMEOUT, async { Ok(answer.next().await) }).await {
        Ok(Ok(page)) => page,
        _ => Err(WalkRefusal::new(
            ErrorCode::Timeout,
            "the shard did not answer the page within its deadline",
        )),
    }
}

/// A walk of a tablet or a key range of one, a page at a time, keeping its cursor and its floor
///
/// The cursor is the last key a page handed out and the floor the highest applied index any page
/// was read at; a driver commits both as its progress, and a walk resumed from them on another
/// copy is never read from a state older than what it already saw.
pub struct TabletWalk<D: ShoalDatabase> {
    /// The shard's own loop
    loop_tx: AsyncSender<ServerMsg<D>>,
    /// What every page asks for, apart from its cursor and floor
    ask: WalkAsk,
    /// Whether the range is walked
    done: bool,
}

impl<D: ShoalDatabase> TabletWalk<D> {
    /// A walk from an ask, which a resumed walk fills with its committed cursor and floor
    ///
    /// # Arguments
    ///
    /// * `loop_tx` - The shard's own loop
    /// * `ask` - What every page asks for, its `after` and `at_least` where the walk resumes
    #[must_use]
    pub fn new(loop_tx: AsyncSender<ServerMsg<D>>, ask: WalkAsk) -> Self {
        TabletWalk {
            loop_tx,
            ask,
            done: false,
        }
    }

    /// The last key a page handed out, which the next page starts after
    #[must_use]
    pub fn cursor(&self) -> Option<u64> {
        self.ask.after
    }

    /// The highest applied index any page was read at, which no later page goes below
    #[must_use]
    pub fn floor(&self) -> Option<u64> {
        self.ask.at_least
    }

    /// The next page, or none once the range is walked
    ///
    /// A refusal leaves the cursor where it was, so the same page can be asked for again.
    ///
    /// # Errors
    ///
    /// Refused by name when the copy cannot answer.
    pub async fn next(&mut self) -> Result<Option<WalkPage>, WalkRefusal> {
        // a walked range has nothing more
        if self.done {
            return Ok(None);
        }
        let page = walk_page(&self.loop_tx, self.ask).await?;
        // the cursor moves to the page's last key, and the floor up to its boundary
        match page.next {
            Some(next) => self.ask.after = Some(next),
            None => self.done = true,
        }
        self.ask.at_least = self.ask.at_least.max(page.boundary);
        Ok(Some(page))
    }
}

impl<D: ShoalDatabase> Shard<D>
where
    [<<<D as ShoalDatabase>::ClientType as crate::shared::traits::QuerySupport>::QueryKinds as rkyv::Archive>::Archived]:
        rkyv::DeserializeUnsized<
            [<<D as ShoalDatabase>::ClientType as crate::shared::traits::QuerySupport>::QueryKinds],
            rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
        >,
{
    /// Take a page of a walk: route it, check the copy, capture it, and read it off the loop
    ///
    /// # Arguments
    ///
    /// * `call` - The page and where its answer goes
    #[instrument(name = "Shard::handle_walk", skip_all, fields(tablet = call.ask.tablet))]
    pub(super) fn handle_walk(&mut self, call: WalkCall) {
        // refused before anything is captured, or captured and read
        let WalkCall {
            ask,
            routed,
            reply,
            permit,
        } = call;
        if let Err(refusal) = self.start_walk(ask, routed, &reply, permit) {
            let _ = reply.try_send(Err(refusal));
        }
    }

    /// Route, check and capture a page, handing its reads to a task
    ///
    /// Returns a refusal to answer the page with; a page routed elsewhere or being read answers
    /// on its own.
    ///
    /// # Arguments
    ///
    /// * `ask` - What the page asks for
    /// * `routed` - Whether another executor already routed it here
    /// * `reply` - Where its answer goes
    /// * `permit` - A scan's page's bytes, if drawn before it reached a loop (F118)
    fn start_walk(
        &mut self,
        ask: WalkAsk,
        routed: bool,
        reply: &kanal::Sender<Result<WalkPage, WalkRefusal>>,
        permit: Option<Permit>,
    ) -> Result<(), WalkRefusal> {
        // a node that answers no scan says so, never as a refusal a scan asks again (F116)
        self.answers_scans(&ask)?;
        // the table, as this schema names it
        let table = D::table_of_id(ask.table).ok_or_else(|| {
            WalkRefusal::new(
                ErrorCode::UnknownOperation,
                format!("this schema has no table with the identity {}", ask.table),
            )
        })?;
        // the range has to be the tablet's, and the bounds the ones a page may ask
        let bounds = Self::walk_bounds(&ask)?;
        // the executor hosting the tablet on this node
        let host = self.walk_host(ask.tablet)?;
        if host != self.shard_id {
            // a page routed once and still not hosted here means the hosting moved under it
            if routed {
                return Err(WalkRefusal::new(
                    ErrorCode::StaleTopology,
                    format!(
                        "tablet {} was routed to executor {} and is not hosted there",
                        ask.tablet, self.shard_id
                    ),
                ));
            }
            let call = WalkCall {
                ask,
                routed: true,
                reply: reply.clone(),
                // its bytes travel with it to the executor that reads it (F118)
                permit,
            };
            let mut comms = self.comms.clone();
            let reply = reply.clone();
            glommio::spawn_local(async move {
                // a mesh that cannot reach the executor answers the page itself
                if comms
                    .send(&ShardContact::Local(host), ServerMsg::Walk(Box::new(call)))
                    .await
                    .is_err()
                {
                    let _ = reply.try_send(Err(WalkRefusal::new(
                        ErrorCode::Unavailable,
                        format!("executor {host} hosting the tablet is not taking messages"),
                    )));
                }
            })
            .detach();
            return Ok(());
        }
        // the copy, checked against its group before anything is captured
        let (group, boundary, led) = self.walk_copy(&ask)?;
        // a cursor at the range's last key leaves nothing to walk
        let Some(bounds) = bounds else {
            let _ = reply.try_send(Ok(WalkPage {
                table: ask.table,
                tablet: ask.tablet,
                ask,
                partitions: Vec::new(),
                bytes: 0,
                boundary,
                led,
                next: None,
                group: group.map(|(group, _)| group),
                resident: 0,
                archived: 0,
                reprobed: 0,
                held: Duration::ZERO,
                read: Duration::ZERO,
                queued: Duration::ZERO,
                drawn: Vec::new(),
            }));
            return Ok(());
        };
        // a shard reads a few pages of each class at once and sheds the rest of that class, so a
        // scan's pages never take a driver's admission (F116)
        let admits = self.walks.admits(ask.class);
        if self.walks.reading(ask.class) >= admits {
            return Err(WalkRefusal::new(
                ErrorCode::Shedding,
                format!("this shard is reading {admits} {} pages already", ask.class.as_str()),
            ));
        }
        // a scan's page waits in the loop's second inbox for its capture, counted from now (F117)
        if ask.class == WalkClass::Scan {
            // its bytes drawn before it waits, here unless they were before it reached a loop (F118)
            let permit = match permit {
                Some(permit) => permit,
                None => {
                    let budget = &self.scans.budget;
                    let bytes = page_bytes(&ask);
                    budget
                        .try_draw(bytes)
                        .ok_or_else(|| scan_refusal(budget.note_shed(bytes)))?
                }
            };
            self.walks.queued += 1;
            faults::count_capture(true);
            self.idle.push(IdleStep::Capture(Box::new(QueuedCapture {
                ask,
                reply: reply.clone(),
                admitted: Instant::now(),
                permit,
            })));
            return Ok(());
        }
        // a driver's page is captured now, as every page was
        self.read_page(ask, table, bounds, (group, boundary, led), reply, Duration::ZERO, Vec::new())
    }

    /// Refuse a scan's page on a node that answers no scans, by name and by a code no scan asks
    /// again, before anything is drawn for it (F116, F118)
    ///
    /// # Arguments
    ///
    /// * `ask` - What the page asks for
    fn answers_scans(&self, ask: &WalkAsk) -> Result<(), WalkRefusal> {
        if ask.class == WalkClass::Scan && !self.walks.scans.enabled {
            return Err(WalkRefusal::new(
                ErrorCode::InvalidRequest,
                "this node answers no scan's pages: its scans.enabled is false",
            ));
        }
        Ok(())
    }

    /// Capture a scan's page the loop's second inbox held, its copy checked again as it is now
    ///
    /// Everything the page reports is taken here: its boundary is this capture's, so a walk that
    /// raises its floor to it never steps back past what the copy held when it was read.
    ///
    /// # Arguments
    ///
    /// * `queued` - The page, as it was admitted
    pub(super) fn capture_queued(&mut self, queued: QueuedCapture) {
        // no longer waiting: from here it is in flight or answered
        self.walks.queued = self.walks.queued.saturating_sub(1);
        faults::count_capture(false);
        let QueuedCapture {
            ask,
            reply,
            admitted,
            permit,
        } = queued;
        if let Err(refusal) = self.capture_now(ask, &reply, admitted.elapsed(), permit) {
            let _ = reply.try_send(Err(refusal));
        }
    }

    /// Refuse a scan's page the loop's second inbox held, for a shard that is stopping
    ///
    /// # Arguments
    ///
    /// * `queued` - The page, as it was admitted
    pub(super) fn refuse_queued(&mut self, queued: QueuedCapture) {
        self.walks.queued = self.walks.queued.saturating_sub(1);
        faults::count_capture(false);
        let _ = queued.reply.try_send(Err(WalkRefusal::new(
            ErrorCode::Unavailable,
            "the shard stopped before the page was captured",
        )));
    }

    /// Check a page's copy as it is now and capture it, the checks admission made made again
    ///
    /// # Arguments
    ///
    /// * `ask` - What the page asks for
    /// * `reply` - Where its answer goes
    /// * `queued` - How long it waited for its capture
    /// * `permit` - Its bytes, drawn from the scan budget (F118)
    fn capture_now(
        &mut self,
        ask: WalkAsk,
        reply: &kanal::Sender<Result<WalkPage, WalkRefusal>>,
        queued: Duration,
        permit: Permit,
    ) -> Result<(), WalkRefusal> {
        // the table and the bounds, as admission found them
        let table = D::table_of_id(ask.table).ok_or_else(|| {
            WalkRefusal::new(
                ErrorCode::UnknownOperation,
                format!("this schema has no table with the identity {}", ask.table),
            )
        })?;
        let bounds = Self::walk_bounds(&ask)?;
        // still hosted here: a rehome or a move cannot have taken it while the page waited
        if self.walk_host(ask.tablet)? != self.shard_id {
            return Err(WalkRefusal::new(
                ErrorCode::StaleTopology,
                format!(
                    "tablet {} left executor {} while its page waited to be captured",
                    ask.tablet, self.shard_id
                ),
            ));
        }
        // the copy as it is now: an install or a quarantine begun since admission refuses it
        let copy = self.walk_copy(&ask)?;
        let Some(bounds) = bounds else {
            // admission answers a page with nothing to walk, so none waits, but say so if one did
            return Err(WalkRefusal::new(
                ErrorCode::Internal,
                "a page with nothing to walk waited to be captured",
            ));
        };
        self.read_page(ask, table, bounds, copy, reply, queued, vec![permit])
    }

    /// Capture a page and hand its reads to a task in its class's queue
    ///
    /// # Arguments
    ///
    /// * `ask` - What the page asks for
    /// * `table` - Its table
    /// * `bounds` - What it is captured by
    /// * `copy` - Its copy's group and installs, boundary and leadership, read with nothing
    ///   awaited since
    /// * `reply` - Where its answer goes
    /// * `queued` - How long it waited for its capture
    /// * `drawn` - What a scan's page drew from the scan budget, none for a driver's (F118)
    #[allow(clippy::too_many_arguments)]
    fn read_page(
        &mut self,
        ask: WalkAsk,
        table: D::TableNames,
        bounds: WalkBounds,
        copy: (Option<(GroupId, u64)>, Option<u64>, Option<bool>),
        reply: &kanal::Sender<Result<WalkPage, WalkRefusal>>,
        queued: Duration,
        mut drawn: Vec<Permit>,
    ) -> Result<(), WalkRefusal> {
        let (group, boundary, led) = copy;
        // the capture, which is the only part the loop waits on
        let started = Instant::now();
        let pending = self.tables.walk_tablet(table, &bounds).map_err(|error| {
            WalkRefusal::new(
                ErrorCode::Internal,
                format!("the page could not be captured: {error:?}"),
            )
        })?;
        let held = started.elapsed();
        // a scan page's capture past what it drew draws its excess now, or is refused, and its
        // reads draw what they read past both (F118)
        let draws = match ask.class {
            WalkClass::Driver => None,
            WalkClass::Scan => {
                let budget = self.scans.budget.clone();
                let captured = pending.held_bytes();
                let mut total: u64 = drawn.iter().map(Permit::bytes).sum();
                if captured > total {
                    let more = captured - total;
                    drawn.push(budget.try_draw(more).ok_or_else(|| scan_refusal(budget.note_shed(more)))?);
                    total += more;
                }
                Some(PageDraws::new(budget, total, captured))
            }
        };
        // the page is known by a number until its reads land
        let id = self.walks.next;
        self.walks.next += 1;
        self.walks.flight.insert(
            id,
            InFlight {
                ask,
                reply: reply.clone(),
                group,
                boundary,
                led,
                held,
                queued,
                drawn,
            },
        );
        // the reads, posting back to this loop: a driver's in the medium queue as they always
        // were, a scan's in the scan queue in steps that yield (F117)
        let loop_tx = self.shard_local_tx.clone();
        let (queue, steps): (TaskQueueHandle, Steps) = match ask.class {
            WalkClass::Driver => (self._medium_priority, Steps::unyielding()),
            WalkClass::Scan => (self.scan_priority, Steps::yielding()),
        };
        let (held_table, class) = (ask.table.0, ask.class);
        let spawned = glommio::spawn_local_into(
            async move {
                // a test may hold a class's reads, so the page stays in flight (F116)
                faults::wait_walk_reads(held_table, class).await;
                let started = Instant::now();
                let outcome = pending.finish(steps, draws).await;
                let _ = loop_tx
                    .send(ServerMsg::Walked {
                        call: id,
                        outcome,
                        read: started.elapsed(),
                    })
                    .await;
            },
            queue,
        );
        match spawned {
            Ok(task) => {
                task.detach();
                Ok(())
            }
            Err(error) => {
                self.walks.flight.remove(&id);
                Err(WalkRefusal::new(
                    ErrorCode::Internal,
                    format!("the page's reads could not be started: {error}"),
                ))
            }
        }
    }

    /// The bounds a page is captured by, once its range is checked against its tablet
    ///
    /// Returns none when the cursor is at the range's last key, which leaves nothing to walk.
    ///
    /// # Arguments
    ///
    /// * `ask` - What the page asks for
    fn walk_bounds(ask: &WalkAsk) -> Result<Option<WalkBounds>, WalkRefusal> {
        // a tablet id is twelve bits
        if usize::from(ask.tablet) >= TABLET_COUNT {
            return Err(WalkRefusal::new(
                ErrorCode::InvalidRequest,
                format!("there is no tablet {}", ask.tablet),
            ));
        }
        // the range is inside the tablet and not empty
        let (tablet_low, tablet_high) = tablet_range(ask.tablet);
        if ask.low < tablet_low || ask.high > tablet_high || ask.low > ask.high {
            return Err(WalkRefusal::new(
                ErrorCode::InvalidRequest,
                format!(
                    "{:016x}..={:016x} is not a range of tablet {}, which is {tablet_low:016x}..={tablet_high:016x}",
                    ask.low, ask.high, ask.tablet
                ),
            ));
        }
        // a page holds at least one partition and at most the most any page may
        if ask.limit == 0 || ask.limit > WALK_MAX_LIMIT {
            return Err(WalkRefusal::new(
                ErrorCode::InvalidRequest,
                format!("a page holds 1 to {WALK_MAX_LIMIT} partitions, not {}", ask.limit),
            ));
        }
        if ask.max_bytes == 0 || ask.max_bytes > WALK_MAX_BYTES {
            return Err(WalkRefusal::new(
                ErrorCode::InvalidRequest,
                format!("a page holds 1 to {WALK_MAX_BYTES} bytes, not {}", ask.max_bytes),
            ));
        }
        // the page starts past its cursor; a cursor at the last key leaves nothing to walk
        let start = match ask.after {
            Some(after) if after >= ask.high => return Ok(None),
            Some(after) => (after + 1).max(ask.low),
            None => ask.low,
        };
        Ok(Some(WalkBounds {
            tablet: ask.tablet,
            start,
            high: ask.high,
            limit: ask.limit as usize,
            max_bytes: page_bytes(ask),
        }))
    }

    /// The executor of this node hosting a tablet's copy
    ///
    /// # Arguments
    ///
    /// * `tablet` - The tablet
    fn walk_host(&self, tablet: u16) -> Result<usize, WalkRefusal> {
        // a standalone node deals its tablets to its executors itself
        let Some(local) = &self.local else {
            return Ok(self.hosting.owner_of_tablet(usize::from(tablet)));
        };
        let node: NodeId = local.borrow().node;
        // a cluster node hosts the copy the map names it for, a learner's included
        let map = self.map.get();
        let mine = map
            .replicas_of(usize::from(tablet))
            .into_iter()
            .chain(map.learner_of(usize::from(tablet)))
            .find(|addr| addr.node == node);
        match mine {
            Some(addr) => Ok(self.hosting.host_of_slot(addr.shard)),
            None => Err(WalkRefusal::new(
                ErrorCode::StaleTopology,
                format!("this node holds no copy of tablet {tablet}"),
            )),
        }
    }

    /// Check the copy a page reads against its group, and say where it is
    ///
    /// Returns the group and how many installs it had begun, the applied index, and whether the
    /// copy leads; all none on a standalone node, whose tables are their own.
    ///
    /// # Arguments
    ///
    /// * `ask` - What the page asks for
    #[allow(clippy::type_complexity)]
    fn walk_copy(
        &self,
        ask: &WalkAsk,
    ) -> Result<(Option<(GroupId, u64)>, Option<u64>, Option<bool>), WalkRefusal> {
        // a standalone node has no groups, and its tables are the whole of its state
        let Some(replication) = &self.replication else {
            return Ok((None, None, None));
        };
        let node = self
            .local
            .as_ref()
            .map_or(NodeId(Uuid::nil()), |local| local.borrow().node);
        // the group serving the tablet of the table on this shard
        let Some(group_id) = replication.tablets.get(&(ask.table, ask.tablet)).copied() else {
            return Err(WalkRefusal::new(
                ErrorCode::Unavailable,
                format!("this shard serves no group for tablet {} of the table yet", ask.tablet),
            ));
        };
        let Some(slot) = replication.groups.get(&group_id) else {
            return Err(WalkRefusal::new(
                ErrorCode::Unavailable,
                format!("group {group_id} is not up on this shard yet"),
            ));
        };
        // a copy starting or down has no state to read
        let Some(raft) = &slot.raft else {
            return Err(WalkRefusal::new(
                ErrorCode::Unavailable,
                format!("the copy of group {group_id} is starting"),
            ));
        };
        if slot.start_failed || slot.core_dead.is_some() {
            return Err(WalkRefusal::new(
                ErrorCode::Unavailable,
                format!("the copy of group {group_id} is down"),
            ));
        }
        let state = slot.state.borrow();
        // a copy being replaced by a snapshot is not whole
        if state.installing || state.pending_install.is_some() {
            return Err(WalkRefusal::new(
                ErrorCode::Unavailable,
                format!("the copy of group {group_id} is installing a snapshot"),
            ));
        }
        // a quarantined copy serves no read, and a walk is one
        if let Some(quarantine) = &state.quarantined {
            return Err(WalkRefusal::new(
                ErrorCode::Quarantined,
                format!("the copy of group {group_id} is quarantined: {quarantine:?}"),
            ));
        }
        // a copy behind the walk's floor would step back past what a page already saw
        let applied = state.applied_index();
        if let Some(at_least) = ask.at_least {
            if applied < at_least {
                return Err(WalkRefusal {
                    code: ErrorCode::Unavailable,
                    msg: format!(
                        "the copy of group {group_id} has applied {applied}, behind the walk's floor of {at_least}"
                    ),
                    applied: Some(applied),
                });
            }
        }
        // whether it leads, a handle that was shut down not counting (Resolved #197)
        let led = {
            let watch = raft.metrics();
            let metrics = watch.borrow_watched();
            metrics.running_state.is_ok() && metrics.current_leader == Some(slot.spec.me(node))
        };
        Ok((Some((group_id, slot.installs.get())), Some(applied), Some(led)))
    }

    /// Answer a page whose reads have landed, once its copy is checked again
    ///
    /// # Arguments
    ///
    /// * `id` - The page's number
    /// * `outcome` - What its reads came to
    /// * `read` - How long they took
    #[instrument(name = "Shard::handle_walked", skip(self, outcome), err(Debug))]
    pub(super) async fn handle_walked(
        &mut self,
        id: u64,
        outcome: Result<WalkRows, ServerError>,
        read: Duration,
    ) -> Result<(), ServerError> {
        // a page nobody is waiting on any more has nothing to answer
        let Some(flight) = self.walks.flight.remove(&id) else {
            return Ok(());
        };
        let answer = match outcome {
            // a scan page whose reads found no room in the scan budget is shed by it (F118)
            Err(ServerError::Shoal(ShoalError::ScanBudget { needed, limit })) => {
                Err(scan_refusal(Shed { needed, limit }))
            }
            // a record that failed its checksum quarantines its copy, as a read that met it would
            Err(ServerError::Shoal(ShoalError::CorruptArchive {
                archive,
                partition_id,
                ..
            })) => {
                if let Some(table) = D::table_of_id(flight.ask.table) {
                    self.quarantine_partition(
                        table,
                        partition_id,
                        crate::server::control::repair::QuarantineReason::Checksum,
                    )
                    .await;
                }
                Err(WalkRefusal::new(
                    ErrorCode::CorruptArchive,
                    format!("partition {partition_id:016x} failed its checksum in archive {archive}"),
                ))
            }
            Err(error) => Err(WalkRefusal::new(
                ErrorCode::StorageRead,
                format!("the page's reads failed: {error:?}"),
            )),
            Ok(rows) => Ok(rows),
        };
        event!(
            Level::DEBUG,
            msg = "a page of a walk was read",
            tablet = flight.ask.tablet,
            held_us = flight.held.as_micros(),
            read_us = read.as_micros(),
            ok = answer.is_ok()
        );
        // the page, its draws handed to it so they drop with its bytes (F118)
        let reply = flight.reply.clone();
        let answer = answer.and_then(|rows| self.walked_page(flight, rows, read));
        let _ = reply.try_send(answer);
        Ok(())
    }

    /// The page a walk hands out, if its copy is still the one it began on
    ///
    /// # Arguments
    ///
    /// * `flight` - The page, as it began
    /// * `rows` - What its reads came to
    /// * `read` - How long they took
    fn walked_page(
        &self,
        flight: InFlight,
        rows: WalkRows,
        read: Duration,
    ) -> Result<WalkPage, WalkRefusal> {
        // the copy has to be whole still, and to have begun no install while the page read
        if let Some((group_id, installs)) = flight.group {
            let changed = match self
                .replication
                .as_ref()
                .and_then(|replication| replication.groups.get(&group_id))
            {
                Some(slot) => {
                    let state = slot.state.borrow();
                    slot.installs.get() != installs
                        || state.installing
                        || state.quarantined.is_some()
                }
                None => true,
            };
            if changed {
                return Err(WalkRefusal::new(
                    ErrorCode::Unavailable,
                    format!(
                        "the copy of group {group_id} was replaced or quarantined while the page read; ask again"
                    ),
                ));
            }
        }
        Ok(WalkPage {
            table: flight.ask.table,
            tablet: flight.ask.tablet,
            ask: flight.ask,
            partitions: rows.partitions,
            bytes: rows.bytes,
            boundary: flight.boundary,
            led: flight.led,
            next: rows.next,
            group: flight.group.map(|(group, _)| group),
            resident: rows.resident,
            archived: rows.archived,
            reprobed: rows.reprobed,
            held: flight.held,
            read,
            queued: flight.queued,
            drawn: flight.drawn.into_iter().chain(rows.drawn).collect(),
        })
    }

    /// Answer a `Walk` admin read from the copy this node holds
    ///
    /// On a cluster only an admin may ask, as for a mutation, since a walk reads a table whole and
    /// a bucket's generated tables are walked like any other; a standalone node, which names no
    /// admins, answers only a connection that is not authenticated.
    ///
    /// # Arguments
    ///
    /// * `tx` - The client's relay
    /// * `id` - The id the request was sent under
    /// * `principal` - Who the connection authenticated as, if it did
    /// * `request` - The request, a `Walk`
    pub(super) fn admin_walk(
        &mut self,
        tx: AsyncSender<Reply>,
        id: Uuid,
        principal: Option<String>,
        request: &AdminRequest,
    ) {
        let map = self.map.get();
        let node = self
            .local
            .as_ref()
            .map_or(NodeId(Uuid::nil()), |local| local.borrow().node);
        let topology_version = map.version;
        // a refusal decided here, before any page is asked
        let refuse = |error: AdminError| AdminResponse {
            node,
            topology_version,
            outcome: Err(error),
        };
        // who may walk: a cluster's admins, or a standalone node's operator
        let allowed = if self.control.is_some() {
            principal
                .as_ref()
                .is_some_and(|principal| map.admins.contains(principal))
        } else {
            principal.is_none()
        };
        if !allowed {
            let msg = if self.control.is_some() {
                format!(
                    "{} may not walk a table; cluster.admins names {:?}",
                    principal
                        .as_deref()
                        .unwrap_or("an unauthenticated connection"),
                    map.admins
                )
            } else {
                format!(
                    "{} may not walk a table: a standalone node names no admins, and answers a walk only where it requires no authentication",
                    principal.as_deref().unwrap_or("this connection")
                )
            };
            let answer = refuse(AdminError::new(ErrorCode::Unauthorized, msg));
            self.answer_admin(&tx, id, &answer);
            return;
        }
        // the page the request names
        let Some(ask) = WalkAsk::of_admin(&request.kind) else {
            return;
        };
        let (reply, rx) = kanal::bounded(1);
        match ask.class {
            // a driver's or an operator's page, handed to the loop now, as every page was
            WalkClass::Driver => self.handle_walk(WalkCall {
                ask,
                routed: false,
                reply,
                permit: None,
            }),
            // a scan's page on a node answering none is refused by name before it draws (F116);
            // otherwise it draws its bytes first, waiting up to a quarter second off the loop, and
            // reaches the loop only once it holds them (F118)
            WalkClass::Scan => match self.answers_scans(&ask) {
                Err(refusal) => {
                    let _ = reply.try_send(Err(refusal));
                }
                Ok(()) => {
                    let budget = self.scans.budget.clone();
                    let loop_tx = self.shard_local_tx.clone();
                    glommio::spawn_local(async move {
                        match budget.draw(page_bytes(&ask), DRAW_WAIT).await {
                            Ok(permit) => {
                                let call = WalkCall {
                                    ask,
                                    routed: false,
                                    reply: reply.clone(),
                                    permit: Some(permit),
                                };
                                if loop_tx.send(ServerMsg::Walk(Box::new(call))).await.is_err() {
                                    let _ = reply.try_send(Err(WalkRefusal::new(
                                        ErrorCode::Unavailable,
                                        "the shard's loop is gone",
                                    )));
                                }
                            }
                            Err(shed) => {
                                let _ = reply.try_send(Err(scan_refusal(shed)));
                            }
                        }
                    })
                    .detach();
                }
            },
        }
        // a task waits for the page so this shard keeps serving meanwhile
        glommio::spawn_local(async move {
            // the receive is kept, never raced bare (Resolved #152)
            let mut page = KeptReceiver::new(rx.to_async());
            let answered =
                glommio::timer::timeout(ADMIN_TIMEOUT, async { Ok(page.next().await) }).await;
            let outcome = match answered {
                Ok(Ok(Ok(page))) => Ok(AdminOutcome::Read(
                    serde_json::to_value(page.answer::<D>()).unwrap_or_default(),
                )),
                Ok(Ok(Err(refusal))) => Err(AdminError::from(refusal)),
                _ => Err(AdminError::new(
                    ErrorCode::Timeout,
                    "the shard hosting the tablet did not answer within the deadline",
                )),
            };
            let answer = AdminResponse {
                node,
                topology_version,
                outcome,
            };
            match serde_json::to_vec(&answer) {
                Ok(json) => {
                    let _ = tx.send(control_reply(id, ReplyKind::Admin, &json)).await;
                }
                Err(error) => event!(Level::ERROR, msg = "a walk's answer did not encode", ?error),
            }
        })
        .detach();
    }
}

#[cfg(test)]
mod tests {
    use super::{page_bytes, WalkAsk};
    use crate::shared::identity::TableId;
    use crate::shared::protocol::admin::{
        AdminKind, WalkClass, SCAN_MAX_BYTES, WALK_DEFAULT_BYTES, WALK_MAX_BYTES,
    };

    /// A scan's page holds at most a scan's bytes whatever it asks, and a driver's what it asks
    /// (F116)
    #[test]
    fn a_scan_page_holds_at_most_its_bytes() {
        let mut ask = WalkAsk::tablet(TableId::of("Item"), 7);
        ask.max_bytes = WALK_MAX_BYTES;
        assert_eq!(page_bytes(&ask), WALK_MAX_BYTES);
        assert_eq!(page_bytes(&ask.as_scan()), SCAN_MAX_BYTES);
        // a scan's page asking less than the cap holds what it asked
        ask.max_bytes = WALK_DEFAULT_BYTES;
        assert_eq!(page_bytes(&ask.as_scan()), WALK_DEFAULT_BYTES);
    }

    /// An admin walk that names no class is a driver's, and one naming a scan's is a scan's (F116)
    #[test]
    fn an_admin_walk_without_a_class_is_a_drivers() {
        let walk = |class| AdminKind::Walk {
            table: "Item".to_string(),
            tablet: 3,
            low: None,
            high: None,
            after: None,
            limit: None,
            bytes: None,
            at_least: None,
            class,
        };
        let bare = WalkAsk::of_admin(&walk(None)).expect("a walk names a page");
        assert_eq!(bare.class, WalkClass::Driver);
        assert_eq!(bare, WalkAsk::tablet(TableId::of("Item"), 3));
        let scan = WalkAsk::of_admin(&walk(Some(WalkClass::Scan))).expect("a walk names a page");
        assert_eq!(scan, WalkAsk::tablet(TableId::of("Item"), 3).as_scan());
    }
}
