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
//! reads run in the shard's medium priority queue and post their outcome back.

use glommio::TaskQueueHandle;
use kanal::AsyncSender;
use openraft_rt::WatchReceiver as _;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{event, instrument, Level};
use uuid::Uuid;

use super::{control_reply, Shard, ADMIN_TIMEOUT};
use crate::server::database::ShoalDatabase;
use crate::server::errors::ShoalError;
use crate::server::messages::{Reply, ReplyKind, ServerMsg};
use crate::server::shard::ShardContact;
use crate::server::tables::walk::{WalkBounds, WalkRows, WalkedPartition};
use crate::server::ServerError;
use crate::shared::identity::{GroupId, NodeId, TableId};
use crate::shared::placement::{tablet_range, TABLET_COUNT};
use crate::shared::protocol::admin::{
    AdminError, AdminKind, AdminOutcome, AdminRequest, AdminResponse, WalkAnswer, WalkedStripe,
    WALK_DEFAULT_BYTES, WALK_DEFAULT_LIMIT, WALK_MAX_BYTES, WALK_MAX_LIMIT,
};
use crate::shared::protocol::error::ErrorCode;
use crate::shared::stripes::StripeRow;
use shoal_channel::KeptReceiver;

/// How many pages one shard reads at once before it sheds another
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
    /// What the page asked for
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
}

/// The pages this shard is reading
#[derive(Default)]
pub(super) struct Walks {
    /// The number the next page is known by
    next: u64,
    /// The pages being read, by number
    flight: HashMap<u64, InFlight>,
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
        if let Err(refusal) = self.start_walk(call.ask, call.routed, &call.reply) {
            let _ = call.reply.try_send(Err(refusal));
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
    fn start_walk(
        &mut self,
        ask: WalkAsk,
        routed: bool,
        reply: &kanal::Sender<Result<WalkPage, WalkRefusal>>,
    ) -> Result<(), WalkRefusal> {
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
            }));
            return Ok(());
        };
        // a shard reads a few pages at once and sheds the rest
        if self.walks.flight.len() >= PAGES_IN_FLIGHT {
            return Err(WalkRefusal::new(
                ErrorCode::Shedding,
                format!("this shard is reading {PAGES_IN_FLIGHT} pages already"),
            ));
        }
        // the capture, which is the only part the loop waits on
        let started = Instant::now();
        let pending = self.tables.walk_tablet(table, &bounds).map_err(|error| {
            WalkRefusal::new(
                ErrorCode::Internal,
                format!("the page could not be captured: {error:?}"),
            )
        })?;
        let held = started.elapsed();
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
            },
        );
        // the reads, in the queue background work runs in, posting back to this loop
        let loop_tx = self.shard_local_tx.clone();
        let queue: TaskQueueHandle = self._medium_priority;
        let spawned = glommio::spawn_local_into(
            async move {
                let started = Instant::now();
                let outcome = pending.finish().await;
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
            max_bytes: ask.max_bytes,
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
            Ok(rows) => self.walked_page(&flight, rows, read),
        };
        event!(
            Level::DEBUG,
            msg = "a page of a walk was read",
            tablet = flight.ask.tablet,
            held_us = flight.held.as_micros(),
            read_us = read.as_micros(),
            ok = answer.is_ok()
        );
        let _ = flight.reply.try_send(answer);
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
        flight: &InFlight,
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
        self.handle_walk(WalkCall {
            ask,
            routed: false,
            reply,
        });
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
