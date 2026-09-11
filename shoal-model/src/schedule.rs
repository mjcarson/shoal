//! Schedules: generated from a seed, written by hand, saved as JSON, and replayed
//!
//! A schedule is an explicit list of events plus the topology and policy it runs under. The
//! seed and weights that produced a generated one are provenance, not inputs to replay: replay
//! reads only the events, so a change to the generator never invalidates a saved file. That is
//! what lets `saved_protocol_schedule_reproduces_failure` promise the same violation forever.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::event::{Actor, ClientOp, Event, MutationOp};
use crate::ids::{Attempt, Key, NodeId, OpId, TabletId, Value};
use crate::invariants::Violation;
use crate::policy::Policy;
use crate::rng::SplitMix64;
use crate::world::{Enabled, World};

/// The current schedule file format; a floor, so an older reader refuses a newer file
pub const FORMAT: u32 = 1;

/// How often the generator picks each category, relative to the others
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Weights {
    /// Deliver a message in flight
    pub deliver: u32,
    /// Drop a message in flight
    pub drop: u32,
    /// Deliver a recent message again
    pub duplicate: u32,
    /// Complete a pending fsync
    pub fsync: u32,
    /// Fire an election timer
    pub election: u32,
    /// Fire a heartbeat timer
    pub heartbeat: u32,
    /// Send a progress report
    pub report: u32,
    /// Send a new client mutation
    pub client: u32,
    /// Send a client read
    pub read: u32,
    /// Retry an unknown attempt
    pub retry: u32,
    /// Give up on a pending attempt
    pub timeout: u32,
    /// Crash a node
    pub crash: u32,
    /// Restart a node
    pub restart: u32,
    /// Pause a node
    pub pause: u32,
    /// Resume a node
    pub resume: u32,
    /// Take a checkpoint
    pub checkpoint: u32,
    /// Mark a node down
    pub mark_down: u32,
}

impl Default for Weights {
    /// A mix that keeps the cluster making progress while everything still happens
    fn default() -> Self {
        Self {
            deliver: 40,
            drop: 3,
            duplicate: 4,
            fsync: 20,
            election: 3,
            heartbeat: 8,
            report: 3,
            client: 10,
            read: 6,
            retry: 4,
            timeout: 3,
            crash: 2,
            restart: 4,
            pause: 1,
            resume: 3,
            checkpoint: 2,
            mark_down: 2,
        }
    }
}

/// The topology and the generator's limits
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleParams {
    /// How many nodes, numbered from one
    pub nodes: u8,
    /// Which of them are learners rather than voters
    #[serde(default)]
    pub learners: Vec<NodeId>,
    /// Which of them acknowledge before fsync, like an `Async` disk
    #[serde(default)]
    pub async_nodes: Vec<NodeId>,
    /// The tablets every node replicates
    pub tablets: Vec<TabletId>,
    /// How many events to generate
    pub steps: u32,
    /// How many client mutations to generate
    pub ops: u32,
    /// How many distinct keys they touch
    pub keys: u8,
    /// How many times an unknown attempt may be retried
    pub max_retries: u8,
    /// How many nodes may be crashed or paused at once
    pub max_down: u8,
    /// How many recent messages are candidates for duplication
    pub dup_window: u32,
    /// The category weights
    pub weights: Weights,
}

impl ScheduleParams {
    /// Three voters, one tablet, a few hundred steps
    pub fn default_small() -> Self {
        Self {
            nodes: 3,
            learners: Vec::new(),
            async_nodes: Vec::new(),
            tablets: vec![TabletId::new(1, 0)],
            steps: 400,
            ops: 48,
            keys: 6,
            max_retries: 1,
            max_down: 1,
            dup_window: 8,
            weights: Weights::default(),
        }
    }
}

/// A saved schedule
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schedule {
    /// The file format
    pub format: u32,
    /// What it is called
    pub name: String,
    /// The seed that generated it; zero for one written by hand
    pub seed: u64,
    /// The topology and limits
    pub params: ScheduleParams,
    /// The policy it runs under
    pub policy: Policy,
    /// The events, in order
    pub events: Vec<Event>,
    /// The violation replaying it is expected to find, if any
    pub expected: Option<Violation>,
}

impl Schedule {
    /// The canonical JSON form
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("a schedule serializes")
    }

    /// Read the JSON form
    ///
    /// # Arguments
    ///
    /// * `json` - The text
    pub fn from_json(json: &str) -> Result<Schedule, serde_json::Error> {
        let schedule: Schedule = serde_json::from_str(json)?;
        // a newer format would mean fields this reader does not know
        assert!(
            schedule.format <= FORMAT,
            "schedule {} is format {}, newer than {FORMAT}",
            schedule.name,
            schedule.format
        );
        Ok(schedule)
    }

    /// Where the saved schedules live
    pub fn dir() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("schedules")
    }

    /// Load one saved schedule
    ///
    /// # Arguments
    ///
    /// * `path` - The file
    pub fn load(path: &Path) -> Schedule {
        let text = std::fs::read_to_string(path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
        Self::from_json(&text)
            .unwrap_or_else(|error| panic!("failed to parse {}: {error}", path.display()))
    }

    /// Write this schedule
    ///
    /// # Arguments
    ///
    /// * `path` - The file
    pub fn save(&self, path: &Path) {
        std::fs::write(path, self.to_json() + "\n")
            .unwrap_or_else(|error| panic!("failed to write {}: {error}", path.display()));
    }

    /// Every saved schedule, by file name
    pub fn load_all() -> Vec<(PathBuf, Schedule)> {
        let mut paths: Vec<PathBuf> = std::fs::read_dir(Self::dir())
            .expect("the schedules directory exists")
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
            .collect();
        paths.sort();
        paths
            .into_iter()
            .map(|path| {
                let schedule = Self::load(&path);
                (path, schedule)
            })
            .collect()
    }
}

/// Generate a schedule from a seed
///
/// A random walk: at every step the world says what could happen, a category is picked by
/// weight among those with a candidate, and a candidate within it by the seed. Each concrete
/// event is applied as it is recorded, so the list is replayable without the seed. A drop
/// records nothing: it is the absence of a delivery.
///
/// # Arguments
///
/// * `name` - What to call it
/// * `seed` - The seed
/// * `params` - The topology and limits
/// * `policy` - The policy
pub fn generate(name: &str, seed: u64, params: &ScheduleParams, policy: Policy) -> Schedule {
    let mut rng = SplitMix64::new(seed);
    let mut world = World::new(params, policy);
    let mut events = Vec::new();
    let mut next_op = 0u32;
    for _ in 0..params.steps {
        let enabled = world.enabled();
        // the categories with something to pick, and their weights
        let w = &params.weights;
        let has_client = next_op < params.ops && !enabled.up.is_empty();
        let candidates: Vec<(Category, u32)> = [
            (Category::Deliver, w.deliver, !enabled.deliver.is_empty()),
            (Category::Drop, w.drop, !enabled.deliver.is_empty()),
            (Category::Duplicate, w.duplicate, !enabled.duplicate.is_empty()),
            (Category::Fsync, w.fsync, !enabled.fsync.is_empty()),
            (Category::Election, w.election, !enabled.election.is_empty()),
            (Category::Heartbeat, w.heartbeat, !enabled.heartbeat.is_empty()),
            (Category::Report, w.report, !enabled.report.is_empty()),
            (Category::Client, w.client, has_client),
            (Category::Read, w.read, !enabled.up.is_empty()),
            (Category::Retry, w.retry, !enabled.retry.is_empty()),
            (Category::Timeout, w.timeout, !enabled.timeout.is_empty()),
            (Category::Crash, w.crash, !enabled.crash.is_empty()),
            (Category::Restart, w.restart, !enabled.restart.is_empty()),
            (Category::Pause, w.pause, !enabled.pause.is_empty()),
            (Category::Resume, w.resume, !enabled.resume.is_empty()),
            (Category::Checkpoint, w.checkpoint, !enabled.checkpoint.is_empty()),
            (Category::MarkDown, w.mark_down, !enabled.mark_down.is_empty()),
        ]
        .into_iter()
        .filter(|(_, weight, possible)| *possible && *weight > 0)
        .map(|(category, weight, _)| (category, weight))
        .collect();
        if candidates.is_empty() {
            break;
        }
        let weights: Vec<u32> = candidates.iter().map(|(_, weight)| *weight).collect();
        let category = candidates[rng.weighted(&weights)].0;
        // a candidate within the category
        let pick = |rng: &mut SplitMix64, events: &[Event]| events[rng.below(events.len() as u64) as usize].clone();
        let event = match category {
            Category::Deliver => pick(&mut rng, &enabled.deliver),
            Category::Drop => {
                // dropped, and nothing recorded
                if let Event::Deliver { msg } = pick(&mut rng, &enabled.deliver) {
                    world.net.drop_message(&msg);
                }
                continue;
            }
            Category::Duplicate => pick(&mut rng, &enabled.duplicate),
            Category::Fsync => pick(&mut rng, &enabled.fsync),
            Category::Election => pick(&mut rng, &enabled.election),
            Category::Heartbeat => pick(&mut rng, &enabled.heartbeat),
            Category::Report => pick(&mut rng, &enabled.report),
            Category::Client => {
                let event = client_op(&mut rng, params, &enabled, next_op);
                next_op += 1;
                event
            }
            Category::Read => {
                let event = read_op(&mut rng, params, &enabled, next_op);
                next_op += 1;
                event
            }
            Category::Retry => {
                let attempt = enabled.retry[rng.below(enabled.retry.len() as u64) as usize];
                let Some((tablet, op)) = world.ledger.op_of(attempt.id) else {
                    continue;
                };
                let target = enabled.up[rng.below(enabled.up.len() as u64) as usize];
                Event::ClientInvoke {
                    attempt,
                    tablet,
                    op,
                    target,
                }
            }
            Category::Timeout => pick(&mut rng, &enabled.timeout),
            Category::Crash => pick(&mut rng, &enabled.crash),
            Category::Restart => pick(&mut rng, &enabled.restart),
            Category::Pause => pick(&mut rng, &enabled.pause),
            Category::Resume => pick(&mut rng, &enabled.resume),
            Category::Checkpoint => pick(&mut rng, &enabled.checkpoint),
            Category::MarkDown => pick(&mut rng, &enabled.mark_down),
        };
        let violation = world.apply(&event);
        events.push(event);
        if violation.is_some() {
            break;
        }
    }
    Schedule {
        format: FORMAT,
        name: name.to_string(),
        seed,
        params: params.clone(),
        policy,
        events,
        expected: world.violation.clone(),
    }
}

/// The generator's categories
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Category {
    Deliver,
    Drop,
    Duplicate,
    Fsync,
    Election,
    Heartbeat,
    Report,
    Client,
    Read,
    Retry,
    Timeout,
    Crash,
    Restart,
    Pause,
    Resume,
    Checkpoint,
    MarkDown,
}

/// A fresh client mutation: a random kind on a random key, with a value unique to the op
fn client_op(rng: &mut SplitMix64, params: &ScheduleParams, enabled: &Enabled, op: u32) -> Event {
    let key = Key(rng.below(u64::from(params.keys)) as u8);
    // the value names the op, so a read that sees it says which op it saw
    let value = Value(1000 + op);
    let mutation = match rng.below(4) {
        0 => MutationOp::Insert { key, value },
        1 => MutationOp::Update { key, value },
        2 => MutationOp::Delete { key },
        _ => MutationOp::Cas {
            key,
            expected: if rng.below(2) == 0 { None } else { Some(Value(1000 + rng.below(u64::from(op.max(1))) as u32)) },
            value,
        },
    };
    let tablet = params.tablets[rng.below(params.tablets.len() as u64) as usize];
    let target = enabled.up[rng.below(enabled.up.len() as u64) as usize];
    Event::ClientInvoke {
        attempt: Attempt {
            id: OpId(op),
            retry: 0,
        },
        tablet,
        op: ClientOp::Mutate(mutation),
        target,
    }
}

/// A client read of a random key from a random up node
fn read_op(rng: &mut SplitMix64, params: &ScheduleParams, enabled: &Enabled, op: u32) -> Event {
    let key = Key(rng.below(u64::from(params.keys)) as u8);
    let tablet = params.tablets[rng.below(params.tablets.len() as u64) as usize];
    let target = enabled.up[rng.below(enabled.up.len() as u64) as usize];
    Event::ClientInvoke {
        attempt: Attempt {
            id: OpId(op),
            retry: 0,
        },
        tablet,
        op: ClientOp::Read { key },
        target,
    }
}

/// Writes a schedule by hand, one concrete event at a time
///
/// Each event is applied as it is recorded, so the builder can see what is in flight and
/// deliver or drop it by name. The result is an ordinary explicit event list.
pub struct Builder {
    /// The world so far
    world: World,
    /// The events so far
    events: Vec<Event>,
    /// What the schedule is called
    name: String,
    /// How many client identities have been used
    next_op: u32,
}

/// How many deliveries a `deliver_all` will make before giving up on a chattering cluster
const DELIVER_ALL_BOUND: usize = 100_000;

impl Builder {
    /// Start a schedule
    ///
    /// # Arguments
    ///
    /// * `name` - What to call it
    /// * `params` - The topology
    /// * `policy` - The policy
    pub fn new(name: &str, params: &ScheduleParams, policy: Policy) -> Self {
        Self {
            world: World::new(params, policy),
            events: Vec::new(),
            name: name.to_string(),
            next_op: 0,
        }
    }

    /// The world as it stands
    pub fn world(&self) -> &World {
        &self.world
    }

    /// Record and apply one event
    ///
    /// # Arguments
    ///
    /// * `event` - The event
    pub fn event(&mut self, event: Event) -> &mut Self {
        self.world.apply(&event);
        self.events.push(event);
        self
    }

    /// Deliver everything in flight, in send order, until nothing is
    pub fn deliver_all(&mut self) -> &mut Self {
        let mut delivered = 0;
        // a frozen world delivers nothing, so there is nothing to wait for
        while self.world.violation.is_none() {
            let Some(msg) = self.world.net.in_flight.first().cloned() else {
                break;
            };
            self.event(Event::Deliver { msg });
            delivered += 1;
            assert!(delivered < DELIVER_ALL_BOUND, "the cluster never went quiet");
        }
        self
    }

    /// Deliver the oldest message in flight from one actor to another
    ///
    /// # Arguments
    ///
    /// * `from` - The sender
    /// * `to` - The receiver
    pub fn deliver_from_to(&mut self, from: Actor, to: Actor) -> &mut Self {
        let found = self
            .world
            .net
            .in_flight
            .iter()
            .find(|msg| msg.from == from && msg.to == to)
            .cloned();
        if let Some(msg) = found {
            self.event(Event::Deliver { msg });
        }
        self
    }

    /// Drop every message in flight from one actor to another, recording nothing
    ///
    /// # Arguments
    ///
    /// * `from` - The sender
    /// * `to` - The receiver
    pub fn drop_from_to(&mut self, from: Actor, to: Actor) -> &mut Self {
        self.world
            .net
            .in_flight
            .retain(|msg| !(msg.from == from && msg.to == to));
        self
    }

    /// Deliver the most recent message from one actor to another again
    ///
    /// # Arguments
    ///
    /// * `from` - The sender
    /// * `to` - The receiver
    pub fn redeliver_last(&mut self, from: Actor, to: Actor) -> &mut Self {
        let found = self
            .world
            .net
            .sent
            .iter()
            .rev()
            .find(|msg| msg.from == from && msg.to == to)
            .cloned();
        if let Some(msg) = found {
            self.event(Event::Deliver { msg });
        }
        self
    }

    /// Complete every pending fsync on one node's tablet
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    /// * `tablet` - The tablet
    pub fn fsync(&mut self, node: NodeId, tablet: TabletId) -> &mut Self {
        while self.world.violation.is_none()
            && !self.world.group(node, tablet).volatile.pending_fsync.is_empty()
        {
            self.event(Event::StorageComplete { node, tablet });
        }
        self
    }

    /// Complete every pending fsync everywhere
    pub fn fsync_all(&mut self) -> &mut Self {
        let nodes = self.world.node_ids();
        let tablets = self.world.params.tablets.clone();
        for node in nodes {
            for tablet in &tablets {
                self.fsync(node, *tablet);
            }
        }
        self
    }

    /// Elect a node: fire its timer, deliver the votes, replicate its noop everywhere
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    /// * `tablet` - The tablet
    pub fn elect(&mut self, node: NodeId, tablet: TabletId) -> &mut Self {
        self.event(Event::ElectionTimeout { node, tablet });
        self.replicate_fully(node, tablet)
    }

    /// Send a mutation from a client to a node
    ///
    /// # Arguments
    ///
    /// * `op` - The mutation
    /// * `tablet` - The tablet
    /// * `target` - The node
    pub fn write(&mut self, op: MutationOp, tablet: TabletId, target: NodeId) -> &mut Self {
        let attempt = Attempt {
            id: OpId(self.next_op),
            retry: 0,
        };
        self.next_op += 1;
        self.event(Event::ClientInvoke {
            attempt,
            tablet,
            op: ClientOp::Mutate(op),
            target,
        })
    }

    /// Bring every replica level with a leader: deliver, fsync, deliver, heartbeat, deliver
    ///
    /// # Arguments
    ///
    /// * `leader` - The leader
    /// * `tablet` - The tablet
    pub fn replicate_fully(&mut self, leader: NodeId, tablet: TabletId) -> &mut Self {
        self.deliver_all();
        self.fsync_all();
        self.deliver_all();
        self.event(Event::HeartbeatTick {
            node: leader,
            tablet,
        });
        self.deliver_all();
        self.fsync_all();
        self.deliver_all()
    }

    /// Finish the schedule
    pub fn finish(self) -> Schedule {
        Schedule {
            format: FORMAT,
            name: self.name,
            seed: 0,
            params: self.world.params.clone(),
            policy: self.world.policy,
            events: self.events,
            expected: self.world.violation.clone(),
        }
    }
}

/// The B=100/C=101/A+B=102 schedule from C7, at any prefix length
///
/// A leads; `prefix` writes reach everyone. B reports its index. The next write reaches C but
/// not B, and C reports. The write after that reaches B but not C and is committed by A and B,
/// and its client is told so. A crashes and is marked down. Under the unsafe election the
/// observer promotes C, whose log ends one short of what was acknowledged.
///
/// # Arguments
///
/// * `prefix` - How many writes precede the divergence; 99 gives the literal 100/101/102
/// * `policy` - The policy to run it under
pub fn stale_report_schedule(prefix: u64, policy: Policy) -> Schedule {
    let params = ScheduleParams {
        steps: 0,
        ops: 0,
        ..ScheduleParams::default_small()
    };
    let tablet = params.tablets[0];
    let (a, b, c) = (NodeId(1), NodeId(2), NodeId(3));
    let name = format!("stale_report_b{}_c{}_ab{}", prefix + 1, prefix + 2, prefix + 3);
    let mut builder = Builder::new(&name, &params, policy);
    let key = Key(1);
    let value = |n: u64| Value(n as u32);
    // A leads, and everyone reports so the observer has an up list
    builder.elect(a, tablet);
    for node in [a, b, c] {
        builder.event(Event::Report { node, tablet });
    }
    builder.deliver_all();
    // the shared prefix
    for n in 0..prefix {
        builder.write(MutationOp::Cas { key, expected: None, value: value(n) }, tablet, a);
        builder.replicate_fully(a, tablet);
    }
    // B says how far it is
    builder.event(Event::Report { node: b, tablet });
    builder.deliver_from_to(Actor::Node(b), Actor::Observer);
    // the next write reaches C and not B; C says how far it is
    builder.write(MutationOp::Cas { key, expected: None, value: value(prefix) }, tablet, a);
    builder.deliver_from_to(Actor::Node(a), Actor::Node(c));
    builder.drop_from_to(Actor::Node(a), Actor::Node(b));
    builder.fsync(c, tablet);
    builder.fsync(a, tablet);
    builder.deliver_from_to(Actor::Node(c), Actor::Node(a));
    builder.event(Event::Report { node: c, tablet });
    builder.deliver_from_to(Actor::Node(c), Actor::Observer);
    // the write after reaches B and not C, and A and B commit it
    builder.write(MutationOp::Cas { key, expected: None, value: value(prefix + 1) }, tablet, a);
    builder.deliver_from_to(Actor::Node(a), Actor::Node(b));
    builder.drop_from_to(Actor::Node(a), Actor::Node(c));
    builder.fsync(b, tablet);
    builder.fsync(a, tablet);
    builder.deliver_from_to(Actor::Node(b), Actor::Node(a));
    // A dies and is marked down; what the observer does next is the policy's. under the unsafe
    // election it promotes C here and the world freezes on the violation
    builder.event(Event::Crash { node: a });
    builder.event(Event::MarkDown { node: a });
    builder.deliver_all();
    // under the contract nothing has happened yet: C stands and is refused by B, whose log is
    // longer, then B stands and wins with C's vote, holding everything acknowledged
    builder.event(Event::ElectionTimeout { node: c, tablet });
    builder.deliver_all();
    builder.event(Event::ElectionTimeout { node: b, tablet });
    builder.deliver_all();
    builder.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The same seed generates the same events
    #[test]
    fn the_same_seed_generates_the_same_schedule() {
        let params = ScheduleParams::default_small();
        let a = generate("a", 3, &params, Policy::safe());
        let b = generate("a", 3, &params, Policy::safe());
        assert_eq!(a.events, b.events);
        assert!(!a.events.is_empty());
        let c = generate("a", 4, &params, Policy::safe());
        assert_ne!(a.events, c.events);
    }

    /// A schedule survives the trip through its file form
    #[test]
    fn a_schedule_round_trips_through_json() {
        let params = ScheduleParams::default_small();
        let schedule = generate("round", 1, &params, Policy::safe());
        let back = Schedule::from_json(&schedule.to_json()).unwrap();
        assert_eq!(back, schedule);
    }

    /// The generator's drops leave no event behind, so replay differs only by what it skips
    #[test]
    fn replaying_a_generated_schedule_skips_nothing() {
        let params = ScheduleParams::default_small();
        let schedule = generate("replay", 2, &params, Policy::safe());
        let outcome = World::replay(&schedule);
        assert_eq!(outcome.skipped, 0);
    }
}
