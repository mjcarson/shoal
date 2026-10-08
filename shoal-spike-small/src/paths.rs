//! The three paths one small write in place takes, each a worker of a cell
//!
//! - **row**: the write's bytes as a row of their own, overwritten through its group's leader.
//!   No read and no condition: the cheapest a write through the log can be.
//! - **staged**: S7's B. The stripe's row read at `Quorum` through its leader; the bytes staged to
//!   every holder at once and the write carried on once two of three answered; a commit of the
//!   small row, conditional on the sequence read, naming the write's tag in every copy's label and
//!   the holder that had not answered; then, once acknowledged, every holder's apply in place.
//! - **inline**: the same read, then the commit with the bytes inside the row; then, once
//!   acknowledged, every holder's fold of them in place.
//!
//! A worker owns [`crate::keys::KEYS_PER_WORKER`] keys and writes them in turn. Before it writes a
//! key again it waits for that key's last deferred work, so a holder applies one stripe's writes in
//! the order they committed, and before any write that touches the holders it takes a permit of
//! each holder's gate ([`crate::deferred`]). Both waits are timed apart from the write.
//!
//! The sequence a commit is conditional on is the one the read returned, and the worker checks it
//! against what it counted: they differ only if a commit's answer was lost after it applied, which
//! is counted as drift.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use shoal::client::SendOptions;
use shoal::shared::protocol::read::ReadLevel;
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit};
use tokio::task::JoinHandle;

use crate::cluster::{Client, Route};
use crate::deferred::Gate;
use crate::drive::{Answer, Op, Outcome, Window, Work};
use crate::keys::{row_hash, stripe_hash, Key};
use crate::lane::Pool;
use crate::shape::{self, COPIES, STRIPE_BYTES};
use crate::stats::{mix, Samples};
use crate::wire::{self, Kind, Request, StageHead, WireLabel};
use crate::StripeHead;

/// A way a write is made
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Path {
    /// The bytes as a row of their own
    Row,
    /// Staged to the holders, then a small commit
    Staged,
    /// Inside the commit
    Inline,
}

impl Path {
    /// Every path, in the order a round runs them before it reverses
    pub const ALL: [Path; 3] = [Path::Row, Path::Staged, Path::Inline];

    /// The path's name in a record
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Path::Row => "row",
            Path::Staged => "staged",
            Path::Inline => "inline",
        }
    }

    /// The path a name means
    ///
    /// # Arguments
    ///
    /// * `name` - The name
    #[must_use]
    pub fn from_name(name: &str) -> Option<Path> {
        Path::ALL.into_iter().find(|path| path.name() == name)
    }

    /// The path's place, which a cell's space is made from
    #[must_use]
    pub fn index(self) -> usize {
        match self {
            Path::Row => 0,
            Path::Staged => 1,
            Path::Inline => 2,
        }
    }

    /// Whether a write on this path leaves work with the holders
    #[must_use]
    pub fn touches_holders(self) -> bool {
        self != Path::Row
    }
}

/// What the work a write leaves behind did, which no write's latency includes
#[derive(Default)]
pub struct Behind {
    /// Each holder's stage latency, in the holders' order
    pub stage: Vec<Mutex<Samples>>,
    /// The latency of the third stage of a write: its slowest holder's
    pub third: Mutex<Samples>,
    /// Each apply's or fold's latency, from its request to its answer
    pub deferred: Mutex<Samples>,
    /// Stages, applies and folds that failed
    pub failed: AtomicU64,
    /// The first few of those failures, as they were said
    pub errors: Mutex<Vec<String>>,
    /// Reads whose sequence was not the one the worker counted
    pub drift: AtomicU64,
}

impl Behind {
    /// An empty record for this many holders
    ///
    /// # Arguments
    ///
    /// * `holders` - How many holders
    #[must_use]
    pub fn new(holders: usize) -> Arc<Behind> {
        Arc::new(Behind {
            stage: (0..holders).map(|_| Mutex::new(Samples::default())).collect(),
            ..Behind::default()
        })
    }

    /// Count a failure of work a write left behind
    ///
    /// # Arguments
    ///
    /// * `error` - What was said
    fn fail(&self, error: String) {
        self.failed.fetch_add(1, Ordering::Relaxed);
        let mut errors = self.errors.lock().expect("not poisoned");
        if errors.len() < 5 {
            errors.push(error);
        }
    }
}

/// What every worker of a cell shares
pub struct Shared {
    /// Every member's client, in the lab's member order
    pub clients: Arc<Vec<Client>>,
    /// Which member leads each tablet of the rows' table
    pub rows: Arc<Route>,
    /// Which member leads each tablet of the stripes' table
    pub stripes: Arc<Route>,
    /// Every holder's lanes, in the holders' order
    pub holders: Vec<Arc<Pool>>,
    /// The holders' gates
    pub gate: Arc<Gate>,
    /// When answers are counted
    pub window: Window,
    /// What work left behind did
    pub behind: Arc<Behind>,
    /// The write's size in bytes
    pub size: usize,
}

/// What a key's last applied write put where, so a cell can read it back
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Last {
    /// Where in the chunk's units it landed
    offset: u32,
    /// The seed its bytes were made from
    seed: u64,
    /// The stream they are named by
    object: u64,
}

/// One key's state, which its worker carries from one write of it to the next
pub struct KeyState {
    /// The key
    key: Key,
    /// The sequence the worker counted for its stripe
    sequence: u64,
    /// The tag of the stripe's last committed write
    tag: u64,
    /// The work its last write left behind, one task a holder
    behind: Vec<JoinHandle<()>>,
    /// Its last applied write
    last: Option<Last>,
}

/// Every key of a cell, each locked by the one write of it in flight
pub type Keys = Vec<Arc<tokio::sync::Mutex<KeyState>>>;

/// A worker writing its keys in turn by one path
pub struct Writer {
    /// The path
    path: Path,
    /// The worker's place in the cell
    worker: usize,
    /// Its keys and their state, each locked by the one write of it in flight
    keys: Keys,
    /// How many writes it has started
    started: u64,
    /// What every worker shares
    shared: Arc<Shared>,
}

impl Writer {
    /// A worker over its keys
    ///
    /// # Arguments
    ///
    /// * `path` - The path it writes by
    /// * `worker` - Its place in the cell
    /// * `keys` - Its keys' states
    /// * `shared` - What every worker of the cell shares
    #[must_use]
    pub fn new(path: Path, worker: usize, keys: Keys, shared: Arc<Shared>) -> Box<dyn Work> {
        Box::new(Writer {
            path,
            worker,
            keys,
            started: 0,
            shared,
        })
    }
}

impl Work for Writer {
    /// The next write: the next key in turn, by the worker's path
    fn next(&mut self, _last: Option<&Outcome>) -> Option<Op> {
        // the key in turn, and the write's identity: its worker, its number and its try
        let state = self.keys[(self.started % self.keys.len() as u64) as usize].clone();
        let tag = mix(((self.worker as u64) << 48) ^ (self.started << 4) ^ 1);
        self.started += 1;
        let shared = self.shared.clone();
        Some(match self.path {
            Path::Row => Box::pin(row(shared, state, tag)),
            Path::Staged => Box::pin(staged(shared, state, tag)),
            Path::Inline => Box::pin(inline(shared, state, tag)),
        })
    }
}

/// The seed a write's bytes are made from, from its tag
///
/// # Arguments
///
/// * `tag` - The write's tag
fn seed_of(tag: u64) -> u64 {
    mix(tag ^ 0x5EED_0008)
}

/// The stream a key's bytes are named by, on every path and every holder alike
///
/// # Arguments
///
/// * `key` - The key
fn object_of(key: &Key) -> u64 {
    key.stripe.2 ^ (key.stripe.1 as u64) ^ u64::from(key.slot)
}

/// Where in a stripe's chunk a write at a sequence lands: a slot of the write's size, moving
///
/// # Arguments
///
/// * `sequence` - The sequence the write makes
/// * `size` - Its size
fn offset_of(sequence: u64, size: usize) -> u32 {
    // the chunk cut into slots of the write's size, one after another by sequence
    let slots = (STRIPE_BYTES / size as u64).max(1);
    u32::try_from((sequence % slots) * size as u64).expect("an offset fits")
}

/// A write by the first path: its bytes as a row of their own, overwritten
///
/// # Arguments
///
/// * `shared` - What the cell's workers share
/// * `state` - The key's state
/// * `tag` - The write's tag
async fn row(shared: Arc<Shared>, state: Arc<tokio::sync::Mutex<KeyState>>, tag: u64) -> Outcome {
    let mut state = state.lock_owned().await;
    let key = state.key.row;
    // the row made before the clock starts, as a client's bytes are in hand before it sends
    let seed = seed_of(tag);
    let row = shape::row(key, seed, shared.size);
    let leader = shared.clients[shared.rows.leader_of_hash(row_hash(key))].clone();
    let started = Instant::now();
    let sent = leader.send_one(row).await;
    let outcome = Outcome::whole(Answer::of(&sent), started.elapsed(), Duration::ZERO);
    if matches!(outcome.answer, Answer::Applied) {
        state.last = Some(Last {
            offset: 0,
            seed,
            object: key,
        });
    }
    outcome
}

/// Wait for a key's last deferred work and take a permit of every holder's gate
///
/// # Arguments
///
/// * `shared` - What the cell's workers share
/// * `state` - The key's state
///
/// Returns the permits and how long the waiting took.
async fn wait_turn(shared: &Shared, state: &mut KeyState) -> (Vec<OwnedSemaphorePermit>, Duration) {
    let started = Instant::now();
    // the key's last write's applies or folds, so one stripe's writes land in commit order
    for task in state.behind.drain(..) {
        let _ = task.await;
    }
    let (permits, _) = shared.gate.enter().await;
    (permits, started.elapsed())
}

/// Read a stripe's row at `Quorum` through its leader, as S7's coordinator does before it stages
///
/// # Arguments
///
/// * `leader` - The leader's client
/// * `state` - The key's state, whose counted sequence the read is checked against
/// * `behind` - Where drift is counted
///
/// # Errors
///
/// When the read fails or finds no row, as what was said.
async fn read_sequence(leader: &Client, state: &KeyState, behind: &Behind) -> Result<u64, String> {
    let options = SendOptions::new().read(ReadLevel::Quorum);
    let response = leader
        .send_one_with(shape::head_get(state.key.stripe), &options)
        .await
        .map_err(|error| format!("the read: {error:?}"))?;
    // the row's head, without the bytes a previous inline write left
    let sequence = response
        .access::<StripeHead>()
        .map_err(|error| format!("the read's rows: {error:?}"))?
        .and_then(|rows| rows.first().map(|head| head.sequence.to_native()))
        .ok_or_else(|| "the read found no row".to_string())?;
    if sequence != state.sequence {
        behind.drift.fetch_add(1, Ordering::Relaxed);
    }
    Ok(sequence)
}

/// A write by B: read, stage to every holder, commit once two of three staged, then apply
///
/// # Arguments
///
/// * `shared` - What the cell's workers share
/// * `state` - The key's state
/// * `tag` - The write's tag
async fn staged(shared: Arc<Shared>, state: Arc<tokio::sync::Mutex<KeyState>>, tag: u64) -> Outcome {
    let mut state = state.lock_owned().await;
    let (permits, wait) = wait_turn(&shared, &mut state).await;
    let key = state.key;
    let leader = shared.clients[shared.stripes.leader_of_hash(stripe_hash(&key.stripe))].clone();
    // one frame of the write's bytes, the same to every holder, made before the clock starts as
    // the other paths' bytes are; its head is written once the read says the sequence
    let mut head = StageHead {
        slot: key.slot,
        offset: 0,
        len: u32::try_from(shared.size).expect("a write fits"),
        label: WireLabel::default(),
        expected: WireLabel::default(),
    };
    let mut frame = wire::stage_frame(&head, seed_of(tag), object_of(&key));
    let started = Instant::now();
    // the read, whose sequence the stage expects and the commit is conditional on
    let sequence = match read_sequence(&leader, &state, &shared.behind).await {
        Ok(sequence) => sequence,
        Err(error) => return Outcome::whole(Answer::Failed(error), started.elapsed(), wait),
    };
    let read = started.elapsed();
    let label = WireLabel {
        sequence: sequence + 1,
        tag,
    };
    head.offset = offset_of(sequence + 1, shared.size);
    head.label = label;
    head.expected = WireLabel {
        sequence,
        tag: state.tag,
    };
    wire::write_stage_head(&mut frame, &head);
    let frame = Arc::new(frame);
    // every holder staged at once, each then waiting to be told whether to apply
    let (staged_tx, mut staged_rx) = mpsc::unbounded_channel::<(usize, bool)>();
    let staged_at = Instant::now();
    let answered = Arc::new(AtomicUsize::new(0));
    let mut decisions = Vec::with_capacity(COPIES);
    let mut tasks = Vec::with_capacity(COPIES);
    for (holder, permit) in permits.into_iter().enumerate() {
        let (decide, decided) = oneshot::channel::<bool>();
        decisions.push(decide);
        tasks.push(tokio::spawn(stage_then_apply(StageTask {
            shared: shared.clone(),
            holder,
            frame: frame.clone(),
            slot: key.slot,
            label,
            staged: staged_tx.clone(),
            decided,
            permit,
            staged_at,
            answered: answered.clone(),
        })));
    }
    drop(staged_tx);
    // on once two of three have it durably, the third named as missed if it has not answered
    let mut ok = [false; COPIES];
    let mut heard = 0;
    while ok.iter().filter(|done| **done).count() < 2 {
        let Some((holder, durable)) = staged_rx.recv().await else {
            break;
        };
        heard += 1;
        ok[holder] = durable;
        if heard == COPIES {
            break;
        }
    }
    let stage = started.elapsed() - read;
    if ok.iter().filter(|done| **done).count() < 2 {
        // a write that cannot reach two holders is refused by name, and nothing is applied
        for decide in decisions {
            let _ = decide.send(false);
        }
        state.behind = tasks;
        return Outcome {
            answer: Answer::Failed("fewer than two holders staged".to_string()),
            took: started.elapsed(),
            wait,
            read: Some(read),
            stage: Some(stage),
            commit: None,
        };
    }
    let missed = ok
        .iter()
        .enumerate()
        .filter(|(_, done)| !**done)
        .fold(0u32, |mask, (holder, _)| mask | (1 << holder));
    // the commit of the small row, conditional on the sequence read
    let committing = Instant::now();
    let sent = leader
        .send_one(shape::staged_commit(key.stripe, sequence, tag, missed))
        .await;
    let commit = committing.elapsed();
    let answer = Answer::of(&sent);
    let applied = matches!(answer, Answer::Applied);
    // every holder told; a holder that staged applies once it hears the commit applied
    for decide in decisions {
        let _ = decide.send(applied);
    }
    if applied {
        state.sequence = sequence + 1;
        state.tag = tag;
        state.last = Some(Last {
            offset: head.offset,
            seed: seed_of(tag),
            object: object_of(&key),
        });
    } else {
        state.sequence = sequence;
    }
    state.behind = tasks;
    Outcome {
        answer,
        took: started.elapsed(),
        wait,
        read: Some(read),
        stage: Some(stage),
        commit: Some(commit),
    }
}

/// What one holder's part of a staged write needs
struct StageTask {
    /// What the cell's workers share
    shared: Arc<Shared>,
    /// Which holder
    holder: usize,
    /// The stage's frame, the same to every holder
    frame: Arc<Vec<u8>>,
    /// The chunk slot
    slot: u32,
    /// The label the write makes
    label: WireLabel,
    /// Where it says whether its stage is durable
    staged: mpsc::UnboundedSender<(usize, bool)>,
    /// Where it hears whether the commit applied
    decided: oneshot::Receiver<bool>,
    /// The holder's permit, given back once its part is durable
    permit: OwnedSemaphorePermit,
    /// When the write's stages were sent
    staged_at: Instant,
    /// How many of the write's holders have answered their stage
    answered: Arc<AtomicUsize>,
}

/// One holder's part of a staged write: its stage, then its apply if the commit applied
///
/// # Arguments
///
/// * `task` - What it needs
async fn stage_then_apply(task: StageTask) {
    let pool = task.shared.holders[task.holder].clone();
    // the stage, timed from when the write sent its stages
    let durable = match pool.call(&task.frame, Kind::Staged).await {
        Ok(_) => true,
        Err(error) => {
            task.shared.behind.fail(format!("stage: {error}"));
            false
        }
    };
    let took = task.staged_at.elapsed();
    if task.shared.window.holds(Instant::now()) {
        task.shared.behind.stage[task.holder]
            .lock()
            .expect("not poisoned")
            .push(took);
        // the last of the three to answer is the write's third stage
        if task.answered.fetch_add(1, Ordering::AcqRel) + 1 == COPIES {
            task.shared.behind.third.lock().expect("not poisoned").push(took);
        }
    } else {
        task.answered.fetch_add(1, Ordering::AcqRel);
    }
    let _ = task.staged.send((task.holder, durable));
    // the apply, once the commit applied, whatever order the holders staged in
    if durable && task.decided.await == Ok(true) {
        let applying = Instant::now();
        let request = Request::Apply {
            slot: task.slot,
            label: task.label,
        };
        match pool.call(&request.frame(), Kind::Applied).await {
            Ok(_) => {
                if task.shared.window.holds(Instant::now()) {
                    task.shared.behind.deferred.lock().expect("not poisoned").push(applying.elapsed());
                }
            }
            Err(error) => task.shared.behind.fail(format!("apply: {error}")),
        }
    }
    // the holder's permit back only now its part is durable
    drop(task.permit);
}

/// A write by the small-write path: read, commit with the bytes inside, then fold
///
/// # Arguments
///
/// * `shared` - What the cell's workers share
/// * `state` - The key's state
/// * `tag` - The write's tag
async fn inline(shared: Arc<Shared>, state: Arc<tokio::sync::Mutex<KeyState>>, tag: u64) -> Outcome {
    let mut state = state.lock_owned().await;
    let (permits, wait) = wait_turn(&shared, &mut state).await;
    let key = state.key;
    let leader = shared.clients[shared.stripes.leader_of_hash(stripe_hash(&key.stripe))].clone();
    // the bytes made before the clock starts, as the row's are on the first path
    let seed = seed_of(tag);
    let object = object_of(&key);
    let bytes = crate::bytes::make(seed, object, shared.size);
    let started = Instant::now();
    // the read, whose sequence the commit is conditional on
    let sequence = match read_sequence(&leader, &state, &shared.behind).await {
        Ok(sequence) => sequence,
        Err(error) => return Outcome::whole(Answer::Failed(error), started.elapsed(), wait),
    };
    let read = started.elapsed();
    // the commit, the bytes inside it
    let sent = leader
        .send_one(shape::inline_commit(key.stripe, sequence, tag, bytes))
        .await;
    let commit = started.elapsed() - read;
    let answer = Answer::of(&sent);
    if matches!(answer, Answer::Applied) {
        state.sequence = sequence + 1;
        state.tag = tag;
        state.last = Some(Last {
            offset: offset_of(sequence + 1, shared.size),
            seed,
            object,
        });
        // every holder folds the bytes, made there from the write's seed
        let fold = Arc::new(
            Request::Fold {
                slot: key.slot,
                offset: offset_of(sequence + 1, shared.size),
                len: u32::try_from(shared.size).expect("a write fits"),
                seed,
                object,
            }
            .frame(),
        );
        state.behind = permits
            .into_iter()
            .enumerate()
            .map(|(holder, permit)| tokio::spawn(fold_on(shared.clone(), holder, fold.clone(), permit)))
            .collect();
    } else {
        state.sequence = sequence;
    }
    Outcome {
        answer,
        took: started.elapsed(),
        wait,
        read: Some(read),
        stage: None,
        commit: Some(commit),
    }
}

/// One holder's fold of a write that rode inside its commit
///
/// # Arguments
///
/// * `shared` - What the cell's workers share
/// * `holder` - Which holder
/// * `frame` - The fold's frame
/// * `permit` - The holder's permit, given back once the fold is durable
async fn fold_on(shared: Arc<Shared>, holder: usize, frame: Arc<Vec<u8>>, permit: OwnedSemaphorePermit) {
    let pool = shared.holders[holder].clone();
    let folding = Instant::now();
    match pool.call(&frame, Kind::Folded).await {
        Ok(_) => {
            if shared.window.holds(Instant::now()) {
                shared.behind.deferred.lock().expect("not poisoned").push(folding.elapsed());
            }
        }
        Err(error) => shared.behind.fail(format!("fold: {error}")),
    }
    drop(permit);
}

/// Every worker of a cell, one a hand of keys, and every key's state to read back after it
///
/// # Arguments
///
/// * `path` - The path every worker writes by
/// * `hands` - Each worker's keys
/// * `shared` - What they share
#[must_use]
pub fn workers(path: Path, hands: &[Vec<Key>], shared: &Arc<Shared>) -> (Vec<Box<dyn Work>>, Keys) {
    let mut all = Vec::new();
    let mut workers = Vec::with_capacity(hands.len());
    for (worker, keys) in hands.iter().enumerate() {
        // each key's state, shared by its worker and the read back after the cell
        let states: Keys = keys
            .iter()
            .map(|key| {
                Arc::new(tokio::sync::Mutex::new(KeyState {
                    key: *key,
                    sequence: 0,
                    tag: shape::first_tag(&key.stripe),
                    behind: Vec::new(),
                    last: None,
                }))
            })
            .collect();
        all.extend(states.iter().cloned());
        workers.push(Writer::new(path, worker, states, shared.clone()));
    }
    (workers, all)
}

/// What reading every key's last write back found
#[derive(Debug, Clone, Default)]
pub struct Readback {
    /// Ranges or rows read back
    pub checked: u64,
    /// Ones that did not hold their last write's bytes
    pub wrong: u64,
    /// The first few failures, as they were said
    pub errors: Vec<String>,
}

/// Read every key's last applied write back once the cell's deferred work is durable
///
/// A row is read at `Quorum` through its leader; a stripe's chunk range on every holder, which
/// says every apply and fold landed what its write carried, in commit order.
///
/// # Arguments
///
/// * `path` - The path the cell wrote by
/// * `shared` - What the cell's workers shared
/// * `keys` - Every key's state
pub async fn read_back(path: Path, shared: &Shared, keys: &Keys) -> Readback {
    let mut found = Readback::default();
    for state in keys {
        let state = state.lock().await;
        let Some(last) = state.last else {
            continue;
        };
        let checks: Vec<Result<bool, String>> = if path == Path::Row {
            // the row as its leader holds it
            let leader = shared.clients[shared.rows.leader_of_hash(row_hash(state.key.row))].clone();
            let options = SendOptions::new().read(ReadLevel::Quorum);
            let query = crate::StripeRowGet::new(vec![state.key.row]);
            vec![match leader.send_one_with(query, &options).await {
                Ok(response) => Ok(response
                    .access::<crate::StripeRow>()
                    .ok()
                    .flatten()
                    .and_then(|rows| rows.first().map(|row| row.bytes.as_slice().to_vec()))
                    .is_some_and(|bytes| bytes == crate::bytes::make(last.seed, last.object, shared.size))),
                Err(error) => Err(format!("{error:?}")),
            }]
        } else {
            // the chunk's range on every holder
            let request = Request::Verify {
                slot: state.key.slot,
                offset: last.offset,
                len: u32::try_from(shared.size).expect("a write fits"),
                seed: last.seed,
                object: last.object,
            }
            .frame();
            let mut checks = Vec::with_capacity(shared.holders.len());
            for pool in &shared.holders {
                checks.push(
                    pool.call(&request, Kind::Verified)
                        .await
                        .map(|body| body.first() == Some(&1))
                        .map_err(|error| error.to_string()),
                );
            }
            checks
        };
        for check in checks {
            found.checked += 1;
            match check {
                Ok(true) => {}
                Ok(false) => found.wrong += 1,
                Err(error) => {
                    found.wrong += 1;
                    if found.errors.len() < 5 {
                        found.errors.push(error);
                    }
                }
            }
        }
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A write's offset is whole blocks inside the chunk at every size and sequence
    #[test]
    fn every_offset_is_inside_its_chunk() {
        for size in [4096usize, 8192, 16_384, 32_768, 65_536, 131_072, 262_144] {
            for sequence in 0..300 {
                let offset = u64::from(offset_of(sequence, size));
                assert_eq!(offset % 4096, 0);
                assert!(offset + size as u64 <= STRIPE_BYTES, "{size} at {sequence}");
            }
        }
    }

    /// Paths are named as the records name them, and only the row path leaves the holders alone
    #[test]
    fn paths_are_named_and_touch_holders() {
        for path in Path::ALL {
            assert_eq!(Path::from_name(path.name()), Some(path));
        }
        assert!(!Path::Row.touches_holders());
        assert!(Path::Staged.touches_holders() && Path::Inline.touches_holders());
        assert_eq!(Path::from_name("heat"), None);
    }
}
