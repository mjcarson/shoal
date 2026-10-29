//! Doing something to the cluster while an arm runs, and leaving marks on the arm's clock
//!
//! The event runs as a task beside the arm's workers, on the arm's own clock, so its marks and
//! the client's answers are on one timeline. A kill is `systemctl kill -s KILL` behind a
//! runtime drop-in that keeps the unit from restarting itself five seconds later - without it
//! the outage would be the unit's `RestartSec`, not the cluster's. Every other event is an
//! admin operation the cluster tab could have sent, followed through the record it writes, and
//! the arm is kept running until it is done (up to `--event-timeout`).
//!
//! A device's event acts on a pool ([F88](../../../docs/src/features/recovery-and-moves.md)):
//! `device_fail` is the operator's `fail-device` of one of the node's devices, and
//! `device_add` empties a device that was failed and moved off before the arm's preload, as a
//! new disk's directory is, and starts its node again, so it joins as a new device. Either is done
//! once every pool's moves settled: its target moved, nothing is moving or behind, and nothing is
//! stale, two reads in a row. `device_drain` is the operator's `drain-device` of one of the node's
//! devices, done once the moves settled and the device holds nothing
//! ([F92](../../../docs/src/features/reclamation-and-device-lifecycle.md)).
//!
//! What the event did is cut into windows afterwards from the arm's seconds
//! (`shoal_loadgen::events`).

use color_eyre::eyre::{bail, eyre};
use shoal::serde_json::Value;
use shoal::shared::protocol::admin::{AdminKind, AdminOutcome, AdminRequest};
use shoal::shared::traits::QuerySupport;
use shoal::Shoal;
use shoal_loadgen::driver::ArmClock;
use shoal_loadgen::events::{converged, Catchup, Mark};
use shoal_loadgen::progress::{BenchEvent, Progress};
use shoal_loadgen::spec::EventKind;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use uuid::Uuid;

use super::hosts::{dropin_path, remove_dropin_script, Change, Restore};
use crate::cluster::{ClusterAction, Follow};
use crate::deploy::remote::{quote, Host};

/// How long past an operation's done an arm keeps running, so the after window has something in it
const AFTER_DONE: Duration = Duration::from_secs(10);

/// What an event acts on, decided before the arm starts
#[derive(Debug, Clone)]
pub struct EventPlan {
    /// The event
    pub kind: EventKind,
    /// The node it acts on, by inventory name, if one
    pub node: Option<String>,
    /// That node's id, if one
    pub node_id: Option<String>,
    /// How ssh reaches that node
    pub target: Option<String>,
    /// The cluster's unit
    pub unit: String,
    /// The table a repair or backup acts on
    pub table: Option<String>,
    /// Where the node's pool devices are mounted, as the inventory names them, which is every
    /// path a device's event may empty
    pub devices: Vec<String>,
    /// Where a backup writes, on every host
    pub backup_dir: String,
    /// When the event starts, since the arm started
    pub at: Duration,
    /// When a killed or stopped node is started again, since the arm started
    pub restart_at: Duration,
    /// The longest the arm is kept running past its time for the event
    pub timeout: Duration,
}

/// What an event did
#[derive(Debug, Clone, Default)]
pub struct EventRun {
    /// What was done when
    pub marks: Vec<Mark>,
    /// `finished`, `unfinished` or `failed: <why>`
    pub outcome: String,
    /// How a node that came back caught up
    pub catchup: Option<Catchup>,
}

/// Leave a mark on the arm's clock, and show it
///
/// # Arguments
///
/// * `run` - Where marks are kept
/// * `clock` - The arm's clock
/// * `progress` - Where it is shown
/// * `kind` - What happened
/// * `note` - Anything worth saying about it
fn mark(run: &mut EventRun, clock: &ArmClock, progress: &Progress, kind: &str, note: Option<String>) {
    let mark = Mark {
        kind: kind.to_string(),
        at_ms: clock.elapsed_ms(),
        note,
    };
    progress.send(BenchEvent::Mark(mark.clone()));
    run.marks.push(mark);
}

/// Wait until a time on the arm's clock, or until the arm was stopped
///
/// # Arguments
///
/// * `clock` - The arm's clock
/// * `at` - When, since the arm started
async fn until(clock: &ArmClock, at: Duration) {
    // a stopped arm wakes the event so it can put its node back
    while clock.started().elapsed() < at && clock.live() {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Run a script on a host off the async runtime, since ssh blocks
///
/// # Arguments
///
/// * `target` - What ssh reaches it through
/// * `script` - The script
async fn ssh(target: String, script: String) -> color_eyre::Result<String> {
    tokio::task::spawn_blocking(move || Host { target }.run(&script))
        .await
        .map_err(|error| eyre!("the ssh task failed: {error}"))?
}

/// Write the drop-in that keeps a unit down once it is killed, recording it to be removed
///
/// # Arguments
///
/// * `target` - The host
/// * `unit` - The unit
/// * `restore` - Where the change is recorded
async fn hold_down(target: &str, unit: &str, restore: &Arc<Mutex<Restore>>) -> color_eyre::Result<()> {
    // recorded first, so a drop-in that half landed is still removed
    restore
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .record(Change::DropIn {
            target: target.to_string(),
            unit: unit.to_string(),
        });
    let path = dropin_path(unit);
    let dir = path.rsplit_once('/').map(|(dir, _)| dir.to_string()).unwrap_or_default();
    let script = format!(
        "sudo -n mkdir -p {dir} && printf '[Service]\\nRestart=no\\n' | sudo -n tee {path} >/dev/null && sudo -n systemctl daemon-reload",
        dir = quote(&dir),
        path = quote(&path),
    );
    ssh(target.to_string(), script).await.map(|_| ())
}

/// Remove the drop-in and start the unit again
///
/// # Arguments
///
/// * `target` - The host
/// * `unit` - The unit
/// * `restore` - Where the drop-in was recorded
async fn bring_back(target: &str, unit: &str, restore: &Arc<Mutex<Restore>>) -> color_eyre::Result<()> {
    // the drop-in goes first, so the unit restarts itself again if it falls over
    ssh(target.to_string(), remove_dropin_script(unit)).await?;
    restore
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .forget(&Change::DropIn {
            target: target.to_string(),
            unit: unit.to_string(),
        });
    ssh(target.to_string(), format!("sudo -n systemctl start {}", quote(unit)))
        .await
        .map(|_| ())
}

/// Sample the cluster's largest replication lag once a second until it holds at zero
///
/// Sampling starts once the returning node is up again: before it reports, the cluster's lag
/// is the lag of the members that stayed, which was never the question.
///
/// # Arguments
///
/// * `admin` - A client of a member that stayed up
/// * `clock` - The arm's clock
/// * `node` - The returning node's id
/// * `deadline` - When to stop sampling, since the arm started
async fn catch_up<S>(admin: &Arc<Shoal<S>>, clock: &ArmClock, node: &str, deadline: Duration) -> Catchup
where
    S: QuerySupport + Send + Sync + 'static,
{
    // the lag every group reports, as the largest of them, once the node is back among them
    let mut samples = Vec::new();
    while clock.started().elapsed() < deadline {
        if let Ok(model) = crate::cluster::poll(admin).await {
            let back = model
                .members
                .iter()
                .any(|member| member.node == node && member.health.eq_ignore_ascii_case("up"));
            if back {
                samples.push((clock.elapsed_ms(), model.lag_max));
                if converged(&samples).is_some() {
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let converged_ms = converged(&samples);
    Catchup { samples, converged_ms }
}

/// Send an admin operation the way the cluster tab does, and follow it until it is done
///
/// # Arguments
///
/// * `admin` - A client of a member that stays up
/// * `line` - The operation, as the cluster tab takes it
/// * `clock` - The arm's clock, kept running while the operation is not done
/// * `timeout` - The longest the arm is kept past its time
/// * `run` - Where marks are kept
/// * `progress` - Where marks are shown
async fn operate<S>(
    admin: &Arc<Shoal<S>>,
    line: &str,
    clock: &ArmClock,
    timeout: Duration,
    run: &mut EventRun,
    progress: &Progress,
) -> color_eyre::Result<bool>
where
    S: QuerySupport + Send + Sync + 'static,
{
    // the tab's own parser, so a line means here what it means there
    let action = ClusterAction::parse(line).map_err(|error| eyre!(error))?;
    let (kind, follow) = action.request();
    let op = Uuid::new_v4();
    let model = crate::cluster::poll(admin).await.map_err(|error| eyre!(error))?;
    mark(run, clock, progress, "requested", Some(line.to_string()));
    let response = admin
        .admin(&AdminRequest {
            op,
            expected_version: model.version,
            kind,
        })
        .await
        .map_err(|error| eyre!("{line}: {error:?}"))?;
    if let Err(error) = response.outcome {
        bail!("{line} was refused: {} ({:?})", error.msg, error.code());
    }
    // nothing to follow is done once accepted
    if follow == Follow::None {
        mark(run, clock, progress, "done", None);
        return Ok(true);
    }
    // the arm runs on until the operation is done, up to the timeout past its own end
    let followed = action.followed(op);
    let started = clock.started().elapsed();
    let cap = started + timeout;
    loop {
        if let Ok((lines, done)) = crate::cluster::follow_once(admin, followed, follow).await {
            if done {
                let failed = lines.iter().any(|line| line.contains("\"Failed\""));
                mark(run, clock, progress, if failed { "failed" } else { "done" }, lines.last().cloned());
                // keep the arm running a little past done, so the after window has something in it
                clock.extend_to((clock.started().elapsed() + AFTER_DONE).min(cap));
                return Ok(!failed);
            }
        }
        let now = clock.started().elapsed();
        if now >= cap || !clock.live() && now >= started + timeout {
            return Ok(false);
        }
        // a running operation keeps the arm running
        clock.extend_to((now + Duration::from_secs(2)).min(cap));
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Run one event against the cluster on the arm's clock
///
/// # Arguments
///
/// * `plan` - What the event does and when
/// * `admin` - A client of a member the event leaves up
/// * `clock` - The arm's clock
/// * `restore` - Where changes to the hosts are recorded
/// * `progress` - Where marks are shown
pub async fn run<S>(
    plan: EventPlan,
    admin: Arc<Shoal<S>>,
    clock: Arc<ArmClock>,
    restore: Arc<Mutex<Restore>>,
    progress: Progress,
) -> EventRun
where
    S: QuerySupport + Send + Sync + 'static,
{
    let mut run = EventRun::default();
    until(&clock, plan.at).await;
    // an arm stopped before its event never runs it
    if !clock.live() {
        run.outcome = "unfinished: the arm stopped before its event".to_string();
        return run;
    }
    let outcome = match plan.kind {
        EventKind::None => Ok(true),
        EventKind::Kill | EventKind::Stop | EventKind::Remove => {
            down_and_back(&plan, &admin, &clock, &restore, &progress, &mut run).await
        }
        EventKind::Rebalance => operate(&admin, "rebalance", &clock, plan.timeout, &mut run, &progress).await,
        EventKind::Decommission => {
            let node = plan.node_id.clone().unwrap_or_default();
            operate(&admin, &format!("decommission {node}"), &clock, plan.timeout, &mut run, &progress).await
        }
        EventKind::Repair => {
            let table = plan.table.clone().unwrap_or_default();
            operate(&admin, &format!("repair {table} verify"), &clock, plan.timeout, &mut run, &progress).await
        }
        EventKind::Backup => {
            let table = plan.table.clone().unwrap_or_default();
            let line = format!("backup {table} {}/{}", plan.backup_dir, Uuid::new_v4().simple());
            operate(&admin, &line, &clock, plan.timeout, &mut run, &progress).await
        }
        EventKind::DeviceFail => device_fail(&plan, &admin, &clock, &progress, &mut run).await,
        EventKind::DeviceAdd => device_add(&plan, &admin, &clock, &progress, &mut run).await,
        EventKind::DeepScrub => deep_scrub(&plan, &admin, &clock, &progress, &mut run).await,
        EventKind::DeviceDrain => device_drain(&plan, &admin, &clock, &progress, &mut run).await,
    };
    run.outcome = match outcome {
        Ok(true) => "finished".to_string(),
        Ok(false) => "unfinished".to_string(),
        Err(error) => {
            mark(&mut run, &clock, &progress, "failed", Some(error.to_string()));
            format!("failed: {error}")
        }
    };
    run
}

/// Take a node down - killed, stopped, or killed for good and removed - and bring it back
///
/// # Arguments
///
/// * `plan` - What the event does and when
/// * `admin` - A client of a member that stays up
/// * `clock` - The arm's clock
/// * `restore` - Where changes to the hosts are recorded
/// * `progress` - Where marks are shown
/// * `run` - Where marks are kept
async fn down_and_back<S>(
    plan: &EventPlan,
    admin: &Arc<Shoal<S>>,
    clock: &ArmClock,
    restore: &Arc<Mutex<Restore>>,
    progress: &Progress,
    run: &mut EventRun,
) -> color_eyre::Result<bool>
where
    S: QuerySupport + Send + Sync + 'static,
{
    let target = plan.target.clone().ok_or_else(|| eyre!("the event has no node"))?;
    let node = plan.node.clone().unwrap_or_default();
    match plan.kind {
        EventKind::Stop => {
            // a clean stop, which the unit does not restart from
            ssh(target.clone(), format!("sudo -n systemctl stop {}", quote(&plan.unit))).await?;
            mark(run, clock, progress, "stop", Some(node.clone()));
        }
        _ => {
            // a crash the unit will not restart from until the drop-in is gone
            hold_down(&target, &plan.unit, restore).await?;
            ssh(target.clone(), format!("sudo -n systemctl kill -s KILL {}", quote(&plan.unit))).await?;
            mark(run, clock, progress, "kill", Some(node.clone()));
        }
    }
    // a removal never brings the node back: it is removed from the cluster instead
    if plan.kind == EventKind::Remove {
        let id = plan.node_id.clone().unwrap_or_default();
        return operate(admin, &format!("remove {id}"), clock, plan.timeout, run, progress).await;
    }
    // back up when it is due, even if the arm was stopped in the meantime
    until(clock, plan.restart_at).await;
    match plan.kind {
        EventKind::Stop => {
            ssh(target.clone(), format!("sudo -n systemctl start {}", quote(&plan.unit))).await?;
        }
        _ => bring_back(&target, &plan.unit, restore).await?,
    }
    mark(run, clock, progress, "restart", Some(node));
    // and how long its groups took to catch up, within the arm and its timeout
    let deadline = clock.started().elapsed() + plan.timeout;
    let catchup = catch_up(admin, clock, plan.node_id.as_deref().unwrap_or_default(), deadline).await;
    if let Some(at) = catchup.converged_ms {
        let converged = Mark {
            kind: "converged".to_string(),
            at_ms: at,
            note: None,
        };
        progress.send(BenchEvent::Mark(converged.clone()));
        run.marks.push(converged);
    }
    let done = catchup.converged_ms.is_some();
    run.catchup = Some(catchup);
    Ok(done)
}

/// One of a node's pool devices, as the devices read answers it (F88)
#[derive(Debug, Clone, PartialEq, Eq)]
struct PoolDevice {
    /// Its id, as the cluster spells it
    id: String,
    /// Where it is mounted on its node
    path: String,
    /// Whether the pool map holds it failed
    failed: bool,
    /// Whether it places now: on the map, in an era that has not ended
    placed: bool,
}

/// Read one of the cluster's admin views
///
/// # Arguments
///
/// * `admin` - A client of a member
/// * `kind` - The read
async fn read<S>(admin: &Arc<Shoal<S>>, kind: AdminKind) -> color_eyre::Result<Value>
where
    S: QuerySupport + Send + Sync + 'static,
{
    let name = kind.name();
    let response = admin
        .admin(&AdminRequest {
            op: Uuid::new_v4(),
            expected_version: 0,
            kind,
        })
        .await
        .map_err(|error| eyre!("{name}: {error:?}"))?;
    match response.outcome {
        Ok(AdminOutcome::Read(value)) => Ok(value),
        Ok(other) => Err(eyre!("{name} answered {other:?}")),
        Err(error) => Err(eyre!("{name}: {} ({:?})", error.msg, error.code())),
    }
}

/// A node's pool devices, from its member's report and the committed pool map
///
/// # Arguments
///
/// * `view` - The devices read's answer
/// * `node` - The node's id
fn pool_devices(view: &Value, node: &str) -> Vec<PoolDevice> {
    // the member's own report names its devices and where they are mounted
    let Some(member) = view["members"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|member| member["node"].as_str() == Some(node))
    else {
        return Vec::new();
    };
    member["devices"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|device| {
            let id = device["id"].as_str()?.to_string();
            let path = device["path"].as_str()?.to_string();
            // and the map says whether each is failed and whether it places
            let record = &view["pool_map"]["devices"][id.as_str()];
            let failed = !record["failed"].is_null();
            let placed = record["eras"]
                .as_array()
                .and_then(|eras| eras.last())
                .is_some_and(|era| era["until"].is_null());
            Some(PoolDevice { id, path, failed, placed })
        })
        .collect()
}

/// Each pool's target and the moves it switched, as its status answers them
///
/// # Arguments
///
/// * `status` - The pool status read's answer
fn targets(status: &Value) -> Vec<(String, Option<u64>, u64)> {
    status
        .as_array()
        .into_iter()
        .flatten()
        .map(|pool| {
            (
                pool["pool"].as_str().unwrap_or_default().to_string(),
                pool["target"].as_u64(),
                pool["recovery"]["moves_switched"].as_u64().unwrap_or(0),
            )
        })
        .collect()
}

/// Whether every pool's moves settled since `before`: some pool's target moved and switched a
/// move, and no pool has a move in flight or a group behind its target
///
/// Stale positions are not waited for: under a load, a put that has `k + f` holders marks a
/// holder slower than its straggler wait missed, so a busy pool may hold some, and its rebuilds
/// run in its slices' idle time (item 247). The count at done is the event's note instead.
///
/// # Arguments
///
/// * `before` - Each pool's target and switched moves before the event
/// * `status` - The pool status read's answer now
fn settled(before: &[(String, Option<u64>, u64)], status: &Value) -> bool {
    // something moved, or the event changed nothing a read can tell
    let now = targets(status);
    let moved = now.iter().any(|(pool, target, switched)| {
        before
            .iter()
            .find(|(was, _, _)| was == pool)
            .is_none_or(|(_, was_target, was_switched)| target != was_target && switched > was_switched)
    });
    // and nothing is left to do in any pool
    let quiet = status.as_array().into_iter().flatten().all(|pool| {
        pool["moves"].as_u64() == Some(0) && pool["recovery"]["behind"].as_u64().unwrap_or(0) == 0
    });
    moved && quiet
}

/// Wait until every pool's moves settled, keeping the arm running while they have not, and say how
/// many positions every pool held stale then
///
/// Settled is two reads in a row a second apart, since a member's figures reach the leader with
/// its next report. None is a wait that ran out.
///
/// # Arguments
///
/// * `admin` - A client of a member
/// * `before` - Each pool's target and switched moves before the event
/// * `clock` - The arm's clock, kept running while the moves have not settled, if an arm runs
/// * `cap` - The longest to wait, since the arm started, or since now without an arm
async fn settle<S>(
    admin: &Arc<Shoal<S>>,
    before: &[(String, Option<u64>, u64)],
    clock: Option<&ArmClock>,
    cap: Duration,
) -> color_eyre::Result<Option<u64>>
where
    S: QuerySupport + Send + Sync + 'static,
{
    let started = std::time::Instant::now();
    let mut quiet = 0;
    loop {
        // a read that failed is a node still coming up, and counts for nothing
        if let Ok(status) = read(admin, AdminKind::PoolStatus).await {
            quiet = if settled(before, &status) { quiet + 1 } else { 0 };
            if quiet >= 2 {
                let stale = status
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|pool| pool["recovery"]["stale"].as_u64().unwrap_or(0))
                    .sum();
                return Ok(Some(stale));
            }
        }
        let now = clock.map_or_else(|| started.elapsed(), |clock| clock.started().elapsed());
        if now >= cap {
            return Ok(None);
        }
        // moves that are running keep the arm running
        if let Some(clock) = clock {
            clock.extend_to((now + Duration::from_secs(2)).min(cap));
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Ask every pool for a deep scrub the way an operator does, and follow it until no tablet group
/// of any of them is pending (F89)
///
/// # Arguments
///
/// * `plan` - What the event does and when
/// * `admin` - A client of a member
/// * `clock` - The arm's clock
/// * `progress` - Where marks are shown
/// * `run` - Where marks are kept
async fn deep_scrub<S>(
    plan: &EventPlan,
    admin: &Arc<Shoal<S>>,
    clock: &ArmClock,
    progress: &Progress,
    run: &mut EventRun,
) -> color_eyre::Result<bool>
where
    S: QuerySupport + Send + Sync + 'static,
{
    // every pool the cluster has, each asked with the tab's own line
    let status = read(admin, AdminKind::PoolStatus).await?;
    let pools: Vec<String> = status
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|pool| pool["pool"].as_str().map(str::to_string))
        .collect();
    if pools.is_empty() {
        bail!("the cluster has no pool to scrub");
    }
    let mut follow = Follow::None;
    for pool in &pools {
        let action = ClusterAction::parse(&format!("scrub {pool} deep")).map_err(|error| eyre!(error))?;
        let (kind, followed) = action.request();
        follow = followed;
        let model = crate::cluster::poll(admin).await.map_err(|error| eyre!(error))?;
        let response = admin
            .admin(&AdminRequest {
                op: Uuid::new_v4(),
                expected_version: model.version,
                kind,
            })
            .await
            .map_err(|error| eyre!("scrub {pool} deep: {error:?}"))?;
        if let Err(error) = response.outcome {
            bail!("scrub {pool} deep was refused: {} ({:?})", error.msg, error.code());
        }
    }
    mark(run, clock, progress, "requested", Some(format!("deep scrub of {}", pools.join(", "))));
    // the arm runs on until every pool's pass is done, up to the timeout past its own end
    let cap = clock.started().elapsed() + plan.timeout;
    loop {
        if let Ok((lines, true)) = crate::cluster::follow_once(admin, Uuid::nil(), follow).await {
            mark(run, clock, progress, "done", lines.last().cloned());
            clock.extend_to((clock.started().elapsed() + AFTER_DONE).min(cap));
            return Ok(true);
        }
        let now = clock.started().elapsed();
        if now >= cap {
            return Ok(false);
        }
        // a pass that is running keeps the arm running
        clock.extend_to((now + Duration::from_secs(2)).min(cap));
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Fail a pool device the way an operator does, by its id
///
/// # Arguments
///
/// * `admin` - A client of a member
/// * `device` - The device's id
async fn fail_device<S>(admin: &Arc<Shoal<S>>, device: &str) -> color_eyre::Result<()>
where
    S: QuerySupport + Send + Sync + 'static,
{
    // the tab's own line, at the version the cluster is at
    let action = ClusterAction::parse(&format!("fail-device {device}")).map_err(|error| eyre!(error))?;
    let (kind, _) = action.request();
    let model = crate::cluster::poll(admin).await.map_err(|error| eyre!(error))?;
    let response = admin
        .admin(&AdminRequest {
            op: Uuid::new_v4(),
            expected_version: model.version,
            kind,
        })
        .await
        .map_err(|error| eyre!("fail-device {device}: {error:?}"))?;
    if let Err(error) = response.outcome {
        bail!("fail-device {device} was refused: {} ({:?})", error.msg, error.code());
    }
    Ok(())
}

/// Fail the node's first placing pool device, and follow its groups off it
///
/// # Arguments
///
/// * `plan` - What the event does and when
/// * `admin` - A client of a member
/// * `clock` - The arm's clock
/// * `progress` - Where marks are shown
/// * `run` - Where marks are kept
async fn device_fail<S>(
    plan: &EventPlan,
    admin: &Arc<Shoal<S>>,
    clock: &ArmClock,
    progress: &Progress,
    run: &mut EventRun,
) -> color_eyre::Result<bool>
where
    S: QuerySupport + Send + Sync + 'static,
{
    let node = plan.node_id.clone().ok_or_else(|| eyre!("the event has no node"))?;
    // the first of its devices that places now
    let view = read(admin, AdminKind::Devices).await?;
    let device = pool_devices(&view, &node)
        .into_iter()
        .find(|device| device.placed && !device.failed)
        .ok_or_else(|| eyre!("node {node} has no pool device placing now"))?;
    let before = targets(&read(admin, AdminKind::PoolStatus).await?);
    fail_device(admin, &device.id).await?;
    mark(run, clock, progress, "requested", Some(format!("{} {}", device.id, device.path)));
    // done once its groups moved off and every stale position was rebuilt
    let cap = clock.started().elapsed() + plan.timeout;
    let done = settle(admin, &before, Some(clock), cap).await?;
    if let Some(stale) = done {
        mark(run, clock, progress, "done", Some(format!("stale {stale}")));
        clock.extend_to((clock.started().elapsed() + AFTER_DONE).min(cap));
    }
    Ok(done.is_some())
}

/// Drain the node's first placing pool device, and follow its groups off it until it holds nothing
/// ([F92](../../../docs/src/features/reclamation-and-device-lifecycle.md))
///
/// # Arguments
///
/// * `plan` - What the event does and when
/// * `admin` - A client of a member
/// * `clock` - The arm's clock
/// * `progress` - Where marks are shown
/// * `run` - Where marks are kept
async fn device_drain<S>(
    plan: &EventPlan,
    admin: &Arc<Shoal<S>>,
    clock: &ArmClock,
    progress: &Progress,
    run: &mut EventRun,
) -> color_eyre::Result<bool>
where
    S: QuerySupport + Send + Sync + 'static,
{
    let node = plan.node_id.clone().ok_or_else(|| eyre!("the event has no node"))?;
    // the first of its devices that places now
    let view = read(admin, AdminKind::Devices).await?;
    let device = pool_devices(&view, &node)
        .into_iter()
        .find(|device| device.placed && !device.failed)
        .ok_or_else(|| eyre!("node {node} has no pool device placing now"))?;
    let before = targets(&read(admin, AdminKind::PoolStatus).await?);
    // the tab's own line, at the version the cluster is at
    let action = ClusterAction::parse(&format!("drain-device {}", device.id)).map_err(|error| eyre!(error))?;
    let (kind, _) = action.request();
    let model = crate::cluster::poll(admin).await.map_err(|error| eyre!(error))?;
    let response = admin
        .admin(&AdminRequest {
            op: Uuid::new_v4(),
            expected_version: model.version,
            kind,
        })
        .await
        .map_err(|error| eyre!("drain-device {}: {error:?}", device.id))?;
    if let Err(error) = response.outcome {
        bail!("drain-device {} was refused: {} ({:?})", device.id, error.msg, error.code());
    }
    mark(run, clock, progress, "requested", Some(format!("{} {}", device.id, device.path)));
    // its groups moved off, then released, so it holds nothing
    let cap = clock.started().elapsed() + plan.timeout;
    if settle(admin, &before, Some(clock), cap).await?.is_none() {
        return Ok(false);
    }
    loop {
        if let Ok(view) = read(admin, AdminKind::Devices).await {
            if device_holds_nothing(&view, &device.id) {
                mark(run, clock, progress, "done", Some(format!("{} holds nothing", device.id)));
                clock.extend_to((clock.started().elapsed() + AFTER_DONE).min(cap));
                return Ok(true);
            }
        }
        let now = clock.started().elapsed();
        if now >= cap {
            return Ok(false);
        }
        // releases that are running keep the arm running
        clock.extend_to((now + Duration::from_secs(2)).min(cap));
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Whether a member's report says a device's every slice holds nothing, chunk or staged byte
///
/// # Arguments
///
/// * `view` - The devices view
/// * `id` - The device's id
fn device_holds_nothing(view: &Value, id: &str) -> bool {
    view["members"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|member| member["devices"].as_array().cloned().unwrap_or_default())
        .find(|device| device["id"].as_str() == Some(id))
        .is_some_and(|device| {
            device["slices"].as_array().into_iter().flatten().all(|slice| {
                slice["chunks"].as_u64().unwrap_or(0) == 0 && slice["staged_bytes"].as_u64().unwrap_or(0) == 0
            })
        })
}

/// Set a device aside before a `device_add` arm's preload: the node's last placing pool device is
/// failed, and its groups moved off, so the arm starts with nothing on it
///
/// # Arguments
///
/// * `plan` - What the event does
/// * `admin` - A client of a member
/// * `progress` - Where what it does is shown
///
/// # Errors
///
/// When the node has fewer than two placing devices, or the moves do not settle in the event's
/// timeout.
pub async fn set_aside<S>(plan: &EventPlan, admin: &Arc<Shoal<S>>, progress: &Progress) -> color_eyre::Result<()>
where
    S: QuerySupport + Send + Sync + 'static,
{
    let node = plan.node_id.clone().ok_or_else(|| eyre!("the event has no node"))?;
    // a cluster just bootstrapped commits its devices and pins its pools in its first seconds, so
    // wait for both: a device failed before its pool is pinned moves nothing anywhere
    let deadline = std::time::Instant::now() + plan.timeout;
    let device = loop {
        let view = read(admin, AdminKind::Devices).await?;
        let placing: Vec<PoolDevice> = pool_devices(&view, &node)
            .into_iter()
            .filter(|device| device.placed && !device.failed)
            .collect();
        let pinned = read(admin, AdminKind::PoolStatus)
            .await
            .ok()
            .and_then(|status| status.as_array().cloned())
            .is_some_and(|pools| !pools.is_empty() && pools.iter().all(|pool| pool["pinned"].is_u64()));
        // the last of its devices that places, so the node keeps one
        if let (true, [_, .., last]) = (pinned, placing.as_slice()) {
            break last.clone();
        }
        if std::time::Instant::now() >= deadline {
            return Err(eyre!(
                "device_add sets one of {node}'s pool devices aside, and in {}s it had {} placing{}",
                plan.timeout.as_secs(),
                placing.len(),
                if pinned { "" } else { " and its pools were not pinned" }
            ));
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    };
    if !plan.devices.contains(&device.path) {
        bail!("{} is not one of the inventory's devices for {node}", device.path);
    }
    progress.log(format!("setting {} aside on {} for device_add", device.path, plan.node.clone().unwrap_or_default()));
    let before = targets(&read(admin, AdminKind::PoolStatus).await?);
    fail_device(admin, &device.id).await?;
    if settle(admin, &before, None, plan.timeout).await?.is_none() {
        bail!("{}'s groups did not move off it within {}s", device.path, plan.timeout.as_secs());
    }
    Ok(())
}

/// Start a node again with the device set aside emptied, as a new disk's directory is, and follow
/// groups onto it
///
/// # Arguments
///
/// * `plan` - What the event does and when
/// * `admin` - A client of a member that stays up
/// * `clock` - The arm's clock
/// * `progress` - Where marks are shown
/// * `run` - Where marks are kept
async fn device_add<S>(
    plan: &EventPlan,
    admin: &Arc<Shoal<S>>,
    clock: &ArmClock,
    progress: &Progress,
    run: &mut EventRun,
) -> color_eyre::Result<bool>
where
    S: QuerySupport + Send + Sync + 'static,
{
    let node = plan.node_id.clone().ok_or_else(|| eyre!("the event has no node"))?;
    let target = plan.target.clone().ok_or_else(|| eyre!("the event has no node"))?;
    // the device set aside: failed, and placing nothing
    let view = read(admin, AdminKind::Devices).await?;
    let device = pool_devices(&view, &node)
        .into_iter()
        .find(|device| device.failed && !device.placed)
        .ok_or_else(|| eyre!("node {node} has no device set aside for device_add"))?;
    // only ever a path the inventory names as one of the node's devices is emptied
    if !plan.devices.contains(&device.path) {
        bail!("{} is not one of the inventory's devices for {node}", device.path);
    }
    let before = targets(&read(admin, AdminKind::PoolStatus).await?);
    // the node stopped, the directory emptied but kept, since a device's path is never created
    ssh(target.clone(), format!("sudo -n systemctl stop {}", quote(&plan.unit))).await?;
    mark(run, clock, progress, "requested", Some(format!("{} {}", device.id, device.path)));
    ssh(
        target.clone(),
        format!("sudo -n find {} -mindepth 1 -maxdepth 1 -exec rm -rf {{}} +", quote(&device.path)),
    )
    .await?;
    ssh(target, format!("sudo -n systemctl start {}", quote(&plan.unit))).await?;
    mark(run, clock, progress, "restart", plan.node.clone());
    // done once groups moved onto it and every stale position was rebuilt
    let cap = clock.started().elapsed() + plan.timeout;
    let done = settle(admin, &before, Some(clock), cap).await?;
    if let Some(stale) = done {
        mark(run, clock, progress, "done", Some(format!("stale {stale}")));
        clock.extend_to((clock.started().elapsed() + AFTER_DONE).min(cap));
    }
    Ok(done.is_some())
}

#[cfg(test)]
mod tests {
    use super::{pool_devices, settled, targets, PoolDevice};
    use shoal::serde_json::json;

    /// A node's devices are read from its member's report and the map's records, and a pool's
    /// moves are settled only once something moved and nothing is in flight or behind (F88)
    #[test]
    fn device_events_read_the_cluster() {
        let view = json!({
            "members": [
                {"node": "n1", "devices": [{"id": "d1", "path": "/xfs/a"}, {"id": "d2", "path": "/xfs/b"}]},
                {"node": "n2", "devices": [{"id": "d3", "path": "/xfs/a"}]},
            ],
            "pool_map": {"devices": {
                "d1": {"eras": [{"from": 1, "until": null}]},
                "d2": {"failed": "operator", "eras": [{"from": 1, "until": 4}]},
                "d3": {"eras": [{"from": 1}]},
            }},
        });
        assert_eq!(
            pool_devices(&view, "n1"),
            vec![
                PoolDevice { id: "d1".into(), path: "/xfs/a".into(), failed: false, placed: true },
                PoolDevice { id: "d2".into(), path: "/xfs/b".into(), failed: true, placed: false },
            ]
        );
        assert!(pool_devices(&view, "n9").is_empty());
        // the target moved and switched, but a move is still in flight
        let before = targets(&json!([{"pool": "fast", "target": 3, "recovery": {"moves_switched": 2}}]));
        let moving = json!([{"pool": "fast", "target": 5, "moves": 1, "recovery": {"moves_switched": 4}}]);
        assert!(!settled(&before, &moving));
        // nothing in flight, but a group behind or a position stale
        let behind = json!([{"pool": "fast", "target": 5, "moves": 0, "recovery": {"moves_switched": 4, "behind": 2}}]);
        assert!(!settled(&before, &behind));
        // stale positions are a busy pool's ordinary state, rebuilt in idle time, and wait for nothing
        let stale = json!([{"pool": "fast", "target": 5, "moves": 0, "recovery": {"moves_switched": 4, "stale": 1}}]);
        assert!(settled(&before, &stale));
        // quiet before anything moved is not settled
        let unmoved = json!([{"pool": "fast", "target": 3, "moves": 0, "recovery": {"moves_switched": 2}}]);
        assert!(!settled(&before, &unmoved));
        // and quiet after it moved is
        let quiet = json!([{"pool": "fast", "target": 5, "moves": 0, "recovery": {"moves_switched": 4}}]);
        assert!(settled(&before, &quiet));
    }
}
