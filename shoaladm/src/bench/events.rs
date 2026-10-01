//! Doing something to the cluster while an arm runs, and leaving marks on the arm's clock
//!
//! The event runs as a task beside the arm's workers, on the arm's own clock, so its marks and
//! the client's answers are on one timeline. A kill is `systemctl kill -s KILL` behind a
//! runtime drop-in that keeps the unit from restarting itself five seconds later - without it
//! the outage would be the unit's `RestartSec`, not the cluster's. Every other event is an
//! admin operation the cluster tab could have sent, followed through the record it writes, and
//! the arm is kept running until it is done (up to `--event-timeout`).
//!
//! What the event did is cut into windows afterwards from the arm's seconds
//! (`shoal_loadgen::events`).

use color_eyre::eyre::{bail, eyre};
use shoal::shared::protocol::admin::AdminRequest;
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
/// # Arguments
///
/// * `admin` - A client of a member that stayed up
/// * `clock` - The arm's clock
/// * `deadline` - When to stop sampling, since the arm started
async fn catch_up<S>(admin: &Arc<Shoal<S>>, clock: &ArmClock, deadline: Duration) -> Catchup
where
    S: QuerySupport + Send + Sync + 'static,
{
    // the lag every group reports, as the largest of them
    let mut samples = Vec::new();
    while clock.started().elapsed() < deadline {
        if let Ok(model) = crate::cluster::poll(admin).await {
            samples.push((clock.elapsed_ms(), model.lag_max));
            if converged(&samples).is_some() {
                break;
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
    let catchup = catch_up(admin, clock, deadline).await;
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
