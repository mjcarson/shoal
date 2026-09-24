//! A cluster's figures as `shoalctl cluster stats` and the cluster tab show them
//!
//! One `Stats` admin read answers every member's standing, tablets, partitions, bytes and
//! trailing write rates, and every plan's progress ([F52](../../../docs/src/features/cluster-stats.md)).
//! Only the control leader holds every member's figures; any other node answers its own and
//! names the leader's client address, so [`leader_stats`] asks there when the node it reached
//! is not the leader, and keeps that client for the next poll.
//!
//! Nothing here draws: the model renders itself to lines, so every decision is testable
//! without a terminal.

use shoal::Shoal;
use shoal::shared::protocol::admin::{AdminKind, AdminOutcome, AdminRequest};
use shoal::shared::protocol::stats::{
    ClusterStatsView, MemberStats, NodeStats, PlanProgress, Rates, TableStats,
};
use shoal::shared::traits::QuerySupport;
use std::future::Future;
use std::sync::Arc;
use uuid::Uuid;

use super::model::bytes;

/// How many finished plans the full view lists under the open ones
const DONE_PLANS_SHOWN: usize = 3;

/// A `Stats` answer, and what the client could not get
#[derive(Debug, Clone, PartialEq)]
pub struct StatsModel {
    /// The answer
    pub view: ClusterStatsView,
    /// Why the answer holds only the answering node's figures, if it does
    pub note: Option<String>,
}

impl StatsModel {
    /// A model of an answer
    ///
    /// # Arguments
    ///
    /// * `view` - The answer
    /// * `note` - Why it holds only the answering node's figures, if it does
    #[must_use]
    pub fn new(view: ClusterStatsView, note: Option<String>) -> Self {
        StatsModel { view, note }
    }

    /// Every figure as the lines `shoalctl cluster stats` prints
    #[must_use]
    pub fn render_lines(&self) -> Vec<String> {
        let mut lines = self.header_lines();
        // the cluster as a whole, counted once per row through each copy's leader
        let total = self.view.cluster_total();
        lines.push(format!(
            "cluster writes/s (10s/1m/5m): insert {}  update {}  delete {}",
            rates(&total.led.inserts),
            rates(&total.led.updates),
            rates(&total.led.deletes),
        ));
        lines.push(format!(
            "cluster bytes/s  (10s/1m/5m): insert {}  update {}  delete {}",
            byte_rates(&total.led.insert_bytes),
            byte_rates(&total.led.update_bytes),
            byte_rates(&total.led.delete_bytes),
        ));
        lines.push(format!(
            "cluster holds {} partitions in {} archived (once per row; {} and {} over every copy)",
            total.partitions_led,
            bytes(total.bytes_led),
            total.partitions,
            bytes(total.bytes),
        ));
        lines.push(String::new());
        // every member: its standing and what it holds
        lines.push(format!(
            "{:<12} {:<9} {:>5} {:>11} {:>13} {:>19} {:>10} {:>10}",
            "member", "state", "age", "groups/led", "tablets/led", "partitions/led", "archived",
            "free"
        ));
        for member in &self.view.members {
            let stats = member.stats.as_ref();
            let pair = |figure: fn(&TableStats) -> u64, led: fn(&TableStats) -> u64| {
                stats.map_or("-".to_string(), |stats| {
                    format!("{}/{}", figure(&stats.total), led(&stats.total))
                })
            };
            lines.push(format!(
                "{:<12} {:<9} {:>5} {:>11} {:>13} {:>19} {:>10} {:>10}",
                short(&member.node.0.to_string()),
                state(member),
                age(member),
                pair(|t| t.groups, |t| t.groups_led),
                pair(|t| t.tablets, |t| t.tablets_led),
                pair(|t| t.partitions, |t| t.partitions_led),
                stats.map_or("-".to_string(), |stats| bytes(stats.total.bytes)),
                stats.map_or("-".to_string(), |stats| bytes(stats.free_bytes)),
            ));
        }
        lines.push(String::new());
        // every member: what it applies, over every copy it hosts
        lines.push(format!(
            "{:<12} {:>22} {:>22} {:>22} {:>28} {:>28}",
            "applied/s", "insert", "update", "delete", "bytes in", "stream out"
        ));
        for member in &self.view.members {
            let Some(stats) = live(member) else {
                lines.push(format!("{:<12} {}", short(&member.node.0.to_string()), "-"));
                continue;
            };
            let applied = &stats.total.applied;
            let mut written = applied.insert_bytes;
            written.absorb(&applied.update_bytes);
            written.absorb(&applied.delete_bytes);
            lines.push(format!(
                "{:<12} {:>22} {:>22} {:>22} {:>28} {:>28}",
                short(&member.node.0.to_string()),
                rates(&applied.inserts),
                rates(&applied.updates),
                rates(&applied.deletes),
                byte_rates(&written),
                byte_rates(&stats.stream_sent),
            ));
        }
        // the tables, summed over the members once per row, when there is more than one
        let tables = self.table_totals();
        if tables.len() > 1 || self.view.table.is_some() {
            lines.push(String::new());
            lines.push(format!(
                "{:<20} {:>14} {:>10} {:>22} {:>22} {:>22}",
                "table", "partitions", "archived", "insert/s", "update/s", "delete/s"
            ));
            for table in &tables {
                lines.push(format!(
                    "{:<20} {:>14} {:>10} {:>22} {:>22} {:>22}",
                    table.table,
                    table.partitions_led,
                    bytes(table.bytes_led),
                    rates(&table.led.inserts),
                    rates(&table.led.updates),
                    rates(&table.led.deletes),
                ));
            }
        }
        // the plans, open ones first, then the last few done
        lines.push(String::new());
        lines.extend(self.plan_lines(DONE_PLANS_SHOWN));
        lines
    }

    /// The figures as the few lines the cluster tab adds under its model
    #[must_use]
    pub fn compact_lines(&self) -> Vec<String> {
        let mut lines = vec![String::new()];
        lines.extend(self.header_lines());
        // per member: its standing, its rows, and its write and stream rates over ten seconds
        lines.push(format!(
            "{:<12} {:<9} {:>5} {:>15} {:>10} {:>8} {:>8} {:>8} {:>10} {:>10}",
            "member", "state", "age", "partitions/led", "archived", "ins/s", "upd/s", "del/s",
            "in B/s", "stream/s"
        ));
        for member in &self.view.members {
            let stats = live(member);
            let figure = |value: Option<String>| value.unwrap_or_else(|| "-".to_string());
            lines.push(format!(
                "{:<12} {:<9} {:>5} {:>15} {:>10} {:>8} {:>8} {:>8} {:>10} {:>10}",
                short(&member.node.0.to_string()),
                state(member),
                age(member),
                figure(member.stats.as_ref().map(|stats| {
                    format!("{}/{}", stats.total.partitions, stats.total.partitions_led)
                })),
                figure(member.stats.as_ref().map(|stats| bytes(stats.total.bytes))),
                figure(stats.map(|stats| rate(stats.total.applied.inserts.r10s))),
                figure(stats.map(|stats| rate(stats.total.applied.updates.r10s))),
                figure(stats.map(|stats| rate(stats.total.applied.deletes.r10s))),
                figure(stats.map(|stats| {
                    let applied = &stats.total.applied;
                    byte_rate(
                        applied.insert_bytes.r10s
                            + applied.update_bytes.r10s
                            + applied.delete_bytes.r10s,
                    )
                })),
                figure(stats.map(|stats| byte_rate(stats.stream_sent.r10s))),
            ));
        }
        // and only the open plans, which is what an operator watching a rebalance wants
        let open = self.plan_lines(0);
        if self.view.plans.iter().any(|plan| plan.phase != "done") {
            lines.extend(open);
        }
        lines
    }

    /// The line saying who answered and whose figures these are
    fn header_lines(&self) -> Vec<String> {
        let view = &self.view;
        let mut line = format!(
            "stats from {} at version {}",
            short(&view.answered_by.0.to_string()),
            view.version
        );
        if let Some(table) = &view.table {
            line.push_str(&format!(", table {table}"));
        }
        let mut lines = vec![line];
        // a view from a follower holds its own figures only, and says where the rest are
        if !view.is_leader_view() {
            let leader = match (&view.leader, &view.leader_client) {
                (Some(leader), Some(client)) => format!("{} at {client}", short(&leader.0.to_string())),
                (Some(leader), None) => short(&leader.0.to_string()),
                _ => "unknown".to_string(),
            };
            lines.push(format!(
                "local view: only this node's figures; the leader {leader} holds every member's"
            ));
        }
        if let Some(note) = &self.note {
            lines.push(format!("({note})"));
        }
        lines
    }

    /// Every table's figures summed over the members, each row counted through its leader
    fn table_totals(&self) -> Vec<TableStats> {
        let mut tables: Vec<TableStats> = Vec::new();
        for stats in self.view.members.iter().filter_map(|member| member.stats.as_ref()) {
            for row in &stats.tables {
                match tables.iter_mut().find(|table| table.table == row.table) {
                    Some(table) => table.absorb(row),
                    None => tables.push(row.clone()),
                }
            }
        }
        tables.sort_by(|a, b| a.table.cmp(&b.table));
        tables
    }

    /// The open plans' lines, then up to some of the finished ones
    ///
    /// # Arguments
    ///
    /// * `done_shown` - How many finished plans to list after the open ones
    fn plan_lines(&self, done_shown: usize) -> Vec<String> {
        let (open, done): (Vec<&PlanProgress>, Vec<&PlanProgress>) = self
            .view
            .plans
            .iter()
            .partition(|plan| plan.phase != "done");
        let mut lines = Vec::new();
        if open.is_empty() {
            lines.push("no open plans".to_string());
        } else {
            lines.push("open plans".to_string());
            lines.extend(open.iter().map(|plan| format!("  {}", plan_line(plan))));
        }
        // the finished ones newest last, as the server orders them
        if done_shown > 0 && !done.is_empty() {
            lines.push("finished plans".to_string());
            let skip = done.len().saturating_sub(done_shown);
            lines.extend(done.iter().skip(skip).map(|plan| format!("  {}", plan_line(plan))));
        }
        lines
    }
}

/// One plan's progress as one line
///
/// # Arguments
///
/// * `plan` - The plan
#[must_use]
pub fn plan_line(plan: &PlanProgress) -> String {
    // what it is and how many of its steps are through
    let mut line = format!(
        "{} {} {} {}/{} moved",
        short(&plan.op.to_string()),
        plan.kind,
        plan.phase,
        plan.moved,
        plan.steps_total
    );
    if plan.moving + plan.pending + plan.failed > 0 {
        line.push_str(&format!(
            " ({} moving, {} pending, {} failed)",
            plan.moving, plan.pending, plan.failed
        ));
    }
    // its bytes, its time and its pace
    line.push_str(&format!(
        "  {} of {}",
        bytes(plan.bytes_moved),
        bytes(plan.bytes_planned)
    ));
    if plan.bytes_streamed > 0 {
        line.push_str(&format!(" (streamed {})", bytes(plan.bytes_streamed)));
    }
    if let Some(elapsed) = plan.elapsed_ms {
        line.push_str(&format!("  elapsed {}", duration(elapsed)));
    }
    if let Some(mean) = plan.mean_step_ms {
        line.push_str(&format!("  {}/step", duration(mean)));
    }
    if let Some(avg) = plan.throughput_avg_bps {
        line.push_str(&format!("  avg {}", byte_rate(avg)));
    }
    if let Some(now) = plan.throughput_now_bps {
        line.push_str(&format!("  now {}", byte_rate(now)));
    }
    if let Some(eta) = plan.eta_ms {
        line.push_str(&format!("  eta {}", duration(eta)));
    }
    if let Some(outcome) = &plan.outcome {
        line.push_str(&format!("  {outcome}"));
    }
    if let Some(blocked) = &plan.blocked {
        line.push_str(&format!(" - blocked: {blocked}"));
    }
    line
}

/// A member's figures, if they are current enough for their rates to be read
///
/// # Arguments
///
/// * `member` - The member
fn live(member: &MemberStats) -> Option<&NodeStats> {
    member.stats.as_ref().filter(|_| !member.stale)
}

/// A member's state, with maintenance said
///
/// # Arguments
///
/// * `member` - The member
fn state(member: &MemberStats) -> String {
    if member.maintenance {
        format!("{} (m)", member.state)
    } else {
        member.state.clone()
    }
}

/// How old a member's figures are, or a dash when none are held
///
/// # Arguments
///
/// * `member` - The member
fn age(member: &MemberStats) -> String {
    match member.report_age_ms {
        Some(ms) if member.stale => format!("{}s!", ms / 1000),
        Some(ms) => format!("{:.1}s", ms as f64 / 1000.0),
        None => "-".to_string(),
    }
}

/// An id's first eight characters, which is how the tab names a node or an operation
///
/// # Arguments
///
/// * `id` - The id
fn short(id: &str) -> String {
    id.chars().take(8).collect()
}

/// A rate per second as a short figure
///
/// # Arguments
///
/// * `value` - The rate
#[must_use]
pub fn rate(value: f64) -> String {
    // thousands, millions and billions past a thousand, a decimal below ten
    const UNITS: [(f64, &str); 3] = [(1e9, "G"), (1e6, "M"), (1e3, "k")];
    for (scale, unit) in UNITS {
        if value >= scale {
            return format!("{:.1}{unit}", value / scale);
        }
    }
    if value >= 10.0 || value == 0.0 {
        format!("{value:.0}")
    } else {
        format!("{value:.1}")
    }
}

/// A byte rate per second as a short figure
///
/// # Arguments
///
/// * `value` - The rate
#[must_use]
pub fn byte_rate(value: f64) -> String {
    format!("{}/s", bytes(value.max(0.0) as u64))
}

/// Three windows of a rate as `10s/1m/5m`
///
/// # Arguments
///
/// * `rates` - The windows
fn rates(rates: &Rates) -> String {
    format!("{}/{}/{}", rate(rates.r10s), rate(rates.r1m), rate(rates.r5m))
}

/// Three windows of a byte rate as `10s/1m/5m`
///
/// # Arguments
///
/// * `rates` - The windows
fn byte_rates(rates: &Rates) -> String {
    format!(
        "{}/{}/{}",
        bytes(rates.r10s.max(0.0) as u64),
        bytes(rates.r1m.max(0.0) as u64),
        bytes(rates.r5m.max(0.0) as u64)
    )
}

/// Milliseconds as a short duration
///
/// # Arguments
///
/// * `ms` - The milliseconds
#[must_use]
pub fn duration(ms: u64) -> String {
    // seconds under a minute, minutes and seconds under an hour, hours and minutes past it
    let secs = ms / 1000;
    if secs < 60 {
        format!("{:.1}s", ms as f64 / 1000.0)
    } else if secs < 3600 {
        format!("{}m{:02}s", secs / 60, secs % 60)
    } else {
        format!("{}h{:02}m", secs / 3600, (secs % 3600) / 60)
    }
}

/// Read a node's `Stats` answer
///
/// # Arguments
///
/// * `shoal` - The client to ask through
/// * `table` - The table to narrow the figures to, if one
///
/// # Errors
///
/// When the read fails or is refused, as the text to show.
pub async fn read_stats<S>(
    shoal: &Arc<Shoal<S>>,
    table: Option<&str>,
) -> Result<ClusterStatsView, String>
where
    S: QuerySupport + Send + Sync + 'static,
{
    // one read, answered as the json the node built for it
    let response = shoal
        .admin(&AdminRequest {
            op: Uuid::new_v4(),
            expected_version: 0,
            kind: AdminKind::Stats {
                table: table.map(str::to_string),
            },
        })
        .await
        .map_err(|error| format!("stats: {error:?}"))?;
    match response.outcome {
        Ok(AdminOutcome::Read(value)) => shoal::serde_json::from_value(value)
            .map_err(|error| format!("stats answered a frame this build cannot read: {error}")),
        Ok(other) => Err(format!("stats answered {other:?}")),
        Err(error) => Err(format!("stats: {} ({:?})", error.msg, error.code())),
    }
}

/// Read the `Stats` answer that holds every member's figures: the leader's
///
/// The node the client reached is asked first unless a leader's client from an earlier poll
/// still answers as the leader. A node that is not the leader names the leader's client
/// address, which is dialed and asked instead and kept for the next poll; if that fails the
/// answer is the node's own, with a note saying why.
///
/// # Arguments
///
/// * `shoal` - The client the operator reached the cluster through
/// * `table` - The table to narrow the figures to, if one
/// * `cache` - The leader's client from an earlier poll, by its address
/// * `dial` - How to reach a node at a client address
///
/// # Errors
///
/// When the node the client reached cannot be read at all.
pub async fn leader_stats<S, D, F>(
    shoal: &Arc<Shoal<S>>,
    table: Option<&str>,
    cache: &mut Option<(String, Arc<Shoal<S>>)>,
    dial: D,
) -> Result<StatsModel, String>
where
    S: QuerySupport + Send + Sync + 'static,
    D: FnOnce(String) -> F,
    F: Future<Output = Result<Arc<Shoal<S>>, String>>,
{
    // the leader from the last poll is asked first, and forgotten once it is not the leader
    if let Some((_, leader)) = cache.as_ref() {
        match read_stats(leader, table).await {
            Ok(view) if view.is_leader_view() => return Ok(StatsModel::new(view, None)),
            _ => *cache = None,
        }
    }
    // then the node the operator reached, which may be the leader
    let view = read_stats(shoal, table).await?;
    if view.is_leader_view() {
        return Ok(StatsModel::new(view, None));
    }
    // a follower names the leader's client address, which is asked instead
    let Some(addr) = view.leader_client.clone() else {
        return Ok(StatsModel::new(view, Some("no leader is known".to_string())));
    };
    let leader = match dial(addr.clone()).await {
        Ok(leader) => leader,
        Err(error) => {
            let note = format!("the leader at {addr} could not be reached: {error}");
            return Ok(StatsModel::new(view, Some(note)));
        }
    };
    match read_stats(&leader, table).await {
        Ok(leader_view) if leader_view.is_leader_view() => {
            *cache = Some((addr, leader));
            Ok(StatsModel::new(leader_view, None))
        }
        Ok(_) => Ok(StatsModel::new(
            view,
            Some(format!("{addr} no longer leads; asking again next poll")),
        )),
        Err(error) => Ok(StatsModel::new(
            view,
            Some(format!("the leader at {addr} could not be read: {error}")),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shoal::serde_json::json;
    use shoal::shared::identity::NodeId;

    /// A server's frame for two members, one in maintenance with no figures, and a plan
    fn frame() -> shoal::serde_json::Value {
        let a = "11111111-1111-1111-1111-111111111111";
        let b = "22222222-2222-2222-2222-222222222222";
        json!({
            "source": "leader",
            "answered_by": a,
            "leader": a,
            "leader_client": "10.0.0.1:12000",
            "version": 31,
            "at_ms": 1000,
            "members": [
                {
                    "node": a, "client": "10.0.0.1:12000", "role": "voter", "health": "up",
                    "phase": "member", "state": "up", "report_age_ms": 400,
                    "stats": {
                        "node": a,
                        "tables": [
                            { "table": "Note", "partitions": 90, "partitions_led": 30,
                              "bytes": 9000, "bytes_led": 3000,
                              "led": { "inserts": { "r10s": 120.0, "r1m": 100.0, "r5m": 80.0 } } },
                            { "table": "Row", "partitions": 0, "groups": 3,
                              "led": { "inserts": { "r10s": 5.0 } } }
                        ],
                        "total": {
                            "groups": 6, "groups_led": 2, "tablets": 4096, "tablets_led": 1365,
                            "partitions": 90, "partitions_led": 30, "bytes": 9000,
                            "bytes_led": 3000,
                            "applied": { "inserts": { "r10s": 360.0, "r1m": 300.0, "r5m": 240.0 } },
                            "led": { "inserts": { "r10s": 125.0, "r1m": 100.0, "r5m": 80.0 } }
                        },
                        "stream_sent": { "r10s": 2048.0, "r1m": 1024.0 },
                        "free_bytes": 1073741824
                    }
                },
                {
                    "node": b, "client": "10.0.0.2:12000", "role": "voter", "health": "down",
                    "phase": "member", "state": "down", "maintenance": true,
                    "grace_remaining_ms": 60000
                }
            ],
            "plans": [
                { "op": "33333333-3333-3333-3333-333333333333", "kind": "rebalance",
                  "phase": "running", "steps_total": 4, "moving": 1, "moved": 2, "pending": 1,
                  "bytes_planned": 4096, "bytes_moved": 2048, "elapsed_ms": 125000,
                  "throughput_now_bps": 1024.0, "eta_ms": 61000 },
                { "op": "44444444-4444-4444-4444-444444444444", "kind": "decommission",
                  "phase": "done", "steps_total": 1, "moved": 1, "outcome": "completed" }
            ]
        })
    }

    /// The full view reads a server frame: the cluster's rows once, the members, the tables and
    /// the plans, open before finished
    #[test]
    fn the_stats_model_reads_a_server_frame() {
        // a frame as the leader writes it
        let view: ClusterStatsView =
            shoal::serde_json::from_value(frame()).expect("a server frame decodes");
        let model = StatsModel::new(view, None);
        let lines = model.render_lines();
        let text = lines.join("\n");
        // the cluster's writes through the leaders, and its partitions once per row
        assert!(text.contains("insert 125/100/80"), "{text}");
        assert!(text.contains("cluster holds 30 partitions"), "{text}");
        // the member with figures, and the one in maintenance without
        let row = |node: &str| {
            lines
                .iter()
                .find(|line| line.starts_with(node))
                .cloned()
                .unwrap_or_default()
        };
        let a = row("11111111");
        assert!(a.contains(" up "), "{text}");
        assert!(a.contains("90/30"), "{text}");
        assert!(a.contains("4096/1365"), "{text}");
        let b = row("22222222");
        assert!(b.contains("down (m)"), "{text}");
        // two tables are listed by name
        assert!(text.contains("Note"), "{text}");
        assert!(text.contains("Row"), "{text}");
        // the open plan before the finished one, with its pace and estimate
        let open = lines.iter().position(|line| line.contains("33333333")).expect("open");
        let done = lines.iter().position(|line| line.contains("44444444")).expect("done");
        assert!(open < done);
        assert!(lines[open].contains("2/4 moved (1 moving, 1 pending, 0 failed)"), "{text}");
        assert!(lines[open].contains("elapsed 2m05s"), "{text}");
        assert!(lines[open].contains("eta 1m01s"), "{text}");
        // the compact lines keep only the open plan
        let compact = model.compact_lines().join("\n");
        assert!(compact.contains("33333333"), "{compact}");
        assert!(!compact.contains("44444444"), "{compact}");
        assert!(compact.contains("360"), "{compact}");
    }

    /// A view from a follower says whose figures it holds, and an empty frame still draws
    #[test]
    fn a_local_view_says_so_and_an_empty_one_draws() {
        // a follower's answer naming the leader
        let node = NodeId(Uuid::new_v4());
        let leader = NodeId(Uuid::new_v4());
        let view: ClusterStatsView = shoal::serde_json::from_value(json!({
            "source": "local", "answered_by": node, "leader": leader,
            "leader_client": "10.0.0.9:12000"
        }))
        .expect("a bare frame decodes");
        let model = StatsModel::new(view, Some("the leader could not be reached".to_string()));
        let text = model.render_lines().join("\n");
        assert!(text.contains("local view"), "{text}");
        assert!(text.contains("10.0.0.9:12000"), "{text}");
        assert!(text.contains("could not be reached"), "{text}");
        assert!(text.contains("no open plans"), "{text}");
    }

    /// A node is only asked for the figures when its `Members` frame says it answers them,
    /// since a build from before F52 closes the connection on a kind it cannot decode
    #[test]
    fn stats_are_asked_only_of_a_node_that_answers_them() {
        use crate::cluster::ClusterModel;
        // a frame from before F52 carries no admin reads
        let empty = json!({});
        let older = ClusterModel::from_frames(
            &json!({ "cluster": "c", "members": [] }),
            &empty,
            &empty,
            &empty,
            &empty,
            &empty,
        );
        assert!(!older.answers("stats"));
        // one from F52 on lists the read
        let newer = ClusterModel::from_frames(
            &json!({ "cluster": "c", "members": [], "admin_reads": ["stats"] }),
            &empty,
            &empty,
            &empty,
            &empty,
            &empty,
        );
        assert!(newer.answers("stats"));
        // and the lines a poll read are drawn under the model
        let mut drawn = newer.clone();
        drawn.stats = vec!["stats from here".to_string()];
        assert_eq!(
            drawn.render_lines().last().map(String::as_str),
            Some("stats from here")
        );
    }

    /// The short figures read the way an operator expects
    #[test]
    fn figures_are_short() {
        // rates scale by thousands and keep a decimal under ten
        assert_eq!(rate(0.0), "0");
        assert_eq!(rate(2.5), "2.5");
        assert_eq!(rate(250.0), "250");
        assert_eq!(rate(12_500.0), "12.5k");
        assert_eq!(rate(3_000_000.0), "3.0M");
        // durations by their largest unit
        assert_eq!(duration(1_500), "1.5s");
        assert_eq!(duration(125_000), "2m05s");
        assert_eq!(duration(7_380_000), "2h03m");
        // byte rates by the binary units the tab uses
        assert_eq!(byte_rate(2048.0), "2.0KiB/s");
    }
}
