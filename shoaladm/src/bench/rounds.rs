//! What each member's strong reads waited for while a run ran, and how long their rounds' appends
//! took ([F99](../../../docs/src/features/strong-read-rounds.md))
//!
//! A member answers the `Replication` admin read with its own shards' figures folded, every one a
//! count since the member started. So the bench asks every member, each as the admin, just before
//! a run's clock starts and again once its last answer is in, the two reads its device counters
//! are taken beside, and keeps what each member's figures gained. A member that does not answer
//! either read is left out of the run's figures: they are a diagnosis, and no run is refused for
//! want of one. Reading the answer is a pure function, so it is tested without a cluster.

use rkyv::Archive;
use serde_json::Value;
use shoal::Shoal;
use shoal::shared::protocol::admin::{AdminKind, AdminOutcome, AdminRequest};
use shoal::shared::traits::QuerySupport;
use shoal_loadgen::results::MemberRounds;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

use crate::deploy::Deployment;

/// How long a member is given to connect, or to answer one read, before it is left out
const MEMBER_TIMEOUT: Duration = Duration::from_secs(10);

/// A client of every deployed member as the admin, by the member's name, leaving out a member
/// that does not answer
///
/// # Arguments
///
/// * `deployment` - The cluster the bench drives
pub async fn members<S>(deployment: &Deployment) -> Vec<(String, Arc<Shoal<S>>)>
where
    S: QuerySupport + Send + Sync + 'static,
    for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    // no record, no members to read
    let Ok(record) = deployment.state.record() else {
        return Vec::new();
    };
    let mut members = Vec::with_capacity(record.nodes.len());
    for (name, node) in &record.nodes {
        // each member at its own client address, as the admin, which the read needs
        let Ok(address) = node.address.parse::<std::net::IpAddr>() else {
            continue;
        };
        let addr = crate::deploy::inventory::socket(address, deployment.inventory.ports.client);
        if let Ok(shoal) = deployment
            .connect_as_admin::<S>(&addr, Instant::now() + MEMBER_TIMEOUT)
            .await
        {
            members.push((name.clone(), shoal));
        }
    }
    members
}

/// Every member's figures so far, by name, leaving out a member that does not answer
///
/// # Arguments
///
/// * `members` - A client of every member as the admin, by name
pub async fn read<S>(members: &[(String, Arc<Shoal<S>>)]) -> BTreeMap<String, MemberRounds>
where
    S: QuerySupport + Send + Sync + 'static,
{
    let mut read = BTreeMap::new();
    for (name, shoal) in members {
        // the member's own report: its shards' figures, folded
        let request = AdminRequest {
            op: Uuid::new_v4(),
            expected_version: 0,
            kind: AdminKind::Replication,
        };
        let answered = tokio::time::timeout(MEMBER_TIMEOUT, shoal.admin(&request)).await;
        if let Ok(Ok(response)) = answered {
            if let Ok(AdminOutcome::Read(report)) = response.outcome {
                read.insert(name.clone(), from_report(&report));
            }
        }
    }
    read
}

/// One member's figures as its replication report gives them, every one a count since it
/// started; a build from before F99 reports no buckets, and its lists are empty
///
/// # Arguments
///
/// * `report` - The member's answer to the `Replication` read
#[must_use]
pub fn from_report(report: &Value) -> MemberRounds {
    // a count, or zero where the report has none
    let count = |value: &Value| value.as_u64().unwrap_or(0);
    // a list of counts by bucket, or none
    let buckets = |value: &Value| {
        value
            .as_array()
            .map(|list| list.iter().map(count).collect())
            .unwrap_or_default()
    };
    let reads = &report["reads"];
    let rounds = &report["rounds"];
    MemberRounds {
        barriers: count(&reads["barriers"]),
        barrier_wait_ns: count(&reads["barrier_wait_ns_total"]),
        apply_wait_ns: count(&reads["apply_wait_ns_total"]),
        barrier_buckets: buckets(&rounds["barriers"]),
        sent_buckets: buckets(&rounds["sent"]),
        answered_buckets: buckets(&rounds["answered"]),
    }
}

/// What every member's figures gained between two reads, a member read only once left out
///
/// # Arguments
///
/// * `before` - The read before the run
/// * `after` - The read after it
#[must_use]
pub fn deltas(
    before: &BTreeMap<String, MemberRounds>,
    after: &BTreeMap<String, MemberRounds>,
) -> BTreeMap<String, MemberRounds> {
    after
        .iter()
        .filter_map(|(name, now)| before.get(name).map(|then| (name.clone(), gained(now, then))))
        .collect()
}

/// What one member's figures gained since an earlier read, a count below its earlier reading
/// read whole, since that member started again and counts from zero
///
/// # Arguments
///
/// * `now` - The later read
/// * `then` - The earlier read
fn gained(now: &MemberRounds, then: &MemberRounds) -> MemberRounds {
    // one count, or all of it when it went backwards
    let one = |now: u64, then: u64| now.checked_sub(then).unwrap_or(now);
    // a list bucket by bucket, as long as the later read's
    let list = |now: &[u64], then: &[u64]| -> Vec<u64> {
        now.iter()
            .enumerate()
            .map(|(bucket, count)| one(*count, then.get(bucket).copied().unwrap_or(0)))
            .collect()
    };
    MemberRounds {
        barriers: one(now.barriers, then.barriers),
        barrier_wait_ns: one(now.barrier_wait_ns, then.barrier_wait_ns),
        apply_wait_ns: one(now.apply_wait_ns, then.apply_wait_ns),
        barrier_buckets: list(&now.barrier_buckets, &then.barrier_buckets),
        sent_buckets: list(&now.sent_buckets, &then.sent_buckets),
        answered_buckets: list(&now.answered_buckets, &then.answered_buckets),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A member's report is read for its barriers, waits and buckets, and a report from before
    /// F99 gives empty lists rather than failing
    #[test]
    fn a_report_is_read_for_its_rounds() {
        // a report as a member answers it, its other fields left out
        let report = json!({
            "groups": 12,
            "reads": {"barriers": 40, "barrier_wait_ns_total": 12_000_000, "apply_wait_ns_total": 300, "barrier_hops": 2},
            "rounds": {"barriers": [0, 0, 5, 35], "sent": [0, 80], "answered": [7]},
        });
        let read = from_report(&report);
        assert_eq!(read.barriers, 40);
        assert_eq!(read.barrier_wait_ns, 12_000_000);
        assert_eq!(read.apply_wait_ns, 300);
        assert_eq!(read.barrier_buckets, vec![0, 0, 5, 35]);
        assert_eq!(read.sent_buckets, vec![0, 80]);
        assert_eq!(read.answered_buckets, vec![7]);
        // an older build's report has reads and no rounds
        let older = from_report(&json!({"reads": {"barriers": 3}}));
        assert_eq!(older.barriers, 3);
        assert!(older.barrier_buckets.is_empty() && older.sent_buckets.is_empty() && older.answered_buckets.is_empty());
    }

    /// A run's figures are what each member gained, bucket by bucket; a member that started again
    /// counts whole, and a member read only once is left out
    #[test]
    fn a_run_keeps_what_each_member_gained() {
        let reading = |barriers: u64, buckets: Vec<u64>| MemberRounds {
            barriers,
            barrier_wait_ns: barriers * 1_000,
            apply_wait_ns: 0,
            barrier_buckets: buckets.clone(),
            sent_buckets: buckets,
            answered_buckets: Vec::new(),
        };
        let before = BTreeMap::from([
            ("europa".to_string(), reading(10, vec![1, 2])),
            ("titan".to_string(), reading(50, vec![9])),
            ("hyperion".to_string(), reading(5, vec![5])),
        ]);
        let after = BTreeMap::from([
            // grew, and gained a slower bucket
            ("europa".to_string(), reading(25, vec![3, 2, 4])),
            // started again, so its counts are read whole
            ("titan".to_string(), reading(7, vec![2])),
            // hyperion did not answer the second read
        ]);
        let gained = deltas(&before, &after);
        assert_eq!(gained.len(), 2, "a member read only once is left out");
        let europa = &gained["europa"];
        assert_eq!(europa.barriers, 15);
        assert_eq!(europa.barrier_wait_ns, 15_000);
        assert_eq!(europa.barrier_buckets, vec![2, 0, 4]);
        let titan = &gained["titan"];
        assert_eq!(titan.barriers, 7);
        assert_eq!(titan.barrier_buckets, vec![2]);
    }
}
