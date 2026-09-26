//! The cluster tab: what a cluster looks like to an operator, and the operations they run on it
//!
//! A cluster tab polls the node its client reached for the admin frames every second -
//! `Members`, `Readiness`, `Replication`, `Plans`, `Backups`, `Recoveries` - and draws one
//! model built from them ([`model::ClusterModel`]): every member with its phase, health,
//! grace, weight, bytes and wire version; the desired and active factor; the under-replicated
//! sets; the groups this node hosts and leads and its widest lag; what is installing and
//! quarantined; every open plan with its blocked reason; the recoveries; the activated wire.
//! Its command line takes an operation ([`actions::ClusterAction`]), renders a preview naming
//! the identity it touches, what will move and the boundary that cannot be undone, submits it
//! on a second `Enter`, and follows the record by operation id until it is done
//! ([F50](../../docs/src/features/cluster-operations.md)).
//!
//! Since [F52](../../docs/src/features/cluster-stats.md) a node that advertises `stats` in its
//! `Members` frame is also asked for every member's figures, from the leader, and the tab draws
//! them under the model ([`stats::StatsModel`]).
//!
//! Nothing here draws: the model renders itself to lines and the app puts them in the tab's
//! content, so every decision can be tested without a terminal.

pub mod actions;
pub mod model;
pub mod stats;

pub use actions::{ClusterAction, Follow};
pub use model::{ClusterModel, MemberRow};
pub use stats::StatsModel;

use shoal::Shoal;
use shoal::shared::traits::QuerySupport;
use std::sync::Arc;
use uuid::Uuid;

/// Poll the frames a cluster tab draws, and build the model from them
///
/// Shared by the tab's poller and `shoalctl cluster`, which waits on the same model
/// ([F51](../../docs/src/features/cluster-deployment.md)).
///
/// # Arguments
///
/// * `shoal` - The client to poll through
pub async fn poll<S>(shoal: &Arc<Shoal<S>>) -> Result<ClusterModel, String>
where
    S: QuerySupport + Send + Sync + 'static,
{
    use shoal::shared::protocol::admin::{AdminKind, AdminOutcome, AdminRequest};
    // one read per frame, each answered as the json the node built for it
    let mut frames = Vec::with_capacity(6);
    for kind in [
        AdminKind::Members,
        AdminKind::Readiness,
        AdminKind::Replication,
        AdminKind::Plans,
        AdminKind::Backups,
        AdminKind::Recoveries,
    ] {
        let name = kind.name();
        let response = shoal
            .admin(&AdminRequest {
                op: Uuid::new_v4(),
                expected_version: 0,
                kind,
            })
            .await
            .map_err(|error| format!("{name}: {error:?}"))?;
        match response.outcome {
            Ok(AdminOutcome::Read(value)) => frames.push(value),
            Ok(other) => return Err(format!("{name} answered {other:?}")),
            Err(error) => return Err(format!("{name}: {} ({:?})", error.msg, error.code())),
        }
    }
    Ok(ClusterModel::from_frames(
        &frames[0], &frames[1], &frames[2], &frames[3], &frames[4], &frames[5],
    ))
}

/// Poll the frames a cluster tab draws, and the leader's figures under them when the node
/// answers them
///
/// A failed figures read never fails the poll: its reason is drawn where the figures would be.
///
/// # Arguments
///
/// * `shoal` - The client to poll through
/// * `leader` - The leader's client from an earlier poll, by its address
/// * `dial` - How to reach a node at a client address
pub async fn poll_with_stats<S, D, F>(
    shoal: &Arc<Shoal<S>>,
    leader: &mut Option<(String, Arc<Shoal<S>>)>,
    dial: D,
) -> Result<ClusterModel, String>
where
    S: QuerySupport + Send + Sync + 'static,
    D: FnOnce(String) -> F,
    F: std::future::Future<Output = Result<Arc<Shoal<S>>, String>>,
{
    // the model as every build draws it
    let mut model = poll(shoal).await?;
    // and the figures, only from a node that says it answers them
    if model.answers("stats") {
        model.stats = match stats::leader_stats(shoal, None, leader, dial).await {
            Ok(figures) => figures.compact_lines(),
            Err(error) => vec![String::new(), format!("stats could not be read: {error}")],
        };
    }
    Ok(model)
}
