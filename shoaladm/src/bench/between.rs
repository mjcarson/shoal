//! What is done around an object arm when its objects are deleted rather than the cluster reset
//! ([F94](../../../docs/src/features/bench-object-endpoints.md))
//!
//! A store the bench did not deploy cannot be wiped and bootstrapped between arms, so every
//! store, Shoal's included, is put back the same way when a capture is to be read beside one:
//! before a run, every host's page cache dropped if the spec says so and every root checked for
//! room; after it, a sample of what it put read back, everything it put deleted, and the store's
//! own settle command run until it gives back what was deleted. The driver is generic over its
//! endpoint, so this is too, and both orchestrators call it.

use std::time::Instant;

use color_eyre::eyre::bail;
use shoal_loadgen::objects::{ObjectArm, ObjectDriver, ObjectEndpoint};
use shoal_loadgen::progress::{BenchEvent, Phase, Progress};
use shoal_loadgen::results::{CleanupFacts, VerifyFacts};
use shoal_loadgen::spec::BenchSpec;

use super::devices::{self, HostRoots, Snapshot};

/// The most objects of one arm read back after it, when its objects are deleted
pub const VERIFY_SAMPLE_OBJECTS: usize = 256;

/// The most bytes of one arm read back after it
pub const VERIFY_SAMPLE_BYTES: u64 = 1 << 30;

/// How many deletes are in flight at once between arms
pub const CLEANUP_CONCURRENCY: usize = 32;

/// The fullest a root's filesystem may be for an arm to start, when no other share is asked for
pub const DEFAULT_MAX_USED: f64 = 80.0;

/// Ready every host for a run and read its devices before it: the page caches dropped when the
/// spec asks, then the read, then the refusal of a root too full to measure on
///
/// Returns the read, or why the hosts could not be read; a run whose caches were asked dropped
/// and could not be is refused, since its reads would not start where every other store's did.
///
/// # Arguments
///
/// * `hosts` - The hosts, or none for a node started by hand
/// * `spec` - The spec
/// * `max_used` - The fullest a root may be, if a share is to be held
///
/// # Errors
///
/// When the caches could not be dropped, or a root is too full.
pub async fn before_run(
    hosts: Option<&[HostRoots]>,
    spec: &BenchSpec,
    max_used: Option<f64>,
) -> color_eyre::Result<Result<Vec<Snapshot>, String>> {
    // a node started by hand comes with no inventory, so no host to read
    let Some(hosts) = hosts else {
        return Ok(Err("no inventory or host was given (--addr, or no --host), so no host's devices can be read".to_string()));
    };
    // every host's cache, before anything is read, so the read is no part of it
    if spec.drop_caches {
        if let Err(error) = devices::drop_caches(hosts).await {
            bail!("the page caches could not be dropped before the run: {error}");
        }
    }
    let before = devices::read_all(hosts).await;
    // a root nearly full measures a full device, not the store
    if let (Ok(before), Some(max_used)) = (&before, max_used) {
        if let Some(refusal) = devices::refuse_full(hosts, before, max_used) {
            bail!("{refusal}");
        }
    }
    Ok(before)
}

/// Read back a sample of what an object arm put, delete all of it, and let the store settle
///
/// Returns the read back, if the arm wrote and acknowledgements are read back, and what the
/// cleanup did.
///
/// # Arguments
///
/// * `objects` - The object driver
/// * `object` - The arm
/// * `spec` - The spec
/// * `settle` - The store's command that gives back what was deleted, run locally under `sh -c`
/// * `progress` - Where the phases are said
pub async fn after_object_arm<E: ObjectEndpoint>(
    objects: &ObjectDriver<E>,
    object: &ObjectArm,
    spec: &BenchSpec,
    settle: Option<&str>,
    progress: &Progress,
) -> (Option<VerifyFacts>, CleanupFacts) {
    // a sample of what the arm put, past the preload, read back byte for byte
    let verify = if object.mix.writes() && spec.verify_acks {
        Some(
            objects
                .verify_new(
                    object.size,
                    VERIFY_SAMPLE_OBJECTS,
                    VERIFY_SAMPLE_BYTES,
                    object.concurrency.max(8),
                    progress,
                )
                .await,
        )
    } else {
        None
    };
    // everything it put deleted, then the store's own settle, when there was anything to give back
    let mut cleanup = objects.clean(CLEANUP_CONCURRENCY, progress).await;
    if let Some(command) = settle.filter(|_| cleanup.deleted > 0) {
        progress.send(BenchEvent::Phase(Phase::Settle));
        let (secs, status) = run_settle(command).await;
        cleanup.settle_secs = Some(secs);
        cleanup.settle_status = Some(status);
        if status != 0 {
            progress.log(format!("the settle command exited {status}: {command}"));
        }
    }
    (verify, cleanup)
}

/// Run a store's settle command locally and say how long it took and what it exited with
///
/// # Arguments
///
/// * `command` - The command, run under `sh -c`
async fn run_settle(command: &str) -> (f64, i32) {
    // off the runtime, since it blocks until the store has given its space back
    let command = command.to_string();
    let started = Instant::now();
    let status = tokio::task::spawn_blocking(move || {
        std::process::Command::new("sh")
            .arg("-c")
            .arg(&command)
            .status()
            .map_or(-1, |status| status.code().unwrap_or(-1))
    })
    .await
    .unwrap_or(-1);
    (started.elapsed().as_secs_f64(), status)
}

#[cfg(test)]
mod tests {
    use super::run_settle;

    /// A settle command is timed and its status kept, a failing one too
    #[tokio::test]
    async fn a_settle_is_timed_and_its_status_kept() {
        let (secs, status) = run_settle("sleep 0.2").await;
        assert!(secs >= 0.2 && status == 0, "{secs} {status}");
        assert_eq!(run_settle("exit 3").await.1, 3);
    }
}
