//! The purpose built benchmarks, and the one list of them
//!
//! Everything here links `shoal`, which is why it sits behind the `workloads` feature and why it
//! is compiled into a second binary rather than into the runner. See `shoal-bench/Cargo.toml`.
//!
//! # Why these live in this crate at all
//!
//! The workload they replace was `shoal/examples/tmdb.rs`, which was an example and a load
//! generator at once and was not very good at being either. Moving the load generator here buys
//! two things that a benchmark kept anywhere else does not have:
//!
//! - **It cannot go stale.** These are a workspace member's targets, so
//!   `cargo check --workspace --all-targets` compiles every one of them against the real engine
//!   API. A query that stops being expressible is a compile error, not a benchmark that quietly
//!   measures something else.
//! - **There is no mirror to keep.** The artifact these write is
//!   [`crate::model::macro_layer`]'s own structs, so the writer and the reader cannot disagree.
//!   The version 1 artifact was a hand kept copy of a struct in another crate, with a catch-all
//!   field and a test over every committed file to notice when the copy drifted.
//!
//! # Adding one
//!
//! Write the workload, add it to [`all`], and add its identifier to [`crate::workload_ids::IDS`].
//! A test asserts those two agree, so forgetting the second is a test failure rather than a
//! workload that is never run.

pub mod conf_sweep;
pub mod fanout;
pub mod fanout_ephemeral;
pub mod grid;
pub mod get_ephemeral;
pub mod encryption;
pub mod harness;
pub mod insert_ephemeral;
pub mod insert_unsorted;
pub mod keyed_get;
pub mod schema;
// the client half of a run's stage records, which every driver gathers the same way. always
// compiled, unlike the report builder below it: it is a zero sized type without the feature, and
// that is what lets a `Measurement` hold one and a driver call into it with no `#[cfg]` of its own
pub mod stage_log;
// only a build with the feature has any stage records to report on. the artifact it writes is
// modelled in `crate::model::stages`, which is always compiled, because the runner has to read a
// committed report whether or not this build could have produced one.
#[cfg(feature = "stage-profile")]
pub mod stages;
pub mod transport;
pub mod workload;

use workload::Workload;

/// Every workload, in the order a capture runs them
///
/// A plain list rather than a registry built by a macro or by `inventory`: it is the single place
/// a reviewer looks to see what exists, and it needs no dependency that the workspace lockfile
/// does not already carry.
///
/// The order is deliberate and runs from the cheapest and most repeatable to the most expensive:
/// the write path first, since every read workload seeds itself through it, then the keyed get
/// pair, then the fanout curve, which is twelve workloads and the longest phase of a capture.
///
/// The ephemeral workloads come last, after every workload they are a control for. That is not
/// about cost — they are the cheapest things here, having no disk to wait on — but about
/// identifiers: a workload's position in this list decides the port a capture gives it, so
/// appending is what keeps every workload declared before them on the port it has always had.
pub fn all() -> Vec<Box<dyn Workload>> {
    let mut built: Vec<Box<dyn Workload>> = vec![
        Box::new(insert_unsorted::InsertUnsorted),
        Box::new(keyed_get::KeyedGet {
            residency: keyed_get::Residency::Resident,
        }),
        Box::new(keyed_get::KeyedGet {
            residency: keyed_get::Residency::Archived,
        }),
    ];
    // the curve mints its own identifiers, one per arm per key count
    built.extend(
        fanout::Fanout::all()
            .into_iter()
            .map(|workload| Box::new(workload) as Box<dyn Workload>),
    );
    // the storage free controls, each paired with the workload above it that it is read against
    built.push(Box::new(insert_ephemeral::InsertEphemeral));
    built.push(Box::new(get_ephemeral::GetEphemeral));
    built.extend(
        fanout_ephemeral::FanoutEphemeral::all()
            .into_iter()
            .map(|workload| Box::new(workload) as Box<dyn Workload>),
    );
    // the client transport modes, appended for the same reason the ephemeral controls are: every
    // workload declared above them keeps the port it has always had
    built.extend(
        transport::Transport::all()
            .into_iter()
            .map(|workload| Box::new(workload) as Box<dyn Workload>),
    );
    // the encryption sweeps, appended last and for the same reason again. they are the widest
    // thing here - forty eight arms - and they are what says how the cost of encryption behaves
    // across row width, load depth and client count rather than merely whether it exists
    built.extend(
        encryption::Encryption::all()
            .into_iter()
            .map(|workload| Box::new(workload) as Box<dyn Workload>),
    );
    // the grid, appended last and for the reason every block above it was: it is seventy four arms
    // and it is the widest thing here, so declaring it anywhere but the end would re-port every
    // workload that came before it. it is also the most expensive phase of a capture, which is the
    // other reason it belongs at the end - the cheap, isolating workloads are answered first
    built.extend(
        grid::Grid::all()
            .into_iter()
            .map(|workload| Box::new(workload) as Box<dyn Workload>),
    );
    // the configuration sweeps, appended after the grid because they are the newest block and for
    // the reason every block above them was appended. they are the grid's reference cell run under
    // forty six different server configurations, so they read after the grid as well as porting
    // after it: the grid says what a mixture costs, and these say what one setting of the
    // configuration that mixture ran under was worth
    built.extend(
        conf_sweep::all()
            .into_iter()
            .map(|workload| Box::new(workload) as Box<dyn Workload>),
    );
    built
}

/// The workload with an identifier, if there is one
///
/// # Arguments
///
/// * `id` - The identifier to look for
pub fn find(id: &str) -> Option<Box<dyn Workload>> {
    // a linear scan over a handful of workloads, which is not worth a map
    all().into_iter().find(|workload| workload.id() == id)
}

#[cfg(test)]
mod tests {
    use super::{all, find};

    /// Every registered workload can be found by the identifier it reports
    #[test]
    fn every_workload_is_findable_by_its_id() {
        for workload in all() {
            let found = find(workload.id()).expect("a registered workload was not findable");
            assert_eq!(found.id(), workload.id());
        }
    }

    /// An unknown identifier finds nothing rather than falling back to something
    #[test]
    fn an_unknown_id_finds_nothing() {
        assert!(find("macro/does_not_exist").is_none());
    }

    /// The runner's copy of who opts into attribution is what the workloads actually say
    ///
    /// Two lists naming one set is how they drift, the same problem [`crate::workload_ids::IDS`]
    /// has. The runner cannot ask a workload anything - it builds with no engine at all - so it
    /// keeps its own copy and this is what stops the copy going stale. Both layers are checked,
    /// because they stopped sharing a list the moment the stage layer needed three workloads and
    /// the hotpath layer still wanted one.
    ///
    /// The doc comments on both constants claimed this test existed before it did.
    #[test]
    fn the_runners_copy_of_the_profiled_workloads_is_current() {
        let registered = all();
        let hotpath: Vec<&str> = registered
            .iter()
            .filter(|workload| workload.profiles())
            .map(|workload| workload.id())
            .collect();
        assert_eq!(
            hotpath,
            crate::registry::PROFILED_WORKLOADS.to_vec(),
            "PROFILED_WORKLOADS is not what the workloads say"
        );
        let staged: Vec<&str> = registered
            .iter()
            .filter(|workload| workload.stage_profiles())
            .map(|workload| workload.id())
            .collect();
        assert_eq!(
            staged,
            crate::registry::STAGED_WORKLOADS.to_vec(),
            "STAGED_WORKLOADS is not what the workloads say"
        );
    }

    /// Every workload says what it is for, since `list` prints it
    #[test]
    fn every_workload_has_a_summary() {
        for workload in all() {
            assert!(
                !workload.summary().is_empty(),
                "{} has no summary",
                workload.id()
            );
        }
    }
}
