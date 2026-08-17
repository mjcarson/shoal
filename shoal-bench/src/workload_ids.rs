//! Every workload's identifier, declared where the runner can read it without the engine
//!
//! The workloads themselves live behind the `workloads` feature, because they link `shoal`. The
//! runner still has to know what exists when that feature is off - `list` has to be able to say
//! what a capture would cover, and a comparison has to be able to name a workload that is missing
//! from one side. So the ids are declared here, as a plain list that costs nothing to compile.
//!
//! Two places naming the same set is how they drift, so they cannot be allowed to. The test at the
//! bottom of this file runs under the feature and asserts this list is exactly what
//! [`crate::workloads::all`] registers, in the same order. Adding a workload without adding it
//! here fails that test rather than producing a capture that silently skips it.
//!
//! These strings are the join key of every comparison, the same way criterion's `full_id` is for
//! the micro layer. **Renaming one orphans every capture taken before the rename** - the old name
//! stops appearing on the new side and the comparison reports it as missing, which is correct and
//! is also not what anybody wanted. Add and deprecate rather than rename.

/// Every workload, in the order a capture runs them
///
/// Ordered so that the cheapest and most repeatable comes first, which matches how the layers
/// themselves are ordered in [`crate::registry::Layer`].
pub const IDS: &[&str] = &[
    // the write path, which every read workload seeds itself through
    "macro/insert_unsorted",
    // the read path, as a control and its null: the same get, with and without a disk read
    "macro/get_resident",
    "macro/get_archived",
    // the fanout curve, one get over n partition keys, both arms across six key counts. this is
    // what makes a quadratic per-partition term visible as a curve rather than argued from source.
    "macro/fanout/resident/1",
    "macro/fanout/resident/2",
    "macro/fanout/resident/4",
    "macro/fanout/resident/16",
    "macro/fanout/resident/64",
    "macro/fanout/resident/256",
    "macro/fanout/evicted/1",
    "macro/fanout/evicted/2",
    "macro/fanout/evicted/4",
    "macro/fanout/evicted/16",
    "macro/fanout/evicted/64",
    "macro/fanout/evicted/256",
    // the storage free controls, appended rather than interleaved with the workloads they are
    // read against. a workload's position in this list decides the port a capture gives it, so
    // putting `macro/insert_ephemeral` next to `macro/insert_unsorted` where it reads best would
    // move every workload after it onto a different port.
    "macro/insert_ephemeral",
    "macro/get_ephemeral",
    "macro/fanout/ephemeral/1",
    "macro/fanout/ephemeral/2",
    "macro/fanout/ephemeral/4",
    "macro/fanout/ephemeral/16",
    "macro/fanout/ephemeral/64",
    "macro/fanout/ephemeral/256",
    // the four client transport modes, each at a narrow row and at a MiB one. the client is the
    // one layer whose total has never been bounded, and the size axis is what makes a per-byte
    // cost such as encryption visible at all - it disappears into the noise at 256 bytes.
    // appended for the same reason the ephemeral controls are, so no earlier workload's port moves
    "macro/transport/send_one/small",
    "macro/transport/send_one/large",
    "macro/transport/send_batched/small",
    "macro/transport/send_batched/large",
    "macro/transport/stream/small",
    "macro/transport/stream/large",
    "macro/transport/stream_unordered/small",
    "macro/transport/stream_unordered/large",
    // the same four modes at the same two widths, over a wire the kernel encrypts. the control
    // pair D4 called a precondition for taking encryption at all: a pair differs in the wire and
    // in nothing else, so the difference between them is what encryption costs.
    //
    // appended after the plaintext eight rather than interleaved with them, for the reason the
    // ephemeral controls are: a workload's position here decides its port, and interleaving would
    // move all eight plaintext arms onto different ones
    "macro/transport/tls/send_one/small",
    "macro/transport/tls/send_one/large",
    "macro/transport/tls/send_batched/small",
    "macro/transport/tls/send_batched/large",
    "macro/transport/tls/stream/small",
    "macro/transport/tls/stream/large",
    "macro/transport/tls/stream_unordered/small",
    "macro/transport/tls/stream_unordered/large",
    // the encryption sweeps, appended last for the reason the transport arms were: a workload's
    // position here decides its port, so nothing earlier may move. the depth sweep varies how many
    // queries are outstanding on one client; the client sweep varies how many independent clients
    // there are, each one query deep. every arm has a twin differing only in the wire.
    "macro/encryption/depth/plain/256/1",
    "macro/encryption/depth/plain/256/8",
    "macro/encryption/depth/plain/256/32",
    "macro/encryption/depth/plain/256/128",
    "macro/encryption/depth/plain/4096/1",
    "macro/encryption/depth/plain/4096/8",
    "macro/encryption/depth/plain/4096/32",
    "macro/encryption/depth/plain/4096/128",
    "macro/encryption/depth/plain/65536/1",
    "macro/encryption/depth/plain/65536/8",
    "macro/encryption/depth/plain/65536/32",
    "macro/encryption/depth/plain/65536/128",
    "macro/encryption/depth/plain/1048576/1",
    "macro/encryption/depth/plain/1048576/8",
    "macro/encryption/depth/plain/1048576/32",
    "macro/encryption/depth/plain/1048576/128",
    "macro/encryption/depth/tls/256/1",
    "macro/encryption/depth/tls/256/8",
    "macro/encryption/depth/tls/256/32",
    "macro/encryption/depth/tls/256/128",
    "macro/encryption/depth/tls/4096/1",
    "macro/encryption/depth/tls/4096/8",
    "macro/encryption/depth/tls/4096/32",
    "macro/encryption/depth/tls/4096/128",
    "macro/encryption/depth/tls/65536/1",
    "macro/encryption/depth/tls/65536/8",
    "macro/encryption/depth/tls/65536/32",
    "macro/encryption/depth/tls/65536/128",
    "macro/encryption/depth/tls/1048576/1",
    "macro/encryption/depth/tls/1048576/8",
    "macro/encryption/depth/tls/1048576/32",
    "macro/encryption/depth/tls/1048576/128",
    "macro/encryption/clients/plain/256/1",
    "macro/encryption/clients/plain/256/2",
    "macro/encryption/clients/plain/256/4",
    "macro/encryption/clients/plain/256/8",
    "macro/encryption/clients/plain/1048576/1",
    "macro/encryption/clients/plain/1048576/2",
    "macro/encryption/clients/plain/1048576/4",
    "macro/encryption/clients/plain/1048576/8",
    "macro/encryption/clients/tls/256/1",
    "macro/encryption/clients/tls/256/2",
    "macro/encryption/clients/tls/256/4",
    "macro/encryption/clients/tls/256/8",
    "macro/encryption/clients/tls/1048576/1",
    "macro/encryption/clients/tls/1048576/2",
    "macro/encryption/clients/tls/1048576/4",
    "macro/encryption/clients/tls/1048576/8",
    // the grid: what a read/write mixture costs, rather than what one path costs.
    // seventy four arms, appended last for the reason every block above was appended -
    // a workload's position here decides its port, so nothing earlier may move.
    //
    // the width sweep first: every width, on every table, at the reference mixture
    "macro/grid/unsorted/r50/64",
    "macro/grid/unsorted/r50/128",
    "macro/grid/unsorted/r50/512",
    "macro/grid/unsorted/r50/1024",
    "macro/grid/unsorted/r50/8192",
    "macro/grid/unsorted/r50/524288",
    "macro/grid/unsorted/r50/1048576",
    "macro/grid/unsorted/r50/4194304",
    "macro/grid/unsorted/r50/mixed_small",
    "macro/grid/unsorted/r50/mixed_mid",
    "macro/grid/unsorted/r50/mixed_large",
    "macro/grid/sorted/r50/64",
    "macro/grid/sorted/r50/128",
    "macro/grid/sorted/r50/512",
    "macro/grid/sorted/r50/1024",
    "macro/grid/sorted/r50/8192",
    "macro/grid/sorted/r50/524288",
    "macro/grid/sorted/r50/1048576",
    "macro/grid/sorted/r50/4194304",
    "macro/grid/sorted/r50/mixed_small",
    "macro/grid/sorted/r50/mixed_mid",
    "macro/grid/sorted/r50/mixed_large",
    "macro/grid/unsorted_mem/r50/64",
    "macro/grid/unsorted_mem/r50/128",
    "macro/grid/unsorted_mem/r50/512",
    "macro/grid/unsorted_mem/r50/1024",
    "macro/grid/unsorted_mem/r50/8192",
    "macro/grid/unsorted_mem/r50/524288",
    "macro/grid/unsorted_mem/r50/1048576",
    "macro/grid/unsorted_mem/r50/4194304",
    "macro/grid/unsorted_mem/r50/mixed_small",
    "macro/grid/unsorted_mem/r50/mixed_mid",
    "macro/grid/unsorted_mem/r50/mixed_large",
    "macro/grid/sorted_mem/r50/64",
    "macro/grid/sorted_mem/r50/128",
    "macro/grid/sorted_mem/r50/512",
    "macro/grid/sorted_mem/r50/1024",
    "macro/grid/sorted_mem/r50/8192",
    "macro/grid/sorted_mem/r50/524288",
    "macro/grid/sorted_mem/r50/1048576",
    "macro/grid/sorted_mem/r50/4194304",
    "macro/grid/sorted_mem/r50/mixed_small",
    "macro/grid/sorted_mem/r50/mixed_mid",
    "macro/grid/sorted_mem/r50/mixed_large",
    // then the mixture sweep: every other mixture, on every table, at the reference width
    "macro/grid/unsorted/r0/1024",
    "macro/grid/unsorted/r30/1024",
    "macro/grid/unsorted/r70/1024",
    "macro/grid/unsorted/r95/1024",
    "macro/grid/unsorted/r100/1024",
    "macro/grid/sorted/r0/1024",
    "macro/grid/sorted/r30/1024",
    "macro/grid/sorted/r70/1024",
    "macro/grid/sorted/r95/1024",
    "macro/grid/sorted/r100/1024",
    "macro/grid/unsorted_mem/r0/1024",
    "macro/grid/unsorted_mem/r30/1024",
    "macro/grid/unsorted_mem/r70/1024",
    "macro/grid/unsorted_mem/r95/1024",
    "macro/grid/unsorted_mem/r100/1024",
    "macro/grid/sorted_mem/r0/1024",
    "macro/grid/sorted_mem/r30/1024",
    "macro/grid/sorted_mem/r70/1024",
    "macro/grid/sorted_mem/r95/1024",
    "macro/grid/sorted_mem/r100/1024",
    // then the key distributions, on the persistent pair only
    "macro/skew/uniform/unsorted",
    "macro/skew/uniform/sorted",
    "macro/skew/zipfian/unsorted",
    "macro/skew/zipfian/sorted",
    "macro/skew/latest/unsorted",
    "macro/skew/latest/sorted",
    // and the load depth ladder, which is what places every arm above on its curve
    "macro/grid/depth/1",
    "macro/grid/depth/8",
    "macro/grid/depth/32",
    "macro/grid/depth/128",
    // the configuration sweeps, appended after the grid for the reason every block here is
    // appended. each is the grid's reference cell - `macro/grid/unsorted/r50/1024` - with exactly
    // one field of the server configuration moved, so the cell is what each of them is read
    // against. the section segment (`storage`, `resources`) is in the identifier so that the group
    // table and the page families can split the sweep without linking the engine.
    //
    // the storage writers first: the barrier a write waits on, then how it is buffered and queued
    "macro/conf/storage/durability/r50/fsync",
    "macro/conf/storage/durability/r50/async",
    "macro/conf/storage/latency_buffer/r50/512",
    "macro/conf/storage/latency_buffer/r50/4Ki",
    "macro/conf/storage/latency_buffer/r50/16Ki",
    "macro/conf/storage/latency_buffer/r50/64Ki",
    "macro/conf/storage/latency_buffer/r50/256Ki",
    "macro/conf/storage/latency_write_behind/r50/1",
    "macro/conf/storage/latency_write_behind/r50/8",
    "macro/conf/storage/latency_write_behind/r50/32",
    "macro/conf/storage/latency_write_behind/r50/128",
    "macro/conf/storage/latency_write_behind/r50/512",
    "macro/conf/storage/intent_log/r50/1Mi",
    "macro/conf/storage/intent_log/r50/10Mi",
    "macro/conf/storage/intent_log/r50/100Mi",
    "macro/conf/storage/intent_log/r50/1Gi",
    // the throughput writer, which item 71 in `docs/src/appendix/known-issues.md` says reaches less
    // of the engine than its name suggests. these arms are swept anyway: a flat line here is that
    // item's evidence, and an unmeasured knob would leave it an argument from reading the source
    "macro/conf/storage/throughput_buffer/r50/32Ki",
    "macro/conf/storage/throughput_buffer/r50/128Ki",
    "macro/conf/storage/throughput_buffer/r50/512Ki",
    "macro/conf/storage/throughput_buffer/r50/1Mi",
    "macro/conf/storage/throughput_write_behind/r50/1",
    "macro/conf/storage/throughput_write_behind/r50/4",
    "macro/conf/storage/throughput_write_behind/r50/16",
    // then what the server is given to run on. the two resource knobs are swept at a pure read
    // share as well, because more shards is more parallelism for reads and more fsync contention
    // for writes, and a mixture alone would blend the two
    "macro/conf/resources/shards/r50/1",
    "macro/conf/resources/shards/r50/2",
    "macro/conf/resources/shards/r50/4",
    "macro/conf/resources/shards/r50/8",
    "macro/conf/resources/shards/r50/12",
    "macro/conf/resources/shards/r100/1",
    "macro/conf/resources/shards/r100/2",
    "macro/conf/resources/shards/r100/4",
    "macro/conf/resources/shards/r100/8",
    "macro/conf/resources/shards/r100/12",
    "macro/conf/resources/memory/r50/1Mi",
    "macro/conf/resources/memory/r50/4Mi",
    "macro/conf/resources/memory/r50/16Mi",
    "macro/conf/resources/memory/r50/64Mi",
    "macro/conf/resources/memory/r50/1Gi",
    "macro/conf/resources/memory/r50/4Gi",
    "macro/conf/resources/memory/r100/1Mi",
    "macro/conf/resources/memory/r100/4Mi",
    "macro/conf/resources/memory/r100/16Mi",
    "macro/conf/resources/memory/r100/64Mi",
    "macro/conf/resources/memory/r100/1Gi",
    "macro/conf/resources/memory/r100/4Gi",
    "macro/conf/resources/frame/r50/1Mi",
    "macro/conf/resources/frame/r50/8Mi",
    "macro/conf/resources/frame/r50/64Mi",
];

/// Whether an id names a workload this build knows about
///
/// # Arguments
///
/// * `id` - The identifier to look for
pub fn is_workload(id: &str) -> bool {
    // a linear scan over a handful of short strings, which is faster than hashing them
    IDS.contains(&id)
}

#[cfg(test)]
mod tests {
    use super::IDS;

    /// No id is declared twice, since a duplicate would run a workload twice and fold it once
    #[test]
    fn every_id_is_unique() {
        let mut seen: Vec<&str> = IDS.to_vec();
        seen.sort_unstable();
        let before = seen.len();
        seen.dedup();
        assert_eq!(before, seen.len(), "a workload id is declared twice");
    }

    /// Every id carries the layer prefix that keeps one flat namespace unambiguous
    ///
    /// Criterion's ids never begin with `macro/`, so a filter that matches one of these cannot
    /// also match a micro benchmark.
    #[test]
    fn every_id_is_namespaced() {
        for id in IDS {
            assert!(
                id.starts_with("macro/"),
                "{id} does not carry its layer prefix"
            );
        }
    }

    /// The declared list is exactly what the registered workloads report, in the same order
    ///
    /// This is the whole reason two lists are tolerable. Without it, a workload added to
    /// `workloads::all` but not here would compile, never run, and never be missed.
    #[cfg(feature = "workloads")]
    #[test]
    fn the_declared_ids_are_the_registered_ones() {
        let registered: Vec<&str> = crate::workloads::all()
            .iter()
            .map(|workload| workload.id())
            .collect();
        assert_eq!(
            registered, IDS,
            "workload_ids::IDS has drifted from workloads::all()"
        );
    }
}
