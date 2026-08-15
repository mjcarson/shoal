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
