//! What a node has left on each storage root it writes to
//!
//! A node reports the size and the free bytes of every root its configuration names, beside
//! what it writes under each, on every status report; the control leader keeps them in memory
//! for the planner and never commits them ([F82](../../../docs/src/features/root-capacity.md)).
//! A root is a directory, and its figures are its filesystem's, so two roots on one filesystem
//! report the same figures and the same device: a reader that adds roots up counts a device
//! once.
//!
//! ```json
//! {"path": "/hdd/shoal", "device": "8:1", "size_bytes": 14000519643136,
//!  "free_bytes": 13731226726400, "uses": ["throughput"]}
//! ```
//!
//! # Invariants
//!
//! **Every root keeps the reserve free, and a root that takes a moved set's bytes keeps those
//! too.** The planner and the receiver of a snapshot stream judge a move by the same rule
//! ([`RootCapacity::need`]), so a root the planner thought had room is one the receiver finds
//! room on, a report's age apart.
//!
//! **A use this build does not know is read, not refused**, so a root a later build names
//! another way (a device, say) never makes a report undecodable to a leader on this one.

use serde::{Deserialize, Serialize};

/// What a node writes under one of its storage roots
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RootUse {
    /// The node's own files: its marker, its WAL, its control log, and the snapshots it receives
    Primary,
    /// Tables' intent logs and archive maps
    Latency,
    /// Tables' archives
    Throughput,
    /// A use a later build names and this one does not
    #[serde(other)]
    Unknown,
}

impl RootUse {
    /// The use's name, as a report spells it
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            RootUse::Primary => "primary",
            RootUse::Latency => "latency",
            RootUse::Throughput => "throughput",
            RootUse::Unknown => "unknown",
        }
    }

    /// Whether a root of this use takes a moved set's bytes: the partial a stream is assembled
    /// in, or the archives it is installed into
    #[must_use]
    pub fn takes_moved_bytes(self) -> bool {
        matches!(self, RootUse::Primary | RootUse::Throughput)
    }
}

/// One storage root of a node, as its filesystem last reported it
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootCapacity {
    /// The root's path, as the node's configuration names it
    pub path: String,
    /// The device its filesystem is on, as `major:minor`; empty when the root could not be read
    #[serde(default)]
    pub device: String,
    /// The filesystem's size in bytes; zero when the root could not be read
    #[serde(default)]
    pub size_bytes: u64,
    /// The bytes an unprivileged writer may still use; zero when the root could not be read
    pub free_bytes: u64,
    /// What the node writes under it
    #[serde(default)]
    pub uses: Vec<RootUse>,
}

impl RootCapacity {
    /// What a report from before every root was reported stands for: one figure, the free
    /// bytes of the node's primary, for everything the node writes
    ///
    /// # Arguments
    ///
    /// * `free_bytes` - The one figure the report carried
    #[must_use]
    pub fn whole_node(free_bytes: u64) -> Self {
        RootCapacity {
            path: String::new(),
            device: String::new(),
            size_bytes: 0,
            free_bytes,
            uses: vec![RootUse::Primary, RootUse::Latency, RootUse::Throughput],
        }
    }

    /// The root as a reason names it: `root /hdd/shoal (throughput)`, or `its storage` for the
    /// one figure of a report from before every root was reported
    #[must_use]
    pub fn describe(&self) -> String {
        if self.path.is_empty() {
            "its storage".to_string()
        } else {
            format!("root {} ({})", self.path, self.uses_label())
        }
    }

    /// Whether this root takes a moved set's bytes, or keeps only the reserve
    #[must_use]
    pub fn takes_moved_bytes(&self) -> bool {
        self.uses
            .iter()
            .any(|root_use| root_use.takes_moved_bytes())
    }

    /// The bytes this root needs free to take a move of `bytes` and keep `reserve` after it
    ///
    /// # Arguments
    ///
    /// * `bytes` - The bytes the move lands on the node
    /// * `reserve` - The bytes every root keeps free
    #[must_use]
    pub fn need(&self, bytes: u64, reserve: u64) -> u64 {
        // a root the move's bytes land on keeps them and the reserve; any other only the reserve
        if self.takes_moved_bytes() {
            reserve.saturating_add(bytes)
        } else {
            reserve
        }
    }

    /// What the node writes under it, as one label: `primary,latency`
    #[must_use]
    pub fn uses_label(&self) -> String {
        // each use by its name, in the order the node reported them
        let names: Vec<&str> = self.uses.iter().map(|root_use| root_use.name()).collect();
        names.join(",")
    }
}

/// The root with the least free bytes, which is the one that fills first
///
/// # Arguments
///
/// * `roots` - A node's roots
#[must_use]
pub fn tightest(roots: &[RootCapacity]) -> Option<&RootCapacity> {
    roots.iter().min_by_key(|root| root.free_bytes)
}

/// The first root short of what a move needs, if any is
///
/// # Arguments
///
/// * `roots` - The roots the move writes to
/// * `bytes` - The bytes the move lands on the node
/// * `reserve` - The bytes every root keeps free
#[must_use]
pub fn short_root(roots: &[RootCapacity], bytes: u64, reserve: u64) -> Option<&RootCapacity> {
    roots
        .iter()
        .find(|root| root.free_bytes < root.need(bytes, reserve))
}

#[cfg(test)]
mod tests {
    use super::{short_root, tightest, RootCapacity, RootUse};

    /// A root of the given free bytes and uses, on a device of its own
    ///
    /// # Arguments
    ///
    /// * `path` - The root
    /// * `free_bytes` - What it has left
    /// * `uses` - What the node writes under it
    fn root(path: &str, free_bytes: u64, uses: &[RootUse]) -> RootCapacity {
        RootCapacity {
            path: path.to_string(),
            device: "8:1".to_string(),
            size_bytes: 1 << 40,
            free_bytes,
            uses: uses.to_vec(),
        }
    }

    /// The documented shape is read and written back the same, and a report from before a field
    /// existed reads it as its default
    #[test]
    fn the_documented_shape_is_read() {
        let read: RootCapacity = serde_json::from_str(
            r#"{"path": "/hdd/shoal", "device": "8:1", "size_bytes": 14000519643136,
                "free_bytes": 13731226726400, "uses": ["throughput"]}"#,
        )
        .expect("the documented shape reads");
        assert_eq!(read.path, "/hdd/shoal");
        assert_eq!(read.device, "8:1");
        assert_eq!(read.size_bytes, 14_000_519_643_136);
        assert_eq!(read.free_bytes, 13_731_226_726_400);
        assert_eq!(read.uses, vec![RootUse::Throughput]);
        // and it round trips
        let written = serde_json::to_string(&read).unwrap();
        assert_eq!(
            serde_json::from_str::<RootCapacity>(&written).unwrap(),
            read
        );
        // a root naming only its path and free bytes reads the rest as nothing
        let bare: RootCapacity =
            serde_json::from_str(r#"{"path": "/opt/shoal", "free_bytes": 7}"#).unwrap();
        assert!(bare.device.is_empty() && bare.uses.is_empty() && bare.size_bytes == 0);
    }

    /// A use a later build names is read as unknown, and does not refuse its report
    #[test]
    fn an_unknown_use_is_read_and_not_refused() {
        let read: RootCapacity = serde_json::from_str(
            r#"{"path": "/dev/nvme1n1", "free_bytes": 1, "uses": ["device", "primary"]}"#,
        )
        .expect("an unknown use reads");
        assert_eq!(read.uses, vec![RootUse::Unknown, RootUse::Primary]);
        assert_eq!(read.uses_label(), "unknown,primary");
        assert_eq!(read.describe(), "root /dev/nvme1n1 (unknown,primary)");
        // and a report's one figure from before every root was reported stands for everything
        let whole = RootCapacity::whole_node(9);
        assert!(whole.takes_moved_bytes());
        assert_eq!(whole.describe(), "its storage");
    }

    /// The tightest root is the least free, and a move is judged on every root by its uses
    #[test]
    fn a_move_is_judged_on_every_root_by_its_uses() {
        let roots = vec![
            root("/optane/shoal", 900, &[RootUse::Primary, RootUse::Latency]),
            root("/hdd/shoal", 5_000, &[RootUse::Throughput]),
            root("/nvme/maps", 150, &[RootUse::Latency]),
        ];
        // the root that fills first
        assert_eq!(tightest(&roots).unwrap().path, "/nvme/maps");
        assert!(tightest(&[]).is_none());
        // a latency-only root keeps the reserve alone; the others the bytes as well
        assert_eq!(roots[2].need(500, 100), 100);
        assert_eq!(roots[0].need(500, 100), 600);
        assert!(short_root(&roots, 500, 100).is_none());
        // the primary is short once the move and the reserve outgrow it
        assert_eq!(short_root(&roots, 850, 100).unwrap().path, "/optane/shoal");
        // and the latency-only root once the reserve alone does
        assert_eq!(short_root(&roots, 1, 200).unwrap().path, "/nvme/maps");
    }
}
