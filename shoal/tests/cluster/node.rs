//! One child process: how it is started, how it reports, and how it is signalled
//!
//! A node is the test binary re-executed with `--exact <child fn> --ignored --nocapture`, the
//! way `ack_survives_sigkill` starts its child, with the request in one environment variable.
//! The child answers on stdout with exactly one of two lines: `READY_LINE` followed by the
//! endpoints it bound, or `FAILED_LINE` followed by why. A reader thread relays those to the
//! parent; everything else the child prints is kept, last lines first, as evidence for a wait
//! that times out.

use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::os::unix::process::CommandExt as _;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use super::{Allocation, FixtureError};

/// The environment variable the request travels in
pub const CHILD_ENV: &str = "SHOAL_CLUSTER_CHILD";

/// What a child prints, followed by its endpoints as json, once it is answering
pub const READY_LINE: &str = "SHOAL_CLUSTER_READY";

/// What a child prints, followed by the reason, if it cannot start or a shard dies
pub const FAILED_LINE: &str = "SHOAL_CLUSTER_FAILED";

/// How many of a child's other lines are kept as evidence
const EVIDENCE_LINES: usize = 20;

/// The three things a child can be
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeKind {
    /// A real `ShoalPool` on the fixture's schema, bootstrapped as a cluster of one
    Server,
    /// A real `ShoalPool` with no `cluster:` block: the shape every deployment had before M1
    Standalone,
    /// A listener that echoes what it is sent
    MockPeer,
}

impl NodeKind {
    /// The test function that runs this kind of child
    pub fn child_fn(self) -> &'static str {
        match self {
            NodeKind::Server | NodeKind::Standalone => "cluster_server_child",
            NodeKind::MockPeer => "cluster_mock_peer_child",
        }
    }
}

/// What a child is asked to be
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChildRequest {
    /// Which kind
    pub kind: NodeKind,
    /// Its storage directory
    pub dir: std::path::PathBuf,
    /// The physical cores it must not use, so it runs on exactly its allocation
    pub exclude_cores: Vec<usize>,
    /// How many cores to run; `None` lets the server decide, for a shared allocation
    pub cores: Option<usize>,
    /// The cpu its control thread is pinned to, for a cluster node; `None` for a standalone one
    pub control_cpu: Option<usize>,
    /// Whether the control thread's core is shared with a shard, for a cluster node
    pub control_shared: bool,
    /// The cpus the child may run on at all, if the test narrowed them
    pub affinity: Option<Vec<usize>>,
    /// Which marker the child should find, if the test staged one: none stages nothing
    #[serde(default)]
    pub staged_marker: Option<String>,
}

/// The endpoints a child bound, and the identity it reported
///
/// Only the client endpoint is bound. The other two are what M2 and M3 add, and are here so
/// that the record has their shape before anything fills them. The identity fields are what M1
/// added: a node id for every server, and a cluster id, a control core and a topology version
/// for a cluster node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Endpoints {
    /// Where clients connect
    pub client: SocketAddr,
    /// Where data peers would connect; none until M2
    #[serde(default)]
    pub data: Option<SocketAddr>,
    /// Where control peers would connect; none until M3
    #[serde(default)]
    pub control: Option<SocketAddr>,
    /// The node's identity, as a string; none for a mock peer
    #[serde(default)]
    pub node: Option<String>,
    /// The cluster's identity; none for a standalone node or a mock peer
    #[serde(default)]
    pub cluster: Option<String>,
    /// The cpu the control thread runs on; none without a control plane
    #[serde(default)]
    pub control_core: Option<usize>,
    /// Whether that cpu's core is shared with a shard
    #[serde(default)]
    pub control_shared: bool,
    /// The topology version the control plane reports; none without one
    #[serde(default)]
    pub topology_version: Option<u64>,
    /// The cpus the shards run on, so a test can check them against the control core
    #[serde(default)]
    pub shard_cpus: Vec<usize>,
}

impl Endpoints {
    /// An empty record, for a node that has not reported yet
    pub fn unbound() -> Self {
        Endpoints {
            client: "0.0.0.0:0".parse().unwrap(),
            data: None,
            control: None,
            node: None,
            cluster: None,
            control_core: None,
            control_shared: false,
            topology_version: None,
            shard_cpus: Vec::new(),
        }
    }
}

/// A line the reader thread relayed
#[derive(Debug)]
enum ChildLine {
    /// The child is answering on these endpoints
    Ready(Endpoints),
    /// The child failed
    Failed(String),
    /// The child's stdout closed: it exited
    Closed,
}

/// A running child
pub struct Node {
    /// Its id in the cluster
    pub id: usize,
    /// What it is
    pub kind: NodeKind,
    /// Its pid
    pub pid: u32,
    /// What it bound; unset until it reports ready
    pub endpoints: Endpoints,
    /// What it was given
    pub allocation: Allocation,
    /// The process
    child: Child,
    /// What the reader thread relays
    lines: Receiver<ChildLine>,
    /// The last lines the child printed that were not a report
    evidence: Arc<Mutex<std::collections::VecDeque<String>>>,
    /// The reader thread
    reader: Option<JoinHandle<()>>,
    /// Whether it has been reaped
    reaped: bool,
}

impl Node {
    /// Start a child
    ///
    /// # Arguments
    ///
    /// * `id` - Its id
    /// * `kind` - What it should be
    /// * `allocation` - The cores it was given
    /// * `dir` - Its storage directory
    pub fn spawn(
        id: usize,
        kind: NodeKind,
        allocation: Allocation,
        dir: &Path,
    ) -> Result<Self, FixtureError> {
        Self::spawn_with(id, kind, allocation, dir, None, None)
    }

    /// Start a child, narrowing the cpus it may run on and staging a marker for it to find
    ///
    /// # Arguments
    ///
    /// * `id` - Its id
    /// * `kind` - What it should be
    /// * `allocation` - The cores it was given
    /// * `dir` - Its storage directory
    /// * `affinity` - The cpus it may run on, applied before it starts; `None` inherits
    /// * `staged_marker` - A marker to write into its directory before it starts
    pub fn spawn_with(
        id: usize,
        kind: NodeKind,
        allocation: Allocation,
        dir: &Path,
        affinity: Option<Vec<usize>>,
        staged_marker: Option<String>,
    ) -> Result<Self, FixtureError> {
        let topology = super::Topology::detect();
        // a cluster node's control thread runs on the first cpu of its control core, or shares
        // cpu 0 when the machine had no core to give it
        let (control_cpu, control_shared) = match (kind, allocation.control) {
            (NodeKind::Server, Some(core)) => (
                Some(topology.cores.get(&core).and_then(|cpus| cpus.first().copied()).unwrap_or(0)),
                false,
            ),
            (NodeKind::Server, None) => (Some(0), true),
            _ => (None, false),
        };
        let request = ChildRequest {
            kind,
            dir: dir.to_path_buf(),
            exclude_cores: super::cores::excluded_for(&allocation, &topology),
            cores: if allocation.shared { None } else { Some(allocation.data.len()) },
            control_cpu,
            control_shared,
            affinity: affinity.clone(),
            staged_marker,
        };
        let request = serde_json::to_string(&request).expect("a request serializes");
        // the test binary again, running only the child function
        let mut command = Command::new(std::env::current_exe()?);
        command
            .args(["--exact", kind.child_fn(), "--ignored", "--nocapture"])
            .env(CHILD_ENV, request)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        // the affinity is applied in the child between fork and exec, which is the one place a
        // process's own mask can be set before anything in it runs
        if let Some(cpus) = affinity {
            // SAFETY: `pre_exec` runs in the forked child before exec; `sched_setaffinity` on
            // pid 0 touches only that child, and a `cpu_set_t` is plain data with no
            // allocation, so nothing here is unsound to run after a fork
            unsafe {
                command.pre_exec(move || {
                    let mut set: libc::cpu_set_t = std::mem::zeroed();
                    for cpu in &cpus {
                        libc::CPU_SET(*cpu, &mut set);
                    }
                    let rc = libc::sched_setaffinity(
                        0,
                        std::mem::size_of::<libc::cpu_set_t>(),
                        &set,
                    );
                    if rc == 0 {
                        Ok(())
                    } else {
                        Err(std::io::Error::last_os_error())
                    }
                });
            }
        }
        let mut child = command.spawn()?;
        let pid = child.id();
        let stdout = child.stdout.take().expect("stdout was piped");
        // relay the child's reports, and keep the rest as evidence
        let (tx, lines) = mpsc::channel();
        let evidence = Arc::new(Mutex::new(std::collections::VecDeque::new()));
        let kept = evidence.clone();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                let line = line.trim().to_string();
                if let Some(json) = line.strip_prefix(READY_LINE) {
                    match serde_json::from_str::<Endpoints>(json.trim()) {
                        Ok(endpoints) => {
                            let _ = tx.send(ChildLine::Ready(endpoints));
                        }
                        Err(error) => {
                            let _ = tx.send(ChildLine::Failed(format!(
                                "unparseable ready line {line:?}: {error}"
                            )));
                        }
                    }
                } else if let Some(reason) = line.strip_prefix(FAILED_LINE) {
                    let _ = tx.send(ChildLine::Failed(reason.trim().to_string()));
                } else {
                    let mut kept = kept.lock().unwrap();
                    if kept.len() == EVIDENCE_LINES {
                        kept.pop_front();
                    }
                    kept.push_back(line);
                }
            }
            let _ = tx.send(ChildLine::Closed);
        });
        Ok(Self {
            id,
            kind,
            pid,
            endpoints: Endpoints::unbound(),
            allocation,
            child,
            lines,
            evidence,
            reader: Some(reader),
            reaped: false,
        })
    }

    /// Wait for the child to report ready, or explain why it did not
    ///
    /// # Arguments
    ///
    /// * `timeout` - How long to wait
    pub fn wait_ready(&mut self, timeout: Duration) -> Result<(), FixtureError> {
        let deadline = Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match self.lines.recv_timeout(remaining) {
                Ok(ChildLine::Ready(endpoints)) => {
                    self.endpoints = endpoints;
                    return Ok(());
                }
                Ok(ChildLine::Failed(reason)) => {
                    return Err(FixtureError::ChildFailed(format!(
                        "node {} ({:?}, pid {}) failed: {reason}",
                        self.id, self.kind, self.pid
                    )));
                }
                Ok(ChildLine::Closed) | Err(RecvTimeoutError::Disconnected) => {
                    return Err(FixtureError::NotReady(self.evidence_report("exited before it was ready")));
                }
                Err(RecvTimeoutError::Timeout) => {
                    return Err(FixtureError::NotReady(
                        self.evidence_report(&format!("not ready after {timeout:?}")),
                    ));
                }
            }
        }
    }

    /// What is known about a child that did not come up
    fn evidence_report(&self, what: &str) -> String {
        let evidence = self.evidence.lock().unwrap();
        let mut report = format!("node {} ({:?}, pid {}) {what}", self.id, self.kind, self.pid);
        if evidence.is_empty() {
            report.push_str("; it printed nothing");
        } else {
            report.push_str("; its last lines were:");
            for line in evidence.iter() {
                report.push_str("\n    ");
                report.push_str(line);
            }
        }
        report
    }

    /// Whether the child has reported a failure since it was ready
    ///
    /// Non blocking. A server child polls its pool's `failure` handle and prints `FAILED_LINE`
    /// when a shard dies, which is what this relays.
    pub fn failure(&self) -> Option<String> {
        match self.lines.try_recv() {
            Ok(ChildLine::Failed(reason)) => Some(reason),
            Ok(ChildLine::Closed) => Some("exited".to_string()),
            _ => None,
        }
    }

    /// The names of every thread the child is running, from procfs
    ///
    /// What a test reads to say whether a control thread exists: glommio names an executor's
    /// thread after the builder, so a cluster node has one beginning `shoal-control` and a
    /// standalone node has none.
    pub fn thread_names(&self) -> Vec<String> {
        let tasks = format!("/proc/{}/task", self.pid);
        let Ok(entries) = std::fs::read_dir(&tasks) else {
            return Vec::new();
        };
        let mut names: Vec<String> = entries
            .filter_map(Result::ok)
            .filter_map(|entry| std::fs::read_to_string(entry.path().join("comm")).ok())
            .map(|name| name.trim().to_string())
            .collect();
        names.sort();
        names
    }

    /// Whether the process is still there
    pub fn is_alive(&self) -> bool {
        !self.reaped && super::is_alive(self.pid)
    }

    /// Kill the child with `SIGKILL` and reap it
    ///
    /// This proves nothing about durability. The process is gone; the page cache, the device
    /// and everything the kernel had not written are exactly as they were. A durability claim
    /// needs injected storage completions, not this.
    pub fn kill(&mut self) -> std::io::Result<()> {
        if self.reaped {
            return Ok(());
        }
        self.child.kill()?;
        self.child.wait()?;
        self.reaped = true;
        Ok(())
    }

    /// Stop the child where it stands with `SIGSTOP`
    ///
    /// A paused process holds every socket and every lock; nothing it owned is lost and nothing
    /// it owed is answered. This is a different state from a cut link and is labelled apart.
    pub fn pause(&self) -> std::io::Result<()> {
        self.signal(libc::SIGSTOP)
    }

    /// Let a paused child run again with `SIGCONT`
    pub fn resume(&self) -> std::io::Result<()> {
        self.signal(libc::SIGCONT)
    }

    /// Send a signal
    fn signal(&self, signal: libc::c_int) -> std::io::Result<()> {
        // SAFETY: `kill` on a pid this fixture spawned and has not reaped
        let rc = unsafe { libc::kill(self.pid as libc::pid_t, signal) };
        if rc == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }
}

impl Drop for Node {
    /// Kill, reap, and join the reader, whatever state the test left the child in
    fn drop(&mut self) {
        // a paused child cannot die of SIGKILL until it runs again
        let _ = self.resume();
        let _ = self.kill();
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}
