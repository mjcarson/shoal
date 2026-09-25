//! What the wizard holds and decides: the draft inventory, the page and field in focus, and
//! what every key does to them
//!
//! Nothing here draws or touches a file. A key is handled into an [`Outcome`] the loop acts on,
//! and the draft is judged into an inventory and the [`Issue`]s standing between it and a file,
//! so every decision can be tested without a terminal.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use std::collections::BTreeMap;
use std::net::IpAddr;
use std::path::{Path, PathBuf};

use crate::deploy::inventory::{
    is_executable, GroupSpec, Inventory, NodeSpec, Ports, Resources, StorageSpec,
};

/// The tracing levels a node can log at, in the order the wizard cycles them
pub const TRACING_LEVELS: [&str; 6] = ["Trace", "Debug", "Info", "Warn", "Error", "Off"];

/// The control group sizes a cluster can have, in the order the wizard cycles them
pub const CONTROL_VOTERS: [&str; 3] = ["1", "3", "5"];

/// The pages of the wizard, in the order they are walked
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Page {
    /// The cluster's name, program, directory and who runs it
    Cluster,
    /// The factor, the voters, the ports and the move pace
    Shape,
    /// The resources and storage every node gets unless its group or itself says otherwise
    Defaults,
    /// The named sets of resources and storage a node can take
    Groups,
    /// The hosts
    Nodes,
    /// The file that will be written, what each node resolves to, and what is wrong
    Review,
}

impl Page {
    /// Every page, in order
    pub const ALL: [Page; 6] = [
        Page::Cluster,
        Page::Shape,
        Page::Defaults,
        Page::Groups,
        Page::Nodes,
        Page::Review,
    ];

    /// The page's title
    #[must_use]
    pub fn title(self) -> &'static str {
        match self {
            Page::Cluster => "Cluster",
            Page::Shape => "Shape",
            Page::Defaults => "Defaults",
            Page::Groups => "Groups",
            Page::Nodes => "Nodes",
            Page::Review => "Review",
        }
    }

    /// The page after this one, if any
    #[must_use]
    pub fn next(self) -> Option<Page> {
        let index = Page::ALL.iter().position(|page| *page == self)?;
        Page::ALL.get(index + 1).copied()
    }

    /// The page before this one, if any
    #[must_use]
    pub fn prev(self) -> Option<Page> {
        let index = Page::ALL.iter().position(|page| *page == self)?;
        index.checked_sub(1).map(|index| Page::ALL[index])
    }
}

/// Every field the wizard edits
///
/// The group and node fields edit whichever group or node is selected on their page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FieldId {
    /// The cluster's name
    Name,
    /// The server program to deploy
    Server,
    /// Where every host keeps the node's files
    RemoteDir,
    /// The system user nodes run as
    User,
    /// The admin principal
    Admin,
    /// The tracing level
    Tracing,
    /// How many replicas each tablet has
    ReplicationFactor,
    /// How many nodes vote in the control group
    ControlVoters,
    /// How long a moved copy is kept
    RetireAfter,
    /// The client port
    ClientPort,
    /// The data peer port
    PeerPort,
    /// The control port
    ControlPort,
    /// Cores, at the level being edited
    Cores,
    /// Excluded cores, at the level being edited
    ExcludeCores,
    /// Memory, at the level being edited
    Memory,
    /// Whether the control thread may share its core, at the level being edited
    ControlCoreShared,
    /// The latency storage directory, at the level being edited
    Latency,
    /// The throughput storage directory, at the level being edited
    Throughput,
    /// A group's name
    GroupName,
    /// A node's name
    NodeName,
    /// A node's ssh target
    NodeSsh,
    /// A node's address
    NodeAddress,
    /// The group a node takes its settings from
    NodeGroup,
    /// Whether a node is in the bootstrap set
    NodeBootstrap,
}

/// How a field is edited
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldKind {
    /// Free text, typed
    Text,
    /// On or off, toggled with space
    Toggle,
    /// One of a fixed set, cycled with the arrows
    Choice,
}

impl FieldId {
    /// The fields of the three form pages, in the order they are focused
    #[must_use]
    pub fn page_fields(page: Page) -> &'static [FieldId] {
        match page {
            Page::Cluster => &[
                FieldId::Name,
                FieldId::Server,
                FieldId::RemoteDir,
                FieldId::User,
                FieldId::Admin,
                FieldId::Tracing,
            ],
            Page::Shape => &[
                FieldId::ReplicationFactor,
                FieldId::ControlVoters,
                FieldId::RetireAfter,
                FieldId::ClientPort,
                FieldId::PeerPort,
                FieldId::ControlPort,
            ],
            Page::Defaults => &[
                FieldId::Cores,
                FieldId::ExcludeCores,
                FieldId::Memory,
                FieldId::ControlCoreShared,
                FieldId::Latency,
                FieldId::Throughput,
            ],
            Page::Groups => &[
                FieldId::GroupName,
                FieldId::Cores,
                FieldId::ExcludeCores,
                FieldId::Memory,
                FieldId::ControlCoreShared,
                FieldId::Latency,
                FieldId::Throughput,
            ],
            Page::Nodes => &[
                FieldId::NodeName,
                FieldId::NodeSsh,
                FieldId::NodeAddress,
                FieldId::NodeGroup,
                FieldId::NodeBootstrap,
                FieldId::Cores,
                FieldId::ExcludeCores,
                FieldId::Memory,
                FieldId::ControlCoreShared,
                FieldId::Latency,
                FieldId::Throughput,
            ],
            Page::Review => &[],
        }
    }

    /// How this field is edited
    #[must_use]
    pub fn kind(self) -> FieldKind {
        match self {
            FieldId::ControlCoreShared | FieldId::NodeBootstrap => FieldKind::Toggle,
            FieldId::Tracing | FieldId::ControlVoters | FieldId::NodeGroup => FieldKind::Choice,
            _ => FieldKind::Text,
        }
    }

    /// The field's label
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            FieldId::Name => "Cluster name",
            FieldId::Server => "Server program",
            FieldId::RemoteDir => "Remote directory",
            FieldId::User => "Run as user",
            FieldId::Admin => "Admin principal",
            FieldId::Tracing => "Tracing level",
            FieldId::ReplicationFactor => "Replication factor",
            FieldId::ControlVoters => "Control voters",
            FieldId::RetireAfter => "Retire after",
            FieldId::ClientPort => "Client port",
            FieldId::PeerPort => "Peer port",
            FieldId::ControlPort => "Control port",
            FieldId::Cores => "Cores",
            FieldId::ExcludeCores => "Exclude cores",
            FieldId::Memory => "Memory",
            FieldId::ControlCoreShared => "Share control core",
            FieldId::Latency => "Latency storage",
            FieldId::Throughput => "Throughput storage",
            FieldId::GroupName => "Group name",
            FieldId::NodeName => "Node name",
            FieldId::NodeSsh => "ssh target",
            FieldId::NodeAddress => "Address",
            FieldId::NodeGroup => "Group",
            FieldId::NodeBootstrap => "Bootstrap",
        }
    }

    /// What the field is for, shown under the form while it has focus
    #[must_use]
    pub fn help(self, page: Page) -> &'static str {
        match (self, page) {
            (FieldId::Name, _) => {
                "Names the systemd unit, the remote directory and the local state. Letters, digits, '-' and '_'."
            }
            (FieldId::Server, _) => {
                "A build of shoal::server::node::main for your schema. A relative path is relative to the inventory file. Build it for the oldest host's cpu, never native."
            }
            (FieldId::RemoteDir, _) => {
                "Where every host keeps the node's program, configuration and keys. destroy deletes it."
            }
            (FieldId::User, _) => {
                "The system user every node runs as, created where missing. Blank runs as the ssh login, which shares its io_uring memory budget."
            }
            (FieldId::Admin, _) => {
                "The principal the deployment authenticates as, and the cluster's one admin."
            }
            (FieldId::Tracing, _) => "How much every node logs. Left and right cycle it.",
            (FieldId::ReplicationFactor, _) => {
                "How many copies of each tablet. Has to fit in the bootstrap set."
            }
            (FieldId::ControlVoters, _) => {
                "How many nodes vote in the control group: 1, 3 or 5. Left and right cycle it."
            }
            (FieldId::RetireAfter, _) => {
                "How long a moved copy is kept before it is reclaimed: 500ms, 15s, 5m or 1h. Blank is the engine's five minutes, the floor under every rebalance step."
            }
            (FieldId::ClientPort | FieldId::PeerPort | FieldId::ControlPort, _) => {
                "Every node listens on the same three ports, so one host runs one node of a cluster."
            }
            (FieldId::Cores, Page::Defaults) => {
                "How many cores run shards. Blank is every core the control plane leaves."
            }
            (FieldId::ExcludeCores, Page::Defaults) => {
                "Physical cores to keep the node off, comma separated."
            }
            (FieldId::Memory, Page::Defaults) => {
                "The memory limit, as shoal.yml writes it (4Gi). Blank is 4Gi."
            }
            (FieldId::ControlCoreShared, Page::Defaults) => {
                "Whether the control thread may share its physical core with a shard. Space toggles it."
            }
            (
                FieldId::Cores
                | FieldId::ExcludeCores
                | FieldId::Memory
                | FieldId::ControlCoreShared,
                _,
            ) => {
                "Resources are taken whole: set any of these and they replace the level above entirely, with a blank memory meaning 4Gi. Leave all four blank to inherit."
            }
            (FieldId::Latency, Page::Defaults) => {
                "Intent logs, maps and the node's marker. Blank is <remote directory>/data. destroy deletes it."
            }
            (FieldId::Throughput, Page::Defaults) => {
                "The archives. Blank is the latency directory. destroy deletes it."
            }
            (FieldId::Latency, _) => {
                "Intent logs, maps and the marker. Blank inherits it from the level above. Each directory is inherited on its own."
            }
            (FieldId::Throughput, _) => {
                "The archives. Blank inherits it from the level above, or the latency directory if nothing sets it."
            }
            (FieldId::GroupName, _) => {
                "The name nodes use to take this group's resources and storage."
            }
            (FieldId::NodeName, _) => {
                "What you call this node, and the ssh target unless one is given."
            }
            (FieldId::NodeSsh, _) => {
                "What ssh is given, user@host or host. Blank is the node name."
            }
            (FieldId::NodeAddress, _) => {
                "The address peers and clients reach it at. Blank resolves the name here, as bootstrap will; a name that resolves only to loopback is refused."
            }
            (FieldId::NodeGroup, _) => {
                "The group this node takes whatever it does not set itself from. Left and right cycle it."
            }
            (FieldId::NodeBootstrap, _) => {
                "Whether bootstrap forms the cluster from this node. A node left out can be joined later with `cluster add`. Space toggles it."
            }
        }
    }

    /// What a blank field means, drawn in its place
    #[must_use]
    pub fn placeholder(self, page: Page) -> &'static str {
        match (self, page) {
            (FieldId::RemoteDir, _) => "/opt/shoal-deploy/<name>",
            (FieldId::User, _) => "the ssh login",
            (FieldId::RetireAfter, _) => "5m (the engine's)",
            (FieldId::Cores, Page::Defaults) => "every core",
            (FieldId::ExcludeCores, Page::Defaults) => "none",
            (FieldId::Memory, Page::Defaults) => "4Gi",
            (FieldId::Latency, Page::Defaults) => "<remote directory>/data",
            (FieldId::Throughput, Page::Defaults) => "the latency directory",
            (FieldId::Cores | FieldId::ExcludeCores | FieldId::Memory, _) => "inherited",
            (FieldId::Latency | FieldId::Throughput, _) => "inherited",
            (FieldId::NodeSsh, _) => "the node name",
            (FieldId::NodeAddress, _) => "resolved from the name",
            _ => "",
        }
    }
}

/// Resources as the wizard edits them, at any level
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResourcesDraft {
    /// Cores, as typed
    pub cores: String,
    /// Excluded cores, as typed, comma separated
    pub exclude_cores: String,
    /// Memory, as typed
    pub memory: String,
    /// Whether the control thread may share its core
    pub control_core_shared: bool,
}

impl ResourcesDraft {
    /// Whether nothing is set, so a group or node inherits its resources
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.cores.trim().is_empty()
            && self.exclude_cores.trim().is_empty()
            && self.memory.trim().is_empty()
            && !self.control_core_shared
    }

    /// Fill the draft from resources
    ///
    /// # Arguments
    ///
    /// * `resources` - The resources
    fn from_resources(resources: &Resources) -> Self {
        ResourcesDraft {
            cores: resources
                .cores
                .map(|cores| cores.to_string())
                .unwrap_or_default(),
            exclude_cores: resources
                .exclude_cores
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", "),
            memory: resources.memory.clone(),
            control_core_shared: resources.control_core_shared,
        }
    }

    /// Build resources from the draft, reporting what does not parse
    ///
    /// # Arguments
    ///
    /// * `at` - Where these fields are, for an issue
    /// * `issues` - Where to report what does not parse
    fn build(&self, at: &Target, issues: &mut Vec<Issue>) -> Resources {
        // start from the defaults, which a blank field keeps
        let mut resources = Resources::default();
        // cores are a count, blank for every core
        if !self.cores.trim().is_empty() {
            match self.cores.trim().parse::<usize>() {
                Ok(cores) if cores > 0 => resources.cores = Some(cores),
                _ => issues
                    .push(at.error(FieldId::Cores, "cores has to be a whole number above zero")),
            }
        }
        // excluded cores are a list of core numbers
        for core in self
            .exclude_cores
            .split([',', ' '])
            .filter(|core| !core.is_empty())
        {
            match core.parse::<usize>() {
                Ok(core) => resources.exclude_cores.push(core),
                Err(_) => issues.push(at.error(
                    FieldId::ExcludeCores,
                    &format!("exclude cores has {core:?}, which is not a core number"),
                )),
            }
        }
        // memory is passed through as shoal.yml writes it, a number and an optional unit
        let memory = self.memory.trim();
        if !memory.is_empty() {
            if memory.starts_with(|c: char| c.is_ascii_digit()) && !memory.contains(' ') {
                resources.memory = memory.to_string();
            } else {
                issues.push(at.error(FieldId::Memory, "memory has to be a size like 4Gi or 512Mi"));
            }
        }
        resources.control_core_shared = self.control_core_shared;
        resources
    }
}

/// Storage as the wizard edits it, at any level
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageDraft {
    /// The latency directory, as typed
    pub latency: String,
    /// The throughput directory, as typed
    pub throughput: String,
}

impl StorageDraft {
    /// Fill the draft from a storage spec
    ///
    /// # Arguments
    ///
    /// * `spec` - The spec, if the level had one
    fn from_spec(spec: Option<&StorageSpec>) -> Self {
        StorageDraft {
            latency: spec
                .and_then(|spec| spec.latency.clone())
                .unwrap_or_default(),
            throughput: spec
                .and_then(|spec| spec.throughput.clone())
                .unwrap_or_default(),
        }
    }

    /// Build a storage spec from the draft, none if both are blank
    fn build(&self) -> Option<StorageSpec> {
        // a blank field sets nothing, so it inherits
        let field = |raw: &str| Some(raw.trim().to_string()).filter(|raw| !raw.is_empty());
        let spec = StorageSpec {
            latency: field(&self.latency),
            throughput: field(&self.throughput),
        };
        (!spec.is_empty()).then_some(spec)
    }
}

/// A group as the wizard edits it
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupDraft {
    /// The group's identity in this draft, which a node's choice names so a rename follows
    pub id: u64,
    /// Its name
    pub name: String,
    /// Its resources, none if every field is blank
    pub resources: ResourcesDraft,
    /// Its storage
    pub storage: StorageDraft,
}

/// A node as the wizard edits it
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeDraft {
    /// Its name
    pub name: String,
    /// Its ssh target, blank for its name
    pub ssh: String,
    /// Its address, blank to resolve its name
    pub address: String,
    /// The draft identity of the group it takes its settings from
    pub group: Option<u64>,
    /// Whether bootstrap forms the cluster from it
    pub bootstrap: bool,
    /// Its own resources, none if every field is blank
    pub resources: ResourcesDraft,
    /// Its own storage
    pub storage: StorageDraft,
}

/// An inventory as the wizard edits it: every field as typed
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Draft {
    /// The cluster's name
    pub name: String,
    /// The server program, as typed
    pub server: String,
    /// The remote directory, blank for the default
    pub remote_dir: String,
    /// The system user, blank for the ssh login
    pub user: String,
    /// The admin principal
    pub admin: String,
    /// The tracing level
    pub tracing: String,
    /// The replication factor
    pub replication_factor: String,
    /// The control voters
    pub control_voters: String,
    /// How long a moved copy is kept, blank for the engine's
    pub retire_after: String,
    /// The client port
    pub client_port: String,
    /// The peer port
    pub peer_port: String,
    /// The control port
    pub control_port: String,
    /// The deployment's resources
    pub resources: ResourcesDraft,
    /// The deployment's storage
    pub storage: StorageDraft,
    /// The groups
    pub groups: Vec<GroupDraft>,
    /// The nodes
    pub nodes: Vec<NodeDraft>,
    /// The bootstrap order an inventory was loaded with, which node order alone would lose
    pub bootstrap_order: Vec<String>,
    /// The identity the next group is given
    pub next_group: u64,
}

impl Default for Draft {
    /// A draft of the documented cluster, with nothing named yet
    fn default() -> Self {
        let ports = Ports::default();
        Draft {
            name: String::new(),
            server: String::new(),
            remote_dir: String::new(),
            // a user of its own keeps the node off the operator's io_uring budget
            user: "shoal".to_string(),
            admin: "admin".to_string(),
            tracing: "Info".to_string(),
            replication_factor: "3".to_string(),
            control_voters: "3".to_string(),
            retire_after: String::new(),
            client_port: ports.client.to_string(),
            peer_port: ports.peer.to_string(),
            control_port: ports.control.to_string(),
            resources: ResourcesDraft {
                memory: "4Gi".to_string(),
                ..ResourcesDraft::default()
            },
            storage: StorageDraft::default(),
            groups: Vec::new(),
            nodes: Vec::new(),
            bootstrap_order: Vec::new(),
            next_group: 0,
        }
    }
}

/// How bad an issue is
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    /// The file can be written, but something should be looked at
    Warning,
    /// The file cannot be written until this is fixed
    Error,
}

/// Where an issue is
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    /// A page as a whole
    Page(Page),
    /// A field of a form page
    Field(Page),
    /// A field of the group at this index
    Group(usize),
    /// A field of the node at this index
    Node(usize),
}

impl Target {
    /// An error on a field here
    ///
    /// # Arguments
    ///
    /// * `field` - The field
    /// * `message` - What is wrong
    fn error(&self, field: FieldId, message: &str) -> Issue {
        Issue {
            severity: Severity::Error,
            target: self.clone(),
            field: Some(field),
            message: message.to_string(),
        }
    }

    /// The page this is on
    #[must_use]
    pub fn page(&self) -> Page {
        match self {
            Target::Page(page) | Target::Field(page) => *page,
            Target::Group(_) => Page::Groups,
            Target::Node(_) => Page::Nodes,
        }
    }
}

/// Something between a draft and a file
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Issue {
    /// How bad it is
    pub severity: Severity,
    /// Where it is
    pub target: Target,
    /// The field it is on, if it is on one
    pub field: Option<FieldId>,
    /// What is wrong
    pub message: String,
}

impl Draft {
    /// Fill a draft from an inventory, so one can be edited
    ///
    /// # Arguments
    ///
    /// * `inventory` - The inventory
    #[must_use]
    pub fn from_inventory(inventory: &Inventory) -> Self {
        // the groups first, each given an identity nodes can name
        let groups: Vec<GroupDraft> = inventory
            .groups
            .iter()
            .enumerate()
            .map(|(id, (name, group))| GroupDraft {
                id: id as u64,
                name: name.clone(),
                resources: group
                    .resources
                    .as_ref()
                    .map(ResourcesDraft::from_resources)
                    .unwrap_or_default(),
                storage: StorageDraft::from_spec(group.storage.as_ref()),
            })
            .collect();
        // then the nodes, naming their group by that identity
        let bootstrap = inventory.bootstrap_names();
        let nodes = inventory
            .nodes
            .iter()
            .map(|node| NodeDraft {
                name: node.name.clone(),
                ssh: node.ssh.clone().unwrap_or_default(),
                address: node
                    .address
                    .map(|address| address.to_string())
                    .unwrap_or_default(),
                group: node.group.as_ref().and_then(|name| {
                    groups
                        .iter()
                        .find(|group| &group.name == name)
                        .map(|group| group.id)
                }),
                bootstrap: bootstrap.contains(&node.name),
                resources: node
                    .resources
                    .as_ref()
                    .map(ResourcesDraft::from_resources)
                    .unwrap_or_default(),
                storage: StorageDraft::from_spec(node.storage.as_ref()),
            })
            .collect();
        Draft {
            name: inventory.name.clone(),
            server: inventory.server.display().to_string(),
            remote_dir: inventory.remote_dir.clone().unwrap_or_default(),
            user: inventory.user.clone().unwrap_or_default(),
            admin: inventory.admin.clone(),
            tracing: inventory.tracing.clone(),
            replication_factor: inventory.replication_factor.to_string(),
            control_voters: inventory.control_voters.to_string(),
            retire_after: inventory.retire_after.clone().unwrap_or_default(),
            client_port: inventory.ports.client.to_string(),
            peer_port: inventory.ports.peer.to_string(),
            control_port: inventory.ports.control.to_string(),
            resources: ResourcesDraft::from_resources(&inventory.resources),
            storage: StorageDraft::from_spec(inventory.storage.as_ref()),
            next_group: groups.len() as u64,
            groups,
            nodes,
            bootstrap_order: inventory.bootstrap.clone().unwrap_or_default(),
        }
    }

    /// The name of the group with this draft identity
    ///
    /// # Arguments
    ///
    /// * `id` - The group's draft identity
    #[must_use]
    pub fn group_name(&self, id: u64) -> Option<&str> {
        self.groups
            .iter()
            .find(|group| group.id == id)
            .map(|group| group.name.as_str())
    }

    /// Build the inventory this draft describes, and everything wrong with it
    ///
    /// An inventory is always returned, with a field that does not parse left at its default,
    /// so the review can show what every node resolves to while something is still wrong. It
    /// is written only when no issue is an error.
    ///
    /// # Arguments
    ///
    /// * `base` - The directory the inventory will be written to, which a relative server path
    ///   is relative to
    #[must_use]
    pub fn build(&self, base: &Path) -> (Inventory, Vec<Issue>) {
        let mut issues = Vec::new();
        // the cluster page
        let cluster = Target::Field(Page::Cluster);
        let text = |raw: &str| Some(raw.trim().to_string()).filter(|raw| !raw.is_empty());
        if self.server.trim().is_empty() {
            issues.push(cluster.error(FieldId::Server, "name the server program to deploy"));
        }
        // the shape page, every number parsed on its own so each is reported where it is
        let shape = Target::Field(Page::Shape);
        let number = |raw: &str, field: FieldId, default: u32, issues: &mut Vec<Issue>| {
            raw.trim().parse::<u32>().unwrap_or_else(|_| {
                issues.push(shape.error(
                    field,
                    &format!("{} has to be a whole number", field.label()),
                ));
                default
            })
        };
        let port = |raw: &str, field: FieldId, default: u16, issues: &mut Vec<Issue>| match raw
            .trim()
            .parse::<u16>()
        {
            Ok(port) if port > 0 => port,
            _ => {
                issues.push(shape.error(
                    field,
                    &format!("{} has to be a port from 1 to 65535", field.label()),
                ));
                default
            }
        };
        let defaults = Ports::default();
        let ports = Ports {
            client: port(
                &self.client_port,
                FieldId::ClientPort,
                defaults.client,
                &mut issues,
            ),
            peer: port(
                &self.peer_port,
                FieldId::PeerPort,
                defaults.peer,
                &mut issues,
            ),
            control: port(
                &self.control_port,
                FieldId::ControlPort,
                defaults.control,
                &mut issues,
            ),
        };
        let replication_factor = number(
            &self.replication_factor,
            FieldId::ReplicationFactor,
            3,
            &mut issues,
        );
        let control_voters = number(&self.control_voters, FieldId::ControlVoters, 3, &mut issues);
        // the defaults page
        let resources = self
            .resources
            .build(&Target::Field(Page::Defaults), &mut issues);
        // the groups, keyed by name
        let mut groups = BTreeMap::new();
        for (index, group) in self.groups.iter().enumerate() {
            let at = Target::Group(index);
            let spec = GroupSpec {
                resources: (!group.resources.is_empty())
                    .then(|| group.resources.build(&at, &mut issues)),
                storage: group.storage.build(),
            };
            if groups.insert(group.name.trim().to_string(), spec).is_some() {
                issues.push(at.error(
                    FieldId::GroupName,
                    &format!("the group {:?} is listed twice", group.name.trim()),
                ));
            }
        }
        // the nodes, in order
        let mut nodes = Vec::with_capacity(self.nodes.len());
        for (index, node) in self.nodes.iter().enumerate() {
            let at = Target::Node(index);
            // an address given has to be one
            let address = match text(&node.address) {
                Some(raw) => {
                    match raw.parse::<IpAddr>() {
                        Ok(address) => Some(address),
                        Err(_) => {
                            issues.push(at.error(
                            FieldId::NodeAddress,
                            &format!("{raw:?} is not an IP address; leave it blank to resolve the name"),
                        ));
                            None
                        }
                    }
                }
                None => None,
            };
            nodes.push(NodeSpec {
                name: node.name.trim().to_string(),
                ssh: text(&node.ssh),
                address,
                group: node
                    .group
                    .and_then(|id| self.group_name(id))
                    .map(|name| name.trim().to_string()),
                resources: (!node.resources.is_empty())
                    .then(|| node.resources.build(&at, &mut issues)),
                storage: node.storage.build(),
            });
            // a name is what every command addresses the node by
            if node.name.trim().is_empty() {
                issues.push(at.error(FieldId::NodeName, "every node needs a name"));
            }
        }
        // the bootstrap set: every node if all are in it, otherwise the chosen ones in the order
        // the inventory was loaded with, then in node order
        let chosen: Vec<&str> = self
            .nodes
            .iter()
            .filter(|node| node.bootstrap)
            .map(|node| node.name.trim())
            .collect();
        let bootstrap = if chosen.len() == self.nodes.len() && self.bootstrap_order.is_empty() {
            None
        } else {
            let mut order: Vec<String> = self
                .bootstrap_order
                .iter()
                .filter(|name| chosen.contains(&name.as_str()))
                .cloned()
                .collect();
            for name in &chosen {
                if !order.iter().any(|known| known == name) {
                    order.push((*name).to_string());
                }
            }
            // every node, in node order, is what an absent bootstrap already says
            let all_in_order = order.len() == self.nodes.len()
                && order
                    .iter()
                    .zip(&self.nodes)
                    .all(|(name, node)| name == node.name.trim());
            (!all_in_order).then_some(order)
        };
        let inventory = Inventory {
            name: self.name.trim().to_string(),
            server: PathBuf::from(self.server.trim()),
            remote_dir: text(&self.remote_dir),
            ports,
            replication_factor,
            control_voters,
            resources,
            storage: self.storage.build(),
            groups,
            tracing: self.tracing.clone(),
            user: text(&self.user),
            admin: text(&self.admin).unwrap_or_else(|| "admin".to_string()),
            retire_after: text(&self.retire_after),
            bootstrap,
            nodes,
        };
        // what the inventory itself refuses, once every field parsed
        if self.nodes.is_empty() {
            issues.push(Issue {
                severity: Severity::Error,
                target: Target::Page(Page::Nodes),
                field: None,
                message: "add at least one node".to_string(),
            });
        } else if issues.iter().all(|issue| issue.severity != Severity::Error) {
            if let Err(error) = inventory.validate_shape() {
                issues.push(self.place(&error.to_string()));
            }
        }
        // a program that is not built yet is worth saying, never worth refusing the file over
        if !self.server.trim().is_empty() {
            let server = if inventory.server.is_relative() {
                base.join(&inventory.server)
            } else {
                inventory.server.clone()
            };
            if !server.is_file() {
                issues.push(Issue {
                    severity: Severity::Warning,
                    target: Target::Field(Page::Cluster),
                    field: Some(FieldId::Server),
                    message: format!(
                        "{} does not exist yet; build it before bootstrap",
                        server.display()
                    ),
                });
            } else if !is_executable(&server) {
                // a source file exists, and bootstrap refuses it as a program
                issues.push(Issue {
                    severity: Severity::Warning,
                    target: Target::Field(Page::Cluster),
                    field: Some(FieldId::Server),
                    message: format!(
                        "{} is not executable; name the built node program, not its source",
                        server.display()
                    ),
                });
            }
        }
        (inventory, issues)
    }

    /// Put an inventory's own refusal on the page and field it is about
    ///
    /// The inventory names the first thing wrong in words, so it is placed by the words it
    /// uses, and on its page as a whole where no field fits.
    ///
    /// # Arguments
    ///
    /// * `message` - The refusal
    fn place(&self, message: &str) -> Issue {
        // the node a message names, by the quoted name it uses
        let node = self
            .nodes
            .iter()
            .position(|node| message.contains(&format!("{:?}", node.name.trim())));
        let group = self
            .groups
            .iter()
            .position(|group| message.contains(&format!("{:?}", group.name.trim())));
        // most specific words first
        let (target, field) = if message.contains("cluster name") {
            (Target::Field(Page::Cluster), Some(FieldId::Name))
        } else if message.starts_with("user ") {
            (Target::Field(Page::Cluster), Some(FieldId::User))
        } else if message.starts_with("remote_dir") {
            (Target::Field(Page::Cluster), Some(FieldId::RemoteDir))
        } else if message.contains("control_voters") {
            (Target::Field(Page::Shape), Some(FieldId::ControlVoters))
        } else if message.contains("replication_factor") {
            (Target::Field(Page::Shape), Some(FieldId::ReplicationFactor))
        } else if message.contains("retire_after") {
            (Target::Field(Page::Shape), Some(FieldId::RetireAfter))
        } else if message.contains("ports") {
            (Target::Field(Page::Shape), Some(FieldId::ClientPort))
        } else if message.contains("group name") {
            (
                group.map_or(Target::Page(Page::Groups), Target::Group),
                Some(FieldId::GroupName),
            )
        } else if message.contains("names the group") {
            (
                node.map_or(Target::Page(Page::Nodes), Target::Node),
                Some(FieldId::NodeGroup),
            )
        } else if message.contains("latency storage") && message.contains("throughput storage") {
            (
                node.map_or(Target::Page(Page::Nodes), Target::Node),
                Some(FieldId::Throughput),
            )
        } else if message.contains("'s latency storage") {
            (
                node.map_or(Target::Page(Page::Nodes), Target::Node),
                Some(FieldId::Latency),
            )
        } else if message.contains("'s throughput storage") {
            (
                node.map_or(Target::Page(Page::Nodes), Target::Node),
                Some(FieldId::Throughput),
            )
        } else if message.contains("the address") {
            (Target::Page(Page::Nodes), Some(FieldId::NodeAddress))
        } else if message.contains("bootstrap") {
            (Target::Page(Page::Nodes), Some(FieldId::NodeBootstrap))
        } else if message.contains("the node") {
            (
                node.map_or(Target::Page(Page::Nodes), Target::Node),
                Some(FieldId::NodeName),
            )
        } else {
            (Target::Page(Page::Review), None)
        };
        Issue {
            severity: Severity::Error,
            target,
            field,
            message: message.to_string(),
        }
    }

    /// The text of a text field at a level, or none for a field that is not text there
    ///
    /// # Arguments
    ///
    /// * `page` - The page the field is on
    /// * `index` - The group or node selected, on those pages
    /// * `field` - The field
    #[must_use]
    pub fn text(&self, page: Page, index: usize, field: FieldId) -> Option<&str> {
        // the resources and storage of whichever level the page edits
        let level = self.level(page, index);
        let value = match field {
            FieldId::Name => &self.name,
            FieldId::Server => &self.server,
            FieldId::RemoteDir => &self.remote_dir,
            FieldId::User => &self.user,
            FieldId::Admin => &self.admin,
            FieldId::Tracing => &self.tracing,
            FieldId::ReplicationFactor => &self.replication_factor,
            FieldId::ControlVoters => &self.control_voters,
            FieldId::RetireAfter => &self.retire_after,
            FieldId::ClientPort => &self.client_port,
            FieldId::PeerPort => &self.peer_port,
            FieldId::ControlPort => &self.control_port,
            FieldId::Cores => &level?.0.cores,
            FieldId::ExcludeCores => &level?.0.exclude_cores,
            FieldId::Memory => &level?.0.memory,
            FieldId::Latency => &level?.1.latency,
            FieldId::Throughput => &level?.1.throughput,
            FieldId::GroupName => &self.groups.get(index)?.name,
            FieldId::NodeName => &self.nodes.get(index)?.name,
            FieldId::NodeSsh => &self.nodes.get(index)?.ssh,
            FieldId::NodeAddress => &self.nodes.get(index)?.address,
            FieldId::NodeGroup => {
                let node = self.nodes.get(index)?;
                return Some(
                    node.group
                        .and_then(|id| self.group_name(id))
                        .unwrap_or("none"),
                );
            }
            FieldId::ControlCoreShared | FieldId::NodeBootstrap => return None,
        };
        Some(value.as_str())
    }

    /// What a blank text field means at a level, drawn in its place
    ///
    /// Resources are taken whole, so once a group or node sets any of them a blank one is the
    /// default rather than inherited, and says so.
    ///
    /// # Arguments
    ///
    /// * `page` - The page the field is on
    /// * `index` - The group or node selected, on those pages
    /// * `field` - The field
    #[must_use]
    pub fn placeholder(&self, page: Page, index: usize, field: FieldId) -> &'static str {
        // a group or node that sets its own resources has the defaults' blanks
        let own = matches!(page, Page::Groups | Page::Nodes)
            && self
                .level(page, index)
                .is_some_and(|(resources, _)| !resources.is_empty());
        if own
            && matches!(
                field,
                FieldId::Cores | FieldId::ExcludeCores | FieldId::Memory
            )
        {
            return field.placeholder(Page::Defaults);
        }
        field.placeholder(page)
    }

    /// The value of a toggle field at a level
    ///
    /// # Arguments
    ///
    /// * `page` - The page the field is on
    /// * `index` - The group or node selected, on those pages
    /// * `field` - The field
    #[must_use]
    pub fn toggle(&self, page: Page, index: usize, field: FieldId) -> bool {
        match field {
            FieldId::ControlCoreShared => self
                .level(page, index)
                .is_some_and(|(resources, _)| resources.control_core_shared),
            FieldId::NodeBootstrap => self.nodes.get(index).is_some_and(|node| node.bootstrap),
            _ => false,
        }
    }

    /// The resources and storage the page edits
    ///
    /// # Arguments
    ///
    /// * `page` - The page
    /// * `index` - The group or node selected, on those pages
    fn level(&self, page: Page, index: usize) -> Option<(&ResourcesDraft, &StorageDraft)> {
        match page {
            Page::Groups => self
                .groups
                .get(index)
                .map(|group| (&group.resources, &group.storage)),
            Page::Nodes => self
                .nodes
                .get(index)
                .map(|node| (&node.resources, &node.storage)),
            _ => Some((&self.resources, &self.storage)),
        }
    }

    /// A text field at a level, to edit
    ///
    /// # Arguments
    ///
    /// * `page` - The page the field is on
    /// * `index` - The group or node selected, on those pages
    /// * `field` - The field
    fn text_mut(&mut self, page: Page, index: usize, field: FieldId) -> Option<&mut String> {
        // the resources and storage of whichever level the page edits
        let level = match page {
            Page::Groups => self
                .groups
                .get_mut(index)
                .map(|group| (&mut group.resources, &mut group.storage)),
            Page::Nodes => self
                .nodes
                .get_mut(index)
                .map(|node| (&mut node.resources, &mut node.storage)),
            _ => Some((&mut self.resources, &mut self.storage)),
        };
        Some(match field {
            FieldId::Name => &mut self.name,
            FieldId::Server => &mut self.server,
            FieldId::RemoteDir => &mut self.remote_dir,
            FieldId::User => &mut self.user,
            FieldId::Admin => &mut self.admin,
            FieldId::RetireAfter => &mut self.retire_after,
            FieldId::ReplicationFactor => &mut self.replication_factor,
            FieldId::ClientPort => &mut self.client_port,
            FieldId::PeerPort => &mut self.peer_port,
            FieldId::ControlPort => &mut self.control_port,
            FieldId::Cores => &mut level?.0.cores,
            FieldId::ExcludeCores => &mut level?.0.exclude_cores,
            FieldId::Memory => &mut level?.0.memory,
            FieldId::Latency => &mut level?.1.latency,
            FieldId::Throughput => &mut level?.1.throughput,
            FieldId::GroupName => &mut self.groups.get_mut(index)?.name,
            FieldId::NodeName => &mut self.nodes.get_mut(index)?.name,
            FieldId::NodeSsh => &mut self.nodes.get_mut(index)?.ssh,
            FieldId::NodeAddress => &mut self.nodes.get_mut(index)?.address,
            _ => return None,
        })
    }

    /// Flip a toggle field at a level
    ///
    /// # Arguments
    ///
    /// * `page` - The page the field is on
    /// * `index` - The group or node selected, on those pages
    /// * `field` - The field
    fn flip(&mut self, page: Page, index: usize, field: FieldId) {
        match (field, page) {
            (FieldId::ControlCoreShared, Page::Groups) => {
                if let Some(group) = self.groups.get_mut(index) {
                    group.resources.control_core_shared ^= true;
                }
            }
            (FieldId::ControlCoreShared, Page::Nodes) => {
                if let Some(node) = self.nodes.get_mut(index) {
                    node.resources.control_core_shared ^= true;
                }
            }
            (FieldId::ControlCoreShared, _) => self.resources.control_core_shared ^= true,
            (FieldId::NodeBootstrap, _) => {
                if let Some(node) = self.nodes.get_mut(index) {
                    node.bootstrap ^= true;
                }
            }
            _ => (),
        }
    }

    /// Cycle a choice field at a level
    ///
    /// # Arguments
    ///
    /// * `index` - The node selected, for its group
    /// * `field` - The field
    /// * `forward` - Whether to step forward rather than back
    fn cycle(&mut self, index: usize, field: FieldId, forward: bool) {
        // step through a fixed list, wrapping, from wherever the value is
        let step = |options: &[&str], current: &str| -> String {
            let at = options
                .iter()
                .position(|option| *option == current)
                .unwrap_or(0);
            let next = if forward {
                (at + 1) % options.len()
            } else {
                (at + options.len() - 1) % options.len()
            };
            options[next].to_string()
        };
        match field {
            FieldId::Tracing => self.tracing = step(&TRACING_LEVELS, &self.tracing),
            FieldId::ControlVoters => {
                self.control_voters = step(&CONTROL_VOTERS, &self.control_voters);
            }
            FieldId::NodeGroup => {
                // none, then every group in order
                let choices: Vec<Option<u64>> = std::iter::once(None)
                    .chain(self.groups.iter().map(|group| Some(group.id)))
                    .collect();
                if let Some(node) = self.nodes.get_mut(index) {
                    let at = choices
                        .iter()
                        .position(|choice| *choice == node.group)
                        .unwrap_or(0);
                    let next = if forward {
                        (at + 1) % choices.len()
                    } else {
                        (at + choices.len() - 1) % choices.len()
                    };
                    node.group = choices[next];
                }
            }
            _ => (),
        }
    }
}

/// A question the wizard is waiting on an answer to
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Confirm {
    /// Leave without writing the changes
    Quit,
    /// Replace the file that is already at the output path
    Overwrite,
}

/// What the loop should do after a key
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Draw again and keep going
    Continue,
    /// Write the inventory and leave
    Save,
    /// Leave without writing
    Quit,
    /// Probe the node at this index over ssh
    Probe(usize),
}

/// What a probe of a host found, or that it is still running or failed
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbeState {
    /// The ssh round trip has not come back
    Running,
    /// What the host said
    Done(ProbeReport),
    /// Why the host could not be asked
    Failed(String),
}

/// What a node's name resolves to on this machine, which is where bootstrap will resolve it
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolution {
    /// The lookup has not come back
    Running,
    /// The address bootstrap would give it
    Resolved(IpAddr),
    /// It resolves, but only to loopback, which bootstrap refuses
    Loopback,
    /// It does not resolve here, and why
    Failed(String),
}

/// What a host told a probe about itself
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProbeReport {
    /// How many cpus it has
    pub cpus: Option<usize>,
    /// How much memory it has, in bytes
    pub memory: Option<u64>,
    /// Each storage directory, and the bytes free on the filesystem that would hold it
    pub free: Vec<(String, Option<u64>)>,
    /// The storage directories that already hold a node's marker
    pub claimed: Vec<String>,
}

/// The whole wizard: the draft, where the operator is in it, and what it is showing them
#[derive(Debug, Clone)]
pub struct Wizard {
    /// The inventory being built
    pub draft: Draft,
    /// Where it will be written
    pub out: PathBuf,
    /// Whether a file is already at that path
    pub out_exists: bool,
    /// The page shown
    pub page: Page,
    /// The field in focus on a form page, or in a list page's edit panel
    pub focus: usize,
    /// The group or node selected on a list page
    pub selected: usize,
    /// Whether a list page's edit panel has focus rather than its list
    pub editing: bool,
    /// Whether the draft changed since it was opened
    pub dirty: bool,
    /// A question waiting on an answer
    pub confirm: Option<Confirm>,
    /// Whether the operator already agreed to replace the file at the output path
    pub overwrite: bool,
    /// A line to show the operator, and whether it is bad news
    pub message: Option<(Severity, String)>,
    /// How far the review is scrolled
    pub scroll: u16,
    /// What each probed node's host said, by node name
    pub probes: BTreeMap<String, ProbeState>,
    /// What each node's name resolves to here, by node name, for the nodes given no address
    pub resolutions: BTreeMap<String, Resolution>,
}

impl Wizard {
    /// Start a wizard on a draft
    ///
    /// # Arguments
    ///
    /// * `draft` - The draft to start from, empty or loaded from an inventory
    /// * `out` - Where the inventory will be written
    /// * `out_exists` - Whether a file is already there
    #[must_use]
    pub fn new(draft: Draft, out: PathBuf, out_exists: bool) -> Self {
        Wizard {
            draft,
            out,
            out_exists,
            page: Page::Cluster,
            focus: 0,
            selected: 0,
            editing: false,
            dirty: false,
            confirm: None,
            overwrite: false,
            message: None,
            scroll: 0,
            probes: BTreeMap::new(),
            resolutions: BTreeMap::new(),
        }
    }

    /// The directory the inventory will be written to
    #[must_use]
    pub fn base(&self) -> PathBuf {
        self.out.parent().map(Path::to_path_buf).unwrap_or_default()
    }

    /// Build the draft's inventory and its issues, with what each blank address resolved to
    #[must_use]
    pub fn build(&self) -> (Inventory, Vec<Issue>) {
        // everything the draft says on its own
        let (inventory, mut issues) = self.draft.build(&self.base());
        // then what bootstrap would find resolving the name of every node given no address
        for (index, spec) in inventory.nodes.iter().enumerate() {
            if spec.address.is_some() || spec.name.is_empty() {
                continue;
            }
            let at = Target::Node(index);
            match self.resolutions.get(&spec.name) {
                // bootstrap from here is certain to refuse it
                Some(Resolution::Loopback) => issues.push(at.error(
                    FieldId::NodeAddress,
                    &format!(
                        "{} resolves only to loopback addresses here; give it an address",
                        spec.name
                    ),
                )),
                // a host that does not exist yet is a draft worth keeping
                Some(Resolution::Failed(_)) => issues.push(Issue {
                    severity: Severity::Warning,
                    target: at,
                    field: Some(FieldId::NodeAddress),
                    message: format!(
                        "{} does not resolve from here; give it an address or check its DNS",
                        spec.name
                    ),
                }),
                // still looking, or an address a peer can dial
                Some(Resolution::Running | Resolution::Resolved(_)) | None => (),
            }
        }
        (inventory, issues)
    }

    /// The names of the nodes given no address whose resolution has not been asked for
    #[must_use]
    pub fn unresolved(&self) -> Vec<String> {
        // a blank address means bootstrap resolves the name, so the wizard has to as well
        let mut names: Vec<String> = self
            .draft
            .nodes
            .iter()
            .filter(|node| node.address.trim().is_empty())
            .map(|node| node.name.trim().to_string())
            .filter(|name| !name.is_empty() && !self.resolutions.contains_key(name))
            .collect();
        // two nodes of one name are asked about once, wherever they are in the list
        names.sort_unstable();
        names.dedup();
        names
    }

    /// The fields the focus moves through right now: a form page's, or the edit panel's
    #[must_use]
    pub fn fields(&self) -> &'static [FieldId] {
        match self.page {
            Page::Groups | Page::Nodes if !self.editing => &[],
            page => FieldId::page_fields(page),
        }
    }

    /// The field in focus, if the page has one in focus
    #[must_use]
    pub fn focused(&self) -> Option<FieldId> {
        self.fields().get(self.focus).copied()
    }

    /// How many items the list page shown has
    fn list_len(&self) -> usize {
        match self.page {
            Page::Groups => self.draft.groups.len(),
            Page::Nodes => self.draft.nodes.len(),
            _ => 0,
        }
    }

    /// Show a page, from its top
    ///
    /// # Arguments
    ///
    /// * `page` - The page
    pub fn go(&mut self, page: Page) {
        self.page = page;
        self.focus = 0;
        self.editing = false;
        self.scroll = 0;
        self.selected = self.selected.min(self.list_len().saturating_sub(1));
    }

    /// Handle one key
    ///
    /// # Arguments
    ///
    /// * `key` - The key
    pub fn handle_key(&mut self, key: KeyEvent) -> Outcome {
        // a question on screen takes the next key as its answer
        if let Some(confirm) = self.confirm.take() {
            return self.answer(confirm, key.code);
        }
        // a message is read once
        self.message = None;
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        // the keys that work everywhere
        match key.code {
            KeyCode::Char('c') if ctrl => return self.quit(),
            KeyCode::Char('n') if ctrl => return self.turn(true),
            KeyCode::PageDown => return self.turn(true),
            KeyCode::Char('p') if ctrl => return self.turn(false),
            KeyCode::PageUp => return self.turn(false),
            KeyCode::Esc if self.editing => {
                // back from the edit panel to its list
                self.editing = false;
                self.focus = 0;
                return Outcome::Continue;
            }
            KeyCode::Esc => return self.quit(),
            _ => (),
        }
        // then whatever the page does with it
        match self.page {
            Page::Review => self.review_key(key.code),
            Page::Groups | Page::Nodes if !self.editing => self.list_key(key.code),
            _ => self.field_key(key),
        }
    }

    /// Turn to the next or the previous page
    ///
    /// # Arguments
    ///
    /// * `forward` - Whether to go to the next page
    fn turn(&mut self, forward: bool) -> Outcome {
        let page = if forward {
            self.page.next()
        } else {
            self.page.prev()
        };
        if let Some(page) = page {
            self.go(page);
        }
        Outcome::Continue
    }

    /// Leave, asking first if anything would be lost
    fn quit(&mut self) -> Outcome {
        if self.dirty {
            self.confirm = Some(Confirm::Quit);
            Outcome::Continue
        } else {
            Outcome::Quit
        }
    }

    /// Take the answer to a question
    ///
    /// # Arguments
    ///
    /// * `confirm` - The question
    /// * `code` - The key answering it
    fn answer(&mut self, confirm: Confirm, code: KeyCode) -> Outcome {
        let yes = matches!(code, KeyCode::Char('y' | 'Y'));
        match (confirm, yes) {
            (Confirm::Quit, true) => Outcome::Quit,
            (Confirm::Overwrite, true) => {
                self.overwrite = true;
                Outcome::Save
            }
            (_, false) => Outcome::Continue,
        }
    }

    /// Handle a key on the review page
    ///
    /// # Arguments
    ///
    /// * `code` - The key
    fn review_key(&mut self, code: KeyCode) -> Outcome {
        match code {
            KeyCode::Up | KeyCode::Char('k') => self.scroll = self.scroll.saturating_sub(1),
            KeyCode::Down | KeyCode::Char('j') => self.scroll = self.scroll.saturating_add(1),
            KeyCode::Home => self.scroll = 0,
            KeyCode::Char('s') => return self.save(),
            _ => (),
        }
        Outcome::Continue
    }

    /// Save, if nothing stands in the way
    fn save(&mut self) -> Outcome {
        // an error keeps the file from being written, and says where it is
        let (_, issues) = self.build();
        let errors = issues
            .iter()
            .filter(|issue| issue.severity == Severity::Error)
            .count();
        if errors > 0 {
            self.message = Some((
                Severity::Error,
                format!(
                    "fix the {errors} error{} listed first",
                    if errors == 1 { "" } else { "s" }
                ),
            ));
            return Outcome::Continue;
        }
        // a file already there is replaced only when the operator says so
        if self.out_exists && !self.overwrite {
            self.confirm = Some(Confirm::Overwrite);
            return Outcome::Continue;
        }
        Outcome::Save
    }

    /// Handle a key on a list page's list
    ///
    /// # Arguments
    ///
    /// * `code` - The key
    fn list_key(&mut self, code: KeyCode) -> Outcome {
        let len = self.list_len();
        match code {
            KeyCode::Up | KeyCode::Char('k') => self.selected = self.selected.saturating_sub(1),
            KeyCode::Down | KeyCode::Char('j') => {
                self.selected = (self.selected + 1).min(len.saturating_sub(1));
            }
            KeyCode::Char('a') => self.add(),
            KeyCode::Char('d') | KeyCode::Delete => self.delete(),
            KeyCode::Enter | KeyCode::Char('e') if len > 0 => {
                self.editing = true;
                self.focus = 0;
            }
            KeyCode::Char(' ') if self.page == Page::Nodes && len > 0 => {
                self.draft
                    .flip(Page::Nodes, self.selected, FieldId::NodeBootstrap);
                self.dirty = true;
            }
            KeyCode::Left | KeyCode::Right if self.page == Page::Nodes && len > 0 => {
                self.draft
                    .cycle(self.selected, FieldId::NodeGroup, code == KeyCode::Right);
                self.dirty = true;
            }
            KeyCode::Char('p') if self.page == Page::Nodes && len > 0 => {
                return Outcome::Probe(self.selected);
            }
            KeyCode::Tab => return self.turn(true),
            KeyCode::BackTab => return self.turn(false),
            _ => (),
        }
        Outcome::Continue
    }

    /// Add a group or a node and open it for editing
    fn add(&mut self) {
        match self.page {
            Page::Groups => {
                let id = self.draft.next_group;
                self.draft.next_group += 1;
                self.draft.groups.push(GroupDraft {
                    id,
                    name: String::new(),
                    resources: ResourcesDraft::default(),
                    storage: StorageDraft::default(),
                });
                self.selected = self.draft.groups.len() - 1;
            }
            Page::Nodes => {
                // a new node takes the group the one before it took, since hosts are commonly
                // listed a group at a time
                let group = self.draft.nodes.last().and_then(|node| node.group);
                self.draft.nodes.push(NodeDraft {
                    name: String::new(),
                    ssh: String::new(),
                    address: String::new(),
                    group,
                    bootstrap: true,
                    resources: ResourcesDraft::default(),
                    storage: StorageDraft::default(),
                });
                self.selected = self.draft.nodes.len() - 1;
            }
            _ => return,
        }
        self.editing = true;
        self.focus = 0;
        self.dirty = true;
    }

    /// Delete the selected group or node, refusing a group a node still names
    fn delete(&mut self) {
        match self.page {
            Page::Groups => {
                let Some(group) = self.draft.groups.get(self.selected) else {
                    return;
                };
                // a node naming it would silently lose its settings
                let users: Vec<&str> = self
                    .draft
                    .nodes
                    .iter()
                    .filter(|node| node.group == Some(group.id))
                    .map(|node| node.name.as_str())
                    .collect();
                if !users.is_empty() {
                    self.message = Some((
                        Severity::Error,
                        format!(
                            "{} is used by {}; move them to another group first",
                            group.name,
                            users.join(", ")
                        ),
                    ));
                    return;
                }
                self.draft.groups.remove(self.selected);
            }
            Page::Nodes => {
                if self.selected >= self.draft.nodes.len() {
                    return;
                }
                let node = self.draft.nodes.remove(self.selected);
                self.probes.remove(&node.name);
                self.resolutions.remove(node.name.trim());
            }
            _ => return,
        }
        self.selected = self.selected.min(self.list_len().saturating_sub(1));
        self.dirty = true;
    }

    /// Handle a key on a form page or an edit panel
    ///
    /// # Arguments
    ///
    /// * `key` - The key
    fn field_key(&mut self, key: KeyEvent) -> Outcome {
        let fields = self.fields();
        let Some(field) = fields.get(self.focus).copied() else {
            return Outcome::Continue;
        };
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match key.code {
            // moving between fields
            KeyCode::Tab | KeyCode::Down => self.focus = (self.focus + 1) % fields.len(),
            KeyCode::BackTab | KeyCode::Up => {
                self.focus = (self.focus + fields.len() - 1) % fields.len();
            }
            KeyCode::Enter => {
                // the last field of an edit panel hands back to its list, otherwise move on
                if self.editing && self.focus + 1 == fields.len() {
                    self.editing = false;
                    self.focus = 0;
                } else if self.focus + 1 == fields.len() {
                    return self.turn(true);
                } else {
                    self.focus += 1;
                }
            }
            // a toggle flips on space
            KeyCode::Char(' ') if field.kind() == FieldKind::Toggle => {
                self.draft.flip(self.page, self.selected, field);
                self.dirty = true;
            }
            // a choice cycles on the arrows, and on space
            KeyCode::Left | KeyCode::Right | KeyCode::Char(' ')
                if field.kind() == FieldKind::Choice =>
            {
                self.draft
                    .cycle(self.selected, field, key.code != KeyCode::Left);
                self.dirty = true;
            }
            // text is typed at its end
            KeyCode::Char('u') if ctrl => {
                if let Some(text) = self.draft.text_mut(self.page, self.selected, field) {
                    text.clear();
                    self.dirty = true;
                }
            }
            KeyCode::Char(c) if !ctrl && field.kind() == FieldKind::Text => {
                if let Some(text) = self.draft.text_mut(self.page, self.selected, field) {
                    text.push(c);
                    self.dirty = true;
                }
            }
            KeyCode::Backspace => {
                if let Some(text) = self.draft.text_mut(self.page, self.selected, field) {
                    text.pop();
                    self.dirty = true;
                }
            }
            _ => (),
        }
        Outcome::Continue
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deploy::inventory::Source;

    /// A key with no modifiers
    ///
    /// # Arguments
    ///
    /// * `code` - The key
    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    /// Type a string into whatever field has focus
    ///
    /// # Arguments
    ///
    /// * `wizard` - The wizard
    /// * `text` - What to type
    fn type_text(wizard: &mut Wizard, text: &str) {
        for c in text.chars() {
            wizard.handle_key(key(KeyCode::Char(c)));
        }
    }

    /// An inventory with a server program that exists, parsed from YAML
    ///
    /// # Arguments
    ///
    /// * `yaml` - The inventory, without its `server` line
    fn parse(yaml: &str) -> Inventory {
        // this test binary always exists, so it stands in for a server program
        let server = std::env::current_exe().expect("the test binary");
        serde_yaml::from_str(&format!("server: {}\n{yaml}", server.display()))
            .expect("an inventory")
    }

    /// An inventory loaded into a draft comes back out of it unchanged
    #[test]
    fn a_draft_round_trips_an_inventory() {
        // the lab as it is committed, a groups inventory, and one bootstrapping in its own order
        let lab = parse(
            "name: lab\nreplication_factor: 3\ncontrol_voters: 3\nretire_after: 15s\nuser: shoal\n\
             resources: {cores: 4, memory: 4Gi}\nnodes:\n  - {name: hyperion, address: 172.16.2.5}\n  - {name: titan, address: 172.16.2.4}\n  - {name: europa, address: 172.16.2.10}\n",
        );
        let groups = parse(
            "name: lab\nreplication_factor: 2\nstorage: {latency: /srv/shoal/logs}\n\
             groups:\n  small: {resources: {cores: 4, exclude_cores: [3]}, storage: {latency: /mnt/nvme/shoal, throughput: /mnt/bulk/shoal}}\n  big: {storage: {throughput: /mnt/raid/shoal}}\n\
             bootstrap: [c, a]\nnodes:\n  - {name: a, address: 10.0.0.1, group: small}\n  - {name: b, ssh: ops@b, group: big, storage: {latency: /mnt/b/shoal}}\n  - {name: c, address: 10.0.0.3, resources: {cores: 2, memory: 1Gi, control_core_shared: true}}\n",
        );
        for inventory in [lab, groups] {
            // loaded, built and judged, it is the same inventory with nothing wrong
            let draft = Draft::from_inventory(&inventory);
            let (built, issues) = draft.build(Path::new("/"));
            assert!(issues.is_empty(), "{issues:?}");
            assert_eq!(built, inventory);
        }
    }

    /// Typing a cluster in page by page builds the inventory it describes
    #[test]
    fn a_cluster_typed_in_builds_its_inventory() {
        // a wizard writing next to this test binary, which stands in for the program
        let server = std::env::current_exe().expect("the test binary");
        let mut wizard = Wizard::new(Draft::default(), server.with_file_name("new.yml"), false);
        // the cluster page: a name, then the program
        type_text(&mut wizard, "lab");
        wizard.handle_key(key(KeyCode::Tab));
        type_text(&mut wizard, &server.display().to_string());
        // the groups page: one group with fast logs
        wizard.go(Page::Groups);
        wizard.handle_key(key(KeyCode::Char('a')));
        type_text(&mut wizard, "small");
        for _ in 0..5 {
            wizard.handle_key(key(KeyCode::Tab));
        }
        assert_eq!(wizard.focused(), Some(FieldId::Latency));
        type_text(&mut wizard, "/mnt/nvme/shoal");
        wizard.handle_key(key(KeyCode::Esc));
        // the nodes page: three nodes, the first put in the group and the rest following it
        wizard.go(Page::Nodes);
        for (name, address) in [("a", "10.0.0.1"), ("b", "10.0.0.2"), ("c", "10.0.0.3")] {
            wizard.handle_key(key(KeyCode::Char('a')));
            type_text(&mut wizard, name);
            wizard.handle_key(key(KeyCode::Tab));
            wizard.handle_key(key(KeyCode::Tab));
            type_text(&mut wizard, address);
            if name == "a" {
                wizard.handle_key(key(KeyCode::Tab));
                wizard.handle_key(key(KeyCode::Right));
            }
            wizard.handle_key(key(KeyCode::Esc));
        }
        // c is left for add
        wizard.handle_key(key(KeyCode::Char(' ')));
        let (inventory, issues) = wizard.build();
        // only fewer bootstrap nodes than the factor is wrong, and it is said on the factor
        assert_eq!(issues.len(), 1, "{issues:?}");
        assert_eq!(issues[0].field, Some(FieldId::ReplicationFactor));
        assert_eq!(issues[0].target, Target::Field(Page::Shape));
        assert_eq!(
            inventory.bootstrap,
            Some(vec!["a".to_string(), "b".to_string()])
        );
        // so the factor goes to two, and the inventory is right
        wizard.go(Page::Shape);
        wizard.handle_key(key(KeyCode::Backspace));
        type_text(&mut wizard, "2");
        let (inventory, issues) = wizard.build();
        assert!(issues.is_empty(), "{issues:?}");
        assert_eq!(inventory.nodes[1].group.as_deref(), Some("small"));
        assert_eq!(
            inventory.node("b").unwrap().storage.latency,
            "/mnt/nvme/shoal"
        );
        assert_eq!(inventory.user.as_deref(), Some("shoal"));
        // and saving it is allowed
        wizard.go(Page::Review);
        assert_eq!(wizard.handle_key(key(KeyCode::Char('s'))), Outcome::Save);
    }

    /// An issue is reported on the page and the field it is about
    #[test]
    fn an_issue_lands_on_its_field() {
        // a draft of a valid inventory
        let inventory = parse(
            "name: lab\nreplication_factor: 1\ngroups:\n  small: {}\nnodes:\n  - {name: a, address: 10.0.0.1, group: small}\n",
        );
        let draft = Draft::from_inventory(&inventory);
        // what each change breaks, and where it has to be reported
        let cases: [(fn(&mut Draft), Target, FieldId); 6] = [
            (
                |draft| draft.name = "my lab".into(),
                Target::Field(Page::Cluster),
                FieldId::Name,
            ),
            (
                |draft| draft.client_port = "http".into(),
                Target::Field(Page::Shape),
                FieldId::ClientPort,
            ),
            (
                |draft| draft.control_voters = "2".into(),
                Target::Field(Page::Shape),
                FieldId::ControlVoters,
            ),
            (
                |draft| draft.groups[0].name = "big box".into(),
                Target::Group(0),
                FieldId::GroupName,
            ),
            (
                |draft| draft.nodes[0].address = "a.lab".into(),
                Target::Node(0),
                FieldId::NodeAddress,
            ),
            (
                |draft| draft.nodes[0].storage.latency = "/mnt".into(),
                Target::Node(0),
                FieldId::Latency,
            ),
        ];
        for (change, target, field) in cases {
            let mut broken = draft.clone();
            change(&mut broken);
            let (_, issues) = broken.build(Path::new("/"));
            let error = issues
                .iter()
                .find(|issue| issue.severity == Severity::Error)
                .unwrap_or_else(|| panic!("no error for {field:?}"));
            assert_eq!(
                (&error.target, error.field),
                (&target, Some(field)),
                "{error:?}"
            );
        }
        // a program that is not built is a warning on its field, and never an error
        let mut unbuilt = draft.clone();
        unbuilt.server = "shoal-node".into();
        let (_, issues) = unbuilt.build(Path::new("/nonexistent"));
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].severity, Severity::Warning);
        assert_eq!(issues[0].field, Some(FieldId::Server));
        assert!(issues[0].message.contains("/nonexistent/shoal-node"));
    }

    /// A group a node names cannot be deleted, and a renamed group keeps its nodes
    #[test]
    fn a_group_in_use_cannot_be_deleted() {
        // a group with one node in it
        let inventory = parse(
            "name: lab\nreplication_factor: 1\ngroups:\n  small: {}\n  spare: {}\nnodes:\n  - {name: a, address: 10.0.0.1, group: small}\n",
        );
        let mut wizard = Wizard::new(
            Draft::from_inventory(&inventory),
            PathBuf::from("/x.yml"),
            false,
        );
        wizard.go(Page::Groups);
        // spare sorts second and nobody uses it, small sorts first and a does
        assert_eq!(wizard.draft.groups[0].name, "small");
        wizard.handle_key(key(KeyCode::Char('d')));
        assert_eq!(wizard.draft.groups.len(), 2, "a group in use was deleted");
        let (severity, message) = wizard.message.clone().expect("a refusal");
        assert_eq!(severity, Severity::Error);
        assert!(message.contains("small") && message.contains('a'));
        // renaming it carries its node with it
        wizard.handle_key(key(KeyCode::Enter));
        type_text(&mut wizard, "er");
        let (built, _) = wizard.build();
        assert_eq!(built.nodes[0].group.as_deref(), Some("smaller"));
        // and the unused one goes
        wizard.handle_key(key(KeyCode::Esc));
        wizard.handle_key(key(KeyCode::Down));
        wizard.handle_key(key(KeyCode::Char('d')));
        assert_eq!(wizard.draft.groups.len(), 1);
    }

    /// Leaving with changes asks first, and saving over a file asks first
    #[test]
    fn losing_or_replacing_work_is_asked_about() {
        // a changed draft asks before it quits, and stays on no
        let mut wizard = Wizard::new(Draft::default(), PathBuf::from("/x.yml"), true);
        type_text(&mut wizard, "lab");
        assert_eq!(wizard.handle_key(key(KeyCode::Esc)), Outcome::Continue);
        assert_eq!(wizard.confirm, Some(Confirm::Quit));
        assert_eq!(
            wizard.handle_key(key(KeyCode::Char('n'))),
            Outcome::Continue
        );
        assert_eq!(wizard.handle_key(key(KeyCode::Esc)), Outcome::Continue);
        assert_eq!(wizard.handle_key(key(KeyCode::Char('y'))), Outcome::Quit);
        // a valid draft whose file exists asks before it saves
        let inventory =
            parse("name: lab\nreplication_factor: 1\nnodes:\n  - {name: a, address: 10.0.0.1}\n");
        let mut wizard = Wizard::new(
            Draft::from_inventory(&inventory),
            PathBuf::from("/x.yml"),
            true,
        );
        wizard.go(Page::Review);
        assert_eq!(
            wizard.handle_key(key(KeyCode::Char('s'))),
            Outcome::Continue
        );
        assert_eq!(wizard.confirm, Some(Confirm::Overwrite));
        assert_eq!(wizard.handle_key(key(KeyCode::Char('y'))), Outcome::Save);
        // and a draft with an error is not saved at all
        wizard.draft.name.clear();
        assert_eq!(
            wizard.handle_key(key(KeyCode::Char('s'))),
            Outcome::Continue
        );
        assert!(
            wizard
                .message
                .as_ref()
                .is_some_and(|(_, message)| message.contains("error"))
        );
    }

    /// A blank resource says it is inherited until the level sets any, then says its default
    #[test]
    fn a_blank_resource_says_what_it_means() {
        // a group that sets nothing inherits every resource
        let inventory = parse(
            "name: lab\nreplication_factor: 1\ngroups:\n  bare: {}\n  small: {resources: {cores: 4}}\nnodes:\n  - {name: a, address: 10.0.0.1}\n",
        );
        let draft = Draft::from_inventory(&inventory);
        assert_eq!(
            draft.placeholder(Page::Groups, 0, FieldId::ExcludeCores),
            "inherited"
        );
        // one that sets its cores has its own resources whole, so a blank is the default
        assert_eq!(
            draft.placeholder(Page::Groups, 1, FieldId::ExcludeCores),
            "none"
        );
        assert_eq!(draft.placeholder(Page::Groups, 1, FieldId::Memory), "4Gi");
        // and storage is still inherited field by field
        assert_eq!(
            draft.placeholder(Page::Groups, 1, FieldId::Latency),
            "inherited"
        );
    }

    /// A node whose name resolves only to loopback is refused before the file is written (item 127)
    #[test]
    fn a_loopback_only_name_is_refused_before_saving() {
        // localhost resolves to nothing but loopback everywhere, as europa does on europa
        let inventory = parse("name: lab\nreplication_factor: 1\nnodes:\n  - {name: localhost}\n");
        let mut wizard = Wizard::new(
            Draft::from_inventory(&inventory),
            PathBuf::from("/x.yml"),
            false,
        );
        // the name is resolved here the way the loop resolves it, once
        assert_eq!(wizard.unresolved(), vec!["localhost".to_string()]);
        for name in wizard.unresolved() {
            let resolution = super::super::probe::resolution(&name);
            wizard.resolutions.insert(name, resolution);
        }
        assert!(wizard.unresolved().is_empty());
        let (built, issues) = wizard.build();
        // bootstrap refuses it, so the wizard has to as well
        assert!(built.bootstrap_nodes().is_err());
        let error = issues
            .iter()
            .find(|issue| issue.severity == Severity::Error)
            .unwrap_or_else(|| panic!("the wizard would save it: {issues:?}"));
        assert_eq!(
            (&error.target, error.field),
            (&Target::Node(0), Some(FieldId::NodeAddress))
        );
        // so saving is refused
        wizard.go(Page::Review);
        assert_eq!(
            wizard.handle_key(key(KeyCode::Char('s'))),
            Outcome::Continue
        );
        // a name that does not resolve is only a warning, since the host may not exist yet
        wizard
            .resolutions
            .insert("localhost".into(), Resolution::Failed("no answer".into()));
        let (_, issues) = wizard.build();
        assert_eq!(issues.len(), 1, "{issues:?}");
        assert_eq!(issues[0].severity, Severity::Warning);
        // and one that resolves, or an address given, is nothing at all
        wizard.resolutions.insert(
            "localhost".into(),
            Resolution::Resolved("10.0.0.1".parse().unwrap()),
        );
        assert!(wizard.build().1.is_empty());
        wizard
            .resolutions
            .insert("localhost".into(), Resolution::Loopback);
        wizard.draft.nodes[0].address = "10.0.0.1".into();
        assert!(wizard.build().1.is_empty());
        assert!(wizard.unresolved().is_empty());
    }

    /// A source file named as the server program is a warning on its field (item 127)
    #[test]
    fn a_source_file_is_not_a_server_program() {
        // a file that exists and has no execute bit, as tables.rs had
        let source = tempfile::NamedTempFile::new().expect("a temp file");
        let mut draft = Draft::from_inventory(&parse(
            "name: lab\nreplication_factor: 1\nnodes:\n  - {name: a, address: 10.0.0.1}\n",
        ));
        draft.server = source.path().display().to_string();
        let (_, issues) = draft.build(Path::new("/"));
        assert_eq!(issues.len(), 1, "{issues:?}");
        assert_eq!(issues[0].severity, Severity::Warning);
        assert_eq!(issues[0].field, Some(FieldId::Server));
        assert!(issues[0].message.contains("not executable"));
    }

    /// What a resolved field is drawn with names where it came from
    #[test]
    fn a_source_names_its_level() {
        assert_eq!(Source::Group("small".into()).to_string(), "group small");
        assert_eq!(Source::Default.to_string(), "default");
    }
}
