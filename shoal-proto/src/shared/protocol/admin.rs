//! The frames a client uses to see and change a cluster, and the topology every client can hold
//!
//! ```text
//!  Topology       : [header][query id (16 B)][json TopologyFrame]     server → client
//!  Admin          : [header][request id (16 B)][json AdminRequest]    client → server
//!  AdminResponse  : [header][request id (16 B)][json AdminResponse]   server → client
//! ```
//!
//! Every one of them carries the sixteen byte id every server frame carries after its header,
//! so the client reads one preamble for these as it does for a response ([`decode_server_frame`]).
//! A topology pushed because the cluster changed carries the nil id, the way a connection level
//! error does: it is about the connection's view of the cluster, not about any query.
//!
//! The bodies are JSON rather than rkyv on purpose. A topology is a few hundred bytes read as
//! often by a person as by a program, an admin request is a handful of fields with a uuid in
//! them, and neither is on any path a query takes; the query bytes stay rkyv and this module
//! never touches them. JSON is also what the control lane already speaks
//! ([F38](../../../../docs/src/features/inter-node-transport.md)), so the same serde types
//! serve both.
//!
//! # Invariants
//!
//! **Nothing here decides anything.** Whether a principal may mutate the cluster, whether a
//! version is current and whether an operation was already applied are the server's to judge;
//! this module only names the request, the answer and the refusal so that both ends spell them
//! the same way ([C9](../../../../docs/src/distributed/operations.md)).
//!
//! [`decode_server_frame`]: super::decode_server_frame

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::error::ErrorCode;
use super::QUERY_ID_LEN;
use crate::shared::identity::{ClusterId, NodeId, TableId};

/// A request a client makes of the cluster rather than of a table
///
/// Every mutation carries an operation id and the topology version it was written against:
/// the id makes a repeat harmless, since the cluster answers a seen id with what it answered
/// before, and the version makes a request written against a stale view refused rather than
/// applied to a cluster it never saw.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdminRequest {
    /// The identity of this operation, minted by the client and kept across a retry
    pub op: Uuid,
    /// The topology version this request was written against
    ///
    /// Ignored by a read; a mutation is refused `StaleVersion` when it differs from the
    /// cluster's current version.
    pub expected_version: u64,
    /// What is asked
    pub kind: AdminKind,
}

/// What an administrative request asks for
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AdminKind {
    /// Every member the cluster knows, with its role and health
    Members,
    /// Whether this node is live, joined and able to serve default reads and writes
    Readiness,
    /// What the failure detector currently believes, committed and local
    Detector,
    /// What the node's tablet groups look like: leaders, applied lag, pending bytes
    ///
    /// The "lag" record [C9](../../../../docs/src/distributed/operations.md) asks for, as every
    /// shard last reported it ([F40](../../../../docs/src/features/replication.md)).
    Replication,
    /// Place every tablet across these nodes, once, in this order
    ///
    /// Explicit by design: nothing places data on a node because it happened to join
    /// ([C4](../../../../docs/src/distributed/tablet-map.md)).
    Initialize {
        /// The members to place tablets over, in placement order
        nodes: Vec<NodeId>,
    },
    /// Change how many nodes vote in the control group
    SetControlVoters {
        /// The new count, which has to be one, three or five
        count: u32,
    },
}

impl AdminKind {
    /// Whether this request changes the cluster, and so needs an admin principal and a version
    #[must_use]
    pub const fn is_mutation(&self) -> bool {
        matches!(
            self,
            AdminKind::Initialize { .. } | AdminKind::SetControlVoters { .. }
        )
    }

    /// The name of this request kind, for a log line
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            AdminKind::Members => "members",
            AdminKind::Readiness => "readiness",
            AdminKind::Detector => "detector",
            AdminKind::Replication => "replication",
            AdminKind::Initialize { .. } => "initialize",
            AdminKind::SetControlVoters { .. } => "set_control_voters",
        }
    }
}

/// Why an administrative request was refused
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminError {
    /// What class of refusal this is, as the number it is written as
    pub code: u16,
    /// What the server said about it, for a person
    pub msg: String,
}

impl AdminError {
    /// Build a refusal
    ///
    /// # Arguments
    ///
    /// * `code` - What class of refusal this is
    /// * `msg` - What to say about it
    pub fn new<M: Into<String>>(code: ErrorCode, msg: M) -> Self {
        AdminError {
            code: code.as_u16(),
            msg: msg.into(),
        }
    }

    /// What class of refusal this is
    #[must_use]
    pub fn code(&self) -> ErrorCode {
        ErrorCode::from_u16(self.code)
    }
}

/// The answer to an administrative request
///
/// Every answer says which node answered and what topology version it held, so a reader can
/// tell a stale answer from a current one whatever the body says.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdminResponse {
    /// The node that answered
    pub node: NodeId,
    /// The topology version that node held when it answered
    pub topology_version: u64,
    /// The answer, or why there is none
    pub outcome: Result<AdminOutcome, AdminError>,
}

/// What an administrative request produced
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AdminOutcome {
    /// A read, as the JSON the server built for it
    Read(serde_json::Value),
    /// A mutation applied, and the topology version it moved the cluster to
    Applied {
        /// The version after the change
        version: u64,
    },
    /// A mutation seen before under this operation id, answered as it was the first time
    Repeated {
        /// The version the first application moved the cluster to
        version: u64,
    },
}

/// One member of the cluster, as a client sees it
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopologyMember {
    /// The node
    pub node: NodeId,
    /// Where clients reach it
    pub client: String,
    /// Where data peers reach it
    pub data: String,
    /// Where control peers reach it
    pub control: String,
    /// How many shards it runs
    pub shards: u16,
    /// Whether it votes in the control group or only learns from it
    pub role: String,
    /// Whether it is joining, up or down, as the control group has committed it
    pub health: String,
    /// Which start of it the cluster has admitted
    pub incarnation: u64,
    /// The shards that have failed on it, by index
    pub shards_failed: Vec<u16>,
}

/// The cluster as a client sees it
///
/// The placement is the ordered node list every node builds its ring from, not a table of
/// tablets: tablet `t` belongs to `placement[t % N]` and, on that node, to shard
/// `(t / N) % shards`, the same rule the server routes with. Empty before the placement is
/// initialized.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TopologyFrame {
    /// The cluster
    pub cluster: ClusterId,
    /// How many committed changes the topology has seen
    pub version: u64,
    /// The control leader, if the answering node knows one
    pub leader: Option<NodeId>,
    /// Every member, in node order
    pub members: Vec<TopologyMember>,
    /// The nodes tablets are placed over, in placement order; empty before initialization
    pub placement: Vec<NodeId>,
    /// How many replicas each tablet is meant to have
    pub desired_rf: u32,
    /// How many replicas each tablet has
    pub active_rf: u32,
    /// What a write waits for, as the policy spells it
    pub write_consistency: String,
    /// What a read is served at
    pub read_consistency: String,
    /// The tables the schema serves, with their stable identities
    pub tables: Vec<(String, TableId)>,
}

/// Write a body of `[id][json]` for any of the three frames
///
/// # Arguments
///
/// * `id` - The query or request id, or nil for a pushed topology
/// * `value` - What to serialize after it
///
/// # Errors
///
/// Fails only if the value cannot be serialized, which none of the types here can fail at.
pub fn encode_body<T: Serialize>(id: &Uuid, value: &T) -> Result<Vec<u8>, serde_json::Error> {
    // the id first, so the client's one preamble read finds it where a response's is
    let mut body = Vec::with_capacity(QUERY_ID_LEN + 256);
    body.extend_from_slice(id.as_bytes());
    serde_json::to_writer(&mut body, value)?;
    Ok(body)
}

/// Read the JSON that follows the id of one of the three frames
///
/// # Arguments
///
/// * `rest` - The bytes after the preamble, which is where the client's reader leaves them
///
/// # Errors
///
/// Fails if the bytes are not the JSON of `T`.
pub fn decode_rest<T: for<'a> Deserialize<'a>>(rest: &[u8]) -> Result<T, serde_json::Error> {
    serde_json::from_slice(rest)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A frame's body is the id and then the JSON, and each type round trips through it
    #[test]
    fn admin_bodies_round_trip() {
        let id = Uuid::new_v4();
        // a request, both kinds
        let request = AdminRequest {
            op: Uuid::new_v4(),
            expected_version: 7,
            kind: AdminKind::Initialize {
                nodes: vec![NodeId::mint(), NodeId::mint()],
            },
        };
        let body = encode_body(&id, &request).expect("a request encodes");
        assert_eq!(&body[..QUERY_ID_LEN], id.as_bytes());
        let back: AdminRequest = decode_rest(&body[QUERY_ID_LEN..]).expect("a request decodes");
        assert_eq!(back, request);
        assert!(request.kind.is_mutation());
        assert!(!AdminKind::Members.is_mutation());
        // an answer, applied and refused
        let response = AdminResponse {
            node: NodeId::mint(),
            topology_version: 8,
            outcome: Ok(AdminOutcome::Applied { version: 8 }),
        };
        let body = encode_body(&Uuid::nil(), &response).expect("a response encodes");
        let back: AdminResponse = decode_rest(&body[QUERY_ID_LEN..]).expect("a response decodes");
        assert_eq!(back, response);
        let refused = AdminResponse {
            node: NodeId::mint(),
            topology_version: 8,
            outcome: Err(AdminError::new(ErrorCode::StaleVersion, "the cluster is at 8")),
        };
        let body = encode_body(&Uuid::nil(), &refused).expect("a refusal encodes");
        let back: AdminResponse = decode_rest(&body[QUERY_ID_LEN..]).expect("a refusal decodes");
        assert_eq!(back, refused);
        assert_eq!(
            back.outcome.as_ref().expect_err("a refusal").code(),
            ErrorCode::StaleVersion
        );
        // a topology
        let frame = TopologyFrame {
            cluster: ClusterId::mint(),
            version: 3,
            leader: Some(NodeId::mint()),
            members: vec![TopologyMember {
                node: NodeId::mint(),
                client: "127.0.0.1:12000".to_string(),
                data: "127.0.0.1:12001".to_string(),
                control: "127.0.0.1:12002".to_string(),
                shards: 4,
                role: "voter".to_string(),
                health: "up".to_string(),
                incarnation: 2,
                shards_failed: vec![1],
            }],
            placement: vec![NodeId::mint()],
            desired_rf: 3,
            active_rf: 1,
            write_consistency: "quorum".to_string(),
            read_consistency: "one".to_string(),
            tables: vec![("Row".to_string(), TableId::of("Row"))],
        };
        let body = encode_body(&Uuid::nil(), &frame).expect("a topology encodes");
        let back: TopologyFrame = decode_rest(&body[QUERY_ID_LEN..]).expect("a topology decodes");
        assert_eq!(back, frame);
        // garbage after the id is a decode failure, not a panic
        assert!(decode_rest::<TopologyFrame>(b"not json").is_err());
    }
}
