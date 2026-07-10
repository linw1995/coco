use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{Anchor, AnchorPayloadKind, NodeMetadata, ToolResult, ToolUse, hash::hex_encode};

/// Represents a node in the memory graph, similar to a git commit
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Node {
    pub id: String,
    pub parent: String,
    pub created_at: Timestamp,
    pub role: Role,
    pub metadata: Option<NodeMetadata>,
    pub kind: Kind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NewNode {
    pub parent: String,
    pub role: Role,
    pub metadata: Option<NodeMetadata>,
    pub kind: Kind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NewNodeContent {
    pub role: Role,
    pub metadata: Option<NodeMetadata>,
    pub kind: Kind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Role {
    User,
    System,
    LLM,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Kind {
    Anchor(Anchor),
    ToolUse(Vec<ToolUse>),
    ToolResult(Vec<ToolResult>),
    Text(String),
    Failure(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KindTag {
    Anchor,
    ToolUse,
    ToolResult,
    Text,
    Failure,
}

impl Node {
    pub fn new(
        parent: String,
        role: Role,
        metadata: Option<NodeMetadata>,
        kind: Kind,
        created_at: Timestamp,
    ) -> Self {
        let id = compute_node_id(&parent, &role, metadata.as_ref(), &kind, &created_at);

        Self {
            id,
            parent,
            created_at,
            role,
            metadata,
            kind,
        }
    }

    pub fn is_root(&self) -> bool {
        self.parent.is_empty()
    }
}

impl Kind {
    pub fn tag(&self) -> KindTag {
        match self {
            Self::Anchor(_) => KindTag::Anchor,
            Self::ToolUse(_) => KindTag::ToolUse,
            Self::ToolResult(_) => KindTag::ToolResult,
            Self::Text(_) => KindTag::Text,
            Self::Failure(_) => KindTag::Failure,
        }
    }

    pub fn anchor_payload_kind(&self) -> Option<AnchorPayloadKind> {
        match self {
            Self::Anchor(anchor) => Some(anchor.payload_kind()),
            Self::ToolUse(_) | Self::ToolResult(_) | Self::Text(_) | Self::Failure(_) => None,
        }
    }

    pub fn tool_use(tool_use: ToolUse) -> Self {
        Self::ToolUse(vec![tool_use])
    }

    pub fn tool_uses(tool_uses: Vec<ToolUse>) -> Self {
        Self::ToolUse(tool_uses)
    }

    pub fn tool_use_items(tool_uses: Vec<ToolUse>) -> Self {
        Self::ToolUse(tool_uses)
    }

    pub fn tool_result(tool_result: ToolResult) -> Self {
        Self::ToolResult(vec![tool_result])
    }

    pub fn tool_results(tool_results: Vec<ToolResult>) -> Self {
        Self::ToolResult(tool_results)
    }

    pub fn tool_result_items(tool_results: Vec<ToolResult>) -> Self {
        Self::ToolResult(tool_results)
    }

    pub fn as_tool_uses(&self) -> Option<&[ToolUse]> {
        match self {
            Self::ToolUse(tool_uses) => Some(tool_uses),
            Self::Anchor(_) | Self::ToolResult(_) | Self::Text(_) | Self::Failure(_) => None,
        }
    }

    pub fn as_tool_results(&self) -> Option<&[ToolResult]> {
        match self {
            Self::ToolResult(tool_results) => Some(tool_results),
            Self::Anchor(_) | Self::ToolUse(_) | Self::Text(_) | Self::Failure(_) => None,
        }
    }
}

impl KindTag {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Anchor => "anchor",
            Self::ToolUse => "tool_use",
            Self::ToolResult => "tool_result",
            Self::Text => "text",
            Self::Failure => "failure",
        }
    }
}

#[derive(Serialize)]
struct NodeHashPayload<'a> {
    parent: &'a str,
    role: &'a Role,
    metadata: Option<&'a NodeMetadata>,
    kind: &'a Kind,
    created_at: &'a Timestamp,
}

fn compute_node_id(
    parent: &str,
    role: &Role,
    metadata: Option<&NodeMetadata>,
    kind: &Kind,
    created_at: &Timestamp,
) -> String {
    let payload = serde_json::to_vec(&NodeHashPayload {
        parent,
        role,
        metadata,
        kind,
        created_at,
    })
    .expect("node hash payload should serialize");

    let mut hasher = Sha256::new();
    hasher.update(format!("node {}\0", payload.len()).as_bytes());
    hasher.update(&payload);

    hex_encode(&hasher.finalize())
}
