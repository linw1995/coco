use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Represents a node in the memory graph, similar to a git commit
#[derive(Debug, Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    pub parent: String,
    pub created_at: Timestamp,
    pub role: Role,

    pub kind: Kind,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NewNode {
    pub parent: String,
    pub role: Role,
    pub kind: Kind,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Role {
    User,
    System,
    LLM,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Kind {
    Anchor(Anchor),
    ToolUse(ToolUse),
    ToolResult(ToolResult),
    Text(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Anchor {
    pub model: String,
    pub tools: Vec<Tool>,
    pub system_prompt: String,
    pub prompt: String,
    pub merge_parents: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ToolUse {
    pub id: String,
    pub name: String,
    pub input: Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ToolResult {
    pub id: String,
    pub output: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Tool {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

impl Node {
    pub fn new(parent: String, role: Role, kind: Kind, created_at: Timestamp) -> Self {
        let id = compute_node_id(&parent, &role, &kind, &created_at);

        Self {
            id,
            parent,
            created_at,
            role,
            kind,
        }
    }
}

#[derive(Serialize)]
struct NodeHashPayload<'a> {
    parent: &'a str,
    role: &'a Role,
    kind: &'a Kind,
    created_at: &'a Timestamp,
}

fn compute_node_id(parent: &str, role: &Role, kind: &Kind, created_at: &Timestamp) -> String {
    let payload = serde_json::to_vec(&NodeHashPayload {
        parent,
        role,
        kind,
        created_at,
    })
    .expect("node hash payload should serialize");

    let mut hasher = Sha256::new();
    hasher.update(format!("node {}\0", payload.len()).as_bytes());
    hasher.update(&payload);

    hex_encode(&hasher.finalize())
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }

    encoded
}

#[cfg(test)]
mod tests {
    use super::{Anchor, Kind, NewNode, Node, Role, Tool, ToolResult};
    use jiff::Timestamp;
    use serde_json::json;

    fn fixed_timestamp() -> Timestamp {
        "2026-03-25T09:10:11Z".parse().unwrap()
    }

    fn make_text_node(parent: &str, text: &str, created_at: Timestamp) -> Node {
        Node::new(
            parent.to_owned(),
            Role::User,
            Kind::Text(text.to_owned()),
            created_at,
        )
    }

    fn make_anchor(merge_parents: &[&str]) -> Anchor {
        Anchor {
            model: "gpt-5.4".to_owned(),
            tools: vec![Tool {
                name: "search".to_owned(),
                description: "Search docs".to_owned(),
                input_schema: json!({"type": "object"}),
            }],
            system_prompt: "system".to_owned(),
            prompt: "prompt".to_owned(),
            merge_parents: merge_parents
                .iter()
                .map(|parent| (*parent).to_owned())
                .collect(),
        }
    }

    #[test]
    fn node_id_is_stable_for_same_payload() {
        let left = make_text_node("parent", "hello", fixed_timestamp());
        let right = make_text_node("parent", "hello", fixed_timestamp());

        assert_eq!(left.id, right.id);
    }

    #[test]
    fn node_id_changes_when_created_at_changes() {
        let left = make_text_node("parent", "hello", "2026-03-25T09:10:11Z".parse().unwrap());
        let right = make_text_node("parent", "hello", "2026-03-25T09:10:12Z".parse().unwrap());

        assert_ne!(left.id, right.id);
    }

    #[test]
    fn node_id_changes_when_anchor_merge_parents_change() {
        let left = Node::new(
            "parent".to_owned(),
            Role::System,
            Kind::Anchor(make_anchor(&["merge-a"])),
            fixed_timestamp(),
        );
        let right = Node::new(
            "parent".to_owned(),
            Role::System,
            Kind::Anchor(make_anchor(&["merge-b"])),
            fixed_timestamp(),
        );

        assert_ne!(left.id, right.id);
    }

    #[test]
    fn node_id_is_lower_hex_sha256() {
        let node = make_text_node("parent", "hello", fixed_timestamp());

        assert_eq!(node.id.len(), 64);
        assert!(node.id.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_eq!(node.id, node.id.to_ascii_lowercase());
    }

    #[test]
    fn node_round_trip_preserves_id_and_created_at() {
        let node = Node::new(
            "parent".to_owned(),
            Role::LLM,
            Kind::ToolResult(ToolResult {
                id: "call-1".to_owned(),
                output: "ok".to_owned(),
            }),
            fixed_timestamp(),
        );

        let encoded = serde_json::to_string(&node).unwrap();
        let decoded: Node = serde_json::from_str(&encoded).unwrap();

        assert_eq!(decoded.id, node.id);
        assert_eq!(decoded.parent, node.parent);
        assert_eq!(decoded.created_at, node.created_at);
    }

    #[test]
    fn new_node_carries_only_unpersisted_fields() {
        let node = NewNode {
            parent: "parent".to_owned(),
            role: Role::System,
            kind: Kind::Anchor(make_anchor(&["merge-a", "merge-b"])),
        };

        let encoded = serde_json::to_value(&node).unwrap();

        assert!(encoded.get("id").is_none());
        assert!(encoded.get("created_at").is_none());
    }

    #[test]
    fn anchor_round_trip_preserves_merge_parents() {
        let node = Node::new(
            "parent".to_owned(),
            Role::System,
            Kind::Anchor(make_anchor(&["merge-a", "merge-b"])),
            fixed_timestamp(),
        );

        let encoded = serde_json::to_string(&node).unwrap();
        let decoded: Node = serde_json::from_str(&encoded).unwrap();
        let Kind::Anchor(anchor) = decoded.kind else {
            panic!("expected anchor node");
        };

        assert_eq!(anchor.merge_parents, vec!["merge-a", "merge-b"]);
    }
}
