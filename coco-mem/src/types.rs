use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Represents a node in the memory graph, similar to a git commit
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Node {
    pub id: String,
    pub parent: String,
    pub created_at: Timestamp,
    pub role: Role,
    pub turn: Option<Turn>,
    pub kind: Kind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NewNode {
    pub parent: String,
    pub role: Role,
    pub turn: Option<Turn>,
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
    ToolUse(ToolUse),
    ToolResult(ToolResult),
    Text(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Anchor {
    pub merge_parents: Vec<String>,
    pub payload: AnchorPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AnchorPayload {
    Session(SessionAnchor),
    Prompt(PromptAnchor),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SessionAnchor {
    pub provider: String,
    pub model: String,
    pub tools: Vec<Tool>,
    pub system_prompt: String,
    pub prompt: String,
    pub temperature: Option<f64>,
    pub max_tokens: Option<u64>,
    pub additional_params: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PromptAnchor {
    pub prompt: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Turn {
    pub id: String,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub request: Option<TurnRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TurnRequest {
    pub user_input: String,
    pub temperature: Option<f64>,
    pub max_tokens: Option<u64>,
    pub additional_params: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolUse {
    pub id: String,
    pub name: String,
    pub input: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolResult {
    pub id: String,
    pub output: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Tool {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

impl Anchor {
    pub fn session(merge_parents: Vec<String>, anchor: SessionAnchor) -> Self {
        Self {
            merge_parents,
            payload: AnchorPayload::Session(anchor),
        }
    }

    pub fn prompt(merge_parents: Vec<String>, anchor: PromptAnchor) -> Self {
        Self {
            merge_parents,
            payload: AnchorPayload::Prompt(anchor),
        }
    }

    pub fn merge_parents(&self) -> &[String] {
        &self.merge_parents
    }

    pub fn as_session(&self) -> Option<&SessionAnchor> {
        match &self.payload {
            AnchorPayload::Session(anchor) => Some(anchor),
            AnchorPayload::Prompt(_) => None,
        }
    }

    pub fn as_prompt(&self) -> Option<&PromptAnchor> {
        match &self.payload {
            AnchorPayload::Session(_) => None,
            AnchorPayload::Prompt(anchor) => Some(anchor),
        }
    }
}

impl Turn {
    pub fn full(id: String, provider: String, model: String, request: TurnRequest) -> Self {
        Self {
            id,
            provider: Some(provider),
            model: Some(model),
            request: Some(request),
        }
    }

    pub fn ref_only(id: String) -> Self {
        Self {
            id,
            provider: None,
            model: None,
            request: None,
        }
    }
}

impl Node {
    pub fn new(
        parent: String,
        role: Role,
        turn: Option<Turn>,
        kind: Kind,
        created_at: Timestamp,
    ) -> Self {
        let id = compute_node_id(&parent, &role, turn.as_ref(), &kind, &created_at);

        Self {
            id,
            parent,
            created_at,
            role,
            turn,
            kind,
        }
    }

    pub fn is_root(&self) -> bool {
        self.parent.is_empty()
    }
}

#[derive(Serialize)]
struct NodeHashPayload<'a> {
    parent: &'a str,
    role: &'a Role,
    turn: Option<&'a Turn>,
    kind: &'a Kind,
    created_at: &'a Timestamp,
}

fn compute_node_id(
    parent: &str,
    role: &Role,
    turn: Option<&Turn>,
    kind: &Kind,
    created_at: &Timestamp,
) -> String {
    let payload = serde_json::to_vec(&NodeHashPayload {
        parent,
        role,
        turn,
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
    use super::{
        Anchor, Kind, NewNode, Node, PromptAnchor, Role, SessionAnchor, Tool, ToolResult, Turn,
        TurnRequest,
    };
    use jiff::Timestamp;
    use serde_json::json;

    fn fixed_timestamp() -> Timestamp {
        "2026-03-25T09:10:11Z".parse().unwrap()
    }

    fn make_turn_full() -> Turn {
        Turn::full(
            "turn-1".to_owned(),
            "openai".to_owned(),
            "gpt-4.1-mini".to_owned(),
            TurnRequest {
                user_input: "hello".to_owned(),
                temperature: Some(0.2),
                max_tokens: Some(32),
                additional_params: Some(json!({"reasoning_effort": "low"})),
            },
        )
    }

    fn make_text_node(parent: &str, text: &str, created_at: Timestamp) -> Node {
        Node::new(
            parent.to_owned(),
            Role::User,
            None,
            Kind::Text(text.to_owned()),
            created_at,
        )
    }

    fn make_session_anchor() -> SessionAnchor {
        SessionAnchor {
            provider: "openai".to_owned(),
            model: "gpt-5.4".to_owned(),
            tools: vec![Tool {
                name: "search".to_owned(),
                description: "Search docs".to_owned(),
                input_schema: json!({"type": "object"}),
            }],
            system_prompt: "system".to_owned(),
            prompt: "prompt".to_owned(),
            temperature: Some(0.1),
            max_tokens: Some(128),
            additional_params: Some(json!({"service_tier": "default"})),
        }
    }

    fn make_prompt_anchor() -> PromptAnchor {
        PromptAnchor {
            prompt: "merge prompt".to_owned(),
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
    fn node_id_changes_when_anchor_variant_changes() {
        let left = Node::new(
            "parent".to_owned(),
            Role::System,
            None,
            Kind::Anchor(Anchor::session(vec![], make_session_anchor())),
            fixed_timestamp(),
        );
        let right = Node::new(
            "parent".to_owned(),
            Role::System,
            None,
            Kind::Anchor(Anchor::prompt(
                vec!["merge-a".to_owned()],
                make_prompt_anchor(),
            )),
            fixed_timestamp(),
        );

        assert_ne!(left.id, right.id);
    }

    #[test]
    fn node_id_changes_when_prompt_anchor_merge_parents_change() {
        let left = Node::new(
            "parent".to_owned(),
            Role::System,
            None,
            Kind::Anchor(Anchor::prompt(
                vec!["merge-a".to_owned()],
                make_prompt_anchor(),
            )),
            fixed_timestamp(),
        );
        let right = Node::new(
            "parent".to_owned(),
            Role::System,
            None,
            Kind::Anchor(Anchor::prompt(
                vec!["merge-b".to_owned()],
                make_prompt_anchor(),
            )),
            fixed_timestamp(),
        );

        assert_ne!(left.id, right.id);
    }

    #[test]
    fn node_id_changes_when_session_anchor_merge_parents_change() {
        let left = Node::new(
            "parent".to_owned(),
            Role::System,
            None,
            Kind::Anchor(Anchor::session(
                vec!["merge-a".to_owned()],
                make_session_anchor(),
            )),
            fixed_timestamp(),
        );
        let right = Node::new(
            "parent".to_owned(),
            Role::System,
            None,
            Kind::Anchor(Anchor::session(
                vec!["merge-b".to_owned()],
                make_session_anchor(),
            )),
            fixed_timestamp(),
        );

        assert_ne!(left.id, right.id);
    }

    #[test]
    fn node_id_changes_when_turn_metadata_changes() {
        let left = Node::new(
            "parent".to_owned(),
            Role::User,
            Some(Turn::ref_only("turn-1".to_owned())),
            Kind::Text("hello".to_owned()),
            fixed_timestamp(),
        );
        let right = Node::new(
            "parent".to_owned(),
            Role::User,
            Some(Turn::ref_only("turn-2".to_owned())),
            Kind::Text("hello".to_owned()),
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
            None,
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
            turn: Some(Turn::ref_only("turn-1".to_owned())),
            kind: Kind::Anchor(Anchor::prompt(
                vec!["merge-a".to_owned(), "merge-b".to_owned()],
                make_prompt_anchor(),
            )),
        };

        let encoded = serde_json::to_value(&node).unwrap();

        assert!(encoded.get("id").is_none());
        assert!(encoded.get("created_at").is_none());
    }

    #[test]
    fn session_anchor_round_trip_preserves_prompt() {
        let node = Node::new(
            "parent".to_owned(),
            Role::System,
            None,
            Kind::Anchor(Anchor::session(
                vec!["merge-a".to_owned(), "merge-b".to_owned()],
                make_session_anchor(),
            )),
            fixed_timestamp(),
        );

        let encoded = serde_json::to_string(&node).unwrap();
        let decoded: Node = serde_json::from_str(&encoded).unwrap();
        let Kind::Anchor(anchor) = decoded.kind else {
            panic!("expected anchor node");
        };
        let session_anchor = anchor.as_session().expect("expected session anchor");

        assert_eq!(session_anchor.prompt, "prompt");
        assert_eq!(anchor.merge_parents(), ["merge-a", "merge-b"]);
    }

    #[test]
    fn prompt_anchor_round_trip_preserves_merge_parents() {
        let node = Node::new(
            "parent".to_owned(),
            Role::System,
            None,
            Kind::Anchor(Anchor::prompt(
                vec!["merge-a".to_owned(), "merge-b".to_owned()],
                make_prompt_anchor(),
            )),
            fixed_timestamp(),
        );

        let encoded = serde_json::to_string(&node).unwrap();
        let decoded: Node = serde_json::from_str(&encoded).unwrap();
        let Kind::Anchor(anchor) = decoded.kind else {
            panic!("expected anchor node");
        };
        let prompt_anchor = anchor.as_prompt().expect("expected prompt anchor");

        assert_eq!(anchor.merge_parents, vec!["merge-a", "merge-b"]);
        assert_eq!(prompt_anchor.prompt, "merge prompt");
    }

    #[test]
    fn turn_round_trip_preserves_optional_metadata() {
        let node = Node::new(
            "parent".to_owned(),
            Role::User,
            Some(make_turn_full()),
            Kind::Text("hello".to_owned()),
            fixed_timestamp(),
        );

        let encoded = serde_json::to_string(&node).unwrap();
        let decoded: Node = serde_json::from_str(&encoded).unwrap();
        let turn = decoded.turn.expect("expected turn metadata");

        assert_eq!(turn.id, "turn-1");
        assert_eq!(turn.provider.as_deref(), Some("openai"));
        assert_eq!(
            turn.request.expect("expected request metadata").user_input,
            "hello"
        );
    }
}
