use super::*;

#[derive(Queryable, QueryableByName)]
pub struct NodeRow {
    #[diesel(sql_type = Text)]
    pub id: String,
    #[diesel(sql_type = Text)]
    pub parent_id: String,
    #[diesel(sql_type = Text)]
    pub created_at: String,
    #[diesel(sql_type = Text)]
    pub role: String,
    #[diesel(sql_type = Text)]
    pub kind: String,
    #[diesel(sql_type = Bool)]
    pub metadata_present: bool,
    #[diesel(sql_type = Nullable<Text>)]
    pub content: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Queryable)]
pub struct NodeAnchorSessionRow {
    pub node_id: String,
    pub role: String,
    pub provider_profile: Option<String>,
    pub provider: Option<String>,
    pub model: String,
    pub system_prompt: String,
    pub prompt: String,
    pub temperature: Option<f64>,
    pub max_tokens: Option<String>,
    pub additional_params_json: Option<String>,
    pub enable_coco_shim: bool,
    pub active_skill_name: Option<String>,
    pub active_skill_handoff: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Queryable)]
pub struct NodeAnchorSessionToolRow {
    pub node_id: String,
    pub ordinal: i32,
    pub name: String,
    pub description: String,
    pub input_schema_json: String,
}

#[derive(Clone, Debug, PartialEq, Queryable)]
pub struct NodeAnchorSessionPatchRow {
    pub node_id: String,
    pub role: Option<String>,
    pub provider_profile_present: bool,
    pub provider_profile: Option<String>,
    pub provider_present: bool,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub tools_present: bool,
    pub system_prompt: Option<String>,
    pub temperature_present: bool,
    pub temperature: Option<f64>,
    pub max_tokens_present: bool,
    pub max_tokens: Option<String>,
    pub additional_params_present: bool,
    pub additional_params_json: Option<String>,
    pub enable_coco_shim: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, Eq, Queryable)]
pub struct NodeAnchorSessionPatchToolRow {
    pub node_id: String,
    pub ordinal: i32,
    pub name: String,
    pub description: String,
    pub input_schema_json: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Queryable)]
pub struct NodeAnchorPromptAttachmentRow {
    pub node_id: String,
    pub ordinal: i32,
    pub kind: String,
    pub attachment_id: String,
    pub width: Option<i64>,
    pub height: Option<i64>,
    pub file_size: Option<String>,
    pub media_type: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Queryable)]
pub struct NodeAnchorSkillInvocationRow {
    pub node_id: String,
    pub skill_name: String,
    pub mode: String,
    pub prompt: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Queryable)]
pub struct NodeAnchorSkillResultRow {
    pub node_id: String,
    pub skill_name: String,
    pub output: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Queryable)]
pub struct NodeRelationRow {
    pub child_node_id: String,
    pub parent_node_id: String,
    pub kind: String,
    pub ordinal: i32,
}

pub struct NodeAnchorStorageRows<'a> {
    pub session: Option<&'a NodeAnchorSessionRow>,
    pub session_tools: &'a [NodeAnchorSessionToolRow],
    pub session_patch: Option<&'a NodeAnchorSessionPatchRow>,
    pub session_patch_tools: &'a [NodeAnchorSessionPatchToolRow],
    pub prompt_attachments: &'a [NodeAnchorPromptAttachmentRow],
    pub skill_invocation: Option<&'a NodeAnchorSkillInvocationRow>,
    pub skill_result: Option<&'a NodeAnchorSkillResultRow>,
    pub relations: &'a [NodeRelationRow],
}

pub struct NodeStorageRows<'a> {
    pub anchor: NodeAnchorStorageRows<'a>,
    pub metadata: &'a [NodeMetadataRow],
    pub tool_uses: &'a [NodeToolUseRow],
    pub tool_results: &'a [NodeToolResultRow],
}

#[derive(Default)]
pub struct NodeStorageIds {
    pub anchor_sessions: Vec<String>,
    pub anchor_session_patches: Vec<String>,
    pub anchor_prompts: Vec<String>,
    pub anchor_skill_invocations: Vec<String>,
    pub anchor_skill_results: Vec<String>,
    pub anchors: Vec<String>,
    pub metadata: Vec<String>,
    pub tool_uses: Vec<String>,
    pub tool_results: Vec<String>,
}

impl NodeStorageIds {
    pub fn from_rows(rows: &[NodeRow]) -> Self {
        let mut ids = Self::default();
        for row in rows {
            if row.metadata_present {
                ids.metadata.push(row.id.clone());
            }
            match row.kind.as_str() {
                NODE_KIND_ANCHOR_SESSION => {
                    ids.anchor_sessions.push(row.id.clone());
                    ids.anchors.push(row.id.clone());
                }
                NODE_KIND_ANCHOR_SESSION_PATCH => {
                    ids.anchor_session_patches.push(row.id.clone());
                    ids.anchors.push(row.id.clone());
                }
                NODE_KIND_ANCHOR_PROMPT => {
                    ids.anchor_prompts.push(row.id.clone());
                    ids.anchors.push(row.id.clone());
                }
                NODE_KIND_ANCHOR_SKILL_INVOCATION => {
                    ids.anchor_skill_invocations.push(row.id.clone());
                    ids.anchors.push(row.id.clone());
                }
                NODE_KIND_ANCHOR_SKILL_RESULT => {
                    ids.anchor_skill_results.push(row.id.clone());
                    ids.anchors.push(row.id.clone());
                }
                "tool_use" => ids.tool_uses.push(row.id.clone()),
                "tool_result" => ids.tool_results.push(row.id.clone()),
                "text" | "failure" => {}
                _ => {}
            }
        }
        ids
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Queryable)]
pub struct NodeMetadataRow {
    pub node_id: String,
    pub ordinal: i32,
    pub execution_id: Option<String>,
    pub call_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Queryable)]
pub struct NodeToolUseRow {
    pub node_id: String,
    pub ordinal: i32,
    pub tool_use_id: String,
    pub name: String,
    pub input_json: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Queryable)]
pub struct NodeToolResultRow {
    pub node_id: String,
    pub ordinal: i32,
    pub tool_result_id: String,
    pub output: String,
}
