use super::*;

pub async fn persist_node_without_transaction(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node: &Node,
    graph_mutation_revision: i64,
) -> Result<()> {
    let row = NodeRow::from_node(node);
    diesel::insert_into(nodes::table)
        .values((
            nodes::id.eq(row.id),
            nodes::parent_id.eq(row.parent_id),
            nodes::created_at.eq(row.created_at),
            nodes::role.eq(row.role),
            nodes::kind.eq(row.kind),
            nodes::metadata_present.eq(row.metadata_present),
            nodes::content.eq(row.content),
        ))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;

    persist_node_anchor_session_row(connection, path, node).await?;
    persist_node_anchor_session_tool_rows(connection, path, node).await?;
    persist_node_anchor_session_patch_row(connection, path, node).await?;
    persist_node_anchor_session_patch_tool_rows(connection, path, node).await?;
    persist_node_anchor_prompt_attachment_rows(connection, path, node).await?;
    persist_node_anchor_skill_invocation_row(connection, path, node).await?;
    persist_node_anchor_skill_result_row(connection, path, node).await?;
    let relations = node_relations(node);
    for relation in relations {
        diesel::insert_into(node_relations::table)
            .values((
                node_relations::child_node_id.eq(relation.child_node_id),
                node_relations::parent_node_id.eq(relation.parent_node_id),
                node_relations::kind.eq(relation.kind),
                node_relations::ordinal.eq(relation.ordinal),
                node_relations::created_revision.eq(graph_mutation_revision),
            ))
            .execute(connection)
            .await
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?;
    }
    persist_node_metadata_rows(connection, path, node).await?;
    persist_node_tool_use_rows(connection, path, node).await?;
    persist_node_tool_result_rows(connection, path, node).await?;
    Ok(())
}

pub async fn upsert_node_without_transaction(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node: &Node,
    graph_mutation_revision: i64,
) -> Result<bool> {
    let row = NodeRow::from_node(node);
    let inserted = diesel::insert_into(nodes::table)
        .values((
            nodes::id.eq(row.id),
            nodes::parent_id.eq(row.parent_id),
            nodes::created_at.eq(row.created_at),
            nodes::role.eq(row.role),
            nodes::kind.eq(row.kind),
            nodes::metadata_present.eq(row.metadata_present),
            nodes::content.eq(row.content),
        ))
        .on_conflict(nodes::id)
        .do_nothing()
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;

    if inserted == 0 {
        let existing = load_node_by_exact_id(connection, path, &node.id).await?;
        ensure!(
            existing == *node,
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "content-addressed SQLite node {:?} conflicts with immutable stored data",
                    node.id
                ),
            }
        );
        return Ok(false);
    }

    persist_node_anchor_session_row(connection, path, node).await?;
    persist_node_anchor_session_tool_rows(connection, path, node).await?;
    persist_node_anchor_session_patch_row(connection, path, node).await?;
    persist_node_anchor_session_patch_tool_rows(connection, path, node).await?;
    persist_node_anchor_prompt_attachment_rows(connection, path, node).await?;
    persist_node_anchor_skill_invocation_row(connection, path, node).await?;
    persist_node_anchor_skill_result_row(connection, path, node).await?;
    let relations = node_relations(node);
    for relation in relations {
        diesel::insert_into(node_relations::table)
            .values((
                node_relations::child_node_id.eq(relation.child_node_id),
                node_relations::parent_node_id.eq(relation.parent_node_id),
                node_relations::kind.eq(relation.kind),
                node_relations::ordinal.eq(relation.ordinal),
                node_relations::created_revision.eq(graph_mutation_revision),
            ))
            .execute(connection)
            .await
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?;
    }
    persist_node_metadata_rows(connection, path, node).await?;
    persist_node_tool_use_rows(connection, path, node).await?;
    persist_node_tool_result_rows(connection, path, node).await?;
    Ok(true)
}

async fn persist_node_anchor_session_row(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node: &Node,
) -> Result<()> {
    let Some(row) = NodeAnchorSessionRow::from_node(node, path)? else {
        return Ok(());
    };
    diesel::insert_into(node_anchor_sessions::table)
        .values((
            node_anchor_sessions::node_id.eq(row.node_id),
            node_anchor_sessions::role.eq(row.role),
            node_anchor_sessions::provider_profile.eq(row.provider_profile),
            node_anchor_sessions::provider.eq(row.provider),
            node_anchor_sessions::model.eq(row.model),
            node_anchor_sessions::system_prompt.eq(row.system_prompt),
            node_anchor_sessions::prompt.eq(row.prompt),
            node_anchor_sessions::temperature.eq(row.temperature),
            node_anchor_sessions::max_tokens.eq(row.max_tokens),
            node_anchor_sessions::additional_params_json.eq(row.additional_params_json),
            node_anchor_sessions::enable_coco_shim.eq(row.enable_coco_shim),
            node_anchor_sessions::active_skill_name.eq(row.active_skill_name),
            node_anchor_sessions::active_skill_handoff.eq(row.active_skill_handoff),
        ))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(())
}

async fn persist_node_anchor_session_tool_rows(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node: &Node,
) -> Result<()> {
    for row in node_anchor_session_tool_rows(node, path)? {
        diesel::insert_into(node_anchor_session_tools::table)
            .values((
                node_anchor_session_tools::node_id.eq(row.node_id),
                node_anchor_session_tools::ordinal.eq(row.ordinal),
                node_anchor_session_tools::name.eq(row.name),
                node_anchor_session_tools::description.eq(row.description),
                node_anchor_session_tools::input_schema_json.eq(row.input_schema_json),
            ))
            .execute(connection)
            .await
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?;
    }
    Ok(())
}

async fn persist_node_anchor_session_patch_row(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node: &Node,
) -> Result<()> {
    let Some(row) = NodeAnchorSessionPatchRow::from_node(node, path)? else {
        return Ok(());
    };
    diesel::insert_into(node_anchor_session_patches::table)
        .values((
            node_anchor_session_patches::node_id.eq(row.node_id),
            node_anchor_session_patches::role.eq(row.role),
            node_anchor_session_patches::provider_profile_present.eq(row.provider_profile_present),
            node_anchor_session_patches::provider_profile.eq(row.provider_profile),
            node_anchor_session_patches::provider_present.eq(row.provider_present),
            node_anchor_session_patches::provider.eq(row.provider),
            node_anchor_session_patches::model.eq(row.model),
            node_anchor_session_patches::tools_present.eq(row.tools_present),
            node_anchor_session_patches::system_prompt.eq(row.system_prompt),
            node_anchor_session_patches::temperature_present.eq(row.temperature_present),
            node_anchor_session_patches::temperature.eq(row.temperature),
            node_anchor_session_patches::max_tokens_present.eq(row.max_tokens_present),
            node_anchor_session_patches::max_tokens.eq(row.max_tokens),
            node_anchor_session_patches::additional_params_present
                .eq(row.additional_params_present),
            node_anchor_session_patches::additional_params_json.eq(row.additional_params_json),
            node_anchor_session_patches::enable_coco_shim.eq(row.enable_coco_shim),
        ))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(())
}

async fn persist_node_anchor_session_patch_tool_rows(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node: &Node,
) -> Result<()> {
    for row in node_anchor_session_patch_tool_rows(node, path)? {
        diesel::insert_into(node_anchor_session_patch_tools::table)
            .values((
                node_anchor_session_patch_tools::node_id.eq(row.node_id),
                node_anchor_session_patch_tools::ordinal.eq(row.ordinal),
                node_anchor_session_patch_tools::name.eq(row.name),
                node_anchor_session_patch_tools::description.eq(row.description),
                node_anchor_session_patch_tools::input_schema_json.eq(row.input_schema_json),
            ))
            .execute(connection)
            .await
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?;
    }
    Ok(())
}

async fn persist_node_anchor_prompt_attachment_rows(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node: &Node,
) -> Result<()> {
    for row in node_anchor_prompt_attachment_rows(node) {
        diesel::insert_into(node_anchor_prompt_attachments::table)
            .values((
                node_anchor_prompt_attachments::node_id.eq(row.node_id),
                node_anchor_prompt_attachments::ordinal.eq(row.ordinal),
                node_anchor_prompt_attachments::kind.eq(row.kind),
                node_anchor_prompt_attachments::attachment_id.eq(row.attachment_id),
                node_anchor_prompt_attachments::width.eq(row.width),
                node_anchor_prompt_attachments::height.eq(row.height),
                node_anchor_prompt_attachments::file_size.eq(row.file_size),
                node_anchor_prompt_attachments::media_type.eq(row.media_type),
            ))
            .execute(connection)
            .await
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?;
    }
    Ok(())
}

async fn persist_node_anchor_skill_invocation_row(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node: &Node,
) -> Result<()> {
    let Some(row) = NodeAnchorSkillInvocationRow::from_node(node) else {
        return Ok(());
    };
    diesel::insert_into(node_anchor_skill_invocations::table)
        .values((
            node_anchor_skill_invocations::node_id.eq(row.node_id),
            node_anchor_skill_invocations::skill_name.eq(row.skill_name),
            node_anchor_skill_invocations::mode.eq(row.mode),
            node_anchor_skill_invocations::prompt.eq(row.prompt),
        ))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(())
}

async fn persist_node_anchor_skill_result_row(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node: &Node,
) -> Result<()> {
    let Some(row) = NodeAnchorSkillResultRow::from_node(node) else {
        return Ok(());
    };
    diesel::insert_into(node_anchor_skill_results::table)
        .values((
            node_anchor_skill_results::node_id.eq(row.node_id),
            node_anchor_skill_results::skill_name.eq(row.skill_name),
            node_anchor_skill_results::output.eq(row.output),
        ))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(())
}

async fn persist_node_metadata_rows(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node: &Node,
) -> Result<()> {
    for metadata_row in node_metadata_rows(node) {
        diesel::insert_into(node_metadata::table)
            .values((
                node_metadata::node_id.eq(metadata_row.node_id),
                node_metadata::ordinal.eq(metadata_row.ordinal),
                node_metadata::execution_id.eq(metadata_row.execution_id),
                node_metadata::call_id.eq(metadata_row.call_id),
            ))
            .execute(connection)
            .await
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?;
    }
    Ok(())
}

async fn persist_node_tool_use_rows(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node: &Node,
) -> Result<()> {
    for row in node_tool_use_rows(node, path)? {
        diesel::insert_into(node_tool_uses::table)
            .values((
                node_tool_uses::node_id.eq(row.node_id),
                node_tool_uses::ordinal.eq(row.ordinal),
                node_tool_uses::tool_use_id.eq(row.tool_use_id),
                node_tool_uses::name.eq(row.name),
                node_tool_uses::input_json.eq(row.input_json),
            ))
            .execute(connection)
            .await
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?;
    }
    Ok(())
}

async fn persist_node_tool_result_rows(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node: &Node,
) -> Result<()> {
    for row in node_tool_result_rows(node) {
        diesel::insert_into(node_tool_results::table)
            .values((
                node_tool_results::node_id.eq(row.node_id),
                node_tool_results::ordinal.eq(row.ordinal),
                node_tool_results::tool_result_id.eq(row.tool_result_id),
                node_tool_results::output.eq(row.output),
            ))
            .execute(connection)
            .await
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?;
    }
    Ok(())
}

struct NodeRelation {
    child_node_id: String,
    parent_node_id: String,
    kind: String,
    ordinal: i32,
}

fn node_metadata_rows(node: &Node) -> Vec<NodeMetadataRow> {
    expected_node_metadata_rows(&node.id, node.metadata.as_ref())
}

fn node_anchor_session_tool_rows(
    node: &Node,
    path: &Path,
) -> Result<Vec<NodeAnchorSessionToolRow>> {
    let Kind::Anchor(Anchor {
        payload: AnchorPayload::Session(anchor),
        ..
    }) = &node.kind
    else {
        return Ok(Vec::new());
    };
    anchor
        .tools
        .iter()
        .enumerate()
        .map(|(ordinal, tool)| {
            Ok(NodeAnchorSessionToolRow {
                node_id: node.id.clone(),
                ordinal: ordinal as i32,
                name: tool.name.clone(),
                description: tool.description.clone(),
                input_schema_json: serde_json::to_string(&tool.input_schema).context(
                    ParseSqliteStoreValueSnafu {
                        path: path.to_owned(),
                        column: "node_anchor_session_tools.input_schema_json".to_owned(),
                    },
                )?,
            })
        })
        .collect()
}

fn node_anchor_session_patch_tool_rows(
    node: &Node,
    path: &Path,
) -> Result<Vec<NodeAnchorSessionPatchToolRow>> {
    let Kind::Anchor(Anchor {
        payload: AnchorPayload::SessionPatch(patch),
        ..
    }) = &node.kind
    else {
        return Ok(Vec::new());
    };
    patch
        .tools
        .iter()
        .flatten()
        .enumerate()
        .map(|(ordinal, tool)| {
            Ok(NodeAnchorSessionPatchToolRow {
                node_id: node.id.clone(),
                ordinal: ordinal as i32,
                name: tool.name.clone(),
                description: tool.description.clone(),
                input_schema_json: serde_json::to_string(&tool.input_schema).context(
                    ParseSqliteStoreValueSnafu {
                        path: path.to_owned(),
                        column: "node_anchor_session_patch_tools.input_schema_json".to_owned(),
                    },
                )?,
            })
        })
        .collect()
}

fn node_anchor_prompt_attachment_rows(node: &Node) -> Vec<NodeAnchorPromptAttachmentRow> {
    let Kind::Anchor(Anchor {
        payload: AnchorPayload::Prompt(prompt),
        ..
    }) = &node.kind
    else {
        return Vec::new();
    };
    prompt
        .attachments
        .iter()
        .enumerate()
        .map(|(ordinal, attachment)| match attachment {
            PromptAttachment::Image(image) => NodeAnchorPromptAttachmentRow {
                node_id: node.id.clone(),
                ordinal: ordinal as i32,
                kind: "image".to_owned(),
                attachment_id: image.id.clone(),
                width: image.width.map(i64::from),
                height: image.height.map(i64::from),
                file_size: image.file_size.map(|value| value.to_string()),
                media_type: image.media_type.clone(),
            },
        })
        .collect()
}

pub fn expected_node_metadata_rows(
    node_id: &str,
    metadata: Option<&NodeMetadata>,
) -> Vec<NodeMetadataRow> {
    metadata
        .into_iter()
        .flat_map(|metadata| metadata.iter())
        .enumerate()
        .map(|(ordinal, metadata)| NodeMetadataRow {
            node_id: node_id.to_owned(),
            ordinal: ordinal as i32,
            execution_id: metadata.execution_id.clone(),
            call_id: metadata.call_id.clone(),
        })
        .collect()
}

fn node_tool_use_rows(node: &Node, path: &Path) -> Result<Vec<NodeToolUseRow>> {
    let Kind::ToolUse(tool_uses) = &node.kind else {
        return Ok(Vec::new());
    };

    tool_uses
        .iter()
        .enumerate()
        .map(|(ordinal, tool_use)| {
            Ok(NodeToolUseRow {
                node_id: node.id.clone(),
                ordinal: ordinal as i32,
                tool_use_id: tool_use.id.clone(),
                name: tool_use.name.clone(),
                input_json: serde_json::to_string(&tool_use.input).context(
                    ParseSqliteStoreValueSnafu {
                        path: path.to_owned(),
                        column: "node_tool_uses.input_json".to_owned(),
                    },
                )?,
            })
        })
        .collect()
}

fn node_tool_result_rows(node: &Node) -> Vec<NodeToolResultRow> {
    let Kind::ToolResult(tool_results) = &node.kind else {
        return Vec::new();
    };

    tool_results
        .iter()
        .enumerate()
        .map(|(ordinal, tool_result)| NodeToolResultRow {
            node_id: node.id.clone(),
            ordinal: ordinal as i32,
            tool_result_id: tool_result.id.clone(),
            output: tool_result.output.clone(),
        })
        .collect()
}

fn node_relations(node: &Node) -> Vec<NodeRelation> {
    let mut relations = Vec::new();
    if !node.parent.is_empty() {
        relations.push(NodeRelation {
            child_node_id: node.id.clone(),
            parent_node_id: node.parent.clone(),
            kind: "primary".to_owned(),
            ordinal: 0,
        });
    }
    if let Kind::Anchor(anchor) = &node.kind {
        relations.extend(
            anchor
                .merge_parents()
                .iter()
                .enumerate()
                .map(|(ordinal, parent)| NodeRelation {
                    child_node_id: node.id.clone(),
                    parent_node_id: parent.node_id().to_owned(),
                    kind: merge_parent_relation_kind(parent).to_owned(),
                    ordinal: ordinal as i32,
                }),
        );
    }
    relations
}

fn merge_parent_relation_kind(parent: &MergeParent) -> &'static str {
    if parent.is_shadow() {
        "shadow"
    } else {
        "merge"
    }
}

pub fn node_storage_kind(kind: &Kind) -> &'static str {
    match kind {
        Kind::Anchor(Anchor {
            payload: AnchorPayload::Session(_),
            ..
        }) => NODE_KIND_ANCHOR_SESSION,
        Kind::Anchor(Anchor {
            payload: AnchorPayload::SessionPatch(_),
            ..
        }) => NODE_KIND_ANCHOR_SESSION_PATCH,
        Kind::Anchor(Anchor {
            payload: AnchorPayload::Prompt(_),
            ..
        }) => NODE_KIND_ANCHOR_PROMPT,
        Kind::Anchor(Anchor {
            payload: AnchorPayload::SkillInvocation(_),
            ..
        }) => NODE_KIND_ANCHOR_SKILL_INVOCATION,
        Kind::Anchor(Anchor {
            payload: AnchorPayload::SkillResult(_),
            ..
        }) => NODE_KIND_ANCHOR_SKILL_RESULT,
        Kind::ToolUse(_) => "tool_use",
        Kind::ToolResult(_) => "tool_result",
        Kind::Text(_) => "text",
        Kind::Failure(_) => "failure",
    }
}

impl NodeRow {
    fn from_node(node: &Node) -> Self {
        let kind = node_storage_kind(&node.kind).to_owned();
        let metadata_present = node.metadata.is_some();
        Self {
            id: node.id.clone(),
            parent_id: node.parent.clone(),
            created_at: node.created_at.to_string(),
            role: role_name(&node.role).to_owned(),
            kind,
            content: match &node.kind {
                Kind::Text(content) | Kind::Failure(content) => Some(content.clone()),
                Kind::Anchor(Anchor {
                    payload: AnchorPayload::Prompt(prompt),
                    ..
                }) => Some(prompt.prompt.clone()),
                Kind::Anchor(_) | Kind::ToolUse(_) | Kind::ToolResult(_) => None,
            },
            metadata_present,
        }
    }

    pub fn into_node(self, path: &Path, rows: NodeStorageRows<'_>) -> Result<Node> {
        let kind = self.kind_from_storage(path, &rows.anchor, rows.tool_uses, rows.tool_results)?;
        ensure!(
            self.kind == node_storage_kind(&kind),
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "SQLite node kind column {:?} does not match stored payload",
                    self.kind
                ),
            }
        );
        let metadata =
            node_metadata_from_rows(path, &self.id, self.metadata_present, rows.metadata)?;
        Ok(Node {
            id: self.id,
            parent: self.parent_id,
            created_at: self.created_at.parse().map_err(|source| {
                crate::StoreError::CorruptedStore {
                    path: path.to_owned(),
                    message: format!("invalid SQLite node timestamp: {source}"),
                }
            })?,
            role: parse_role(&self.role, path)?,
            metadata,
            kind,
        })
    }

    fn kind_from_storage(
        &self,
        path: &Path,
        anchor_rows: &NodeAnchorStorageRows<'_>,
        tool_use_rows: &[NodeToolUseRow],
        tool_result_rows: &[NodeToolResultRow],
    ) -> Result<Kind> {
        match self.kind.as_str() {
            NODE_KIND_ANCHOR_SESSION => self.anchor_kind_from_storage(
                path,
                AnchorPayloadKind::Session,
                anchor_rows,
                tool_use_rows,
                tool_result_rows,
            ),
            NODE_KIND_ANCHOR_SESSION_PATCH => self.anchor_kind_from_storage(
                path,
                AnchorPayloadKind::SessionPatch,
                anchor_rows,
                tool_use_rows,
                tool_result_rows,
            ),
            NODE_KIND_ANCHOR_PROMPT => self.anchor_kind_from_storage(
                path,
                AnchorPayloadKind::Prompt,
                anchor_rows,
                tool_use_rows,
                tool_result_rows,
            ),
            NODE_KIND_ANCHOR_SKILL_INVOCATION => self.anchor_kind_from_storage(
                path,
                AnchorPayloadKind::SkillInvocation,
                anchor_rows,
                tool_use_rows,
                tool_result_rows,
            ),
            NODE_KIND_ANCHOR_SKILL_RESULT => self.anchor_kind_from_storage(
                path,
                AnchorPayloadKind::SkillResult,
                anchor_rows,
                tool_use_rows,
                tool_result_rows,
            ),
            "tool_use" => {
                self.ensure_no_content(path)?;
                ensure_no_tool_result_rows(path, &self.id, tool_result_rows)?;
                node_tool_uses_from_rows(path, &self.id, tool_use_rows).map(Kind::tool_use_items)
            }
            "tool_result" => {
                self.ensure_no_content(path)?;
                ensure_no_tool_use_rows(path, &self.id, tool_use_rows)?;
                node_tool_results_from_rows(path, &self.id, tool_result_rows)
                    .map(Kind::tool_result_items)
            }
            "text" => {
                ensure_no_tool_use_rows(path, &self.id, tool_use_rows)?;
                ensure_no_tool_result_rows(path, &self.id, tool_result_rows)?;
                self.required_content(path).map(Kind::Text)
            }
            "failure" => {
                ensure_no_tool_use_rows(path, &self.id, tool_use_rows)?;
                ensure_no_tool_result_rows(path, &self.id, tool_result_rows)?;
                self.required_content(path).map(Kind::Failure)
            }
            _ => CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!("invalid SQLite node kind {:?}", self.kind),
            }
            .fail(),
        }
    }

    fn anchor_kind_from_storage(
        &self,
        path: &Path,
        payload_kind: AnchorPayloadKind,
        anchor_rows: &NodeAnchorStorageRows<'_>,
        tool_use_rows: &[NodeToolUseRow],
        tool_result_rows: &[NodeToolResultRow],
    ) -> Result<Kind> {
        let prompt = match payload_kind {
            AnchorPayloadKind::Prompt => Some(self.required_content(path)?),
            AnchorPayloadKind::Session
            | AnchorPayloadKind::SessionPatch
            | AnchorPayloadKind::SkillInvocation
            | AnchorPayloadKind::SkillResult => {
                self.ensure_no_content(path)?;
                None
            }
        };
        ensure_no_tool_use_rows(path, &self.id, tool_use_rows)?;
        ensure_no_tool_result_rows(path, &self.id, tool_result_rows)?;
        node_anchor_kind_from_storage(
            path,
            &self.id,
            &self.parent_id,
            payload_kind,
            prompt,
            anchor_rows,
        )
    }

    fn ensure_no_content(&self, path: &Path) -> Result<()> {
        ensure!(
            self.content.is_none(),
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "SQLite node {:?} of kind {:?} unexpectedly has content",
                    self.id, self.kind
                ),
            }
        );
        Ok(())
    }

    fn required_content(&self, path: &Path) -> Result<String> {
        self.content.clone().context(CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "missing SQLite node content for {:?} node {:?}",
                self.kind, self.id
            ),
        })
    }
}

fn node_anchor_kind_from_storage(
    path: &Path,
    node_id: &str,
    parent_id: &str,
    payload_kind: AnchorPayloadKind,
    prompt: Option<String>,
    rows: &NodeAnchorStorageRows<'_>,
) -> Result<Kind> {
    match payload_kind {
        AnchorPayloadKind::Session => {
            let session_row = rows.session.context(CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!("missing SQLite node anchor session row for {node_id:?}"),
            })?;
            session_row.kind_from_storage(
                path,
                node_id,
                parent_id,
                rows.session_tools,
                rows.relations,
            )
        }
        AnchorPayloadKind::SessionPatch => {
            let patch_row = rows.session_patch.context(CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!("missing SQLite node anchor session patch row for {node_id:?}"),
            })?;
            patch_row.kind_from_storage(
                path,
                node_id,
                parent_id,
                rows.session_patch_tools,
                rows.relations,
            )
        }
        AnchorPayloadKind::Prompt => {
            let prompt = prompt.context(CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!("missing SQLite node anchor prompt content for {node_id:?}"),
            })?;
            let attachments = prompt_attachments_from_rows(path, node_id, rows.prompt_attachments)?;
            let merge_parents =
                merge_parents_from_relation_rows(path, node_id, parent_id, rows.relations)?;
            Ok(Kind::Anchor(Anchor::prompt(
                merge_parents,
                PromptAnchor {
                    prompt,
                    attachments,
                },
            )))
        }
        AnchorPayloadKind::SkillInvocation => {
            let invocation_row = rows.skill_invocation.context(CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!("missing SQLite node anchor skill invocation row for {node_id:?}"),
            })?;
            invocation_row.kind_from_storage(path, node_id, parent_id, rows.relations)
        }
        AnchorPayloadKind::SkillResult => {
            let result_row = rows.skill_result.context(CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!("missing SQLite node anchor skill result row for {node_id:?}"),
            })?;
            result_row.kind_from_storage(path, node_id, parent_id, rows.relations)
        }
    }
}

impl NodeAnchorSessionRow {
    fn from_node(node: &Node, path: &Path) -> Result<Option<Self>> {
        let Kind::Anchor(Anchor {
            payload: AnchorPayload::Session(session),
            ..
        }) = &node.kind
        else {
            return Ok(None);
        };
        let additional_params_json = session
            .additional_params
            .as_ref()
            .map(|value| {
                serde_json::to_string(value).context(ParseSqliteStoreValueSnafu {
                    path: path.to_owned(),
                    column: "node_anchor_sessions.additional_params_json".to_owned(),
                })
            })
            .transpose()?;
        Ok(Some(Self {
            node_id: node.id.clone(),
            role: session.role.as_str().to_owned(),
            provider_profile: session.provider_profile.clone(),
            provider: session.provider.clone(),
            model: session.model.clone(),
            system_prompt: session.system_prompt.clone(),
            prompt: session.prompt.clone(),
            temperature: session.temperature,
            max_tokens: session.max_tokens.map(|value| value.to_string()),
            additional_params_json,
            enable_coco_shim: session.enable_coco_shim,
            active_skill_name: session
                .active_skill
                .as_ref()
                .map(|skill| skill.name.clone()),
            active_skill_handoff: session
                .active_skill
                .as_ref()
                .and_then(|skill| skill.handoff.clone()),
        }))
    }

    fn kind_from_storage(
        &self,
        path: &Path,
        node_id: &str,
        parent_id: &str,
        tool_rows: &[NodeAnchorSessionToolRow],
        relation_rows: &[NodeRelationRow],
    ) -> Result<Kind> {
        ensure!(
            self.node_id == node_id,
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "SQLite session row {:?} does not belong to node {node_id:?}",
                    self.node_id
                ),
            }
        );
        let role = parse_session_role(&self.role, path)?;
        let max_tokens = self
            .max_tokens
            .as_deref()
            .map(|value| parse_u64_column(path, "node_anchor_sessions.max_tokens", value))
            .transpose()?;
        let additional_params = self
            .additional_params_json
            .as_deref()
            .map(|value| {
                parse_json_column(path, "node_anchor_sessions.additional_params_json", value)
            })
            .transpose()?;
        let active_skill = match self.active_skill_name.as_deref() {
            Some(name) => Some(SkillRuntimeContext {
                name: name.to_owned(),
                handoff: self.active_skill_handoff.clone(),
            }),
            None => {
                ensure!(
                    self.active_skill_handoff.is_none(),
                    CorruptedStoreSnafu {
                        path: path.to_owned(),
                        message: "SQLite session active skill handoff is present without a name"
                            .to_owned(),
                    }
                );
                None
            }
        };
        let merge_parents =
            merge_parents_from_relation_rows(path, node_id, parent_id, relation_rows)?;
        let tools = session_tools_from_rows(path, node_id, tool_rows)?;
        Ok(Kind::Anchor(Anchor::session(
            merge_parents,
            SessionAnchor {
                role,
                provider_profile: self.provider_profile.clone(),
                provider: self.provider.clone(),
                model: self.model.clone(),
                tools,
                system_prompt: self.system_prompt.clone(),
                prompt: self.prompt.clone(),
                temperature: self.temperature,
                max_tokens,
                additional_params,
                enable_coco_shim: self.enable_coco_shim,
                active_skill,
            },
        )))
    }
}

impl NodeAnchorSkillInvocationRow {
    fn from_node(node: &Node) -> Option<Self> {
        let Kind::Anchor(Anchor {
            payload: AnchorPayload::SkillInvocation(invocation),
            ..
        }) = &node.kind
        else {
            return None;
        };
        let (mode, prompt) = match &invocation.mode {
            SkillInvocationMode::InheritContext => ("inherit_context".to_owned(), None),
            SkillInvocationMode::Handoff { prompt } => ("handoff".to_owned(), Some(prompt.clone())),
        };
        Some(Self {
            node_id: node.id.clone(),
            skill_name: invocation.skill_name.clone(),
            mode,
            prompt,
        })
    }

    fn kind_from_storage(
        &self,
        path: &Path,
        node_id: &str,
        parent_id: &str,
        relation_rows: &[NodeRelationRow],
    ) -> Result<Kind> {
        ensure!(
            self.node_id == node_id,
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "SQLite skill invocation row {:?} does not belong to node {node_id:?}",
                    self.node_id
                ),
            }
        );
        let mode = match self.mode.as_str() {
            "inherit_context" => {
                ensure!(
                    self.prompt.is_none(),
                    CorruptedStoreSnafu {
                        path: path.to_owned(),
                        message: "SQLite inherit-context skill invocation has a prompt".to_owned(),
                    }
                );
                SkillInvocationMode::InheritContext
            }
            "handoff" => SkillInvocationMode::Handoff {
                prompt: required_anchor_summary(
                    path,
                    "node_anchor_skill_invocations.prompt",
                    self.prompt.as_deref(),
                )?,
            },
            mode => {
                return CorruptedStoreSnafu {
                    path: path.to_owned(),
                    message: format!("invalid SQLite skill invocation mode {mode:?}"),
                }
                .fail();
            }
        };
        let merge_parents =
            merge_parents_from_relation_rows(path, node_id, parent_id, relation_rows)?;
        Ok(Kind::Anchor(Anchor::skill_invocation(
            merge_parents,
            SkillInvocationAnchor {
                skill_name: self.skill_name.clone(),
                mode,
            },
        )))
    }
}

impl NodeAnchorSkillResultRow {
    fn from_node(node: &Node) -> Option<Self> {
        let Kind::Anchor(Anchor {
            payload: AnchorPayload::SkillResult(result),
            ..
        }) = &node.kind
        else {
            return None;
        };
        Some(Self {
            node_id: node.id.clone(),
            skill_name: result.skill_name.clone(),
            output: result.output.clone(),
        })
    }

    fn kind_from_storage(
        &self,
        path: &Path,
        node_id: &str,
        parent_id: &str,
        relation_rows: &[NodeRelationRow],
    ) -> Result<Kind> {
        ensure!(
            self.node_id == node_id,
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "SQLite skill result row {:?} does not belong to node {node_id:?}",
                    self.node_id
                ),
            }
        );
        let merge_parents =
            merge_parents_from_relation_rows(path, node_id, parent_id, relation_rows)?;
        Ok(Kind::Anchor(Anchor::skill_result(
            merge_parents,
            SkillResultAnchor {
                skill_name: self.skill_name.clone(),
                output: self.output.clone(),
            },
        )))
    }
}

impl NodeAnchorSessionPatchRow {
    fn from_node(node: &Node, path: &Path) -> Result<Option<Self>> {
        let Kind::Anchor(Anchor {
            payload: AnchorPayload::SessionPatch(patch),
            ..
        }) = &node.kind
        else {
            return Ok(None);
        };
        let additional_params_json = patch
            .additional_params
            .as_ref()
            .and_then(Option::as_ref)
            .map(|value| {
                serde_json::to_string(value).context(ParseSqliteStoreValueSnafu {
                    path: path.to_owned(),
                    column: "node_anchor_session_patches.additional_params_json".to_owned(),
                })
            })
            .transpose()?;
        Ok(Some(Self {
            node_id: node.id.clone(),
            role: patch.role.map(|role| role.as_str().to_owned()),
            provider_profile_present: patch.provider_profile.is_some(),
            provider_profile: patch.provider_profile.clone().flatten(),
            provider_present: patch.provider.is_some(),
            provider: patch.provider.clone().flatten(),
            model: patch.model.clone(),
            tools_present: patch.tools.is_some(),
            system_prompt: patch.system_prompt.clone(),
            temperature_present: patch.temperature.is_some(),
            temperature: patch.temperature.flatten(),
            max_tokens_present: patch.max_tokens.is_some(),
            max_tokens: patch.max_tokens.flatten().map(|value| value.to_string()),
            additional_params_present: patch.additional_params.is_some(),
            additional_params_json,
            enable_coco_shim: patch.enable_coco_shim,
        }))
    }

    fn kind_from_storage(
        &self,
        path: &Path,
        node_id: &str,
        parent_id: &str,
        tool_rows: &[NodeAnchorSessionPatchToolRow],
        relation_rows: &[NodeRelationRow],
    ) -> Result<Kind> {
        ensure!(
            self.node_id == node_id,
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "SQLite session patch row {:?} does not belong to node {node_id:?}",
                    self.node_id
                ),
            }
        );
        let role = self
            .role
            .as_deref()
            .map(|role| parse_session_role(role, path))
            .transpose()?;
        let tools = if self.tools_present {
            Some(session_patch_tools_from_rows(path, node_id, tool_rows)?)
        } else {
            ensure!(
                tool_rows.is_empty(),
                CorruptedStoreSnafu {
                    path: path.to_owned(),
                    message: format!(
                        "SQLite session patch {node_id:?} has tools without tools_present"
                    ),
                }
            );
            None
        };
        let temperature = nested_optional_column(
            path,
            "node_anchor_session_patches.temperature",
            self.temperature_present,
            self.temperature,
        )?;
        let max_tokens_value = self
            .max_tokens
            .as_deref()
            .map(|value| parse_u64_column(path, "node_anchor_session_patches.max_tokens", value))
            .transpose()?;
        let max_tokens = nested_optional_column(
            path,
            "node_anchor_session_patches.max_tokens",
            self.max_tokens_present,
            max_tokens_value,
        )?;
        let additional_params_value = self
            .additional_params_json
            .as_deref()
            .map(|value| {
                parse_json_column(
                    path,
                    "node_anchor_session_patches.additional_params_json",
                    value,
                )
            })
            .transpose()?;
        let additional_params = nested_optional_column(
            path,
            "node_anchor_session_patches.additional_params_json",
            self.additional_params_present,
            additional_params_value,
        )?;
        let patch = SessionAnchorPatch {
            role,
            provider_profile: nested_optional_column(
                path,
                "node_anchor_session_patches.provider_profile",
                self.provider_profile_present,
                self.provider_profile.clone(),
            )?,
            provider: nested_optional_column(
                path,
                "node_anchor_session_patches.provider",
                self.provider_present,
                self.provider.clone(),
            )?,
            model: self.model.clone(),
            tools,
            system_prompt: self.system_prompt.clone(),
            temperature,
            max_tokens,
            additional_params,
            enable_coco_shim: self.enable_coco_shim,
        };
        let merge_parents =
            merge_parents_from_relation_rows(path, node_id, parent_id, relation_rows)?;
        Ok(Kind::Anchor(Anchor::session_patch(merge_parents, patch)))
    }
}

fn nested_optional_column<T>(
    path: &Path,
    column: &str,
    present: bool,
    value: Option<T>,
) -> Result<Option<Option<T>>> {
    if present {
        return Ok(Some(value));
    }
    ensure!(
        value.is_none(),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("SQLite {column} is set without its presence flag"),
        }
    );
    Ok(None)
}

fn session_tools_from_rows(
    path: &Path,
    node_id: &str,
    rows: &[NodeAnchorSessionToolRow],
) -> Result<Vec<Tool>> {
    rows.iter()
        .enumerate()
        .map(|(ordinal, row)| {
            ensure!(
                row.node_id == node_id && row.ordinal == ordinal as i32,
                CorruptedStoreSnafu {
                    path: path.to_owned(),
                    message: format!("invalid SQLite session tool ordinal for node {node_id:?}"),
                }
            );
            Ok(Tool {
                name: row.name.clone(),
                description: row.description.clone(),
                input_schema: parse_json_column(
                    path,
                    "node_anchor_session_tools.input_schema_json",
                    &row.input_schema_json,
                )?,
            })
        })
        .collect()
}

fn session_patch_tools_from_rows(
    path: &Path,
    node_id: &str,
    rows: &[NodeAnchorSessionPatchToolRow],
) -> Result<Vec<Tool>> {
    rows.iter()
        .enumerate()
        .map(|(ordinal, row)| {
            ensure!(
                row.node_id == node_id && row.ordinal == ordinal as i32,
                CorruptedStoreSnafu {
                    path: path.to_owned(),
                    message: format!(
                        "invalid SQLite session patch tool ordinal for node {node_id:?}"
                    ),
                }
            );
            Ok(Tool {
                name: row.name.clone(),
                description: row.description.clone(),
                input_schema: parse_json_column(
                    path,
                    "node_anchor_session_patch_tools.input_schema_json",
                    &row.input_schema_json,
                )?,
            })
        })
        .collect()
}

fn prompt_attachments_from_rows(
    path: &Path,
    node_id: &str,
    rows: &[NodeAnchorPromptAttachmentRow],
) -> Result<Vec<PromptAttachment>> {
    rows.iter()
        .enumerate()
        .map(|(ordinal, row)| {
            ensure!(
                row.node_id == node_id && row.ordinal == ordinal as i32,
                CorruptedStoreSnafu {
                    path: path.to_owned(),
                    message: format!(
                        "invalid SQLite prompt attachment ordinal for node {node_id:?}"
                    ),
                }
            );
            ensure!(
                row.kind == "image",
                CorruptedStoreSnafu {
                    path: path.to_owned(),
                    message: format!("invalid SQLite prompt attachment kind {:?}", row.kind),
                }
            );
            let width = row
                .width
                .map(|value| parse_u32_column(path, "node_anchor_prompt_attachments.width", value))
                .transpose()?;
            let height = row
                .height
                .map(|value| parse_u32_column(path, "node_anchor_prompt_attachments.height", value))
                .transpose()?;
            let file_size = row
                .file_size
                .as_deref()
                .map(|value| {
                    parse_u64_column(path, "node_anchor_prompt_attachments.file_size", value)
                })
                .transpose()?;
            Ok(PromptAttachment::Image(PromptImageAttachment {
                id: row.attachment_id.clone(),
                width,
                height,
                file_size,
                media_type: row.media_type.clone(),
            }))
        })
        .collect()
}

fn parse_u32_column(path: &Path, column: &str, value: i64) -> Result<u32> {
    value
        .try_into()
        .map_err(|source| StoreError::CorruptedStore {
            path: path.to_owned(),
            message: format!("invalid SQLite {column}: {source}"),
        })
}

fn merge_parents_from_relation_rows(
    path: &Path,
    node_id: &str,
    parent_id: &str,
    rows: &[NodeRelationRow],
) -> Result<Vec<MergeParent>> {
    let primary_rows = rows
        .iter()
        .filter(|row| row.kind == "primary")
        .collect::<Vec<_>>();
    ensure!(
        primary_rows.len() == 1
            && primary_rows[0].parent_node_id == parent_id
            && primary_rows[0].ordinal == 0,
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("invalid SQLite primary relation for anchor node {node_id:?}"),
        }
    );
    let mut merge_parents = Vec::new();
    for row in rows.iter().filter(|row| row.kind != "primary") {
        ensure!(
            row.child_node_id == node_id && row.ordinal == merge_parents.len() as i32,
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!("invalid SQLite merge relation for anchor node {node_id:?}"),
            }
        );
        let merge_parent = match row.kind.as_str() {
            "merge" => MergeParent::merge(row.parent_node_id.clone()),
            "shadow" => MergeParent::shadow(row.parent_node_id.clone()),
            _ => {
                return CorruptedStoreSnafu {
                    path: path.to_owned(),
                    message: format!(
                        "invalid SQLite relation kind {:?} for anchor node {node_id:?}",
                        row.kind
                    ),
                }
                .fail();
            }
        };
        merge_parents.push(merge_parent);
    }
    Ok(merge_parents)
}

fn required_anchor_summary(path: &Path, column: &str, value: Option<&str>) -> Result<String> {
    value.map(str::to_owned).context(CorruptedStoreSnafu {
        path: path.to_owned(),
        message: format!("missing SQLite {column}"),
    })
}

fn node_metadata_from_rows(
    path: &Path,
    node_id: &str,
    metadata_present: bool,
    metadata_rows: &[NodeMetadataRow],
) -> Result<Option<NodeMetadata>> {
    ensure!(
        metadata_present || metadata_rows.is_empty(),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("SQLite node {node_id:?} has metadata rows without metadata_present"),
        }
    );
    if !metadata_present {
        return Ok(None);
    }
    metadata_rows
        .iter()
        .enumerate()
        .map(|(ordinal, row)| {
            ensure_node_item_row(
                path,
                node_id,
                ordinal,
                &row.node_id,
                row.ordinal,
                "metadata",
            )?;
            Ok(BackendMetadata {
                execution_id: row.execution_id.clone(),
                call_id: row.call_id.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

fn node_tool_uses_from_rows(
    path: &Path,
    node_id: &str,
    rows: &[NodeToolUseRow],
) -> Result<Vec<ToolUse>> {
    rows.iter()
        .enumerate()
        .map(|(ordinal, row)| {
            ensure_node_item_row(
                path,
                node_id,
                ordinal,
                &row.node_id,
                row.ordinal,
                "tool use",
            )?;
            Ok(ToolUse {
                id: row.tool_use_id.clone(),
                name: row.name.clone(),
                input: serde_json::from_str(&row.input_json).context(
                    ParseSqliteStoreValueSnafu {
                        path: path.to_owned(),
                        column: "node_tool_uses.input_json".to_owned(),
                    },
                )?,
            })
        })
        .collect()
}

fn node_tool_results_from_rows(
    path: &Path,
    node_id: &str,
    rows: &[NodeToolResultRow],
) -> Result<Vec<ToolResult>> {
    rows.iter()
        .enumerate()
        .map(|(ordinal, row)| {
            ensure_node_item_row(
                path,
                node_id,
                ordinal,
                &row.node_id,
                row.ordinal,
                "tool result",
            )?;
            Ok(ToolResult {
                id: row.tool_result_id.clone(),
                output: row.output.clone(),
            })
        })
        .collect()
}

fn ensure_no_tool_use_rows(path: &Path, node_id: &str, rows: &[NodeToolUseRow]) -> Result<()> {
    ensure!(
        rows.is_empty(),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("SQLite node {node_id:?} has unexpected tool use rows"),
        }
    );
    Ok(())
}

fn ensure_no_tool_result_rows(
    path: &Path,
    node_id: &str,
    rows: &[NodeToolResultRow],
) -> Result<()> {
    ensure!(
        rows.is_empty(),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("SQLite node {node_id:?} has unexpected tool result rows"),
        }
    );
    Ok(())
}

fn ensure_node_item_row(
    path: &Path,
    node_id: &str,
    expected_ordinal: usize,
    row_node_id: &str,
    row_ordinal: i32,
    item_kind: &str,
) -> Result<()> {
    ensure!(
        row_node_id == node_id && row_ordinal == expected_ordinal as i32,
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "invalid SQLite node {item_kind} row for {node_id:?} at ordinal {expected_ordinal}"
            ),
        }
    );
    Ok(())
}

pub fn expected_node_tool_use_rows(
    node_id: &str,
    kind: &Kind,
    path: &Path,
) -> Result<Vec<NodeToolUseRow>> {
    let Kind::ToolUse(tool_uses) = kind else {
        return Ok(Vec::new());
    };

    tool_uses
        .iter()
        .enumerate()
        .map(|(ordinal, tool_use)| {
            Ok(NodeToolUseRow {
                node_id: node_id.to_owned(),
                ordinal: ordinal as i32,
                tool_use_id: tool_use.id.clone(),
                name: tool_use.name.clone(),
                input_json: serde_json::to_string(&tool_use.input).context(
                    ParseSqliteStoreValueSnafu {
                        path: path.to_owned(),
                        column: "node_tool_uses.input_json".to_owned(),
                    },
                )?,
            })
        })
        .collect()
}

pub fn expected_node_tool_result_rows(node_id: &str, kind: &Kind) -> Vec<NodeToolResultRow> {
    let Kind::ToolResult(tool_results) = kind else {
        return Vec::new();
    };

    tool_results
        .iter()
        .enumerate()
        .map(|(ordinal, tool_result)| NodeToolResultRow {
            node_id: node_id.to_owned(),
            ordinal: ordinal as i32,
            tool_result_id: tool_result.id.clone(),
            output: tool_result.output.clone(),
        })
        .collect()
}

fn role_name(role: &Role) -> &'static str {
    match role {
        Role::User => "user",
        Role::System => "system",
        Role::LLM => "llm",
    }
}

fn parse_role(role: &str, path: &Path) -> Result<Role> {
    match role {
        "user" => Ok(Role::User),
        "system" => Ok(Role::System),
        "llm" => Ok(Role::LLM),
        _ => CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("invalid SQLite node role {role:?}"),
        }
        .fail(),
    }
}
