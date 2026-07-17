use super::*;

macro_rules! node_row_columns {
    () => {
        (
            nodes::id,
            nodes::parent_id,
            nodes::created_at,
            nodes::role,
            nodes::kind,
            nodes::metadata_present,
            nodes::content,
        )
    };
}

pub async fn node_count(connection: &mut AsyncSqliteConnection, path: &Path) -> Result<i64> {
    nodes::table
        .select(diesel::dsl::count_star())
        .first(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
}

pub async fn load_root_id(connection: &mut AsyncSqliteConnection, path: &Path) -> Result<String> {
    let root_ids = nodes::table
        .filter(nodes::parent_id.eq(""))
        .select(nodes::id)
        .limit(2)
        .load::<String>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    match root_ids.as_slice() {
        [root_id] => Ok(root_id.clone()),
        [] => CorruptedStoreSnafu {
            path: path.to_owned(),
            message: "missing SQLite root node".to_owned(),
        }
        .fail(),
        _ => CorruptedStoreSnafu {
            path: path.to_owned(),
            message: "multiple SQLite root nodes".to_owned(),
        }
        .fail(),
    }
}

async fn load_node_metadata_rows_for_ids(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node_ids: Option<&[String]>,
) -> Result<HashMap<String, Vec<NodeMetadataRow>>> {
    let mut query = node_metadata::table
        .select((
            node_metadata::node_id,
            node_metadata::ordinal,
            node_metadata::execution_id,
            node_metadata::call_id,
        ))
        .into_boxed();
    if let Some(node_ids) = node_ids {
        if node_ids.is_empty() {
            return Ok(HashMap::new());
        }
        query = query.filter(node_metadata::node_id.eq_any(node_ids));
    }
    let rows = query
        .order((node_metadata::node_id, node_metadata::ordinal))
        .load::<NodeMetadataRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(group_node_metadata_rows(rows))
}

async fn load_node_anchor_session_rows_for_ids(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node_ids: Option<&[String]>,
) -> Result<HashMap<String, NodeAnchorSessionRow>> {
    let mut query = node_anchor_sessions::table
        .select((
            node_anchor_sessions::node_id,
            node_anchor_sessions::role,
            node_anchor_sessions::provider_profile,
            node_anchor_sessions::provider,
            node_anchor_sessions::model,
            node_anchor_sessions::system_prompt,
            node_anchor_sessions::prompt,
            node_anchor_sessions::temperature,
            node_anchor_sessions::max_tokens,
            node_anchor_sessions::additional_params_json,
            node_anchor_sessions::enable_coco_shim,
            node_anchor_sessions::active_skill_name,
            node_anchor_sessions::active_skill_handoff,
        ))
        .into_boxed();
    if let Some(node_ids) = node_ids {
        if node_ids.is_empty() {
            return Ok(HashMap::new());
        }
        query = query.filter(node_anchor_sessions::node_id.eq_any(node_ids));
    }
    query
        .load::<NodeAnchorSessionRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
        .map(|rows| {
            rows.into_iter()
                .map(|row| (row.node_id.clone(), row))
                .collect()
        })
}

async fn load_node_anchor_session_tool_rows_for_ids(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node_ids: Option<&[String]>,
) -> Result<HashMap<String, Vec<NodeAnchorSessionToolRow>>> {
    let mut query = node_anchor_session_tools::table
        .select((
            node_anchor_session_tools::node_id,
            node_anchor_session_tools::ordinal,
            node_anchor_session_tools::name,
            node_anchor_session_tools::description,
            node_anchor_session_tools::input_schema_json,
        ))
        .into_boxed();
    if let Some(node_ids) = node_ids {
        if node_ids.is_empty() {
            return Ok(HashMap::new());
        }
        query = query.filter(node_anchor_session_tools::node_id.eq_any(node_ids));
    }
    let rows = query
        .order((
            node_anchor_session_tools::node_id,
            node_anchor_session_tools::ordinal,
        ))
        .load::<NodeAnchorSessionToolRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(group_node_anchor_session_tool_rows(rows))
}

async fn load_node_anchor_session_patch_rows_for_ids(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node_ids: Option<&[String]>,
) -> Result<HashMap<String, NodeAnchorSessionPatchRow>> {
    let mut query = node_anchor_session_patches::table
        .select((
            node_anchor_session_patches::node_id,
            node_anchor_session_patches::role,
            node_anchor_session_patches::provider_profile_present,
            node_anchor_session_patches::provider_profile,
            node_anchor_session_patches::provider_present,
            node_anchor_session_patches::provider,
            node_anchor_session_patches::model,
            node_anchor_session_patches::tools_present,
            node_anchor_session_patches::system_prompt,
            node_anchor_session_patches::temperature_present,
            node_anchor_session_patches::temperature,
            node_anchor_session_patches::max_tokens_present,
            node_anchor_session_patches::max_tokens,
            node_anchor_session_patches::additional_params_present,
            node_anchor_session_patches::additional_params_json,
            node_anchor_session_patches::enable_coco_shim,
        ))
        .into_boxed();
    if let Some(node_ids) = node_ids {
        if node_ids.is_empty() {
            return Ok(HashMap::new());
        }
        query = query.filter(node_anchor_session_patches::node_id.eq_any(node_ids));
    }
    query
        .load::<NodeAnchorSessionPatchRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
        .map(|rows| {
            rows.into_iter()
                .map(|row| (row.node_id.clone(), row))
                .collect()
        })
}

async fn load_node_anchor_session_patch_tool_rows_for_ids(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node_ids: Option<&[String]>,
) -> Result<HashMap<String, Vec<NodeAnchorSessionPatchToolRow>>> {
    let mut query = node_anchor_session_patch_tools::table
        .select((
            node_anchor_session_patch_tools::node_id,
            node_anchor_session_patch_tools::ordinal,
            node_anchor_session_patch_tools::name,
            node_anchor_session_patch_tools::description,
            node_anchor_session_patch_tools::input_schema_json,
        ))
        .into_boxed();
    if let Some(node_ids) = node_ids {
        if node_ids.is_empty() {
            return Ok(HashMap::new());
        }
        query = query.filter(node_anchor_session_patch_tools::node_id.eq_any(node_ids));
    }
    let rows = query
        .order((
            node_anchor_session_patch_tools::node_id,
            node_anchor_session_patch_tools::ordinal,
        ))
        .load::<NodeAnchorSessionPatchToolRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(group_node_anchor_session_patch_tool_rows(rows))
}

async fn load_node_anchor_prompt_attachment_rows_for_ids(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node_ids: Option<&[String]>,
) -> Result<HashMap<String, Vec<NodeAnchorPromptAttachmentRow>>> {
    let mut query = node_anchor_prompt_attachments::table
        .select((
            node_anchor_prompt_attachments::node_id,
            node_anchor_prompt_attachments::ordinal,
            node_anchor_prompt_attachments::kind,
            node_anchor_prompt_attachments::attachment_id,
            node_anchor_prompt_attachments::width,
            node_anchor_prompt_attachments::height,
            node_anchor_prompt_attachments::file_size,
            node_anchor_prompt_attachments::media_type,
        ))
        .into_boxed();
    if let Some(node_ids) = node_ids {
        if node_ids.is_empty() {
            return Ok(HashMap::new());
        }
        query = query.filter(node_anchor_prompt_attachments::node_id.eq_any(node_ids));
    }
    let rows = query
        .order((
            node_anchor_prompt_attachments::node_id,
            node_anchor_prompt_attachments::ordinal,
        ))
        .load::<NodeAnchorPromptAttachmentRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(group_node_anchor_prompt_attachment_rows(rows))
}

async fn load_node_anchor_skill_invocation_rows_for_ids(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node_ids: Option<&[String]>,
) -> Result<HashMap<String, NodeAnchorSkillInvocationRow>> {
    let mut query = node_anchor_skill_invocations::table
        .select((
            node_anchor_skill_invocations::node_id,
            node_anchor_skill_invocations::skill_name,
            node_anchor_skill_invocations::mode,
            node_anchor_skill_invocations::prompt,
        ))
        .into_boxed();
    if let Some(node_ids) = node_ids {
        if node_ids.is_empty() {
            return Ok(HashMap::new());
        }
        query = query.filter(node_anchor_skill_invocations::node_id.eq_any(node_ids));
    }
    query
        .load::<NodeAnchorSkillInvocationRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
        .map(|rows| {
            rows.into_iter()
                .map(|row| (row.node_id.clone(), row))
                .collect()
        })
}

async fn load_node_anchor_skill_result_rows_for_ids(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node_ids: Option<&[String]>,
) -> Result<HashMap<String, NodeAnchorSkillResultRow>> {
    let mut query = node_anchor_skill_results::table
        .select((
            node_anchor_skill_results::node_id,
            node_anchor_skill_results::skill_name,
            node_anchor_skill_results::output,
        ))
        .into_boxed();
    if let Some(node_ids) = node_ids {
        if node_ids.is_empty() {
            return Ok(HashMap::new());
        }
        query = query.filter(node_anchor_skill_results::node_id.eq_any(node_ids));
    }
    query
        .load::<NodeAnchorSkillResultRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
        .map(|rows| {
            rows.into_iter()
                .map(|row| (row.node_id.clone(), row))
                .collect()
        })
}

async fn load_node_relation_rows_for_ids(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node_ids: Option<&[String]>,
) -> Result<HashMap<String, Vec<NodeRelationRow>>> {
    let mut query = node_relations::table
        .select((
            node_relations::child_node_id,
            node_relations::parent_node_id,
            node_relations::kind,
            node_relations::ordinal,
        ))
        .into_boxed();
    if let Some(node_ids) = node_ids {
        if node_ids.is_empty() {
            return Ok(HashMap::new());
        }
        query = query.filter(node_relations::child_node_id.eq_any(node_ids));
    }
    let rows = query
        .order((node_relations::child_node_id, node_relations::ordinal))
        .load::<NodeRelationRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(group_node_relation_rows(rows))
}

fn group_node_metadata_rows(rows: Vec<NodeMetadataRow>) -> HashMap<String, Vec<NodeMetadataRow>> {
    let mut grouped = HashMap::new();
    for row in rows {
        grouped
            .entry(row.node_id.clone())
            .or_insert_with(Vec::new)
            .push(row);
    }
    grouped
}

fn group_node_anchor_session_tool_rows(
    rows: Vec<NodeAnchorSessionToolRow>,
) -> HashMap<String, Vec<NodeAnchorSessionToolRow>> {
    let mut grouped = HashMap::new();
    for row in rows {
        grouped
            .entry(row.node_id.clone())
            .or_insert_with(Vec::new)
            .push(row);
    }
    grouped
}

fn group_node_anchor_session_patch_tool_rows(
    rows: Vec<NodeAnchorSessionPatchToolRow>,
) -> HashMap<String, Vec<NodeAnchorSessionPatchToolRow>> {
    let mut grouped = HashMap::new();
    for row in rows {
        grouped
            .entry(row.node_id.clone())
            .or_insert_with(Vec::new)
            .push(row);
    }
    grouped
}

fn group_node_anchor_prompt_attachment_rows(
    rows: Vec<NodeAnchorPromptAttachmentRow>,
) -> HashMap<String, Vec<NodeAnchorPromptAttachmentRow>> {
    let mut grouped = HashMap::new();
    for row in rows {
        grouped
            .entry(row.node_id.clone())
            .or_insert_with(Vec::new)
            .push(row);
    }
    grouped
}

fn group_node_relation_rows(rows: Vec<NodeRelationRow>) -> HashMap<String, Vec<NodeRelationRow>> {
    let mut grouped = HashMap::new();
    for row in rows {
        grouped
            .entry(row.child_node_id.clone())
            .or_insert_with(Vec::new)
            .push(row);
    }
    grouped
}

async fn load_node_tool_use_rows_for_ids(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node_ids: Option<&[String]>,
) -> Result<HashMap<String, Vec<NodeToolUseRow>>> {
    let mut query = node_tool_uses::table
        .select((
            node_tool_uses::node_id,
            node_tool_uses::ordinal,
            node_tool_uses::tool_use_id,
            node_tool_uses::name,
            node_tool_uses::input_json,
        ))
        .into_boxed();
    if let Some(node_ids) = node_ids {
        if node_ids.is_empty() {
            return Ok(HashMap::new());
        }
        query = query.filter(node_tool_uses::node_id.eq_any(node_ids));
    }
    let rows = query
        .order((node_tool_uses::node_id, node_tool_uses::ordinal))
        .load::<NodeToolUseRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(group_node_tool_use_rows(rows))
}

fn group_node_tool_use_rows(rows: Vec<NodeToolUseRow>) -> HashMap<String, Vec<NodeToolUseRow>> {
    let mut grouped = HashMap::new();
    for row in rows {
        grouped
            .entry(row.node_id.clone())
            .or_insert_with(Vec::new)
            .push(row);
    }
    grouped
}

async fn load_node_tool_result_rows_for_ids(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node_ids: Option<&[String]>,
) -> Result<HashMap<String, Vec<NodeToolResultRow>>> {
    let mut query = node_tool_results::table
        .select((
            node_tool_results::node_id,
            node_tool_results::ordinal,
            node_tool_results::tool_result_id,
            node_tool_results::output,
        ))
        .into_boxed();
    if let Some(node_ids) = node_ids {
        if node_ids.is_empty() {
            return Ok(HashMap::new());
        }
        query = query.filter(node_tool_results::node_id.eq_any(node_ids));
    }
    let rows = query
        .order((node_tool_results::node_id, node_tool_results::ordinal))
        .load::<NodeToolResultRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(group_node_tool_result_rows(rows))
}

fn group_node_tool_result_rows(
    rows: Vec<NodeToolResultRow>,
) -> HashMap<String, Vec<NodeToolResultRow>> {
    let mut grouped = HashMap::new();
    for row in rows {
        grouped
            .entry(row.node_id.clone())
            .or_insert_with(Vec::new)
            .push(row);
    }
    grouped
}

fn node_metadata_slice<'a>(
    rows: &'a HashMap<String, Vec<NodeMetadataRow>>,
    node_id: &str,
) -> &'a [NodeMetadataRow] {
    rows.get(node_id).map(Vec::as_slice).unwrap_or_default()
}

fn node_anchor_session_tool_slice<'a>(
    rows: &'a HashMap<String, Vec<NodeAnchorSessionToolRow>>,
    node_id: &str,
) -> &'a [NodeAnchorSessionToolRow] {
    rows.get(node_id).map(Vec::as_slice).unwrap_or_default()
}

fn node_anchor_session_patch_tool_slice<'a>(
    rows: &'a HashMap<String, Vec<NodeAnchorSessionPatchToolRow>>,
    node_id: &str,
) -> &'a [NodeAnchorSessionPatchToolRow] {
    rows.get(node_id).map(Vec::as_slice).unwrap_or_default()
}

fn node_anchor_prompt_attachment_slice<'a>(
    rows: &'a HashMap<String, Vec<NodeAnchorPromptAttachmentRow>>,
    node_id: &str,
) -> &'a [NodeAnchorPromptAttachmentRow] {
    rows.get(node_id).map(Vec::as_slice).unwrap_or_default()
}

fn node_relation_slice<'a>(
    rows: &'a HashMap<String, Vec<NodeRelationRow>>,
    node_id: &str,
) -> &'a [NodeRelationRow] {
    rows.get(node_id).map(Vec::as_slice).unwrap_or_default()
}

fn node_tool_use_slice<'a>(
    rows: &'a HashMap<String, Vec<NodeToolUseRow>>,
    node_id: &str,
) -> &'a [NodeToolUseRow] {
    rows.get(node_id).map(Vec::as_slice).unwrap_or_default()
}

fn node_tool_result_slice<'a>(
    rows: &'a HashMap<String, Vec<NodeToolResultRow>>,
    node_id: &str,
) -> &'a [NodeToolResultRow] {
    rows.get(node_id).map(Vec::as_slice).unwrap_or_default()
}

pub(super) async fn node_rows_into_nodes(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    rows: Vec<NodeRow>,
) -> Result<Vec<Node>> {
    let ids = NodeStorageIds::from_rows(&rows);
    let anchor_session_rows =
        load_node_anchor_session_rows_for_ids(connection, path, Some(&ids.anchor_sessions)).await?;
    let anchor_session_tool_rows =
        load_node_anchor_session_tool_rows_for_ids(connection, path, Some(&ids.anchor_sessions))
            .await?;
    let anchor_session_patch_rows = load_node_anchor_session_patch_rows_for_ids(
        connection,
        path,
        Some(&ids.anchor_session_patches),
    )
    .await?;
    let anchor_session_patch_tool_rows = load_node_anchor_session_patch_tool_rows_for_ids(
        connection,
        path,
        Some(&ids.anchor_session_patches),
    )
    .await?;
    let anchor_prompt_attachment_rows = load_node_anchor_prompt_attachment_rows_for_ids(
        connection,
        path,
        Some(&ids.anchor_prompts),
    )
    .await?;
    let anchor_skill_invocation_rows = load_node_anchor_skill_invocation_rows_for_ids(
        connection,
        path,
        Some(&ids.anchor_skill_invocations),
    )
    .await?;
    let anchor_skill_result_rows = load_node_anchor_skill_result_rows_for_ids(
        connection,
        path,
        Some(&ids.anchor_skill_results),
    )
    .await?;
    let relation_rows =
        load_node_relation_rows_for_ids(connection, path, Some(&ids.anchors)).await?;
    let metadata_rows =
        load_node_metadata_rows_for_ids(connection, path, Some(&ids.metadata)).await?;
    let tool_use_rows =
        load_node_tool_use_rows_for_ids(connection, path, Some(&ids.tool_uses)).await?;
    let tool_result_rows =
        load_node_tool_result_rows_for_ids(connection, path, Some(&ids.tool_results)).await?;
    rows.into_iter()
        .map(|row| {
            let node_id = row.id.clone();
            row.into_node(
                path,
                NodeStorageRows {
                    anchor: NodeAnchorStorageRows {
                        session: anchor_session_rows.get(&node_id),
                        session_tools: node_anchor_session_tool_slice(
                            &anchor_session_tool_rows,
                            &node_id,
                        ),
                        session_patch: anchor_session_patch_rows.get(&node_id),
                        session_patch_tools: node_anchor_session_patch_tool_slice(
                            &anchor_session_patch_tool_rows,
                            &node_id,
                        ),
                        prompt_attachments: node_anchor_prompt_attachment_slice(
                            &anchor_prompt_attachment_rows,
                            &node_id,
                        ),
                        skill_invocation: anchor_skill_invocation_rows.get(&node_id),
                        skill_result: anchor_skill_result_rows.get(&node_id),
                        relations: node_relation_slice(&relation_rows, &node_id),
                    },
                    metadata: node_metadata_slice(&metadata_rows, &node_id),
                    tool_uses: node_tool_use_slice(&tool_use_rows, &node_id),
                    tool_results: node_tool_result_slice(&tool_result_rows, &node_id),
                },
            )
        })
        .collect()
}

pub async fn load_nodes_by_exact_ids(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    ids: &[String],
) -> Result<Vec<Node>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let rows = nodes::table
        .filter(nodes::id.eq_any(ids))
        .select(node_row_columns!())
        .load::<NodeRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    ensure!(
        rows.len() == ids.len(),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "SQLite graph batch requested {} nodes but returned {}",
                ids.len(),
                rows.len()
            ),
        }
    );
    node_rows_into_nodes(connection, path, rows).await
}

pub async fn load_child_ids_by_parent_ids(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    parent_ids: &[String],
) -> Result<HashMap<String, Vec<String>>> {
    if parent_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let rows = node_relations::table
        .filter(node_relations::parent_node_id.eq_any(parent_ids))
        .select((
            node_relations::parent_node_id,
            node_relations::child_node_id,
        ))
        .order((
            node_relations::parent_node_id,
            node_relations::child_node_id,
        ))
        .load::<(String, String)>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    let mut children = HashMap::<String, Vec<String>>::new();
    for (parent_id, child_id) in rows {
        children.entry(parent_id).or_default().push(child_id);
    }
    Ok(children)
}

pub async fn load_child_ids_page(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    parent_id: &str,
    cursor: Option<(&str, &str)>,
    limit: usize,
) -> Result<Vec<(String, String)>> {
    let mut query = node_relations::table
        .inner_join(nodes::table.on(nodes::id.eq(node_relations::child_node_id)))
        .filter(node_relations::parent_node_id.eq(parent_id))
        .select((nodes::created_at, nodes::id))
        .distinct()
        .into_boxed();
    if let Some((created_at, node_id)) = cursor {
        query = query.filter(
            nodes::created_at
                .gt(created_at)
                .or(nodes::created_at.eq(created_at).and(nodes::id.gt(node_id))),
        );
    }
    query
        .order((nodes::created_at, nodes::id))
        .limit(i64::try_from(limit).expect("graph child page limit should fit in i64"))
        .load::<(String, String)>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
}

pub async fn load_node_by_exact_id(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    id: &str,
) -> Result<Node> {
    let row = nodes::table
        .filter(nodes::id.eq(id))
        .select(node_row_columns!())
        .get_result::<NodeRow>(connection)
        .await
        .optional()
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?
        .context(NotFoundSnafu { id: id.to_owned() })?;
    node_rows_into_nodes(connection, path, vec![row])
        .await?
        .pop()
        .context(CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("SQLite node query for {id:?} returned no rows"),
        })
}

async fn node_exists_by_id(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    id: &str,
) -> Result<bool> {
    let count = nodes::table
        .filter(nodes::id.eq(id))
        .count()
        .get_result::<i64>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(count > 0)
}

pub async fn load_node_by_prefix_or_branch(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    reference: &str,
) -> Result<Node> {
    if let Some(head_id) = maybe_load_branch_head(connection, path, reference).await? {
        return load_node_by_exact_id(connection, path, &head_id).await;
    }

    match load_node_by_exact_id(connection, path, reference).await {
        Ok(node) => Ok(node),
        Err(crate::StoreError::NotFound { .. }) => {
            load_node_by_prefix(connection, path, reference).await
        }
        Err(error) => Err(error),
    }
}

async fn load_node_by_prefix(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    prefix: &str,
) -> Result<Node> {
    match load_node_ids_by_prefix(connection, path, prefix)
        .await?
        .as_slice()
    {
        [matched] => load_node_by_exact_id(connection, path, matched).await,
        [] => NotFoundSnafu {
            id: prefix.to_owned(),
        }
        .fail(),
        matches => AmbiguousNodePrefixSnafu {
            prefix: prefix.to_owned(),
            matches: matches.to_vec(),
        }
        .fail(),
    }
}

async fn load_node_ids_by_prefix(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    prefix: &str,
) -> Result<Vec<String>> {
    nodes::table
        .filter(nodes::id.like(format!("{prefix}%")))
        .select(nodes::id)
        .order(nodes::id)
        .load::<String>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
}

pub async fn resolve_ref_id(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    reference: &str,
) -> Result<String> {
    if node_exists_by_id(connection, path, reference).await? {
        return Ok(reference.to_owned());
    }
    if let Some(head_id) = maybe_load_branch_head(connection, path, reference).await? {
        load_node_by_exact_id(connection, path, &head_id).await?;
        return Ok(head_id);
    }

    if is_node_id(reference) {
        return NotFoundSnafu {
            id: reference.to_owned(),
        }
        .fail();
    }
    BranchNotFoundSnafu {
        name: reference.to_owned(),
    }
    .fail()
}

pub async fn load_ancestry_nodes(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    head_ref: &str,
) -> Result<Vec<Node>> {
    let mut current_id = resolve_ref_id(connection, path, head_ref).await?;
    let mut rows = Vec::new();
    let mut seen = HashSet::new();
    loop {
        ensure!(
            seen.insert(current_id.clone()),
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: "SQLite nodes contain cyclic parents".to_owned(),
            }
        );
        let row = nodes::table
            .filter(nodes::id.eq(&current_id))
            .select(node_row_columns!())
            .get_result::<NodeRow>(connection)
            .await
            .optional()
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?
            .context(ParentNotFoundSnafu {
                id: current_id.clone(),
            })?;
        let parent_id = row.parent_id.clone();
        let is_root = parent_id.is_empty();
        rows.push(row);
        if is_root {
            break;
        }
        current_id = parent_id;
    }
    node_rows_into_nodes(connection, path, rows).await
}

pub async fn load_log_nodes(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    base_ref: &str,
    head_ref: &str,
) -> Result<Vec<Node>> {
    let base_id = resolve_ref_id(connection, path, base_ref).await?;
    let mut nodes = load_ancestry_nodes(connection, path, head_ref).await?;
    let Some(index) = nodes.iter().position(|node| node.id == base_id) else {
        return RefsNotConnectedSnafu {
            base_ref: base_ref.to_owned(),
            head_ref: head_ref.to_owned(),
        }
        .fail();
    };
    nodes.truncate(index + 1);
    Ok(nodes)
}

pub async fn load_child_nodes(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node_id: &str,
) -> Result<Vec<Node>> {
    load_node_by_exact_id(connection, path, node_id).await?;
    let rows = node_relations::table
        .inner_join(nodes::table.on(nodes::id.eq(node_relations::child_node_id)))
        .filter(node_relations::parent_node_id.eq(node_id))
        .select(node_row_columns!())
        .order((nodes::created_at, nodes::id))
        .load::<NodeRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    node_rows_into_nodes(connection, path, rows).await
}

pub async fn validate_new_node(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    node: &Node,
) -> Result<()> {
    ensure!(
        node_exists_by_id(connection, path, &node.parent).await?,
        ParentNotFoundSnafu {
            id: node.parent.clone(),
        }
    );
    validate_anchor_merge_parents(connection, path, &node.parent, &node.kind).await
}

async fn validate_anchor_merge_parents(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    parent: &str,
    kind: &Kind,
) -> Result<()> {
    let Kind::Anchor(anchor) = kind else {
        return Ok(());
    };

    let mut seen = HashSet::new();
    let mut shadow_parents = Vec::new();
    for merge_parent in anchor.merge_parents() {
        let node_id = merge_parent.node_id();
        ensure!(
            node_id != parent,
            MergeParentMatchesParentSnafu {
                id: node_id.to_owned(),
            }
        );
        ensure!(
            seen.insert(node_id),
            DuplicateMergeParentSnafu {
                id: node_id.to_owned(),
            }
        );
        ensure!(
            node_exists_by_id(connection, path, node_id).await?,
            ParentNotFoundSnafu {
                id: node_id.to_owned(),
            }
        );
        if merge_parent.is_shadow() {
            shadow_parents.push(node_id.to_owned());
        }
    }
    ensure!(
        shadow_parents.len() <= 1,
        MultipleShadowParentsSnafu {
            ids: shadow_parents,
        }
    );

    Ok(())
}

fn is_node_id(reference: &str) -> bool {
    reference.len() == 64 && reference.bytes().all(|byte| byte.is_ascii_hexdigit())
}
