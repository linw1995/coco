use std::collections::{HashMap, HashSet};
use std::path::Path;

use async_trait::async_trait;
use diesel::prelude::*;
use diesel::result::OptionalExtension;
use diesel::sql_types::{Nullable, Text};
use diesel_async::RunQueryDsl;
use snafu::prelude::*;

use super::node::{
    load_ancestry_nodes, load_node_by_exact_id, persist_node_without_transaction, resolve_ref_id,
    upsert_node_without_transaction, validate_new_node,
};
use super::{
    AsyncSqliteConnection, GraphBranchRecord, SqliteGraphStore, SqliteStore, SqliteTransactionError,
};
use crate::error::{
    BranchExistsSnafu, BranchHeadMovedSnafu, BranchNotFoundSnafu, CorruptedStoreSnafu,
    InvalidAnchorSnafu, InvalidSessionHandoffPromptSnafu, MissingSessionAnchorSnafu,
    ParentNotFoundSnafu, QuerySqliteStoreSnafu, RefsNotConnectedSnafu, SessionStateMovedSnafu,
};
use crate::schema::{branches, nodes, sessions};
use crate::store::{BranchAppendSessionState, BranchStore, SessionStore};
use crate::{
    Anchor, AnchorPayload, Kind, NewNodeContent, Node, PauseReason, Role, SessionAnchorPatch,
    SessionState, StoreResult as Result,
};

#[derive(Queryable, QueryableByName)]
pub struct SessionRow {
    #[diesel(sql_type = Text)]
    pub branch_name: String,
    #[diesel(sql_type = Text)]
    pub state: String,
    #[diesel(sql_type = Nullable<Text>)]
    pub target_branch: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    pub base_head_id: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    pub pause_reason: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    pub merged_anchor_id: Option<String>,
}

#[derive(Queryable)]
struct GraphBranchRow {
    name: String,
    head_id: String,
    state: String,
    target_branch: Option<String>,
    base_head_id: Option<String>,
    pause_reason: Option<String>,
    merged_anchor_id: Option<String>,
}

pub async fn load_graph_branch_records(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    names: Option<&[String]>,
    after: Option<&str>,
    through: Option<&str>,
    limit: Option<usize>,
) -> Result<Vec<GraphBranchRecord>> {
    let mut query = branches::table
        .inner_join(sessions::table.on(sessions::branch_name.eq(branches::name)))
        .select((
            branches::name,
            branches::head_id,
            sessions::state,
            sessions::target_branch,
            sessions::base_head_id,
            sessions::pause_reason,
            sessions::merged_anchor_id,
        ))
        .into_boxed();
    if let Some(names) = names {
        if names.is_empty() {
            return Ok(Vec::new());
        }
        query = query.filter(branches::name.eq_any(names));
    }
    if let Some(after) = after {
        query = query.filter(branches::name.gt(after));
    }
    if let Some(through) = through {
        query = query.filter(branches::name.le(through));
    }
    if let Some(limit) = limit {
        query = query.limit(limit as i64);
    }
    query
        .order(branches::name)
        .load::<GraphBranchRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?
        .into_iter()
        .map(|row| {
            let (name, state) = session_row_into_state(
                path,
                SessionRow {
                    branch_name: row.name,
                    state: row.state,
                    target_branch: row.target_branch,
                    base_head_id: row.base_head_id,
                    pause_reason: row.pause_reason,
                    merged_anchor_id: row.merged_anchor_id,
                },
            )?;
            Ok(GraphBranchRecord {
                name,
                head_id: row.head_id,
                state,
            })
        })
        .collect()
}

pub async fn load_graph_branch_name_high_watermark(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<Option<String>> {
    branches::table
        .inner_join(sessions::table.on(sessions::branch_name.eq(branches::name)))
        .select(branches::name)
        .order(branches::name.desc())
        .first::<String>(connection)
        .await
        .optional()
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
}

async fn load_session_states(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<HashMap<String, SessionState>> {
    let sessions = sessions::table
        .select((
            sessions::branch_name,
            sessions::state,
            sessions::target_branch,
            sessions::base_head_id,
            sessions::pause_reason,
            sessions::merged_anchor_id,
        ))
        .order(sessions::branch_name)
        .load::<SessionRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    sessions
        .into_iter()
        .map(|session| session_row_into_state(path, session))
        .collect()
}

async fn load_session_state(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    name: &str,
) -> Result<SessionState> {
    let session = sessions::table
        .filter(sessions::branch_name.eq(name))
        .select((
            sessions::branch_name,
            sessions::state,
            sessions::target_branch,
            sessions::base_head_id,
            sessions::pause_reason,
            sessions::merged_anchor_id,
        ))
        .get_result::<SessionRow>(connection)
        .await
        .optional()
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?
        .context(BranchNotFoundSnafu {
            name: name.to_owned(),
        })?;
    let (_, state) = session_row_into_state(path, session)?;
    Ok(state)
}

pub fn session_row_into_state(path: &Path, session: SessionRow) -> Result<(String, SessionState)> {
    let SessionRow {
        branch_name,
        state,
        target_branch,
        base_head_id,
        pause_reason,
        merged_anchor_id,
    } = session;
    let parsed = match (
        state.as_str(),
        target_branch,
        base_head_id,
        pause_reason.as_deref(),
        merged_anchor_id,
    ) {
        ("active", None, None, None, None) => SessionState::Active,
        ("attached", Some(target_branch), Some(base_head_id), None, None) => {
            SessionState::Attached {
                target_branch,
                base_head_id,
            }
        }
        ("paused", Some(target_branch), None, Some("closed"), None) => SessionState::Paused {
            target_branch,
            reason: PauseReason::Closed,
        },
        ("paused", Some(target_branch), None, Some("merged"), Some(merged_anchor_id)) => {
            SessionState::Paused {
                target_branch,
                reason: PauseReason::Merged { merged_anchor_id },
            }
        }
        (state, target_branch, base_head_id, pause_reason, merged_anchor_id) => {
            return CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "invalid SQLite session row for branch {branch_name:?}: \
                     state={state:?}, target_branch={target_branch:?}, \
                     base_head_id={base_head_id:?}, pause_reason={pause_reason:?}, \
                     merged_anchor_id={merged_anchor_id:?}"
                ),
            }
            .fail();
        }
    };
    Ok((branch_name, parsed))
}

async fn persist_branch(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    head_id: &str,
) -> Result<()> {
    diesel::insert_into(branches::table)
        .values((branches::name.eq(branch), branches::head_id.eq(head_id)))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(())
}

async fn create_branch(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    from_ref: &str,
) -> Result<String> {
    connection
        .immediate_transaction::<String, SqliteTransactionError, _>(async |connection| {
            if maybe_load_branch_head(connection, path, branch)
                .await
                .map_err(SqliteTransactionError::Operation)?
                .is_some()
            {
                return Err(SqliteTransactionError::Operation(
                    BranchExistsSnafu {
                        name: branch.to_owned(),
                    }
                    .build(),
                ));
            }
            let head_id = resolve_ref_id(connection, path, from_ref)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            persist_branch(connection, path, branch, &head_id)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            persist_session_state(connection, path, branch, &SessionState::Active)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            Ok(head_id)
        })
        .await
        .map_err(|error| error.into_store_error(path))
}

async fn persist_session_state(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    state: &SessionState,
) -> Result<()> {
    let pause_reason = state.pause_reason();
    diesel::insert_into(sessions::table)
        .values((
            sessions::branch_name.eq(branch),
            sessions::state.eq(state.as_str()),
            sessions::target_branch.eq(state.target_branch()),
            sessions::base_head_id.eq(state.base_head_id()),
            sessions::pause_reason.eq(pause_reason.map(|reason| reason.as_str())),
            sessions::merged_anchor_id
                .eq(pause_reason.and_then(|reason| reason.merged_anchor_id())),
        ))
        .on_conflict(sessions::branch_name)
        .do_update()
        .set((
            sessions::state.eq(diesel::upsert::excluded(sessions::state)),
            sessions::target_branch.eq(diesel::upsert::excluded(sessions::target_branch)),
            sessions::base_head_id.eq(diesel::upsert::excluded(sessions::base_head_id)),
            sessions::pause_reason.eq(diesel::upsert::excluded(sessions::pause_reason)),
            sessions::merged_anchor_id.eq(diesel::upsert::excluded(sessions::merged_anchor_id)),
        ))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(())
}

async fn update_session_state(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    expected: Option<&SessionState>,
    next: SessionState,
) -> Result<SessionState> {
    connection
        .immediate_transaction::<SessionState, SqliteTransactionError, _>(async |connection| {
            update_session_state_in_transaction(connection, path, branch, expected, next).await
        })
        .await
        .map_err(|error| error.into_store_error(path))
}

async fn update_session_state_in_transaction(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    expected: Option<&SessionState>,
    next: SessionState,
) -> std::result::Result<SessionState, SqliteTransactionError> {
    let current = load_session_state(connection, path, branch)
        .await
        .map_err(SqliteTransactionError::Operation)?;
    if let Some(expected) = expected
        && current != *expected
    {
        return Err(SqliteTransactionError::Operation(
            SessionStateMovedSnafu {
                name: branch.to_owned(),
                expected: format!("{expected:?}"),
                actual: format!("{current:?}"),
            }
            .build(),
        ));
    }
    validate_session_state(connection, path, &next)
        .await
        .map_err(SqliteTransactionError::Operation)?;
    persist_session_state(connection, path, branch, &next)
        .await
        .map_err(SqliteTransactionError::Operation)?;
    Ok(next)
}

pub async fn load_session_chain(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
) -> Result<Vec<Node>> {
    let ancestry = load_ancestry_nodes(connection, path, branch).await?;
    let mut chain = Vec::new();
    for node in ancestry {
        let is_session_anchor = matches!(
            node.kind,
            Kind::Anchor(Anchor {
                payload: AnchorPayload::Session(_),
                ..
            })
        );
        chain.push(node);
        if is_session_anchor {
            return Ok(chain);
        }
    }
    MissingSessionAnchorSnafu {
        branch: branch.to_owned(),
    }
    .fail()
}

fn session_anchor_from_node(path: &Path, node: &Node) -> Result<crate::SessionAnchor> {
    match &node.kind {
        Kind::Anchor(anchor) => anchor.as_session().cloned().context(CorruptedStoreSnafu {
            path: path.to_owned(),
            message: "session chain should end with session anchor".to_owned(),
        }),
        _ => CorruptedStoreSnafu {
            path: path.to_owned(),
            message: "session chain should end with anchor".to_owned(),
        }
        .fail(),
    }
}

async fn update_branch_head_after_session_write(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    expected_old_head: &str,
    new_head: &str,
) -> Result<()> {
    let updated = update_branch_head(connection, path, branch, expected_old_head, new_head).await?;
    ensure!(
        updated == 1,
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("SQLite branch {branch:?} did not match expected head"),
        }
    );
    Ok(())
}

async fn validate_session_state(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    state: &SessionState,
) -> Result<()> {
    match state {
        SessionState::Active => Ok(()),
        SessionState::Attached {
            target_branch,
            base_head_id,
        } => validate_ref_on_branch(connection, path, target_branch, base_head_id).await,
        SessionState::Paused {
            target_branch,
            reason,
        } => match reason {
            PauseReason::Merged { merged_anchor_id } => {
                validate_anchor_on_branch(connection, path, target_branch, merged_anchor_id).await
            }
            PauseReason::Closed => {
                if target_branch.is_empty() {
                    return Ok(());
                }
                load_branch_head(connection, path, target_branch).await?;
                Ok(())
            }
        },
    }
}

async fn validate_ref_on_branch(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    node_id: &str,
) -> Result<()> {
    let head_id = load_branch_head(connection, path, branch).await?;
    load_node_by_exact_id(connection, path, node_id).await?;
    let visible = node_reachable_from_head(connection, path, &head_id, node_id).await?;
    ensure!(
        visible,
        RefsNotConnectedSnafu {
            base_ref: node_id.to_owned(),
            head_ref: branch.to_owned(),
        }
    );
    Ok(())
}

async fn validate_anchor_on_branch(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    node_id: &str,
) -> Result<()> {
    let node = load_node_by_exact_id(connection, path, node_id).await?;
    ensure!(
        matches!(node.kind, Kind::Anchor(_)),
        InvalidAnchorSnafu {
            id: node_id.to_owned(),
        }
    );
    validate_ref_on_branch(connection, path, branch, node_id).await
}

pub async fn load_branch_head(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
) -> Result<String> {
    maybe_load_branch_head(connection, path, branch)
        .await?
        .context(BranchNotFoundSnafu {
            name: branch.to_owned(),
        })
}

pub async fn maybe_load_branch_head(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
) -> Result<Option<String>> {
    branches::table
        .filter(branches::name.eq(branch))
        .select(branches::head_id)
        .get_result::<String>(connection)
        .await
        .optional()
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
}

async fn node_reachable_from_head(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    head_id: &str,
    node_id: &str,
) -> Result<bool> {
    let mut current_id = head_id.to_owned();
    let mut seen = HashSet::new();
    loop {
        if current_id == node_id {
            return Ok(true);
        }
        ensure!(
            seen.insert(current_id.clone()),
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: "SQLite nodes contain cyclic parents".to_owned(),
            }
        );
        let parent_id = nodes::table
            .filter(nodes::id.eq(&current_id))
            .select(nodes::parent_id)
            .get_result::<String>(connection)
            .await
            .optional()
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?
            .context(ParentNotFoundSnafu {
                id: current_id.clone(),
            })?;
        if parent_id.is_empty() {
            return Ok(false);
        }
        current_id = parent_id;
    }
}

async fn update_branch_head(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    expected_old_head: &str,
    new_head: &str,
) -> Result<usize> {
    diesel::update(
        branches::table
            .filter(branches::name.eq(branch))
            .filter(branches::head_id.eq(expected_old_head)),
    )
    .set(branches::head_id.eq(new_head))
    .execute(connection)
    .await
    .context(QuerySqliteStoreSnafu {
        path: path.to_owned(),
    })
}

async fn update_branch_head_checked(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    expected_old_head: &str,
    new_head: &str,
) -> Result<()> {
    connection
        .immediate_transaction::<(), SqliteTransactionError, _>(async |connection| {
            update_branch_head_checked_in_transaction(
                connection,
                path,
                branch,
                expected_old_head,
                new_head,
            )
            .await
        })
        .await
        .map_err(|error| error.into_store_error(path))
}

async fn update_branch_head_checked_in_transaction(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    expected_old_head: &str,
    new_head: &str,
) -> std::result::Result<(), SqliteTransactionError> {
    let actual = load_branch_head(connection, path, branch)
        .await
        .map_err(SqliteTransactionError::Operation)?;
    if actual != expected_old_head {
        return Err(SqliteTransactionError::Operation(
            BranchHeadMovedSnafu {
                name: branch.to_owned(),
                expected: expected_old_head.to_owned(),
                actual,
            }
            .build(),
        ));
    }
    load_node_by_exact_id(connection, path, new_head)
        .await
        .map_err(SqliteTransactionError::Operation)?;
    let updated = update_branch_head(connection, path, branch, expected_old_head, new_head)
        .await
        .map_err(SqliteTransactionError::Operation)?;
    if updated != 1 {
        return Err(SqliteTransactionError::Operation(
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!("SQLite branch {branch:?} did not match expected head"),
            }
            .build(),
        ));
    }
    Ok(())
}

async fn append_nodes_and_set_branch_head_in_transaction(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    expected_old_head: &str,
    parent: &str,
    new_head: Option<&str>,
    nodes: Vec<NewNodeContent>,
) -> std::result::Result<String, SqliteTransactionError> {
    let actual = load_branch_head(connection, path, branch)
        .await
        .map_err(SqliteTransactionError::Operation)?;
    if actual != expected_old_head {
        return Err(SqliteTransactionError::Operation(
            BranchHeadMovedSnafu {
                name: branch.to_owned(),
                expected: expected_old_head.to_owned(),
                actual,
            }
            .build(),
        ));
    }
    load_node_by_exact_id(connection, path, parent)
        .await
        .map_err(SqliteTransactionError::Operation)?;

    let mut head = parent.to_owned();
    for content in nodes {
        let node = Node::new(
            head,
            content.role,
            content.metadata,
            content.kind,
            jiff::Timestamp::now(),
        );
        validate_new_node(connection, path, &node)
            .await
            .map_err(SqliteTransactionError::Operation)?;
        persist_node_without_transaction(connection, path, &node)
            .await
            .map_err(SqliteTransactionError::Operation)?;
        head = node.id;
    }

    update_branch_head_checked_in_transaction(
        connection,
        path,
        branch,
        expected_old_head,
        new_head.unwrap_or(&head),
    )
    .await?;
    Ok(head)
}

async fn append_nodes_and_set_branch_head_with_session_state_in_transaction(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    update: BranchAppendSessionState,
) -> std::result::Result<String, SqliteTransactionError> {
    let head = append_nodes_and_set_branch_head_in_transaction(
        connection,
        path,
        &update.branch,
        &update.expected_old_head,
        &update.parent,
        update.new_head.as_deref(),
        update.nodes,
    )
    .await?;
    let next_session = update.next_session.into_session_state(&head);
    update_session_state_in_transaction(
        connection,
        path,
        &update.session_branch,
        update.expected_session.as_ref(),
        next_session,
    )
    .await?;
    Ok(head)
}

async fn delete_branch_record(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
) -> Result<()> {
    diesel::delete(branches::table.filter(branches::name.eq(branch)))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(())
}

async fn delete_branch_checked(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
) -> Result<()> {
    load_branch_head(connection, path, branch).await?;
    delete_branch_record(connection, path, branch).await
}

#[cfg(test)]
pub async fn persist_session_nodes_and_branch_head(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    expected_old_head: &str,
    new_head: &str,
    nodes: &[Node],
) -> Result<()> {
    connection
        .immediate_transaction::<(), SqliteTransactionError, _>(async |connection| {
            persist_session_nodes_and_branch_head_in_transaction(
                connection,
                path,
                branch,
                expected_old_head,
                new_head,
                nodes,
            )
            .await
            .map_err(SqliteTransactionError::Operation)
        })
        .await
        .map_err(|error| error.into_store_error(path))
}

#[cfg(test)]
async fn persist_session_nodes_and_branch_head_in_transaction(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    branch: &str,
    expected_old_head: &str,
    new_head: &str,
    nodes: &[Node],
) -> Result<()> {
    for node in nodes {
        upsert_node_without_transaction(connection, path, node).await?;
    }
    let updated = update_branch_head(connection, path, branch, expected_old_head, new_head).await?;
    ensure!(
        updated == 1,
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("SQLite branch {branch:?} did not match expected head"),
        }
    );
    Ok(())
}

#[async_trait]
impl BranchStore for SqliteGraphStore {
    async fn fork(&self, _name: &str, _from_ref: &str) -> Result<String> {
        self.ensure_read_only()
    }

    async fn get_branch_head(&self, name: &str) -> Result<String> {
        self.branch_head(name).await?.context(BranchNotFoundSnafu {
            name: name.to_owned(),
        })
    }

    async fn delete_branch(&self, _name: &str) -> Result<()> {
        self.ensure_read_only()
    }

    async fn set_branch_head(
        &self,
        _name: &str,
        _expected_old_head: &str,
        _new_head: &str,
    ) -> Result<()> {
        self.ensure_read_only()
    }

    async fn append_nodes_and_set_branch_head(
        &self,
        _name: &str,
        _expected_old_head: &str,
        _parent: &str,
        _nodes: Vec<NewNodeContent>,
    ) -> Result<String> {
        self.ensure_read_only()
    }

    async fn append_nodes_and_set_branch_head_to(
        &self,
        _name: &str,
        _expected_old_head: &str,
        _parent: &str,
        _new_head: &str,
        _nodes: Vec<NewNodeContent>,
    ) -> Result<String> {
        self.ensure_read_only()
    }

    async fn append_nodes_and_set_branch_head_with_session_state(
        &self,
        _update: BranchAppendSessionState,
    ) -> Result<String> {
        self.ensure_read_only()
    }
}

#[async_trait]
impl SessionStore for SqliteGraphStore {
    async fn list_session_states(&self) -> Result<std::collections::HashMap<String, SessionState>> {
        let path = self.database_path.clone();
        self.with_connection(move |connection| {
            Box::pin(async move { load_session_states(connection, &path).await })
        })
        .await
    }

    async fn get_session_state(&self, name: &str) -> Result<SessionState> {
        let name = name.to_owned();
        let path = self.database_path.clone();
        self.with_connection(move |connection| {
            Box::pin(async move { load_session_state(connection, &path, &name).await })
        })
        .await
    }

    async fn set_session_state(
        &self,
        _name: &str,
        _expected: Option<&SessionState>,
        _next: SessionState,
    ) -> Result<SessionState> {
        self.ensure_read_only()
    }

    async fn rebase_session(&self, _name: &str, _patch: &SessionAnchorPatch) -> Result<String> {
        self.ensure_read_only()
    }

    async fn handoff_session(
        &self,
        _name: &str,
        _patch: &SessionAnchorPatch,
        _prompt: &str,
    ) -> Result<String> {
        self.ensure_read_only()
    }
}

#[async_trait]
impl BranchStore for SqliteStore {
    async fn fork(&self, name: &str, from_ref: &str) -> Result<String> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        create_branch(&mut connection, &self.database_path, name, from_ref).await
    }

    async fn get_branch_head(&self, name: &str) -> Result<String> {
        let mut connection = self.connect().await?;
        load_branch_head(&mut connection, &self.database_path, name).await
    }

    async fn delete_branch(&self, name: &str) -> Result<()> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        delete_branch_checked(&mut connection, &self.database_path, name).await
    }

    async fn set_branch_head(
        &self,
        name: &str,
        expected_old_head: &str,
        new_head: &str,
    ) -> Result<()> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        update_branch_head_checked(
            &mut connection,
            &self.database_path,
            name,
            expected_old_head,
            new_head,
        )
        .await
    }

    async fn append_nodes_and_set_branch_head(
        &self,
        name: &str,
        expected_old_head: &str,
        parent: &str,
        nodes: Vec<NewNodeContent>,
    ) -> Result<String> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        connection
            .immediate_transaction::<String, SqliteTransactionError, _>(async |connection| {
                append_nodes_and_set_branch_head_in_transaction(
                    connection,
                    &self.database_path,
                    name,
                    expected_old_head,
                    parent,
                    None,
                    nodes,
                )
                .await
            })
            .await
            .map_err(|error| error.into_store_error(&self.database_path))
    }

    async fn append_nodes_and_set_branch_head_to(
        &self,
        name: &str,
        expected_old_head: &str,
        parent: &str,
        new_head: &str,
        nodes: Vec<NewNodeContent>,
    ) -> Result<String> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        connection
            .immediate_transaction::<String, SqliteTransactionError, _>(async |connection| {
                append_nodes_and_set_branch_head_in_transaction(
                    connection,
                    &self.database_path,
                    name,
                    expected_old_head,
                    parent,
                    Some(new_head),
                    nodes,
                )
                .await
            })
            .await
            .map_err(|error| error.into_store_error(&self.database_path))
    }

    async fn append_nodes_and_set_branch_head_with_session_state(
        &self,
        update: BranchAppendSessionState,
    ) -> Result<String> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        connection
            .immediate_transaction::<String, SqliteTransactionError, _>(async |connection| {
                append_nodes_and_set_branch_head_with_session_state_in_transaction(
                    connection,
                    &self.database_path,
                    update,
                )
                .await
            })
            .await
            .map_err(|error| error.into_store_error(&self.database_path))
    }
}

#[async_trait]
impl SessionStore for SqliteStore {
    async fn list_session_states(&self) -> Result<std::collections::HashMap<String, SessionState>> {
        let mut connection = self.connect().await?;
        load_session_states(&mut connection, &self.database_path).await
    }

    async fn get_session_state(&self, name: &str) -> Result<SessionState> {
        let mut connection = self.connect().await?;
        load_session_state(&mut connection, &self.database_path, name).await
    }

    async fn set_session_state(
        &self,
        name: &str,
        expected: Option<&SessionState>,
        next: SessionState,
    ) -> Result<SessionState> {
        let expected = expected.cloned();
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        update_session_state(
            &mut connection,
            &self.database_path,
            name,
            expected.as_ref(),
            next,
        )
        .await
    }

    async fn rebase_session(&self, name: &str, patch: &SessionAnchorPatch) -> Result<String> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        let (new_head, _) = connection
            .immediate_transaction::<(String, Vec<Node>), SqliteTransactionError, _>(
                async |connection| {
                    let expected_old_head = load_branch_head(connection, &self.database_path, name)
                        .await
                        .map_err(SqliteTransactionError::Operation)?;
                    let mut chain = load_session_chain(connection, &self.database_path, name)
                        .await
                        .map_err(SqliteTransactionError::Operation)?;
                    chain.reverse();
                    let session_node = chain
                        .as_slice()
                        .first()
                        .expect("session chain should not be empty");
                    let session_anchor =
                        session_anchor_from_node(&self.database_path, session_node)
                            .map_err(SqliteTransactionError::Operation)?;
                    let rebased_session_anchor = session_anchor.apply_patch(patch);

                    let mut previous_new_id = None;
                    let mut new_head = String::new();
                    let mut nodes = Vec::with_capacity(chain.len());
                    for (index, node) in chain.into_iter().enumerate() {
                        let parent = previous_new_id
                            .clone()
                            .unwrap_or_else(|| node.parent.clone());
                        let kind = if index == 0 {
                            let Kind::Anchor(anchor) = &node.kind else {
                                unreachable!("session chain should start with anchor");
                            };
                            Kind::Anchor(Anchor::session(
                                anchor.merge_parents().to_vec(),
                                rebased_session_anchor.clone(),
                            ))
                        } else {
                            node.kind.clone()
                        };
                        let new_node =
                            Node::new(parent, node.role, node.metadata, kind, node.created_at);
                        upsert_node_without_transaction(connection, &self.database_path, &new_node)
                            .await
                            .map_err(SqliteTransactionError::Operation)?;
                        previous_new_id = Some(new_node.id.clone());
                        new_head = new_node.id.clone();
                        nodes.push(new_node);
                    }

                    update_branch_head_after_session_write(
                        connection,
                        &self.database_path,
                        name,
                        &expected_old_head,
                        &new_head,
                    )
                    .await
                    .map_err(SqliteTransactionError::Operation)?;
                    Ok((new_head, nodes))
                },
            )
            .await
            .map_err(|error| error.into_store_error(&self.database_path))?;
        Ok(new_head)
    }

    async fn handoff_session(
        &self,
        name: &str,
        patch: &SessionAnchorPatch,
        prompt: &str,
    ) -> Result<String> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        let prompt = prompt.trim().to_owned();
        ensure!(!prompt.is_empty(), InvalidSessionHandoffPromptSnafu);
        let (new_head, _) = connection
            .immediate_transaction::<(String, Node), SqliteTransactionError, _>(
                async |connection| {
                    let expected_old_head = load_branch_head(connection, &self.database_path, name)
                        .await
                        .map_err(SqliteTransactionError::Operation)?;
                    let chain = load_session_chain(connection, &self.database_path, name)
                        .await
                        .map_err(SqliteTransactionError::Operation)?;
                    let session_node = chain.last().expect("session chain should not be empty");
                    let session_anchor =
                        session_anchor_from_node(&self.database_path, session_node)
                            .map_err(SqliteTransactionError::Operation)?;
                    let mut handoff_session_anchor = session_anchor.apply_patch(patch);
                    handoff_session_anchor.prompt = prompt;

                    let node = Node::new(
                        expected_old_head.clone(),
                        Role::System,
                        None,
                        Kind::Anchor(Anchor::session(vec![], handoff_session_anchor)),
                        jiff::Timestamp::now(),
                    );
                    validate_new_node(connection, &self.database_path, &node)
                        .await
                        .map_err(SqliteTransactionError::Operation)?;
                    persist_node_without_transaction(connection, &self.database_path, &node)
                        .await
                        .map_err(SqliteTransactionError::Operation)?;
                    update_branch_head_after_session_write(
                        connection,
                        &self.database_path,
                        name,
                        &expected_old_head,
                        &node.id,
                    )
                    .await
                    .map_err(SqliteTransactionError::Operation)?;
                    Ok((node.id.clone(), node))
                },
            )
            .await
            .map_err(|error| error.into_store_error(&self.database_path))?;
        Ok(new_head)
    }
}
