use std::collections::BTreeSet;
use std::path::Path;

use diesel::prelude::*;
use diesel::result::OptionalExtension;
use diesel::sql_types::BigInt;
use diesel_async::RunQueryDsl;
use snafu::prelude::*;

use super::branch::load_graph_branch_records;
use super::{
    AsyncSqliteConnection, GraphBranchRecord, GraphMutationBranchChangeKind,
    GraphMutationBranchChangeRecord, GraphMutationEvent,
};
use crate::error::{CorruptedStoreSnafu, ParseSqliteStoreValueSnafu, QuerySqliteStoreSnafu};
use crate::schema::{
    graph_branch_history, graph_branch_names, graph_mutation_event_branch_change_prune_staging,
    graph_mutation_event_branch_changes, graph_mutation_event_dirty_parent_prune_staging,
    graph_mutation_event_dirty_parents, graph_mutation_events, graph_relation_state,
};
use crate::store::inserted_parent_ids;
use crate::{Node, SessionState, StoreResult as Result};

const BRANCH_CHANGE_UPSERTED: &str = "upserted";
const BRANCH_CHANGE_REMOVED: &str = "removed";
const GRAPH_MUTATION_RETENTION_WINDOW: i64 = 4_096;
const GRAPH_MUTATION_PRUNE_BATCH_SIZE: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphMutationBranch {
    Upserted(String),
    Removed(String),
}

#[derive(Queryable)]
struct BranchChangeRow {
    name: String,
    kind: String,
    head_id: Option<String>,
    state_json: Option<String>,
}

pub async fn begin_graph_mutation(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<i64> {
    let updated = diesel::update(
        graph_relation_state::table
            .filter(graph_relation_state::singleton.eq(1))
            .filter(graph_relation_state::current_revision.lt(i64::MAX)),
    )
    .set(graph_relation_state::current_revision.eq(graph_relation_state::current_revision + 1_i64))
    .execute(connection)
    .await
    .context(QuerySqliteStoreSnafu {
        path: path.to_owned(),
    })?;
    ensure!(
        updated == 1,
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: "missing or exhausted SQLite graph mutation revision state".to_owned(),
        }
    );
    graph_relation_state::table
        .filter(graph_relation_state::singleton.eq(1))
        .select(graph_relation_state::current_revision)
        .get_result(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
}

pub fn dirty_parent_ids(nodes: &[Node]) -> BTreeSet<String> {
    inserted_parent_ids(
        nodes.iter().map(|node| node.parent.as_str()),
        nodes.iter().map(|node| &node.kind),
    )
}

pub async fn finish_graph_mutation(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    revision: i64,
    dirty_parent_ids: &BTreeSet<String>,
    branch_changes: &[GraphMutationBranch],
) -> Result<()> {
    diesel::insert_into(graph_mutation_events::table)
        .values(graph_mutation_events::revision.eq(revision))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;

    for parent_id in dirty_parent_ids {
        diesel::insert_into(graph_mutation_event_dirty_parents::table)
            .values((
                graph_mutation_event_dirty_parents::revision.eq(revision),
                graph_mutation_event_dirty_parents::parent_id.eq(parent_id),
            ))
            .execute(connection)
            .await
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?;
    }

    let mut seen_names = BTreeSet::new();
    for change in branch_changes {
        let name = match change {
            GraphMutationBranch::Upserted(name) | GraphMutationBranch::Removed(name) => name,
        };
        ensure!(
            seen_names.insert(name.clone()),
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "graph mutation revision {revision} contains duplicate branch {name:?}"
                ),
            }
        );
        match change {
            GraphMutationBranch::Upserted(name) => {
                let records = load_graph_branch_records(
                    connection,
                    path,
                    Some(std::slice::from_ref(name)),
                    None,
                    None,
                    Some(1),
                )
                .await?;
                let record = records.into_iter().next().context(CorruptedStoreSnafu {
                    path: path.to_owned(),
                    message: format!(
                        "graph mutation revision {revision} is missing updated branch {name:?}"
                    ),
                })?;
                let state_json = serialize_session_state(path, &record.state)?;
                diesel::insert_into(graph_mutation_event_branch_changes::table)
                    .values((
                        graph_mutation_event_branch_changes::revision.eq(revision),
                        graph_mutation_event_branch_changes::name.eq(name),
                        graph_mutation_event_branch_changes::kind.eq(BRANCH_CHANGE_UPSERTED),
                        graph_mutation_event_branch_changes::head_id.eq(Some(&record.head_id)),
                        graph_mutation_event_branch_changes::state_json.eq(Some(&state_json)),
                    ))
                    .execute(connection)
                    .await
                    .context(QuerySqliteStoreSnafu {
                        path: path.to_owned(),
                    })?;
                diesel::insert_into(graph_branch_history::table)
                    .values((
                        graph_branch_history::name.eq(name),
                        graph_branch_history::revision.eq(revision),
                        graph_branch_history::head_id.eq(Some(&record.head_id)),
                        graph_branch_history::state_json.eq(Some(&state_json)),
                        graph_branch_history::removed.eq(false),
                    ))
                    .execute(connection)
                    .await
                    .context(QuerySqliteStoreSnafu {
                        path: path.to_owned(),
                    })?;
            }
            GraphMutationBranch::Removed(name) => {
                diesel::insert_into(graph_mutation_event_branch_changes::table)
                    .values((
                        graph_mutation_event_branch_changes::revision.eq(revision),
                        graph_mutation_event_branch_changes::name.eq(name),
                        graph_mutation_event_branch_changes::kind.eq(BRANCH_CHANGE_REMOVED),
                        graph_mutation_event_branch_changes::head_id.eq(None::<&str>),
                        graph_mutation_event_branch_changes::state_json.eq(None::<&str>),
                    ))
                    .execute(connection)
                    .await
                    .context(QuerySqliteStoreSnafu {
                        path: path.to_owned(),
                    })?;
                diesel::insert_into(graph_branch_history::table)
                    .values((
                        graph_branch_history::name.eq(name),
                        graph_branch_history::revision.eq(revision),
                        graph_branch_history::head_id.eq(None::<&str>),
                        graph_branch_history::state_json.eq(None::<&str>),
                        graph_branch_history::removed.eq(true),
                    ))
                    .execute(connection)
                    .await
                    .context(QuerySqliteStoreSnafu {
                        path: path.to_owned(),
                    })?;
            }
        }
    }
    prune_graph_mutation_journal(connection, path, revision, GRAPH_MUTATION_RETENTION_WINDOW).await
}

async fn prune_graph_mutation_journal(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    current_revision: i64,
    retention_window: i64,
) -> Result<()> {
    ensure!(
        retention_window > 0,
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("invalid graph mutation retention window {retention_window}"),
        }
    );
    let (baseline_revision, persisted_current_revision) = graph_relation_state::table
        .filter(graph_relation_state::singleton.eq(1))
        .select((
            graph_relation_state::baseline_revision,
            graph_relation_state::current_revision,
        ))
        .first::<(i64, i64)>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    ensure!(
        persisted_current_revision == current_revision,
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "graph mutation journal finished revision {current_revision}, but current revision is {persisted_current_revision}"
            ),
        }
    );

    let target_baseline = current_revision
        .saturating_sub(retention_window)
        .max(baseline_revision);
    // Readers merge staged rows with the live journal until the event and baseline move
    // atomically, so a serviceable revision never exposes a partially pruned page.
    stage_graph_mutation_branch_changes(connection, path, baseline_revision, target_baseline)
        .await?;
    stage_graph_mutation_dirty_parents(connection, path, baseline_revision, target_baseline)
        .await?;

    let event_revisions = graph_mutation_events::table
        .filter(graph_mutation_events::revision.gt(baseline_revision))
        .filter(graph_mutation_events::revision.le(target_baseline))
        .select(graph_mutation_events::revision)
        .order(graph_mutation_events::revision)
        .limit(GRAPH_MUTATION_PRUNE_BATCH_SIZE as i64)
        .load::<i64>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    if target_baseline > baseline_revision {
        ensure!(
            event_revisions.as_slice().first() == Some(&(baseline_revision + 1)),
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "graph mutation journal is missing revision {} before retention target {target_baseline}",
                    baseline_revision + 1
                ),
            }
        );
    }
    for revisions in event_revisions.windows(2) {
        ensure!(
            revisions[1] == revisions[0] + 1,
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "graph mutation journal is missing a revision between {} and {}",
                    revisions[0], revisions[1]
                ),
            }
        );
    }

    let mut deletable_event_revisions = Vec::with_capacity(event_revisions.len());
    for revision in event_revisions {
        let has_branch_changes = graph_mutation_event_branch_changes::table
            .filter(graph_mutation_event_branch_changes::revision.eq(revision))
            .select(graph_mutation_event_branch_changes::revision)
            .first::<i64>(connection)
            .await
            .optional()
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?
            .is_some();
        let has_dirty_parents = graph_mutation_event_dirty_parents::table
            .filter(graph_mutation_event_dirty_parents::revision.eq(revision))
            .select(graph_mutation_event_dirty_parents::revision)
            .first::<i64>(connection)
            .await
            .optional()
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?
            .is_some();
        if has_branch_changes || has_dirty_parents {
            break;
        }
        deletable_event_revisions.push(revision);
    }

    let next_baseline = if let Some(next_baseline) = deletable_event_revisions.last().copied() {
        let deleted = diesel::delete(
            graph_mutation_events::table
                .filter(graph_mutation_events::revision.eq_any(&deletable_event_revisions)),
        )
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
        ensure!(
            deleted == deletable_event_revisions.len(),
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "deleted {deleted} graph mutation events while advancing through {next_baseline}, expected {}",
                    deletable_event_revisions.len()
                ),
            }
        );
        let updated = diesel::update(
            graph_relation_state::table
                .filter(graph_relation_state::singleton.eq(1))
                .filter(graph_relation_state::baseline_revision.eq(baseline_revision)),
        )
        .set(graph_relation_state::baseline_revision.eq(next_baseline))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
        ensure!(
            updated == 1,
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "graph mutation baseline changed while advancing from {baseline_revision} to {next_baseline}"
                ),
            }
        );
        next_baseline
    } else {
        baseline_revision
    };

    delete_staged_graph_mutation_rows(connection, path, next_baseline).await?;
    delete_stale_graph_branch_history(connection, path, next_baseline).await
}

async fn stage_graph_mutation_branch_changes(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    baseline_revision: i64,
    target_baseline: i64,
) -> Result<()> {
    diesel::sql_query(
        "INSERT INTO graph_mutation_event_branch_change_prune_staging (\
             revision, name, kind, head_id, state_json\
         ) \
         SELECT revision, name, kind, head_id, state_json \
         FROM graph_mutation_event_branch_changes \
         WHERE revision > ? AND revision <= ? \
         ORDER BY revision, name \
         LIMIT 128",
    )
    .bind::<BigInt, _>(baseline_revision)
    .bind::<BigInt, _>(target_baseline)
    .execute(connection)
    .await
    .context(QuerySqliteStoreSnafu {
        path: path.to_owned(),
    })?;
    diesel::sql_query(
        "DELETE FROM graph_mutation_event_branch_changes \
         WHERE rowid IN (\
             SELECT source.rowid \
             FROM graph_mutation_event_branch_changes AS source \
             INNER JOIN graph_mutation_event_branch_change_prune_staging AS staged \
                 ON staged.revision = source.revision AND staged.name = source.name \
             WHERE source.revision > ? AND source.revision <= ? \
             ORDER BY source.revision, source.name \
             LIMIT 128\
         )",
    )
    .bind::<BigInt, _>(baseline_revision)
    .bind::<BigInt, _>(target_baseline)
    .execute(connection)
    .await
    .context(QuerySqliteStoreSnafu {
        path: path.to_owned(),
    })?;
    Ok(())
}

async fn stage_graph_mutation_dirty_parents(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    baseline_revision: i64,
    target_baseline: i64,
) -> Result<()> {
    diesel::sql_query(
        "INSERT INTO graph_mutation_event_dirty_parent_prune_staging (revision, parent_id) \
         SELECT revision, parent_id \
         FROM graph_mutation_event_dirty_parents \
         WHERE revision > ? AND revision <= ? \
         ORDER BY revision, parent_id \
         LIMIT 128",
    )
    .bind::<BigInt, _>(baseline_revision)
    .bind::<BigInt, _>(target_baseline)
    .execute(connection)
    .await
    .context(QuerySqliteStoreSnafu {
        path: path.to_owned(),
    })?;
    diesel::sql_query(
        "DELETE FROM graph_mutation_event_dirty_parents \
         WHERE rowid IN (\
             SELECT source.rowid \
             FROM graph_mutation_event_dirty_parents AS source \
             INNER JOIN graph_mutation_event_dirty_parent_prune_staging AS staged \
                 ON staged.revision = source.revision \
                    AND staged.parent_id = source.parent_id \
             WHERE source.revision > ? AND source.revision <= ? \
             ORDER BY source.revision, source.parent_id \
             LIMIT 128\
         )",
    )
    .bind::<BigInt, _>(baseline_revision)
    .bind::<BigInt, _>(target_baseline)
    .execute(connection)
    .await
    .context(QuerySqliteStoreSnafu {
        path: path.to_owned(),
    })?;
    Ok(())
}

async fn delete_staged_graph_mutation_rows(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    baseline_revision: i64,
) -> Result<()> {
    diesel::sql_query(
        "DELETE FROM graph_mutation_event_branch_change_prune_staging \
         WHERE rowid IN (\
             SELECT rowid \
             FROM graph_mutation_event_branch_change_prune_staging \
             WHERE revision <= ? \
             ORDER BY revision, name \
             LIMIT 128\
         )",
    )
    .bind::<BigInt, _>(baseline_revision)
    .execute(connection)
    .await
    .context(QuerySqliteStoreSnafu {
        path: path.to_owned(),
    })?;
    diesel::sql_query(
        "DELETE FROM graph_mutation_event_dirty_parent_prune_staging \
         WHERE rowid IN (\
             SELECT rowid \
             FROM graph_mutation_event_dirty_parent_prune_staging \
             WHERE revision <= ? \
             ORDER BY revision, parent_id \
             LIMIT 128\
         )",
    )
    .bind::<BigInt, _>(baseline_revision)
    .execute(connection)
    .await
    .context(QuerySqliteStoreSnafu {
        path: path.to_owned(),
    })?;
    Ok(())
}

async fn delete_stale_graph_branch_history(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    baseline_revision: i64,
) -> Result<()> {
    diesel::sql_query(
        "DELETE FROM graph_branch_history \
         WHERE rowid IN (\
             SELECT stale.rowid \
             FROM graph_branch_history AS stale \
             WHERE stale.revision <= ? \
               AND EXISTS (\
                   SELECT 1 \
                   FROM graph_branch_history AS newer \
                   WHERE newer.name = stale.name \
                     AND newer.revision <= ? \
                     AND newer.revision > stale.revision\
               ) \
             ORDER BY stale.revision, stale.name \
             LIMIT 128\
         )",
    )
    .bind::<BigInt, _>(baseline_revision)
    .bind::<BigInt, _>(baseline_revision)
    .execute(connection)
    .await
    .context(QuerySqliteStoreSnafu {
        path: path.to_owned(),
    })?;
    Ok(())
}

pub async fn load_graph_mutation_events(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    after_revision: i64,
    limit: usize,
) -> Result<Vec<GraphMutationEvent>> {
    graph_mutation_events::table
        .filter(graph_mutation_events::revision.gt(after_revision))
        .select(graph_mutation_events::revision)
        .order(graph_mutation_events::revision)
        .limit(limit as i64)
        .load::<i64>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
        .map(|revisions| {
            revisions
                .into_iter()
                .map(|revision| GraphMutationEvent { revision })
                .collect()
        })
}

pub async fn load_graph_mutation_branch_changes(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    revision: i64,
    after_name: Option<&str>,
    limit: usize,
) -> Result<Vec<GraphMutationBranchChangeRecord>> {
    let mut live_query = graph_mutation_event_branch_changes::table
        .filter(graph_mutation_event_branch_changes::revision.eq(revision))
        .select((
            graph_mutation_event_branch_changes::name,
            graph_mutation_event_branch_changes::kind,
            graph_mutation_event_branch_changes::head_id,
            graph_mutation_event_branch_changes::state_json,
        ))
        .into_boxed();
    if let Some(after_name) = after_name {
        live_query = live_query.filter(graph_mutation_event_branch_changes::name.gt(after_name));
    }
    let mut rows = live_query
        .order(graph_mutation_event_branch_changes::name)
        .limit(limit as i64)
        .load::<BranchChangeRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    let mut staging_query = graph_mutation_event_branch_change_prune_staging::table
        .filter(graph_mutation_event_branch_change_prune_staging::revision.eq(revision))
        .select((
            graph_mutation_event_branch_change_prune_staging::name,
            graph_mutation_event_branch_change_prune_staging::kind,
            graph_mutation_event_branch_change_prune_staging::head_id,
            graph_mutation_event_branch_change_prune_staging::state_json,
        ))
        .into_boxed();
    if let Some(after_name) = after_name {
        staging_query = staging_query
            .filter(graph_mutation_event_branch_change_prune_staging::name.gt(after_name));
    }
    rows.extend(
        staging_query
            .order(graph_mutation_event_branch_change_prune_staging::name)
            .limit(limit as i64)
            .load::<BranchChangeRow>(connection)
            .await
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?,
    );
    rows.sort_unstable_by(|left, right| left.name.cmp(&right.name));
    for pair in rows.windows(2) {
        ensure!(
            pair[0].name != pair[1].name,
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "graph mutation revision {revision} has duplicate branch change {:?}",
                    pair[0].name
                ),
            }
        );
    }
    rows.truncate(limit);
    rows.into_iter()
        .map(|row| branch_change_row_into_record(path, row))
        .collect()
}

pub async fn load_graph_mutation_dirty_parents(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    revision: i64,
    after_parent_id: Option<&str>,
    limit: usize,
) -> Result<Vec<String>> {
    let mut live_query = graph_mutation_event_dirty_parents::table
        .filter(graph_mutation_event_dirty_parents::revision.eq(revision))
        .select(graph_mutation_event_dirty_parents::parent_id)
        .into_boxed();
    if let Some(after_parent_id) = after_parent_id {
        live_query =
            live_query.filter(graph_mutation_event_dirty_parents::parent_id.gt(after_parent_id));
    }
    let mut parent_ids = live_query
        .order(graph_mutation_event_dirty_parents::parent_id)
        .limit(limit as i64)
        .load::<String>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    let mut staging_query = graph_mutation_event_dirty_parent_prune_staging::table
        .filter(graph_mutation_event_dirty_parent_prune_staging::revision.eq(revision))
        .select(graph_mutation_event_dirty_parent_prune_staging::parent_id)
        .into_boxed();
    if let Some(after_parent_id) = after_parent_id {
        staging_query = staging_query
            .filter(graph_mutation_event_dirty_parent_prune_staging::parent_id.gt(after_parent_id));
    }
    parent_ids.extend(
        staging_query
            .order(graph_mutation_event_dirty_parent_prune_staging::parent_id)
            .limit(limit as i64)
            .load::<String>(connection)
            .await
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?,
    );
    parent_ids.sort_unstable();
    for pair in parent_ids.windows(2) {
        ensure!(
            pair[0] != pair[1],
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "graph mutation revision {revision} has duplicate dirty parent {:?}",
                    pair[0]
                ),
            }
        );
    }
    parent_ids.truncate(limit);
    Ok(parent_ids)
}

pub async fn load_graph_branches_at_revision_by_names(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    revision: i64,
    names: &[String],
) -> Result<Vec<GraphBranchRecord>> {
    let names = names.iter().collect::<BTreeSet<_>>();
    let mut records = Vec::with_capacity(names.len());
    for name in names {
        if let Some(row) = graph_branch_history::table
            .filter(graph_branch_history::name.eq(name))
            .filter(graph_branch_history::revision.le(revision))
            .select((
                graph_branch_history::head_id,
                graph_branch_history::state_json,
                graph_branch_history::removed,
            ))
            .order(graph_branch_history::revision.desc())
            .first::<(Option<String>, Option<String>, bool)>(connection)
            .await
            .optional()
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?
            && !row.2
        {
            records.push(branch_history_values_into_record(
                path,
                name.clone(),
                row.0,
                row.1,
            )?);
        }
    }
    records.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(records)
}

pub async fn load_graph_branch_names_at_revision_page(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    revision: i64,
    after_name: Option<&str>,
    through_name: Option<&str>,
    limit: usize,
) -> Result<Vec<String>> {
    let after = if let Some(after_name) = after_name {
        Some(
            graph_branch_names::table
                .find(after_name)
                .select((graph_branch_names::first_revision, graph_branch_names::name))
                .first::<(i64, String)>(connection)
                .await
                .context(QuerySqliteStoreSnafu {
                    path: path.to_owned(),
                })?,
        )
    } else {
        None
    };
    let through = if let Some(through_name) = through_name {
        Some(
            graph_branch_names::table
                .find(through_name)
                .select((graph_branch_names::first_revision, graph_branch_names::name))
                .first::<(i64, String)>(connection)
                .await
                .context(QuerySqliteStoreSnafu {
                    path: path.to_owned(),
                })?,
        )
    } else {
        None
    };
    let mut query = graph_branch_names::table
        .filter(graph_branch_names::first_revision.le(revision))
        .select(graph_branch_names::name)
        .into_boxed();
    if let Some((after_revision, after_name)) = after.as_ref() {
        query = query.filter(
            graph_branch_names::first_revision.gt(*after_revision).or(
                graph_branch_names::first_revision
                    .eq(*after_revision)
                    .and(graph_branch_names::name.gt(after_name)),
            ),
        );
    }
    if let Some((through_revision, through_name)) = through.as_ref() {
        query = query.filter(
            graph_branch_names::first_revision.lt(*through_revision).or(
                graph_branch_names::first_revision
                    .eq(*through_revision)
                    .and(graph_branch_names::name.le(through_name)),
            ),
        );
    }
    query
        .order((graph_branch_names::first_revision, graph_branch_names::name))
        .limit(limit as i64)
        .load::<String>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
}

pub async fn load_graph_branch_name_high_watermark_at_revision(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    revision: i64,
) -> Result<Option<String>> {
    graph_branch_names::table
        .filter(graph_branch_names::first_revision.le(revision))
        .order((
            graph_branch_names::first_revision.desc(),
            graph_branch_names::name.desc(),
        ))
        .select(graph_branch_names::name)
        .first::<String>(connection)
        .await
        .optional()
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })
}

fn serialize_session_state(path: &Path, state: &SessionState) -> Result<String> {
    serde_json::to_string(state).context(ParseSqliteStoreValueSnafu {
        path: path.to_owned(),
        column: "graph branch state".to_owned(),
    })
}

fn parse_session_state(path: &Path, state_json: &str) -> Result<SessionState> {
    serde_json::from_str(state_json).context(ParseSqliteStoreValueSnafu {
        path: path.to_owned(),
        column: "graph branch state".to_owned(),
    })
}

fn branch_change_row_into_record(
    path: &Path,
    row: BranchChangeRow,
) -> Result<GraphMutationBranchChangeRecord> {
    let (kind, state) = match (
        row.kind.as_str(),
        row.head_id.as_ref(),
        row.state_json.as_deref(),
    ) {
        (BRANCH_CHANGE_UPSERTED, Some(_), Some(state_json)) => (
            GraphMutationBranchChangeKind::Upserted,
            Some(parse_session_state(path, state_json)?),
        ),
        (BRANCH_CHANGE_REMOVED, None, None) => (GraphMutationBranchChangeKind::Removed, None),
        _ => {
            return CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "invalid graph mutation branch change for {:?}: kind={:?}, head_id={:?}, state_json={:?}",
                    row.name, row.kind, row.head_id, row.state_json
                ),
            }
            .fail();
        }
    };
    Ok(GraphMutationBranchChangeRecord {
        name: row.name,
        kind,
        head_id: row.head_id,
        state,
    })
}

fn branch_history_values_into_record(
    path: &Path,
    name: String,
    head_id: Option<String>,
    state_json: Option<String>,
) -> Result<GraphBranchRecord> {
    let head_id = head_id.context(CorruptedStoreSnafu {
        path: path.to_owned(),
        message: format!("active graph branch history row {name:?} has no head"),
    })?;
    let state_json = state_json.context(CorruptedStoreSnafu {
        path: path.to_owned(),
        message: format!("active graph branch history row {name:?} has no state"),
    })?;
    Ok(GraphBranchRecord {
        name,
        head_id,
        state: parse_session_state(path, &state_json)?,
    })
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::*;
    use crate::schema::{graph_mutation_event_dirty_parents, graph_relation_state};
    use crate::store::NodeStore;
    use crate::store::sqlite::{
        GraphBranchPageCursor, GraphMutationBranchChangePageCursor,
        GraphMutationDirtyParentPageCursor, GraphMutationRevisionBounds, SqliteGraphStore,
        SqliteStore, SqliteTransactionError,
    };
    use crate::{Kind, NewNode, Role, SessionState, StoreError};

    const HIGH_CARDINALITY: i64 = 300;

    #[derive(diesel::QueryableByName)]
    struct QueryPlanDetail {
        #[diesel(sql_type = diesel::sql_types::Text)]
        detail: String,
    }

    #[derive(diesel::QueryableByName)]
    struct CountRow {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        count: i64,
    }

    async fn prune_once(store: &SqliteStore, current_revision: i64, retention_window: i64) {
        let database_path = store.database_path().to_owned();
        let mut connection = store.connect().await.unwrap();
        connection
            .immediate_transaction::<(), SqliteTransactionError, _>(async |connection| {
                prune_graph_mutation_journal(
                    connection,
                    &database_path,
                    current_revision,
                    retention_window,
                )
                .await
                .map_err(SqliteTransactionError::Operation)
            })
            .await
            .map_err(|error| error.into_store_error(&database_path))
            .unwrap();
    }

    async fn table_count(table: &str, store: &SqliteStore) -> i64 {
        let mut connection = store.connect().await.unwrap();
        diesel::sql_query(format!("SELECT COUNT(*) AS count FROM {table}"))
            .get_result::<CountRow>(&mut connection)
            .await
            .unwrap()
            .count
    }

    async fn all_branch_changes(
        graph: &SqliteGraphStore,
        revision: i64,
    ) -> Vec<GraphMutationBranchChangeRecord> {
        let page_size = NonZeroUsize::new(37).unwrap();
        let mut cursor: Option<GraphMutationBranchChangePageCursor> = None;
        let mut changes = Vec::new();
        loop {
            let page = graph
                .graph_mutation_branch_changes_page(revision, cursor.as_ref(), page_size)
                .await
                .unwrap();
            changes.extend(page.changes);
            if page.complete {
                break;
            }
            cursor = page.next_cursor;
        }
        changes
    }

    async fn all_dirty_parents(graph: &SqliteGraphStore, revision: i64) -> Vec<String> {
        let page_size = NonZeroUsize::new(41).unwrap();
        let mut cursor: Option<GraphMutationDirtyParentPageCursor> = None;
        let mut parent_ids = Vec::new();
        loop {
            let page = graph
                .graph_mutation_dirty_parents_page(revision, cursor.as_ref(), page_size)
                .await
                .unwrap();
            parent_ids.extend(page.parent_ids);
            if page.complete {
                break;
            }
            cursor = page.next_cursor;
        }
        parent_ids
    }

    async fn all_branches(graph: &SqliteGraphStore, revision: i64) -> Vec<GraphBranchRecord> {
        let page_size = NonZeroUsize::new(43).unwrap();
        let mut cursor: Option<GraphBranchPageCursor> = None;
        let mut branches = Vec::new();
        loop {
            let page = graph
                .graph_branches_at_revision_page(revision, cursor.as_ref(), None, page_size)
                .await
                .unwrap();
            branches.extend(page.branches);
            if page.complete {
                break;
            }
            cursor = page.next_cursor;
        }
        branches
    }

    #[tokio::test]
    async fn high_cardinality_revision_is_staged_without_partial_consumer_reads() {
        let store = SqliteStore::open_temporary().await.unwrap();
        let database_path = store.database_path().to_owned();
        let mut connection = store.connect().await.unwrap();
        connection
            .immediate_transaction::<(), SqliteTransactionError, _>(async |connection| {
                diesel::delete(
                    graph_mutation_event_dirty_parents::table
                        .filter(graph_mutation_event_dirty_parents::revision.eq(1)),
                )
                .execute(connection)
                .await?;
                diesel::sql_query(
                    "WITH RECURSIVE entries(value) AS (\
                         SELECT 0 \
                         UNION ALL \
                         SELECT value + 1 FROM entries WHERE value < 299\
                     ) \
                     INSERT INTO graph_mutation_event_branch_changes (\
                         revision, name, kind, head_id, state_json\
                     ) \
                     SELECT 1, printf('branch-%03d', value), 'upserted', \
                            printf('%064d', value), json_quote('Active') \
                     FROM entries",
                )
                .execute(connection)
                .await?;
                diesel::sql_query(
                    "WITH RECURSIVE entries(value) AS (\
                         SELECT 0 \
                         UNION ALL \
                         SELECT value + 1 FROM entries WHERE value < 299\
                     ) \
                     INSERT INTO graph_mutation_event_dirty_parents (revision, parent_id) \
                     SELECT 1, printf('parent-%03d', value) FROM entries",
                )
                .execute(connection)
                .await?;
                diesel::sql_query(
                    "WITH RECURSIVE entries(value) AS (\
                         SELECT 0 \
                         UNION ALL \
                         SELECT value + 1 FROM entries WHERE value < 299\
                     ) \
                     INSERT INTO graph_branch_history (\
                         name, revision, head_id, state_json, removed\
                     ) \
                     SELECT printf('branch-%03d', value), 0, printf('%064d', value), \
                            json_quote('Active'), 0 \
                     FROM entries",
                )
                .execute(connection)
                .await?;
                diesel::sql_query(
                    "WITH RECURSIVE entries(value) AS (\
                         SELECT 0 \
                         UNION ALL \
                         SELECT value + 1 FROM entries WHERE value < 299\
                     ) \
                     INSERT INTO graph_branch_history (\
                         name, revision, head_id, state_json, removed\
                     ) \
                     SELECT printf('branch-%03d', value), 1, printf('%064d', value + 1), \
                            json_quote('Active'), 0 \
                     FROM entries",
                )
                .execute(connection)
                .await?;
                diesel::sql_query("INSERT INTO graph_mutation_events (revision) VALUES (2), (3)")
                    .execute(connection)
                    .await?;
                diesel::update(
                    graph_relation_state::table.filter(graph_relation_state::singleton.eq(1)),
                )
                .set(graph_relation_state::current_revision.eq(3_i64))
                .execute(connection)
                .await?;
                Ok(())
            })
            .await
            .map_err(|error| error.into_store_error(&database_path))
            .unwrap();
        drop(connection);

        let mut connection = store.connect().await.unwrap();
        for table in [
            "graph_mutation_event_branch_changes",
            "graph_mutation_event_dirty_parents",
        ] {
            let plans = diesel::sql_query(format!(
                "EXPLAIN QUERY PLAN SELECT revision FROM {table} WHERE revision = 1 LIMIT 1"
            ))
            .load::<QueryPlanDetail>(&mut connection)
            .await
            .unwrap();
            assert!(plans.iter().any(|plan| plan.detail.contains("INDEX")));
            assert!(plans.iter().all(|plan| !plan.detail.contains("SCAN")));
        }
        drop(connection);

        prune_once(&store, 3, 1).await;
        let graph = SqliteGraphStore::open_read_only(store.store_path())
            .await
            .unwrap();
        assert_eq!(
            graph.graph_mutation_revision_bounds().await.unwrap(),
            GraphMutationRevisionBounds {
                baseline_revision: 0,
                current_revision: 3,
            }
        );
        assert_eq!(all_branch_changes(&graph, 1).await.len(), 300);
        assert_eq!(all_dirty_parents(&graph, 1).await.len(), 300);
        assert_eq!(
            table_count("graph_mutation_event_branch_change_prune_staging", &store,).await,
            128
        );
        assert_eq!(
            table_count("graph_mutation_event_branch_changes", &store).await,
            172
        );
        assert_eq!(
            table_count("graph_mutation_event_dirty_parent_prune_staging", &store,).await,
            128
        );
        assert_eq!(
            table_count("graph_mutation_event_dirty_parents", &store).await,
            172
        );

        prune_once(&store, 3, 1).await;
        drop(graph);
        let graph = SqliteGraphStore::open_read_only(store.store_path())
            .await
            .unwrap();
        assert_eq!(all_branch_changes(&graph, 1).await.len(), 300);
        assert_eq!(all_dirty_parents(&graph, 1).await.len(), 300);
        assert_eq!(
            table_count("graph_mutation_event_branch_change_prune_staging", &store,).await,
            256
        );

        prune_once(&store, 3, 1).await;
        assert_eq!(
            graph.graph_mutation_revision_bounds().await.unwrap(),
            GraphMutationRevisionBounds {
                baseline_revision: 2,
                current_revision: 3,
            }
        );
        assert_eq!(table_count("graph_mutation_events", &store).await, 1);
        let stale_error = graph
            .graph_mutation_events_page(1, NonZeroUsize::new(1).unwrap())
            .await
            .unwrap_err();
        assert!(matches!(
            stale_error,
            StoreError::GraphRevisionOutOfRange {
                requested: 1,
                minimum: 2,
                maximum: 3,
            }
        ));
        let branches = all_branches(&graph, 2).await;
        assert_eq!(branches.len(), 300);
        assert!(
            branches
                .iter()
                .all(|branch| branch.state == SessionState::Active)
        );

        prune_once(&store, 3, 1).await;
        prune_once(&store, 3, 1).await;
        drop(graph);
        let graph = SqliteGraphStore::open_read_only(store.store_path())
            .await
            .unwrap();
        assert_eq!(all_branches(&graph, 2).await.len(), 300);
        assert_eq!(
            table_count("graph_mutation_event_branch_change_prune_staging", &store,).await,
            0
        );
        assert_eq!(
            table_count("graph_mutation_event_dirty_parent_prune_staging", &store,).await,
            0
        );
        assert_eq!(
            table_count("graph_branch_history", &store).await,
            HIGH_CARDINALITY
        );
        assert_eq!(
            table_count("graph_branch_names", &store).await,
            HIGH_CARDINALITY
        );
        let event_page = graph
            .graph_mutation_events_page(2, NonZeroUsize::new(1).unwrap())
            .await
            .unwrap();
        assert_eq!(
            event_page
                .events
                .into_iter()
                .map(|event| event.revision)
                .collect::<Vec<_>>(),
            vec![3]
        );
    }

    #[tokio::test]
    async fn supported_mutation_enforces_the_production_revision_window() {
        let store = SqliteStore::open_temporary().await.unwrap();
        let root_id = store.root_id();
        let seeded_current_revision = GRAPH_MUTATION_RETENTION_WINDOW + 1;
        let database_path = store.database_path().to_owned();
        let mut connection = store.connect().await.unwrap();
        connection
            .immediate_transaction::<(), SqliteTransactionError, _>(async |connection| {
                diesel::sql_query(
                    "WITH RECURSIVE revisions(value) AS (\
                         SELECT 2 \
                         UNION ALL \
                         SELECT value + 1 FROM revisions WHERE value < ?\
                     ) \
                     INSERT INTO graph_mutation_events (revision) \
                     SELECT value FROM revisions",
                )
                .bind::<BigInt, _>(seeded_current_revision)
                .execute(connection)
                .await?;
                diesel::update(
                    graph_relation_state::table.filter(graph_relation_state::singleton.eq(1)),
                )
                .set(graph_relation_state::current_revision.eq(seeded_current_revision))
                .execute(connection)
                .await?;
                Ok(())
            })
            .await
            .map_err(|error| error.into_store_error(&database_path))
            .unwrap();
        drop(connection);

        let child_id = store
            .append(NewNode {
                parent: root_id.clone(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text("retained child".to_owned()),
            })
            .await
            .unwrap();
        let graph = SqliteGraphStore::open_read_only(store.store_path())
            .await
            .unwrap();
        let current_revision = seeded_current_revision + 1;
        assert_eq!(
            graph.graph_mutation_revision_bounds().await.unwrap(),
            GraphMutationRevisionBounds {
                baseline_revision: 2,
                current_revision,
            }
        );
        assert_eq!(
            table_count("graph_mutation_events", &store).await,
            GRAPH_MUTATION_RETENTION_WINDOW
        );
        assert!(
            graph
                .graph_child_ids_page_at_revision(&root_id, None, 2, NonZeroUsize::new(1).unwrap(),)
                .await
                .unwrap()
                .child_ids
                .is_empty()
        );
        assert_eq!(
            graph
                .graph_child_ids_page_at_revision(
                    &root_id,
                    None,
                    current_revision,
                    NonZeroUsize::new(1).unwrap(),
                )
                .await
                .unwrap()
                .child_ids,
            vec![child_id]
        );
        drop(graph);
        let reopened = SqliteGraphStore::open_read_only(store.store_path())
            .await
            .unwrap();
        let page = reopened
            .graph_mutation_events_page(2, NonZeroUsize::new(2).unwrap())
            .await
            .unwrap();
        assert_eq!(
            page.events
                .into_iter()
                .map(|event| event.revision)
                .collect::<Vec<_>>(),
            vec![3, 4]
        );
    }
}
