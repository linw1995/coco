use std::time::Duration;

use coco_mem::{NewSchedulerTask, SchedulerStore, SchedulerTask, SchedulerTaskPatch, Timestamp};
use serde::Serialize;
use snafu::prelude::*;

use crate::{
    Result,
    cli::{
        SchedulerAddCommand, SchedulerCommand, SchedulerDeleteCommand, SchedulerListCommand,
        SchedulerShowCommand, SchedulerSubcommand, SchedulerUpdateCommand,
    },
    error::{InvalidSchedulerTimestampSnafu, StoreSnafu},
};

#[derive(Debug, Serialize)]
struct SchedulerTaskView {
    id: String,
    branch: String,
    prompt: String,
    interval_secs: u64,
    next_run_at: String,
    last_run_at: Option<String>,
    run_count: u64,
    enabled: bool,
    created_at: String,
    updated_at: String,
}

#[derive(Debug, Serialize)]
struct SchedulerDeleteResult {
    id: String,
    deleted: bool,
}

pub(super) async fn run_scheduler_command(
    command: SchedulerCommand,
    store: &impl SchedulerStore,
) -> Result<Option<String>> {
    match command.command {
        SchedulerSubcommand::Add(command) => {
            let json = command.json;
            let task = run_scheduler_add(command, store)?;
            Ok(Some(if json {
                render_json(task)
            } else {
                render_scheduler_task_text(&task)
            }))
        }
        SchedulerSubcommand::Update(command) => {
            let json = command.json;
            let task = run_scheduler_update(command, store)?;
            Ok(Some(if json {
                render_json(task)
            } else {
                render_scheduler_task_text(&task)
            }))
        }
        SchedulerSubcommand::Delete(command) => {
            let result = run_scheduler_delete(command, store)?;
            Ok(Some(render_scheduler_delete_text(&result)))
        }
        SchedulerSubcommand::List(command) => {
            let json = command.json;
            let tasks = run_scheduler_list(command, store)?;
            Ok(Some(if json {
                render_json(tasks)
            } else {
                render_scheduler_list_text(&tasks)
            }))
        }
        SchedulerSubcommand::Show(command) => {
            let json = command.json;
            let task = run_scheduler_show(command, store)?;
            Ok(Some(if json {
                render_json(task)
            } else {
                render_scheduler_task_text(&task)
            }))
        }
    }
}

fn run_scheduler_add(
    command: SchedulerAddCommand,
    store: &impl SchedulerStore,
) -> Result<SchedulerTaskView> {
    let task = store
        .add_scheduler_task(NewSchedulerTask {
            id: command.id,
            branch: command.branch,
            prompt: command.prompt.join(" "),
            interval_secs: command.interval_secs,
            next_run_at: resolve_next_run_at(command.next_run_at, command.initial_delay_secs)?,
            enabled: !command.disabled,
        })
        .context(StoreSnafu)?;

    Ok(scheduler_task_view(&task))
}

fn run_scheduler_update(
    command: SchedulerUpdateCommand,
    store: &impl SchedulerStore,
) -> Result<SchedulerTaskView> {
    let enabled = if command.enable {
        Some(true)
    } else if command.disable {
        Some(false)
    } else {
        None
    };
    let next_run_at = match (command.next_run_at, command.initial_delay_secs) {
        (None, None) => None,
        (next_run_at, initial_delay_secs) => {
            Some(resolve_next_run_at(next_run_at, initial_delay_secs)?)
        }
    };
    let task = store
        .update_scheduler_task(
            &command.id,
            &SchedulerTaskPatch {
                branch: command.branch,
                prompt: command.prompt,
                interval_secs: command.interval_secs,
                next_run_at,
                enabled,
            },
        )
        .context(StoreSnafu)?;

    Ok(scheduler_task_view(&task))
}

fn run_scheduler_delete(
    command: SchedulerDeleteCommand,
    store: &impl SchedulerStore,
) -> Result<SchedulerDeleteResult> {
    store
        .delete_scheduler_task(&command.id)
        .context(StoreSnafu)?;
    Ok(SchedulerDeleteResult {
        id: command.id,
        deleted: true,
    })
}

fn run_scheduler_list(
    _command: SchedulerListCommand,
    store: &impl SchedulerStore,
) -> Result<Vec<SchedulerTaskView>> {
    let mut tasks = store
        .list_scheduler_tasks()
        .context(StoreSnafu)?
        .into_values()
        .map(|task| scheduler_task_view(&task))
        .collect::<Vec<_>>();
    tasks.sort_by(|left, right| {
        left.next_run_at
            .cmp(&right.next_run_at)
            .then_with(|| left.id.cmp(&right.id))
    });
    Ok(tasks)
}

fn run_scheduler_show(
    command: SchedulerShowCommand,
    store: &impl SchedulerStore,
) -> Result<SchedulerTaskView> {
    let task = store.get_scheduler_task(&command.id).context(StoreSnafu)?;
    Ok(scheduler_task_view(&task))
}

fn resolve_next_run_at(
    next_run_at: Option<String>,
    initial_delay_secs: Option<u64>,
) -> Result<Timestamp> {
    if let Some(value) = next_run_at {
        return value.parse::<Timestamp>().map_err(|source| {
            InvalidSchedulerTimestampSnafu {
                value,
                message: source.to_string(),
            }
            .build()
        });
    }

    let delay = Duration::from_secs(initial_delay_secs.unwrap_or(0));
    Timestamp::now().checked_add(delay).map_err(|source| {
        InvalidSchedulerTimestampSnafu {
            value: format!("now+{}s", delay.as_secs()),
            message: source.to_string(),
        }
        .build()
    })
}

fn scheduler_task_view(task: &SchedulerTask) -> SchedulerTaskView {
    SchedulerTaskView {
        id: task.id.clone(),
        branch: task.branch.clone(),
        prompt: task.prompt.clone(),
        interval_secs: task.interval_secs,
        next_run_at: task.next_run_at.to_string(),
        last_run_at: task.last_run_at.map(|time| time.to_string()),
        run_count: task.run_count,
        enabled: task.enabled,
        created_at: task.created_at.to_string(),
        updated_at: task.updated_at.to_string(),
    }
}

fn render_scheduler_list_text(tasks: &[SchedulerTaskView]) -> String {
    if tasks.is_empty() {
        return "No scheduler tasks found.".to_owned();
    }

    tasks
        .iter()
        .map(|task| {
            format!(
                "{} branch={} enabled={} interval_secs={} next_run_at={} prompt={}",
                task.id,
                task.branch,
                task.enabled,
                task.interval_secs,
                task.next_run_at,
                task.prompt
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_scheduler_task_text(task: &SchedulerTaskView) -> String {
    format!(
        "id: {}\nbranch: {}\nenabled: {}\ninterval_secs: {}\nnext_run_at: {}\nlast_run_at: {}\nrun_count: {}\nprompt: {}",
        task.id,
        task.branch,
        task.enabled,
        task.interval_secs,
        task.next_run_at,
        task.last_run_at.as_deref().unwrap_or("none"),
        task.run_count,
        task.prompt
    )
}

fn render_scheduler_delete_text(result: &SchedulerDeleteResult) -> String {
    format!("deleted scheduler task {}", result.id)
}

fn render_json<T>(value: T) -> String
where
    T: Serialize,
{
    serde_json::to_string_pretty(&value).expect("scheduler output should serialize")
}
