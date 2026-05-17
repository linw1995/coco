use coco_mem::{MessageQueueItem, Store};
use serde::Serialize;
use snafu::prelude::*;

use crate::{
    Result,
    cli::{MqCommand, MqSubcommand},
    error::{ParseMqPayloadSnafu, StoreSnafu},
};

#[derive(Debug, Serialize)]
struct MqEnqueueView {
    message_id: String,
    queue: String,
}

pub(super) async fn run_mq_command(
    command: MqCommand,
    store: &impl Store,
) -> Result<Option<String>> {
    match command.command {
        MqSubcommand::Enqueue(command) => {
            let payload =
                serde_json::from_str(&command.payload_json).context(ParseMqPayloadSnafu)?;
            let item = store
                .enqueue_message(&command.queue, payload)
                .context(StoreSnafu)?;
            let view = mq_enqueue_view(&item);
            Ok(Some(if command.json {
                serde_json::to_string_pretty(&view).expect("mq enqueue output should serialize")
            } else {
                format!("message_id: {}\nqueue: {}", view.message_id, view.queue)
            }))
        }
    }
}

fn mq_enqueue_view(item: &MessageQueueItem) -> MqEnqueueView {
    MqEnqueueView {
        message_id: item.message_id.clone(),
        queue: item.queue.clone(),
    }
}
