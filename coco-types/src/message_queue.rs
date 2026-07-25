use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::hash::hex_encode;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NewMessageQueueItem {
    pub queue: String,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MessageQueueItem {
    pub message_id: String,
    pub queue: String,
    pub created_at: Timestamp,
    pub payload: Value,
}

impl MessageQueueItem {
    pub fn new(queue: impl Into<String>, payload: Value, created_at: Timestamp) -> Self {
        let queue = queue.into();
        let message_id = compute_message_queue_item_id(&queue, &payload, &created_at);

        Self {
            message_id,
            queue,
            created_at,
            payload,
        }
    }
}

#[derive(Serialize)]
struct MessageQueueItemHashPayload<'a> {
    queue: &'a str,
    payload: &'a Value,
    created_at: &'a Timestamp,
}

fn compute_message_queue_item_id(queue: &str, payload: &Value, created_at: &Timestamp) -> String {
    let payload = serde_json::to_vec(&MessageQueueItemHashPayload {
        queue,
        payload,
        created_at,
    })
    .expect("message queue item hash payload should serialize");

    let mut hasher = Sha256::new();
    hasher.update(format!("message_queue_item {}\0", payload.len()).as_bytes());
    hasher.update(&payload);

    hex_encode(&hasher.finalize())
}
