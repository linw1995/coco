use crate::Node;

pub trait LogEntry {
    fn log_key(&self) -> &str;
}

impl LogEntry for Node {
    fn log_key(&self) -> &str {
        &self.id
    }
}
