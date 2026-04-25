pub trait Snapshot {
    fn snapshot_key(&self) -> &str;
}
