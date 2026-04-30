use std::collections::{BTreeMap, HashMap};

use crate::{BranchConfigRecord, Node, SkillRecord};

pub trait CollectionRecord {
    fn collection_key(&self) -> &str;
}

pub trait Collection {
    type Record: CollectionRecord;

    fn get_record(&self, key: &str) -> Option<&Self::Record>;
}

impl<R> Collection for HashMap<String, R>
where
    R: CollectionRecord,
{
    type Record = R;

    fn get_record(&self, key: &str) -> Option<&Self::Record> {
        self.get(key)
    }
}

impl<R> Collection for BTreeMap<String, R>
where
    R: CollectionRecord,
{
    type Record = R;

    fn get_record(&self, key: &str) -> Option<&Self::Record> {
        self.get(key)
    }
}

impl CollectionRecord for BranchConfigRecord {
    fn collection_key(&self) -> &str {
        &self.name
    }
}

impl CollectionRecord for Node {
    fn collection_key(&self) -> &str {
        &self.id
    }
}

impl CollectionRecord for SkillRecord {
    fn collection_key(&self) -> &str {
        &self.name
    }
}
