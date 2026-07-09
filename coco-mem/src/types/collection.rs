use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(transparent)]
pub struct OneOrMany<T>(Vec<T>);

impl<T> OneOrMany<T> {
    pub fn one(value: T) -> Self {
        Self(vec![value])
    }

    pub fn many(values: Vec<T>) -> Self {
        Self::from_items(values)
    }

    pub fn from_items(items: Vec<T>) -> Self {
        Self(items)
    }

    pub fn items(&self) -> &[T] {
        &self.0
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.items().iter()
    }

    pub fn first(&self) -> Option<&T> {
        self.items().first()
    }

    pub fn as_one(&self) -> Option<&T> {
        if self.0.len() == 1 {
            self.0.first()
        } else {
            None
        }
    }
}

impl<T> From<T> for OneOrMany<T> {
    fn from(value: T) -> Self {
        Self::one(value)
    }
}
