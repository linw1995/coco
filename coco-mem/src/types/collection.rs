use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum ManyOrOne<T> {
    One(T),
    Many(Vec<T>),
}

impl<T> ManyOrOne<T> {
    pub fn one(value: T) -> Self {
        Self::One(value)
    }

    pub fn many(values: Vec<T>) -> Self {
        Self::from_items(values)
    }

    pub fn from_items(mut items: Vec<T>) -> Self {
        if items.len() == 1 {
            Self::One(items.pop().expect("items length is one"))
        } else {
            Self::Many(items)
        }
    }

    pub fn items(&self) -> &[T] {
        match self {
            Self::One(item) => std::slice::from_ref(item),
            Self::Many(items) => items,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.items().iter()
    }

    pub fn first(&self) -> Option<&T> {
        self.items().first()
    }

    pub fn as_one(&self) -> Option<&T> {
        match self {
            Self::One(item) => Some(item),
            Self::Many(_) => None,
        }
    }
}

impl<T> From<T> for ManyOrOne<T> {
    fn from(value: T) -> Self {
        Self::one(value)
    }
}
