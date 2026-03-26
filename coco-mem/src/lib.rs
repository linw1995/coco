// CoCo Memory Implementation

pub mod store;
mod types;

pub use store::memory::{Error as StoreError, SharedStore, Store};
pub use types::*;
