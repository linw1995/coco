// CoCo Memory Implementation

mod error;
pub mod store;
mod types;

pub use error::{StoreError, StoreResult};
pub use store::Store;
pub use store::fs::FsStore;
pub use store::memory::MemoryStore;
pub use types::*;
