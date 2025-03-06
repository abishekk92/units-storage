#![allow(dead_code)]

pub mod id;
pub mod objects;
pub mod proofs;
pub mod storage;
pub mod storage_traits;

// Re-export the main types for convenience
pub use id::UnitsObjectId;
pub use objects::{TokenType, TokenizedObject};
pub use proofs::{StateProof, TokenizedObjectProof};
pub use storage_traits::{UnitsStorage, UnitsStorageIterator, UnitsStorageProofEngine};

// Re-export the storage implementations
#[cfg(feature = "rocksdb")]
pub use storage::RocksDbStorage;
#[cfg(feature = "sqlite")]
pub use storage::SqliteStorage;
