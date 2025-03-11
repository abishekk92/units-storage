#![allow(dead_code)]

pub mod error;
pub mod id;
pub mod objects;
pub mod proofs;
pub mod runtime;
pub mod storage;
pub mod storage_traits;
pub mod verification;

// Re-export the main types for convenience
pub use error::StorageError;
pub use id::UnitsObjectId;
pub use objects::{TokenType, TokenizedObject};
pub use proofs::{ProofEngine, StateProof, TokenizedObjectProof, SlotNumber};
pub use runtime::{
    AccessIntent,
    Instruction,
    Transaction,
    TransactionHash,
    TransactionResult,
    TransactionReceipt,
    ConflictResult,
    Runtime,
    MockRuntime,
    InMemoryReceiptStorage,
    InMemoryReceiptIterator
};
pub use storage_traits::{
    UnitsStorage, 
    UnitsStorageIterator, 
    UnitsStorageProofEngine,
    UnitsWriteAheadLog,
    UnitsProofIterator,
    UnitsStateProofIterator,
    UnitsReceiptIterator,
    TransactionReceiptStorage,
    WALEntry
};
pub use verification::{
    VerificationResult,
    ProofVerifier,
    verify_transaction_included,
    detect_double_spend
};
pub use storage::FileWriteAheadLog;

// Re-export the storage implementations
#[cfg(feature = "rocksdb")]
pub use storage::RocksDbStorage;

// SQLite implementation needs to be updated to match the new interfaces
// #[cfg(feature = "sqlite")]
// pub use storage::SqliteStorage;
