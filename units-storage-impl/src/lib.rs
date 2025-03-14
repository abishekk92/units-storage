pub mod lock_manager;
pub mod storage_traits;
pub mod wal;

#[cfg(feature = "sqlite")]
pub mod sqlite;

// Re-export the main types for convenience
pub use storage_traits::{
    UnitsProofIterator, UnitsReceiptIterator, UnitsStateProofIterator,
    UnitsStorage, UnitsStorageIterator, UnitsStorageProofEngine,
    TransactionReceiptStorage, UnitsWriteAheadLog, WALEntry,
};

pub use wal::FileWriteAheadLog;

// Re-export lock manager implementations
#[cfg(feature = "sqlite")]
pub use lock_manager::SqliteLockManager;
pub use lock_manager::RocksDbLockManager;

// Re-export the storage implementations
#[cfg(feature = "sqlite")]
pub use sqlite::SqliteStorage;
