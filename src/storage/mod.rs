// Re-export the storage trait
pub use crate::storage_traits::{
    UnitsStorage, 
    UnitsStorageIterator, 
    UnitsStorageProofEngine, 
    UnitsWriteAheadLog,
    UnitsProofIterator,
    UnitsStateProofIterator,
    WALEntry
};

// Export the write-ahead log implementation
mod wal;
pub use wal::FileWriteAheadLog;

// Export implementations
// SQLite implementation needs to be updated to match the new interfaces
// #[cfg(feature = "sqlite")]
// mod sqlite;
// #[cfg(feature = "sqlite")]
// pub use sqlite::{SqliteStorage, SqliteStorageIterator};

#[cfg(feature = "rocksdb")]
mod rocksdb;
#[cfg(feature = "rocksdb")]
pub use rocksdb::{RocksDbStorage, RocksDbStorageIterator};