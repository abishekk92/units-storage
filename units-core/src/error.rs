use std::io;
use thiserror::Error;

/// Represents all possible errors that can occur when interacting with UNITS storage
#[derive(Error, Debug)]
pub enum StorageError {
    /// IO errors that occur when reading/writing files
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Database errors that occur with the underlying storage backend
    #[error("Database error: {0}")]
    Database(String),

    /// Serialization/deserialization errors
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Errors related to missing or invalid data
    #[error("Not found: {0}")]
    NotFound(String),

    /// Errors related to proof verification failures
    #[error("Proof verification failed: {0}")]
    ProofVerification(String),

    /// Errors that occur during write-ahead log operations
    #[error("Write-ahead log error: {0}")]
    WAL(String),

    /// Errors that occur when an object version is not found at a specific slot
    #[error("Object not found at slot {0}")]
    ObjectNotAtSlot(u64),

    /// Errors that occur when a proof version is not found at a specific slot
    #[error("Proof not found at slot {0}")]
    ProofNotAtSlot(u64),

    /// Errors that occur when a proof is not found for an object
    #[error("Proof not found for object {0:?}")]
    ProofNotFound(crate::id::UnitsObjectId),

    /// Errors when a proof chain validation fails
    #[error("Proof chain validation failed: {0}")]
    ProofChainInvalid(String),

    /// Errors when proof chain validation fails due to missing data
    #[error("Proof chain missing data for object {0:?}: {1}")]
    ProofMissingData(crate::id::UnitsObjectId, String),
    
    /// Errors when a transaction is not found
    #[error("Transaction not found: {0:?}")]
    TransactionNotFound([u8; 32]),
    
    /// Errors when an operation is invalid due to transaction state
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),
    
    /// Errors when a feature is not implemented
    #[error("Unimplemented: {0}")]
    Unimplemented(String),

    /// Generic errors that don't fit in other categories
    #[error("Other error: {0}")]
    Other(String),

    /// Anyhow error wrapper for error context
    #[error(transparent)]
    Context(#[from] anyhow::Error),
}

// Additional From conversions for common error types

impl From<bincode::Error> for StorageError {
    fn from(err: bincode::Error) -> Self {
        StorageError::Serialization(err.to_string())
    }
}


#[cfg(feature = "sqlite")]
impl From<sqlx::Error> for StorageError {
    fn from(err: sqlx::Error) -> Self {
        match err {
            sqlx::Error::RowNotFound => StorageError::NotFound("Row not found".to_string()),
            _ => StorageError::Database(err.to_string()),
        }
    }
}

impl From<String> for StorageError {
    fn from(err: String) -> Self {
        StorageError::Other(err)
    }
}

impl From<&str> for StorageError {
    fn from(err: &str) -> Self {
        StorageError::Other(err.to_string())
    }
}
