use std::fmt::Debug;
use units_core::error::StorageError;
use units_core::id::UnitsObjectId;
use units_core::locks::{AccessIntent, LockInfo, LockType, PersistentLockManager, UnitsLockIterator};
use std::time::{SystemTime, UNIX_EPOCH};
#[cfg(feature = "sqlite")]
use sqlx::Row;

/// Direct implementation of the PersistentLockManager trait from units-core
/// The LockManagerTrait is no longer needed as we can directly implement PersistentLockManager

/// SQL query constants to avoid repetition
#[cfg(feature = "sqlite")]
mod sql {
    pub const CREATE_LOCKS_TABLE: &str = r#"
        CREATE TABLE IF NOT EXISTS units_locks (
            object_id BLOB NOT NULL,
            lock_type TEXT NOT NULL,
            transaction_hash BLOB NOT NULL,
            acquired_at INTEGER NOT NULL,
            timeout_ms INTEGER,
            PRIMARY KEY (object_id, transaction_hash)
        );
    "#;
    
    pub const GET_LOCK_BY_OBJECT_ID: &str = r#"
        SELECT lock_type, transaction_hash, acquired_at, timeout_ms
        FROM units_locks
        WHERE object_id = ?
        LIMIT 1
    "#;
    
    pub const GET_ALL_LOCKS_BY_OBJECT_ID: &str = r#"
        SELECT lock_type, transaction_hash, acquired_at, timeout_ms
        FROM units_locks
        WHERE object_id = ?
    "#;
    
    pub const GET_LOCKS_BY_TRANSACTION: &str = r#"
        SELECT object_id, lock_type, acquired_at, timeout_ms
        FROM units_locks
        WHERE transaction_hash = ?
    "#;
    
    pub const DELETE_LOCK: &str = r#"
        DELETE FROM units_locks
        WHERE object_id = ? AND transaction_hash = ?
    "#;
    
    pub const UPDATE_LOCK_TYPE: &str = r#"
        UPDATE units_locks
        SET lock_type = 'write'
        WHERE object_id = ? AND transaction_hash = ?
    "#;
    
    pub const INSERT_LOCK: &str = r#"
        INSERT INTO units_locks (object_id, lock_type, transaction_hash, acquired_at, timeout_ms)
        VALUES (?, ?, ?, ?, ?)
    "#;
    
    pub const DELETE_EXPIRED_LOCKS: &str = r#"
        DELETE FROM units_locks
        WHERE timeout_ms IS NOT NULL AND acquired_at + timeout_ms / 1000 <= ?
    "#;
}

/// A unified iterator that can handle different types of lock info sources
pub struct LockInfoIterator<E> {
    inner: Box<dyn Iterator<Item = Result<LockInfo, E>> + Send + 'static>,
}

impl<E: 'static + Send> LockInfoIterator<E> {
    /// Create a new empty iterator
    pub fn empty() -> Self {
        let iter = std::iter::empty();
        Self { inner: Box::new(iter) }
    }
    
    /// Create a new iterator from a single result
    pub fn once(item: Result<LockInfo, E>) -> Self {
        let iter = std::iter::once(item);
        Self { inner: Box::new(iter) }
    }
    
    /// Create a new iterator from a vector of results
    pub fn from_vec(items: Vec<Result<LockInfo, E>>) -> Self {
        let iter = items.into_iter();
        Self { inner: Box::new(iter) }
    }
    
    /// Create a new iterator from a successful vector of LockInfo
    pub fn from_lock_infos(items: Vec<LockInfo>) -> Self 
    where
        E: Debug + std::fmt::Display + Send,
    {
        let iter = items.into_iter().map(Ok);
        Self { inner: Box::new(iter) }
    }
}

impl<E> Iterator for LockInfoIterator<E> {
    type Item = Result<LockInfo, E>;
    
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<E: Debug + std::fmt::Display + Send> UnitsLockIterator<E> for LockInfoIterator<E> {}

/// Helper function to get the current timestamp in seconds
pub fn current_time_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// SQLite-based lock manager implementation
#[cfg(feature = "sqlite")]
pub struct SqliteLockManager {
    /// The SQLite pool for database connections
    pool: sqlx::SqlitePool,
    /// Shared runtime for async operations
    rt: std::sync::Arc<tokio::runtime::Runtime>,
}

#[cfg(feature = "sqlite")]
impl SqliteLockManager {
    /// Create a new SqliteLockManager with the given SQLite connection pool and runtime
    pub fn new(pool: sqlx::SqlitePool, rt: std::sync::Arc<tokio::runtime::Runtime>) -> Self {
        Self { pool, rt }
    }

    /// Initialize the database schema for locks
    pub async fn initialize(&self) -> Result<(), StorageError> {
        // Create the locks table if it doesn't exist
        sqlx::query(sql::CREATE_LOCKS_TABLE)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                StorageError::Database(format!("Failed to create locks table: {}", e))
            })?;
            
        // Create index on transaction_hash
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_locks_transaction_hash ON units_locks(transaction_hash);"
        )
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(format!("Failed to create locks table: {}", e)))?;

        Ok(())
    }

    /// Convert a sqlx Row to a LockInfo
    fn row_to_lock_info(&self, row: sqlx::sqlite::SqliteRow, object_id: &UnitsObjectId) -> Result<LockInfo, StorageError> {
        let lock_type_str: String = row.get("lock_type");
        let lock_type = match lock_type_str.as_str() {
            "read" => LockType::Read,
            "write" => LockType::Write,
            _ => {
                return Err(StorageError::Database(format!(
                    "Unknown lock type: {}",
                    lock_type_str
                )))
            }
        };

        let transaction_hash: Vec<u8> = row.get("transaction_hash");
        let mut tx_hash = [0u8; 32];
        if transaction_hash.len() >= 32 {
            tx_hash.copy_from_slice(&transaction_hash[0..32]);
        }

        let acquired_at: i64 = row.get("acquired_at");
        let timeout_ms: Option<i64> = row.get("timeout_ms");

        Ok(LockInfo {
            object_id: *object_id,
            lock_type,
            transaction_hash: tx_hash,
            acquired_at: acquired_at as u64,
            timeout_ms: timeout_ms.map(|t| t as u64),
        })
    }

    /// Helper to get the lock info for an object
    async fn get_object_lock_info_internal(
        &self,
        object_id: &UnitsObjectId,
    ) -> Result<Option<LockInfo>, StorageError> {
        let result = sqlx::query(sql::GET_LOCK_BY_OBJECT_ID)
        .bind(object_id.as_ref())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            StorageError::Database(format!("Failed to query lock for object: {}", e))
        })?;

        if let Some(row) = result {
            Ok(Some(self.row_to_lock_info(row, object_id)?))
        } else {
            Ok(None)
        }
    }

    /// Helper to get all locks for an object
    async fn get_all_object_locks_internal(
        &self,
        object_id: &UnitsObjectId,
    ) -> Result<Vec<LockInfo>, StorageError> {
        let results = sqlx::query(sql::GET_ALL_LOCKS_BY_OBJECT_ID)
        .bind(object_id.as_ref())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            StorageError::Database(format!("Failed to query locks for object: {}", e))
        })?;

        let mut lock_infos = Vec::with_capacity(results.len());
        for row in results {
            lock_infos.push(self.row_to_lock_info(row, object_id)?);
        }

        Ok(lock_infos)
    }

    /// Helper to check if a transaction already has a lock
    async fn check_transaction_has_lock(
        &self,
        object_id: &UnitsObjectId,
        transaction_hash: &[u8; 32],
    ) -> Result<Option<LockInfo>, StorageError> {
        let result = sqlx::query(
            r#"
            SELECT lock_type, acquired_at, timeout_ms
            FROM units_locks
            WHERE object_id = ? AND transaction_hash = ?
            "#,
        )
        .bind(object_id.as_ref())
        .bind(transaction_hash.as_ref())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            StorageError::Database(format!("Failed to query transaction lock: {}", e))
        })?;

        if let Some(row) = result {
            let lock_type_str: String = row.get("lock_type");
            let lock_type = match lock_type_str.as_str() {
                "read" => LockType::Read,
                "write" => LockType::Write,
                _ => {
                    return Err(StorageError::Database(format!(
                        "Unknown lock type: {}",
                        lock_type_str
                    )))
                }
            };

            let acquired_at: i64 = row.get("acquired_at");
            let timeout_ms: Option<i64> = row.get("timeout_ms");

            Ok(Some(LockInfo {
                object_id: *object_id,
                lock_type,
                transaction_hash: *transaction_hash,
                acquired_at: acquired_at as u64,
                timeout_ms: timeout_ms.map(|t| t as u64),
            }))
        } else {
            Ok(None)
        }
    }

    /// Helper to check if a lock request is compatible with existing locks
    async fn is_lock_compatible(
        &self,
        object_id: &UnitsObjectId,
        requested_type: LockType,
        transaction_hash: &[u8; 32],
    ) -> Result<bool, StorageError> {
        // Check if the transaction already holds a lock
        let has_lock = self.check_transaction_has_lock(object_id, transaction_hash).await?;
        if has_lock.is_some() {
            return Ok(true); // This transaction already has a lock, so it's compatible
        }

        // Get all locks on this object
        let locks = self.get_all_object_locks_internal(object_id).await?;
        if locks.is_empty() {
            return Ok(true); // No locks, so any lock is compatible
        }

        // If any lock is a write lock, no other locks are compatible
        if locks.iter().any(|l| l.lock_type == LockType::Write) {
            return Ok(false);
        }

        // Read locks are compatible with each other
        if requested_type == LockType::Read {
            return Ok(true);
        }

        // Write locks are not compatible with any existing locks
        Ok(false)
    }
}

#[cfg(feature = "sqlite")]
impl PersistentLockManager for SqliteLockManager {
    type Error = StorageError;

    fn acquire_lock(
        &self,
        object_id: &UnitsObjectId,
        lock_type: LockType,
        transaction_hash: &[u8; 32],
        timeout_ms: Option<u64>,
    ) -> Result<bool, Self::Error> {
        // Use the shared runtime
        self.rt.block_on(async {
            // Check if this transaction already has a lock
            let existing_lock = self.check_transaction_has_lock(object_id, transaction_hash).await?;
            if let Some(lock) = existing_lock {
                // If the transaction already has a write lock, it's compatible with any request
                if lock.lock_type == LockType::Write {
                    return Ok(true);
                }
                
                // If the transaction has a read lock and wants a read lock, it's fine
                if lock.lock_type == LockType::Read && lock_type == LockType::Read {
                    return Ok(true);
                }
                
                // If the transaction has a read lock and wants a write lock, we need to check if upgrade is possible
                if lock.lock_type == LockType::Read && lock_type == LockType::Write {
                    // Check if there are any other read locks on this object
                    let all_locks = self.get_all_object_locks_internal(object_id).await?;
                    if all_locks.len() > 1 {
                        // There are other locks, can't upgrade
                        return Ok(false);
                    }
                    
                    // Only this transaction has a lock, so upgrade is possible
                    // Delete the read lock and create a write lock
                    sqlx::query(sql::UPDATE_LOCK_TYPE)
                    .bind(object_id.as_ref())
                    .bind(transaction_hash.as_ref())
                    .execute(&self.pool)
                    .await
                    .map_err(|e| {
                        StorageError::Database(format!("Failed to upgrade lock: {}", e))
                    })?;
                    
                    return Ok(true);
                }
            }

            // Check if the requested lock is compatible with existing locks
            let compatible = self.is_lock_compatible(object_id, lock_type, transaction_hash).await?;
            if !compatible {
                return Ok(false);
            }

            // No existing lock, create a new one
            let now = current_time_secs();
            let lock_type_str = match lock_type {
                LockType::Read => "read",
                LockType::Write => "write",
            };

            // Insert the new lock record
            sqlx::query(sql::INSERT_LOCK)
            .bind(object_id.as_ref())
            .bind(lock_type_str)
            .bind(transaction_hash.as_ref())
            .bind(now as i64)
            .bind(timeout_ms.map(|t| t as i64))
            .execute(&self.pool)
            .await
            .map_err(|e| {
                StorageError::Database(format!("Failed to insert lock record: {}", e))
            })?;

            Ok(true)
        })
    }

    fn release_lock(
        &self,
        object_id: &UnitsObjectId,
        transaction_hash: &[u8; 32],
    ) -> Result<bool, Self::Error> {
        self.rt.block_on(async {
            // Delete the lock record
            let result = sqlx::query(sql::DELETE_LOCK)
            .bind(object_id.as_ref())
            .bind(transaction_hash.as_ref())
            .execute(&self.pool)
            .await
            .map_err(|e| {
                StorageError::Database(format!("Failed to delete lock record: {}", e))
            })?;

            Ok(result.rows_affected() > 0)
        })
    }

    fn get_lock_info(
        &self,
        object_id: &UnitsObjectId,
    ) -> Result<Option<LockInfo>, Self::Error> {
        self.rt.block_on(async { self.get_object_lock_info_internal(object_id).await })
    }

    fn can_acquire_lock(
        &self,
        object_id: &UnitsObjectId,
        intent: AccessIntent,
        transaction_hash: &[u8; 32],
    ) -> Result<bool, Self::Error> {
        self.rt.block_on(async {
            // Convert AccessIntent to LockType
            let lock_type = match intent {
                AccessIntent::Read => LockType::Read,
                AccessIntent::Write => LockType::Write,
            };

            // Check if a lock already exists for this transaction
            let existing_lock = self.check_transaction_has_lock(object_id, transaction_hash).await?;
            if let Some(lock) = existing_lock {
                // Already has the appropriate lock or better
                if lock.lock_type == LockType::Write || (lock.lock_type == LockType::Read && lock_type == LockType::Read) {
                    return Ok(true);
                }
                
                // Trying to upgrade from read to write
                let all_locks = self.get_all_object_locks_internal(object_id).await?;
                return Ok(all_locks.len() <= 1); // Can upgrade if no other transactions have locks
            }

            // Check compatibility with existing locks
            self.is_lock_compatible(object_id, lock_type, transaction_hash).await
        })
    }

    fn release_transaction_locks(
        &self,
        transaction_hash: &[u8; 32],
    ) -> Result<usize, Self::Error> {
        let rt = tokio::runtime::Runtime::new().map_err(|e| {
            StorageError::Other(format!("Failed to create runtime: {}", e))
        })?;

        rt.block_on(async {
            // Delete all locks held by this transaction
            let result = sqlx::query(
                r#"
                DELETE FROM units_locks
                WHERE transaction_hash = ?
                "#,
            )
            .bind(transaction_hash.as_ref())
            .execute(&self.pool)
            .await
            .map_err(|e| {
                StorageError::Database(format!("Failed to delete transaction locks: {}", e))
            })?;

            Ok(result.rows_affected() as usize)
        })
    }

    fn get_transaction_locks(
        &self,
        transaction_hash: &[u8; 32],
    ) -> Box<dyn UnitsLockIterator<Self::Error> + '_> {
        // Get locks for this transaction
        let results = match self.rt.block_on(async {
            sqlx::query(sql::GET_LOCKS_BY_TRANSACTION)
            .bind(transaction_hash.as_ref())
            .fetch_all(&self.pool)
            .await
        }) {
            Ok(rows) => rows,
            Err(e) => {
                let error = StorageError::Database(format!("Failed to query transaction locks: {}", e));
                return Box::new(LockInfoIterator::once(Err(error)));
            }
        };

        // Convert rows to LockInfo objects
        let mut lock_infos = Vec::with_capacity(results.len());
        for row in results {
            let object_id_bytes: Vec<u8> = row.get("object_id");
            if object_id_bytes.len() != 32 {
                let error = StorageError::Database(format!("Invalid object ID length: {}", object_id_bytes.len()));
                lock_infos.push(Err(error));
                continue;
            }

            let mut id_array = [0u8; 32];
            id_array.copy_from_slice(&object_id_bytes);
            let object_id = UnitsObjectId::from_bytes(id_array);
            
            // Use the helper method to create the lock info
            match self.row_to_lock_info(row, &object_id) {
                Ok(mut lock_info) => {
                    // Ensure transaction hash matches the requested one
                    lock_info.transaction_hash = *transaction_hash;
                    lock_infos.push(Ok(lock_info));
                },
                Err(e) => lock_infos.push(Err(e)),
            }
        }

        Box::new(LockInfoIterator::from_vec(lock_infos))
    }

    fn get_object_locks(
        &self,
        object_id: &UnitsObjectId,
    ) -> Box<dyn UnitsLockIterator<Self::Error> + '_> {
        match self.rt.block_on(self.get_all_object_locks_internal(object_id)) {
            Ok(locks) => {
                if locks.is_empty() {
                    Box::new(LockInfoIterator::empty())
                } else {
                    Box::new(LockInfoIterator::from_lock_infos(locks))
                }
            }
            Err(e) => {
                Box::new(LockInfoIterator::once(Err(e)))
            }
        }
    }

    fn cleanup_expired_locks(&self) -> Result<usize, Self::Error> {
        self.rt.block_on(async {
            let now = current_time_secs() as i64;

            // Delete all expired locks
            let result = sqlx::query(sql::DELETE_EXPIRED_LOCKS)
            .bind(now)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                StorageError::Database(format!("Failed to cleanup expired locks: {}", e))
            })?;

            Ok(result.rows_affected() as usize)
        })
    }
}

#[cfg(feature = "sqlite")]
impl Debug for SqliteLockManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteLockManager").finish()
    }
}


// The adapter is no longer needed as we directly implement PersistentLockManager