use std::fmt::Debug;
use units_core::error::StorageError;
use units_core::id::UnitsObjectId;
use units_core::locks::{AccessIntent, LockInfo, LockType, PersistentLockManager, UnitsLockIterator};
use std::time::{SystemTime, UNIX_EPOCH};
#[cfg(feature = "sqlite")]
use sqlx::Row;

/// LockManagerTrait defines the interface for managing locks in the UNITS system
/// This trait is separated from storage concerns to follow the single responsibility principle
pub trait LockManagerTrait {
    type Error: Debug + std::fmt::Display;

    /// Acquire a lock on an object for a transaction
    fn acquire_object_lock(
        &self,
        object_id: &UnitsObjectId,
        lock_type: LockType,
        transaction_hash: &[u8; 32],
        timeout_ms: Option<u64>,
    ) -> Result<bool, Self::Error>;

    /// Release a lock on an object for a transaction
    fn release_object_lock(
        &self,
        object_id: &UnitsObjectId,
        transaction_hash: &[u8; 32],
    ) -> Result<bool, Self::Error>;

    /// Check if a lock exists and get its information
    fn get_object_lock_info(
        &self,
        object_id: &UnitsObjectId,
    ) -> Result<Option<LockInfo>, Self::Error>;

    /// Check if a transaction can acquire a lock on an object
    fn can_acquire_object_lock(
        &self,
        object_id: &UnitsObjectId,
        intent: AccessIntent,
        transaction_hash: &[u8; 32],
    ) -> Result<bool, Self::Error>;

    /// Release all locks held by a transaction
    fn release_all_transaction_locks(
        &self,
        transaction_hash: &[u8; 32],
    ) -> Result<usize, Self::Error>;

    /// Get all locks held by a transaction
    fn get_all_transaction_locks(
        &self,
        transaction_hash: &[u8; 32],
    ) -> Box<dyn Iterator<Item = Result<LockInfo, Self::Error>> + '_>;

    /// Get all locks on an object
    fn get_all_object_locks(
        &self, 
        object_id: &UnitsObjectId
    ) -> Box<dyn Iterator<Item = Result<LockInfo, Self::Error>> + '_>;

    /// Check for expired locks and release them
    fn cleanup_expired_object_locks(&self) -> Result<usize, Self::Error>;
}

/// An empty iterator implementation for LockInfo
pub struct EmptyLockIterator<E>(std::marker::PhantomData<E>);

impl<E> EmptyLockIterator<E> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<E> Iterator for EmptyLockIterator<E> {
    type Item = Result<LockInfo, E>;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

impl<E: Debug + std::fmt::Display> UnitsLockIterator<E> for EmptyLockIterator<E> {}

// Wrapper type to implement UnitsLockIterator for IntoIter
pub struct LockInfoVecIterator(std::vec::IntoIter<Result<LockInfo, StorageError>>);

impl Iterator for LockInfoVecIterator {
    type Item = Result<LockInfo, StorageError>;
    
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl UnitsLockIterator<StorageError> for LockInfoVecIterator {}

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
}

#[cfg(feature = "sqlite")]
impl SqliteLockManager {
    /// Create a new SqliteLockManager with the given SQLite connection pool
    pub fn new(pool: sqlx::SqlitePool) -> Self {
        Self { pool }
    }

    /// Initialize the database schema for locks
    pub async fn initialize(&self) -> Result<(), StorageError> {
        // Create the locks table if it doesn't exist
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS units_locks (
                object_id BLOB NOT NULL,
                lock_type TEXT NOT NULL,
                transaction_hash BLOB NOT NULL,
                acquired_at INTEGER NOT NULL,
                timeout_ms INTEGER,
                PRIMARY KEY (object_id, transaction_hash)
            );
            
            CREATE INDEX IF NOT EXISTS idx_locks_transaction_hash ON units_locks(transaction_hash);
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Database(format!("Failed to create locks table: {}", e)))?;

        Ok(())
    }

    /// Helper to get the lock info for an object
    async fn get_object_lock_info_internal(
        &self,
        object_id: &UnitsObjectId,
    ) -> Result<Option<LockInfo>, StorageError> {
        let result = sqlx::query(
            r#"
            SELECT lock_type, transaction_hash, acquired_at, timeout_ms
            FROM units_locks
            WHERE object_id = ?
            LIMIT 1
            "#,
        )
        .bind(object_id.as_ref())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            StorageError::Database(format!("Failed to query lock for object: {}", e))
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

            let transaction_hash: Vec<u8> = row.get("transaction_hash");
            let mut tx_hash = [0u8; 32];
            if transaction_hash.len() >= 32 {
                tx_hash.copy_from_slice(&transaction_hash[0..32]);
            }

            let acquired_at: i64 = row.get("acquired_at");
            let timeout_ms: Option<i64> = row.get("timeout_ms");

            Ok(Some(LockInfo {
                object_id: *object_id,
                lock_type,
                transaction_hash: tx_hash,
                acquired_at: acquired_at as u64,
                timeout_ms: timeout_ms.map(|t| t as u64),
            }))
        } else {
            Ok(None)
        }
    }

    /// Helper to get all locks for an object
    async fn get_all_object_locks_internal(
        &self,
        object_id: &UnitsObjectId,
    ) -> Result<Vec<LockInfo>, StorageError> {
        let results = sqlx::query(
            r#"
            SELECT lock_type, transaction_hash, acquired_at, timeout_ms
            FROM units_locks
            WHERE object_id = ?
            "#,
        )
        .bind(object_id.as_ref())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            StorageError::Database(format!("Failed to query locks for object: {}", e))
        })?;

        let mut lock_infos = Vec::with_capacity(results.len());
        for row in results {
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

            lock_infos.push(LockInfo {
                object_id: *object_id,
                lock_type,
                transaction_hash: tx_hash,
                acquired_at: acquired_at as u64,
                timeout_ms: timeout_ms.map(|t| t as u64),
            });
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
impl LockManagerTrait for SqliteLockManager {
    type Error = StorageError;

    fn acquire_object_lock(
        &self,
        object_id: &UnitsObjectId,
        lock_type: LockType,
        transaction_hash: &[u8; 32],
        timeout_ms: Option<u64>,
    ) -> Result<bool, Self::Error> {
        // Create a runtime for async operations
        let rt = tokio::runtime::Runtime::new().map_err(|e| {
            StorageError::Other(format!("Failed to create runtime: {}", e))
        })?;

        rt.block_on(async {
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
                    sqlx::query(
                        r#"
                        UPDATE units_locks
                        SET lock_type = 'write'
                        WHERE object_id = ? AND transaction_hash = ?
                        "#,
                    )
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
            sqlx::query(
                r#"
                INSERT INTO units_locks
                (object_id, lock_type, transaction_hash, acquired_at, timeout_ms)
                VALUES (?, ?, ?, ?, ?)
                "#,
            )
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

    fn release_object_lock(
        &self,
        object_id: &UnitsObjectId,
        transaction_hash: &[u8; 32],
    ) -> Result<bool, Self::Error> {
        let rt = tokio::runtime::Runtime::new().map_err(|e| {
            StorageError::Other(format!("Failed to create runtime: {}", e))
        })?;

        rt.block_on(async {
            // Delete the lock record
            let result = sqlx::query(
                r#"
                DELETE FROM units_locks
                WHERE object_id = ? AND transaction_hash = ?
                "#,
            )
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

    fn get_object_lock_info(
        &self,
        object_id: &UnitsObjectId,
    ) -> Result<Option<LockInfo>, Self::Error> {
        let rt = tokio::runtime::Runtime::new().map_err(|e| {
            StorageError::Other(format!("Failed to create runtime: {}", e))
        })?;

        rt.block_on(async { self.get_object_lock_info_internal(object_id).await })
    }

    fn can_acquire_object_lock(
        &self,
        object_id: &UnitsObjectId,
        intent: AccessIntent,
        transaction_hash: &[u8; 32],
    ) -> Result<bool, Self::Error> {
        let rt = tokio::runtime::Runtime::new().map_err(|e| {
            StorageError::Other(format!("Failed to create runtime: {}", e))
        })?;

        rt.block_on(async {
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

    fn release_all_transaction_locks(
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

    fn get_all_transaction_locks(
        &self,
        transaction_hash: &[u8; 32],
    ) -> Box<dyn Iterator<Item = Result<LockInfo, Self::Error>> + '_> {
        let rt = match tokio::runtime::Runtime::new() {
            Ok(rt) => rt,
            Err(e) => {
                let error = StorageError::Other(format!("Failed to create runtime: {}", e));
                let iter = std::iter::once(Err(error));
                return Box::new(iter);
            }
        };

        // Get locks for this transaction
        let results = match rt.block_on(async {
            sqlx::query(
                r#"
                SELECT object_id, lock_type, acquired_at, timeout_ms
                FROM units_locks
                WHERE transaction_hash = ?
                "#,
            )
            .bind(transaction_hash.as_ref())
            .fetch_all(&self.pool)
            .await
        }) {
            Ok(rows) => rows,
            Err(e) => {
                let error = StorageError::Database(format!("Failed to query transaction locks: {}", e));
                let iter = std::iter::once(Err(error));
                return Box::new(iter);
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

            let lock_type_str: String = row.get("lock_type");
            let lock_type = match lock_type_str.as_str() {
                "read" => LockType::Read,
                "write" => LockType::Write,
                _ => {
                    let error = StorageError::Database(format!("Unknown lock type: {}", lock_type_str));
                    lock_infos.push(Err(error));
                    continue;
                }
            };

            let acquired_at: i64 = row.get("acquired_at");
            let timeout_ms: Option<i64> = row.get("timeout_ms");

            lock_infos.push(Ok(LockInfo {
                object_id,
                lock_type,
                transaction_hash: *transaction_hash,
                acquired_at: acquired_at as u64,
                timeout_ms: timeout_ms.map(|t| t as u64),
            }));
        }

        Box::new(LockInfoVecIterator(lock_infos.into_iter()))
    }

    fn get_all_object_locks(
        &self,
        object_id: &UnitsObjectId,
    ) -> Box<dyn Iterator<Item = Result<LockInfo, Self::Error>> + '_> {
        let rt = match tokio::runtime::Runtime::new() {
            Ok(rt) => rt,
            Err(e) => {
                let error = StorageError::Other(format!("Failed to create runtime: {}", e));
                let iter = std::iter::once(Err(error));
                return Box::new(iter);
            }
        };

        match rt.block_on(self.get_all_object_locks_internal(object_id)) {
            Ok(locks) => {
                if locks.is_empty() {
                    Box::new(EmptyLockIterator::new())
                } else {
                    let iter = locks.into_iter().map(Ok);
                    Box::new(LockInfoVecIterator(iter.collect::<Vec<_>>().into_iter()))
                }
            }
            Err(e) => {
                Box::new(std::iter::once(Err(e)))
            }
        }
    }

    fn cleanup_expired_object_locks(&self) -> Result<usize, Self::Error> {
        let rt = tokio::runtime::Runtime::new().map_err(|e| {
            StorageError::Other(format!("Failed to create runtime: {}", e))
        })?;

        rt.block_on(async {
            let now = current_time_secs() as i64;

            // Delete all expired locks
            let result = sqlx::query(
                r#"
                DELETE FROM units_locks
                WHERE timeout_ms IS NOT NULL AND acquired_at + timeout_ms / 1000 <= ?
                "#,
            )
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

// RocksDb implementation was removed, but we'll keep a stub for backwards compatibility
#[derive(Debug)]
pub struct RocksDbLockManager {
    // Empty placeholder
    _private: (),
}

impl RocksDbLockManager {
    pub fn new(_db: std::sync::Arc<()>, _locks_cf: String) -> Self {
        Self { _private: () }
    }
}

impl LockManagerTrait for RocksDbLockManager {
    type Error = StorageError;

    fn acquire_object_lock(
        &self,
        _object_id: &UnitsObjectId,
        _lock_type: LockType,
        _transaction_hash: &[u8; 32],
        _timeout_ms: Option<u64>,
    ) -> Result<bool, Self::Error> {
        Err(StorageError::Unimplemented("RocksDB support is removed".to_string()))
    }

    fn release_object_lock(
        &self,
        _object_id: &UnitsObjectId,
        _transaction_hash: &[u8; 32],
    ) -> Result<bool, Self::Error> {
        Err(StorageError::Unimplemented("RocksDB support is removed".to_string()))
    }

    fn get_object_lock_info(
        &self,
        _object_id: &UnitsObjectId,
    ) -> Result<Option<LockInfo>, Self::Error> {
        Err(StorageError::Unimplemented("RocksDB support is removed".to_string()))
    }

    fn can_acquire_object_lock(
        &self,
        _object_id: &UnitsObjectId,
        _intent: AccessIntent,
        _transaction_hash: &[u8; 32],
    ) -> Result<bool, Self::Error> {
        Err(StorageError::Unimplemented("RocksDB support is removed".to_string()))
    }

    fn release_all_transaction_locks(
        &self,
        _transaction_hash: &[u8; 32],
    ) -> Result<usize, Self::Error> {
        Err(StorageError::Unimplemented("RocksDB support is removed".to_string()))
    }

    fn get_all_transaction_locks(
        &self,
        _transaction_hash: &[u8; 32],
    ) -> Box<dyn Iterator<Item = Result<LockInfo, Self::Error>> + '_> {
        let error = Err(StorageError::Unimplemented("RocksDB support is removed".to_string()));
        Box::new(std::iter::once(error))
    }

    fn get_all_object_locks(
        &self, 
        _object_id: &UnitsObjectId
    ) -> Box<dyn Iterator<Item = Result<LockInfo, Self::Error>> + '_> {
        let error = Err(StorageError::Unimplemented("RocksDB support is removed".to_string()));
        Box::new(std::iter::once(error))
    }

    fn cleanup_expired_object_locks(&self) -> Result<usize, Self::Error> {
        Err(StorageError::Unimplemented("RocksDB support is removed".to_string()))
    }
}

// Adapter to implement PersistentLockManager for LockManagerTrait implementations
pub struct LockManagerAdapter<T: LockManagerTrait<Error = StorageError> + Debug> {
    inner: T,
}

impl<T: LockManagerTrait<Error = StorageError> + Debug> Debug for LockManagerAdapter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LockManagerAdapter")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<T: LockManagerTrait<Error = StorageError> + Debug> LockManagerAdapter<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T: LockManagerTrait<Error = StorageError> + Debug> PersistentLockManager for LockManagerAdapter<T> {
    type Error = T::Error;

    fn acquire_lock(
        &self,
        object_id: &UnitsObjectId,
        lock_type: LockType,
        transaction_hash: &[u8; 32],
        timeout_ms: Option<u64>,
    ) -> Result<bool, Self::Error> {
        self.inner.acquire_object_lock(object_id, lock_type, transaction_hash, timeout_ms)
    }

    fn release_lock(
        &self,
        object_id: &UnitsObjectId,
        transaction_hash: &[u8; 32],
    ) -> Result<bool, Self::Error> {
        self.inner.release_object_lock(object_id, transaction_hash)
    }

    fn get_lock_info(
        &self,
        object_id: &UnitsObjectId,
    ) -> Result<Option<LockInfo>, Self::Error> {
        self.inner.get_object_lock_info(object_id)
    }

    fn can_acquire_lock(
        &self,
        object_id: &UnitsObjectId,
        intent: AccessIntent,
        transaction_hash: &[u8; 32],
    ) -> Result<bool, Self::Error> {
        self.inner.can_acquire_object_lock(object_id, intent, transaction_hash)
    }

    fn release_transaction_locks(
        &self,
        transaction_hash: &[u8; 32],
    ) -> Result<usize, Self::Error> {
        self.inner.release_all_transaction_locks(transaction_hash)
    }

    fn get_transaction_locks(
        &self,
        transaction_hash: &[u8; 32],
    ) -> Box<dyn UnitsLockIterator<Self::Error> + '_> {
        let locks = self.inner.get_all_transaction_locks(transaction_hash);
        let lock_vec: Vec<Result<LockInfo, Self::Error>> = locks.collect();
        Box::new(LockInfoVecIterator(lock_vec.into_iter()))
    }

    fn get_object_locks(
        &self,
        object_id: &UnitsObjectId,
    ) -> Box<dyn UnitsLockIterator<Self::Error> + '_> {
        let locks = self.inner.get_all_object_locks(object_id);
        let lock_vec: Vec<Result<LockInfo, Self::Error>> = locks.collect();
        Box::new(LockInfoVecIterator(lock_vec.into_iter()))
    }

    fn cleanup_expired_locks(&self) -> Result<usize, Self::Error> {
        self.inner.cleanup_expired_object_locks()
    }
}

/// Create an adapter to use a LockManagerTrait implementation with the PersistentLockManager trait
pub fn create_lock_manager_adapter<T: LockManagerTrait<Error = StorageError> + Debug>(
    lock_manager: T,
) -> LockManagerAdapter<T> {
    LockManagerAdapter::new(lock_manager)
}