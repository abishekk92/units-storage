use std::fmt::Debug;
use units_core::error::StorageError;
use units_core::id::UnitsObjectId;
use units_core::locks::{AccessIntent, LockInfo, LockType, PersistentLockManager, UnitsLockIterator};
use std::time::{SystemTime, UNIX_EPOCH};
#[cfg(feature = "sqlite")]
use sqlx::Row;

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
        .expect("Time went backwards")
        .as_secs()
}

/// Implementation of lock manager for SQLite
#[cfg(feature = "sqlite")]
pub struct SqliteLockManager {
    pool: sqlx::sqlite::SqlitePool,
    rt: std::sync::Arc<tokio::runtime::Runtime>,
}

#[cfg(feature = "sqlite")]
impl SqliteLockManager {
    /// Create a new SQLite lock manager
    pub fn new(
        pool: sqlx::sqlite::SqlitePool,
        rt: std::sync::Arc<tokio::runtime::Runtime>,
    ) -> Result<Self, StorageError> {
        let lock_manager = Self { pool, rt };
        lock_manager.initialize_schema()?;
        Ok(lock_manager)
    }

    /// Initialize the lock tables
    fn initialize_schema(&self) -> Result<(), StorageError> {
        self.rt.block_on(async {
            // Enable foreign key constraints
            sqlx::query("PRAGMA foreign_keys = ON")
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    StorageError::Database(format!("Failed to enable foreign keys: {}", e))
                })?;
            
            // Table for locks with foreign key to objects table
            sqlx::query(
                "CREATE TABLE IF NOT EXISTS object_locks (
                    object_id BLOB NOT NULL,
                    lock_type INTEGER NOT NULL,
                    transaction_hash BLOB NOT NULL,
                    acquired_at INTEGER NOT NULL,
                    timeout_ms INTEGER,
                    PRIMARY KEY (object_id),
                    FOREIGN KEY (object_id) REFERENCES objects(id) ON DELETE CASCADE
                )",
            )
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Database(format!("Failed to create locks table: {}", e)))?;

            // Index for transaction locks
            sqlx::query(
                "CREATE INDEX IF NOT EXISTS idx_object_locks_transaction_hash 
                 ON object_locks(transaction_hash)",
            )
            .execute(&self.pool)
            .await
            .map_err(|e| {
                StorageError::Database(format!("Failed to create transaction hash index: {}", e))
            })?;
            
            // Index for monitoring expired locks
            sqlx::query(
                "CREATE INDEX IF NOT EXISTS idx_object_locks_expiry 
                 ON object_locks(acquired_at, timeout_ms) 
                 WHERE timeout_ms IS NOT NULL",
            )
            .execute(&self.pool)
            .await
            .map_err(|e| {
                StorageError::Database(format!("Failed to create expiry index: {}", e))
            })?;

            Ok(())
        })
    }

    // Helper method to convert LockType to integer for storage
    fn lock_type_to_int(lock_type: LockType) -> i64 {
        match lock_type {
            LockType::Read => 0,
            LockType::Write => 1,
        }
    }

    // Helper method to convert integer to LockType
    fn int_to_lock_type(value: i64) -> Result<LockType, StorageError> {
        match value {
            0 => Ok(LockType::Read),
            1 => Ok(LockType::Write),
            _ => Err(StorageError::Other(format!(
                "Invalid lock type value: {}",
                value
            ))),
        }
    }

    // Check if read lock can be acquired
    async fn can_acquire_read_lock(
        &self,
        object_id: &UnitsObjectId,
        transaction_hash: &[u8; 32],
    ) -> Result<bool, StorageError> {
        // Get current lock if any
        let row = sqlx::query(
            "SELECT lock_type, transaction_hash FROM object_locks WHERE object_id = ?",
        )
        .bind(object_id.as_ref())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Database(format!("Database error: {}", e)))?;

        match row {
            Some(row) => {
                let lock_type: i64 = row.get(0);
                let existing_tx_hash: Vec<u8> = row.get(1);

                // Convert the existing transaction hash to array
                let mut tx_hash_array = [0u8; 32];
                if existing_tx_hash.len() == 32 {
                    tx_hash_array.copy_from_slice(&existing_tx_hash);
                }

                // If the lock is already held by this transaction, allow it
                if &tx_hash_array == transaction_hash {
                    return Ok(true);
                }

                // Read locks are compatible with other read locks
                match Self::int_to_lock_type(lock_type)? {
                    LockType::Read => Ok(true),
                    LockType::Write => Ok(false), // Cannot acquire read lock if write lock exists
                }
            }
            None => Ok(true), // No existing lock, can acquire read lock
        }
    }

    // Check if write lock can be acquired
    async fn can_acquire_write_lock(
        &self,
        object_id: &UnitsObjectId,
        transaction_hash: &[u8; 32],
    ) -> Result<bool, StorageError> {
        // Get current lock if any
        let row = sqlx::query(
            "SELECT lock_type, transaction_hash FROM object_locks WHERE object_id = ?",
        )
        .bind(object_id.as_ref())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Database(format!("Database error: {}", e)))?;

        match row {
            Some(row) => {
                let existing_tx_hash: Vec<u8> = row.get(1);

                // Convert the existing transaction hash to array
                let mut tx_hash_array = [0u8; 32];
                if existing_tx_hash.len() == 32 {
                    tx_hash_array.copy_from_slice(&existing_tx_hash);
                }

                // If the lock is already held by this transaction, allow it
                if &tx_hash_array == transaction_hash {
                    return Ok(true);
                }

                // Otherwise, cannot acquire write lock if any lock exists
                Ok(false)
            }
            None => Ok(true), // No existing lock, can acquire write lock
        }
    }

    // Helper to clean up expired lock
    async fn check_and_cleanup_expired_lock(
        &self,
        object_id: &UnitsObjectId,
    ) -> Result<bool, StorageError> {
        let now = current_time_secs();

        // Get the lock info if it exists
        let row = sqlx::query(
            "SELECT acquired_at, timeout_ms FROM object_locks 
             WHERE object_id = ?",
        )
        .bind(object_id.as_ref())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Database(format!("Database error: {}", e)))?;

        match row {
            Some(row) => {
                let acquired_at: i64 = row.get(0);
                let timeout_ms: Option<i64> = row.get(1);

                // Check if the lock has expired
                if let Some(timeout) = timeout_ms {
                    let timeout_secs = timeout / 1000; // Convert to seconds
                    if now >= (acquired_at as u64 + timeout_secs as u64) {
                        // Lock has expired, remove it
                        sqlx::query("DELETE FROM object_locks WHERE object_id = ?")
                            .bind(object_id.as_ref())
                            .execute(&self.pool)
                            .await
                            .map_err(|e| {
                                StorageError::Database(format!("Failed to delete expired lock: {}", e))
                            })?;
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            None => Ok(false), // No lock exists
        }
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
        self.rt.block_on(async {
            // First check if there's an expired lock that needs to be cleaned up
            let _cleaned_up = self.check_and_cleanup_expired_lock(object_id).await?;

            // Check if we can acquire the lock
            let can_acquire = match lock_type {
                LockType::Read => self.can_acquire_read_lock(object_id, transaction_hash).await?,
                LockType::Write => self.can_acquire_write_lock(object_id, transaction_hash).await?,
            };

            if !can_acquire {
                return Err(StorageError::Other(format!("Lock conflict for object ID {:?}", *object_id)));
            }

            // Acquire the lock
            let lock_type_int = Self::lock_type_to_int(lock_type);
            let now = current_time_secs();

            sqlx::query(
                "INSERT OR REPLACE INTO object_locks 
                 (object_id, lock_type, transaction_hash, acquired_at, timeout_ms) 
                 VALUES (?, ?, ?, ?, ?)",
            )
            .bind(object_id.as_ref())
            .bind(lock_type_int)
            .bind(transaction_hash.as_ref())
            .bind(now as i64)
            .bind(timeout_ms.map(|ms| ms as i64))
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Database(format!("Failed to acquire lock: {}", e)))?;

            Ok(true)
        })
    }

    fn release_lock(
        &self,
        object_id: &UnitsObjectId,
        transaction_hash: &[u8; 32],
    ) -> Result<bool, Self::Error> {
        self.rt.block_on(async {
            // Release the lock only if it's held by this transaction
            let result = sqlx::query(
                "DELETE FROM object_locks 
                 WHERE object_id = ? AND transaction_hash = ?",
            )
            .bind(object_id.as_ref())
            .bind(transaction_hash.as_ref())
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Database(format!("Failed to release lock: {}", e)))?;

            // If rows affected is 0, lock was not held by this transaction
            Ok(result.rows_affected() > 0)
        })
    }

    fn get_lock_info(&self, object_id: &UnitsObjectId) -> Result<Option<LockInfo>, Self::Error> {
        self.rt.block_on(async {
            // First check if there's an expired lock that needs to be cleaned up
            let cleaned_up = self.check_and_cleanup_expired_lock(object_id).await?;
            if cleaned_up {
                return Ok(None);
            }

            // Get the lock info
            let row = sqlx::query(
                "SELECT lock_type, transaction_hash, acquired_at, timeout_ms 
                 FROM object_locks WHERE object_id = ?",
            )
            .bind(object_id.as_ref())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Database(format!("Database error: {}", e)))?;

            match row {
                Some(row) => {
                    let lock_type_int: i64 = row.get(0);
                    let transaction_hash_blob: Vec<u8> = row.get(1);
                    let acquired_at: i64 = row.get(2);
                    let timeout_ms: Option<i64> = row.get(3);

                    // Convert lock type
                    let lock_type = Self::int_to_lock_type(lock_type_int)?;

                    // Convert transaction hash to array
                    let mut transaction_hash = [0u8; 32];
                    if transaction_hash_blob.len() == 32 {
                        transaction_hash.copy_from_slice(&transaction_hash_blob);
                    }

                    Ok(Some(LockInfo {
                        object_id: *object_id,
                        lock_type,
                        transaction_hash,
                        acquired_at: acquired_at as u64,
                        timeout_ms: timeout_ms.map(|ms| ms as u64),
                    }))
                }
                None => Ok(None),
            }
        })
    }

    fn can_acquire_lock(
        &self,
        object_id: &UnitsObjectId,
        intent: AccessIntent,
        transaction_hash: &[u8; 32],
    ) -> Result<bool, Self::Error> {
        self.rt.block_on(async {
            // First check if there's an expired lock that needs to be cleaned up
            let _cleaned_up = self.check_and_cleanup_expired_lock(object_id).await?;

            // Convert AccessIntent to LockType
            let lock_type = match intent {
                AccessIntent::Read => LockType::Read,
                AccessIntent::Write => LockType::Write,
            };

            // Check if we can acquire the lock
            match lock_type {
                LockType::Read => self.can_acquire_read_lock(object_id, transaction_hash).await,
                LockType::Write => self.can_acquire_write_lock(object_id, transaction_hash).await,
            }
        })
    }

    fn release_transaction_locks(&self, transaction_hash: &[u8; 32]) -> Result<usize, Self::Error> {
        self.rt.block_on(async {
            // Release all locks held by this transaction
            let result = sqlx::query(
                "DELETE FROM object_locks 
                 WHERE transaction_hash = ?",
            )
            .bind(transaction_hash.as_ref())
            .execute(&self.pool)
            .await
            .map_err(|e| {
                StorageError::Database(format!("Failed to release transaction locks: {}", e))
            })?;

            Ok(result.rows_affected() as usize)
        })
    }

    fn get_transaction_locks(
        &self,
        transaction_hash: &[u8; 32],
    ) -> Box<dyn UnitsLockIterator<Self::Error> + '_> {
        // Create a Vec to hold all the locks
        let locks = self.rt.block_on(async {
            let rows = sqlx::query(
                "SELECT object_id, lock_type, acquired_at, timeout_ms 
                 FROM object_locks 
                 WHERE transaction_hash = ?",
            )
            .bind(transaction_hash.as_ref())
            .fetch_all(&self.pool)
            .await;

            match rows {
                Ok(rows) => {
                    let mut locks = Vec::new();
                    for row in rows {
                        let object_id_blob: Vec<u8> = row.get(0);
                        let lock_type_int: i64 = row.get(1);
                        let acquired_at: i64 = row.get(2);
                        let timeout_ms: Option<i64> = row.get(3);

                        // Convert object ID to array
                        let mut object_id_array = [0u8; 32];
                        if object_id_blob.len() == 32 {
                            object_id_array.copy_from_slice(&object_id_blob);
                        }
                        let object_id = UnitsObjectId::new(object_id_array);

                        // Convert lock type
                        let lock_type = match Self::int_to_lock_type(lock_type_int) {
                            Ok(lt) => lt,
                            Err(e) => {
                                return vec![Err(e)];
                            }
                        };

                        locks.push(Ok(LockInfo {
                            object_id,
                            lock_type,
                            transaction_hash: *transaction_hash,
                            acquired_at: acquired_at as u64,
                            timeout_ms: timeout_ms.map(|ms| ms as u64),
                        }));
                    }
                    locks
                }
                Err(e) => {
                    vec![Err(StorageError::Database(format!(
                        "Failed to get transaction locks: {}",
                        e
                    )))]
                }
            }
        });

        // Create an iterator from the Vec using our wrapper
        Box::new(LockInfoVecIterator(locks.into_iter()))
    }

    fn get_object_locks(&self, object_id: &UnitsObjectId) -> Box<dyn UnitsLockIterator<Self::Error> + '_> {
        // First check if there's an expired lock that needs to be cleaned up
        let cleanup_result = self.rt.block_on(async {
            self.check_and_cleanup_expired_lock(object_id).await
        });

        // If the check failed, return an iterator with the error
        if let Err(e) = cleanup_result {
            let error_iter = vec![Err(e)].into_iter();
            return Box::new(LockInfoVecIterator(error_iter));
        }

        // Get the lock info
        let lock_info = self.rt.block_on(async {
            let row = sqlx::query(
                "SELECT lock_type, transaction_hash, acquired_at, timeout_ms 
                 FROM object_locks WHERE object_id = ?",
            )
            .bind(object_id.as_ref())
            .fetch_optional(&self.pool)
            .await;

            match row {
                Ok(Some(row)) => {
                    let lock_type_int: i64 = row.get(0);
                    let transaction_hash_blob: Vec<u8> = row.get(1);
                    let acquired_at: i64 = row.get(2);
                    let timeout_ms: Option<i64> = row.get(3);

                    // Convert lock type
                    let lock_type = match Self::int_to_lock_type(lock_type_int) {
                        Ok(lt) => lt,
                        Err(e) => return vec![Err(e)],
                    };

                    // Convert transaction hash to array
                    let mut transaction_hash = [0u8; 32];
                    if transaction_hash_blob.len() == 32 {
                        transaction_hash.copy_from_slice(&transaction_hash_blob);
                    }

                    vec![Ok(LockInfo {
                        object_id: *object_id,
                        lock_type,
                        transaction_hash,
                        acquired_at: acquired_at as u64,
                        timeout_ms: timeout_ms.map(|ms| ms as u64),
                    })]
                }
                Ok(None) => Vec::new(),
                Err(e) => {
                    vec![Err(StorageError::Database(format!(
                        "Failed to get object locks: {}",
                        e
                    )))]
                }
            }
        });

        // Create an iterator from the Vec using our wrapper
        Box::new(LockInfoVecIterator(lock_info.into_iter()))
    }

    fn cleanup_expired_locks(&self) -> Result<usize, Self::Error> {
        self.rt.block_on(async {
            let now = current_time_secs();

            // Delete all locks that have expired
            let result = sqlx::query(
                "DELETE FROM object_locks 
                 WHERE timeout_ms IS NOT NULL AND (acquired_at + timeout_ms/1000) < ?",
            )
            .bind(now as i64)
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

/// Implementation of lock manager for RocksDB
#[cfg(feature = "rocksdb")]
pub struct RocksDbLockManager {
    db: std::sync::Arc<rocksdb::DB>,
    locks_cf: String,
}

#[cfg(feature = "rocksdb")]
impl RocksDbLockManager {
    /// Create a new RocksDB lock manager
    pub fn new(db: std::sync::Arc<rocksdb::DB>, locks_cf: String) -> Self {
        Self { db, locks_cf }
    }

    // Helper to serialize a LockInfo to bytes
    fn serialize_lock_info(&self, lock_info: &LockInfo) -> Result<Vec<u8>, StorageError> {
        bincode::serialize(lock_info).map_err(|e| {
            StorageError::Serialization(format!("Failed to serialize lock info: {}", e))
        })
    }

    // Helper to deserialize bytes to a LockInfo
    fn deserialize_lock_info(&self, bytes: &[u8]) -> Result<LockInfo, StorageError> {
        bincode::deserialize(bytes).map_err(|e| {
            StorageError::Serialization(format!("Failed to deserialize lock info: {}", e))
        })
    }

    // Make a key for transaction locks index
    fn make_transaction_key(&self, transaction_hash: &[u8; 32], object_id: &UnitsObjectId) -> Vec<u8> {
        let mut key = Vec::with_capacity(65);
        key.extend_from_slice(transaction_hash);
        key.push(b':'); // Separator
        key.extend_from_slice(object_id.as_ref());
        key
    }

    // Check if read lock can be acquired
    fn can_acquire_read_lock(
        &self,
        object_id: &UnitsObjectId,
        transaction_hash: &[u8; 32],
    ) -> Result<bool, StorageError> {
        // Get the lock CF
        let cf = match self.db.cf_handle(&self.locks_cf) {
            Some(cf) => cf,
            None => {
                return Err(StorageError::Database(format!(
                    "Column family {} not found",
                    self.locks_cf
                )))
            }
        };

        // Get current lock if any
        match self.db.get_cf(&cf, object_id.as_ref()) {
            Ok(Some(bytes)) => {
                let lock_info = self.deserialize_lock_info(&bytes)?;

                // If the lock is already held by this transaction, allow it
                if &lock_info.transaction_hash == transaction_hash {
                    return Ok(true);
                }

                // Check if the lock has expired
                if let Some(timeout_ms) = lock_info.timeout_ms {
                    let now = current_time_secs();
                    if now >= (lock_info.acquired_at + timeout_ms / 1000) {
                        // Lock has expired, can acquire
                        return Ok(true);
                    }
                }

                // Read locks are compatible with other read locks
                match lock_info.lock_type {
                    LockType::Read => Ok(true),
                    LockType::Write => Ok(false), // Cannot acquire read lock if write lock exists
                }
            }
            Ok(None) => Ok(true), // No existing lock, can acquire read lock
            Err(e) => Err(StorageError::Database(format!("Database error: {}", e))),
        }
    }

    // Check if write lock can be acquired
    fn can_acquire_write_lock(
        &self,
        object_id: &UnitsObjectId,
        transaction_hash: &[u8; 32],
    ) -> Result<bool, StorageError> {
        // Get the lock CF
        let cf = match self.db.cf_handle(&self.locks_cf) {
            Some(cf) => cf,
            None => {
                return Err(StorageError::Database(format!(
                    "Column family {} not found",
                    self.locks_cf
                )))
            }
        };

        // Get current lock if any
        match self.db.get_cf(&cf, object_id.as_ref()) {
            Ok(Some(bytes)) => {
                let lock_info = self.deserialize_lock_info(&bytes)?;

                // If the lock is already held by this transaction, allow it
                if &lock_info.transaction_hash == transaction_hash {
                    return Ok(true);
                }

                // Check if the lock has expired
                if let Some(timeout_ms) = lock_info.timeout_ms {
                    let now = current_time_secs();
                    if now >= (lock_info.acquired_at + timeout_ms / 1000) {
                        // Lock has expired, can acquire
                        return Ok(true);
                    }
                }

                // Otherwise, cannot acquire write lock if any lock exists
                Ok(false)
            }
            Ok(None) => Ok(true), // No existing lock, can acquire write lock
            Err(e) => Err(StorageError::Database(format!("Database error: {}", e))),
        }
    }

    // Helper to clean up an expired lock
    fn check_and_cleanup_expired_lock(&self, object_id: &UnitsObjectId) -> Result<bool, StorageError> {
        // Get the lock CF
        let cf = match self.db.cf_handle(&self.locks_cf) {
            Some(cf) => cf,
            None => {
                return Err(StorageError::Database(format!(
                    "Column family {} not found",
                    self.locks_cf
                )))
            }
        };

        // Get the lock info if it exists
        match self.db.get_cf(&cf, object_id.as_ref()) {
            Ok(Some(bytes)) => {
                let lock_info = self.deserialize_lock_info(&bytes)?;
                let now = current_time_secs();

                // Check if the lock has expired
                if let Some(timeout_ms) = lock_info.timeout_ms {
                    if now >= (lock_info.acquired_at + timeout_ms / 1000) {
                        // Lock has expired, remove it
                        // Also remove the transaction index entry
                        let transaction_key = self.make_transaction_key(
                            &lock_info.transaction_hash,
                            &lock_info.object_id,
                        );

                        // Use a write batch to delete both entries atomically
                        let mut batch = rocksdb::WriteBatch::default();
                        batch.delete_cf(&cf, object_id.as_ref());
                        batch.delete_cf(&cf, &transaction_key);

                        self.db.write(batch).map_err(|e| {
                            StorageError::Database(format!("Failed to delete expired lock: {}", e))
                        })?;

                        return Ok(true);
                    }
                }
                Ok(false)
            }
            Ok(None) => Ok(false), // No lock exists
            Err(e) => Err(StorageError::Database(format!("Database error: {}", e))),
        }
    }
}

#[cfg(feature = "rocksdb")]
impl PersistentLockManager for RocksDbLockManager {
    type Error = StorageError;

    fn acquire_lock(
        &self,
        object_id: &UnitsObjectId,
        lock_type: LockType,
        transaction_hash: &[u8; 32],
        timeout_ms: Option<u64>,
    ) -> Result<bool, Self::Error> {
        // First check if there's an expired lock that needs to be cleaned up
        let _cleaned_up = self.check_and_cleanup_expired_lock(object_id)?;

        // Check if we can acquire the lock
        let can_acquire = match lock_type {
            LockType::Read => self.can_acquire_read_lock(object_id, transaction_hash)?,
            LockType::Write => self.can_acquire_write_lock(object_id, transaction_hash)?,
        };

        if !can_acquire {
            return Err(StorageError::Other(format!("Lock conflict for object ID {:?}", *object_id)));
        }

        // Get the lock CF
        let cf = match self.db.cf_handle(&self.locks_cf) {
            Some(cf) => cf,
            None => {
                return Err(StorageError::Database(format!(
                    "Column family {} not found",
                    self.locks_cf
                )))
            }
        };

        // Create lock info
        let lock_info = LockInfo {
            object_id: *object_id,
            lock_type,
            transaction_hash: *transaction_hash,
            acquired_at: current_time_secs(),
            timeout_ms,
        };

        // Serialize lock info
        let serialized = self.serialize_lock_info(&lock_info)?;

        // Create transaction key for index
        let transaction_key = self.make_transaction_key(transaction_hash, object_id);

        // Use a write batch to store both entries atomically
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&cf, object_id.as_ref(), &serialized);
        batch.put_cf(&cf, &transaction_key, &[]); // Use empty value, key is enough

        // Write batch
        self.db
            .write(batch)
            .map_err(|e| StorageError::Database(format!("Failed to acquire lock: {}", e)))?;

        Ok(true)
    }

    fn release_lock(
        &self,
        object_id: &UnitsObjectId,
        transaction_hash: &[u8; 32],
    ) -> Result<bool, Self::Error> {
        // Get the lock CF
        let cf = match self.db.cf_handle(&self.locks_cf) {
            Some(cf) => cf,
            None => {
                return Err(StorageError::Database(format!(
                    "Column family {} not found",
                    self.locks_cf
                )))
            }
        };

        // Get the lock info if it exists
        match self.db.get_cf(&cf, object_id.as_ref()) {
            Ok(Some(bytes)) => {
                let lock_info = self.deserialize_lock_info(&bytes)?;

                // Check if the lock is held by this transaction
                if &lock_info.transaction_hash != transaction_hash {
                    return Ok(false);
                }

                // Create transaction key for index
                let transaction_key = self.make_transaction_key(transaction_hash, object_id);

                // Use a write batch to delete both entries atomically
                let mut batch = rocksdb::WriteBatch::default();
                batch.delete_cf(&cf, object_id.as_ref());
                batch.delete_cf(&cf, &transaction_key);

                // Write batch
                self.db
                    .write(batch)
                    .map_err(|e| StorageError::Database(format!("Failed to release lock: {}", e)))?;

                Ok(true)
            }
            Ok(None) => Ok(false), // No lock exists
            Err(e) => Err(StorageError::Database(format!("Database error: {}", e))),
        }
    }

    fn get_lock_info(&self, object_id: &UnitsObjectId) -> Result<Option<LockInfo>, Self::Error> {
        // First check if there's an expired lock that needs to be cleaned up
        let cleaned_up = self.check_and_cleanup_expired_lock(object_id)?;
        if cleaned_up {
            return Ok(None);
        }

        // Get the lock CF
        let cf = match self.db.cf_handle(&self.locks_cf) {
            Some(cf) => cf,
            None => {
                return Err(StorageError::Database(format!(
                    "Column family {} not found",
                    self.locks_cf
                )))
            }
        };

        // Get the lock info if it exists
        match self.db.get_cf(&cf, object_id.as_ref()) {
            Ok(Some(bytes)) => {
                let lock_info = self.deserialize_lock_info(&bytes)?;
                Ok(Some(lock_info))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(StorageError::Database(format!("Database error: {}", e))),
        }
    }

    fn can_acquire_lock(
        &self,
        object_id: &UnitsObjectId,
        intent: AccessIntent,
        transaction_hash: &[u8; 32],
    ) -> Result<bool, Self::Error> {
        // First check if there's an expired lock that needs to be cleaned up
        let _cleaned_up = self.check_and_cleanup_expired_lock(object_id)?;

        // Convert AccessIntent to LockType
        let lock_type = match intent {
            AccessIntent::Read => LockType::Read,
            AccessIntent::Write => LockType::Write,
        };

        // Check if we can acquire the lock
        match lock_type {
            LockType::Read => self.can_acquire_read_lock(object_id, transaction_hash),
            LockType::Write => self.can_acquire_write_lock(object_id, transaction_hash),
        }
    }

    fn release_transaction_locks(&self, transaction_hash: &[u8; 32]) -> Result<usize, Self::Error> {
        // Get the lock CF
        let cf = match self.db.cf_handle(&self.locks_cf) {
            Some(cf) => cf,
            None => {
                return Err(StorageError::Database(format!(
                    "Column family {} not found",
                    self.locks_cf
                )))
            }
        };

        // Use a prefix iterator to find all locks with this transaction hash
        // The prefix for transaction locks is the transaction hash itself
        let prefix = transaction_hash.to_vec();
        let mut count = 0;
        let mut locks_to_delete = Vec::new();

        // Get iterator options with prefix seek
        let mut opts = rocksdb::ReadOptions::default();
        opts.set_prefix_same_as_start(true);

        // Iterate through all keys with the transaction hash prefix
        let iter = self.db.iterator_cf_opt(&cf, opts, rocksdb::IteratorMode::Start);
        for item in iter {
            match item {
                Ok((key, _)) => {
                    // Check if the key starts with our prefix
                    if key.starts_with(&prefix) && key.len() > prefix.len() && key[prefix.len()] == b':' {
                        // Extract the object ID from the key
                        let object_id_bytes = &key[prefix.len() + 1..];
                        if object_id_bytes.len() == 32 {
                            let mut object_id_array = [0u8; 32];
                            object_id_array.copy_from_slice(object_id_bytes);
                            let object_id = UnitsObjectId::new(object_id_array);

                            // Add both the transaction key and the object key to the delete list
                            locks_to_delete.push(key.to_vec()); // Transaction key
                            locks_to_delete.push(object_id.as_ref().to_vec()); // Object key
                            count += 1;
                        }
                    }
                }
                Err(e) => {
                    return Err(StorageError::Database(format!(
                        "Error iterating through transaction locks: {}",
                        e
                    )))
                }
            }
        }

        // Delete all locks in a single batch
        if !locks_to_delete.is_empty() {
            let mut batch = rocksdb::WriteBatch::default();
            for key in locks_to_delete {
                batch.delete_cf(&cf, &key);
            }

            self.db.write(batch).map_err(|e| {
                StorageError::Database(format!(
                    "Failed to release transaction locks: {}",
                    e
                ))
            })?;
        }

        Ok(count)
    }

    fn get_transaction_locks(
        &self,
        transaction_hash: &[u8; 32],
    ) -> Box<dyn UnitsLockIterator<Self::Error> + '_> {
        // To avoid lifetime issues, let's load all locks into a Vec first
        let mut locks = Vec::new();

        // Get the lock CF
        let cf = match self.db.cf_handle(&self.locks_cf) {
            Some(cf) => cf,
            None => {
                let error = StorageError::Database(format!("Column family {} not found", self.locks_cf));
                return Box::new(LockInfoVecIterator(vec![Err(error)].into_iter()));
            }
        };

        // Use a prefix iterator to find all locks with this transaction hash
        let prefix = transaction_hash.to_vec();

        // Get iterator options with prefix seek
        let mut opts = rocksdb::ReadOptions::default();
        opts.set_prefix_same_as_start(true);

        // Iterate through all keys with the transaction hash prefix
        let iter = self.db.iterator_cf_opt(&cf, opts, rocksdb::IteratorMode::Start);
        for item in iter {
            match item {
                Ok((key, _)) => {
                    // Check if the key starts with our prefix
                    if key.starts_with(&prefix) && key.len() > prefix.len() && key[prefix.len()] == b':' {
                        // Extract the object ID from the key
                        let object_id_bytes = &key[prefix.len() + 1..];
                        if object_id_bytes.len() == 32 {
                            let mut object_id_array = [0u8; 32];
                            object_id_array.copy_from_slice(object_id_bytes);
                            let object_id = UnitsObjectId::new(object_id_array);

                            // Get the lock info
                            match self.db.get_cf(&cf, object_id.as_ref()) {
                                Ok(Some(bytes)) => {
                                    match self.deserialize_lock_info(&bytes) {
                                        Ok(lock_info) => {
                                            locks.push(Ok(lock_info));
                                        }
                                        Err(e) => {
                                            locks.push(Err(e));
                                        }
                                    }
                                }
                                Ok(None) => {
                                    // Lock was deleted, skip
                                }
                                Err(e) => {
                                    locks.push(Err(StorageError::Database(format!(
                                        "Error getting lock info: {}",
                                        e
                                    ))));
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    locks.push(Err(StorageError::Database(format!(
                        "Error iterating through transaction locks: {}",
                        e
                    ))));
                    break;
                }
            }
        }

        // If no locks are found, return an empty iterator
        if locks.is_empty() {
            Box::new(EmptyLockIterator::new())
        } else {
            Box::new(LockInfoVecIterator(locks.into_iter()))
        }
    }

    fn get_object_locks(&self, object_id: &UnitsObjectId) -> Box<dyn UnitsLockIterator<Self::Error> + '_> {
        // First check if there's an expired lock that needs to be cleaned up
        match self.check_and_cleanup_expired_lock(object_id) {
            Err(e) => {
                return Box::new(LockInfoVecIterator(vec![Err(e)].into_iter()));
            }
            Ok(true) => {
                // Lock was expired and cleaned up, return empty iterator
                return Box::new(EmptyLockIterator::new());
            }
            Ok(false) => {
                // Continue checking for locks
            }
        }

        // Get the lock CF
        let cf = match self.db.cf_handle(&self.locks_cf) {
            Some(cf) => cf,
            None => {
                let error = StorageError::Database(format!("Column family {} not found", self.locks_cf));
                return Box::new(LockInfoVecIterator(vec![Err(error)].into_iter()));
            }
        };

        // Get the lock info if it exists
        match self.db.get_cf(&cf, object_id.as_ref()) {
            Ok(Some(bytes)) => {
                match self.deserialize_lock_info(&bytes) {
                    Ok(lock_info) => {
                        Box::new(LockInfoVecIterator(vec![Ok(lock_info)].into_iter()))
                    }
                    Err(e) => {
                        Box::new(LockInfoVecIterator(vec![Err(e)].into_iter()))
                    }
                }
            }
            Ok(None) => {
                Box::new(EmptyLockIterator::new())
            }
            Err(e) => {
                let error = StorageError::Database(format!("Error getting lock info: {}", e));
                Box::new(LockInfoVecIterator(vec![Err(error)].into_iter()))
            }
        }
    }

    fn cleanup_expired_locks(&self) -> Result<usize, Self::Error> {
        // Get the lock CF
        let cf = match self.db.cf_handle(&self.locks_cf) {
            Some(cf) => cf,
            None => {
                return Err(StorageError::Database(format!(
                    "Column family {} not found",
                    self.locks_cf
                )))
            }
        };

        let now = current_time_secs();
        let mut count = 0;
        let mut expired_locks = Vec::new();

        // Iterate through all locks
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        for item in iter {
            match item {
                Ok((key, value)) => {
                    // Only process keys that are object IDs (32 bytes)
                    if key.len() == 32 {
                        match self.deserialize_lock_info(&value) {
                            Ok(lock_info) => {
                                // Check if the lock has expired
                                if let Some(timeout_ms) = lock_info.timeout_ms {
                                    if now >= (lock_info.acquired_at + timeout_ms / 1000) {
                                        // Create transaction key for index
                                        let transaction_key = self.make_transaction_key(
                                            &lock_info.transaction_hash,
                                            &lock_info.object_id,
                                        );

                                        // Add both keys to the expired locks list
                                        expired_locks.push(key.to_vec());
                                        expired_locks.push(transaction_key);
                                        count += 1;
                                    }
                                }
                            }
                            Err(_) => {
                                // Skip invalid lock info
                            }
                        }
                    }
                }
                Err(e) => {
                    return Err(StorageError::Database(format!(
                        "Error iterating through locks: {}",
                        e
                    )))
                }
            }
        }

        // Delete all expired locks in a single batch
        if !expired_locks.is_empty() {
            let mut batch = rocksdb::WriteBatch::default();
            for key in expired_locks {
                batch.delete_cf(&cf, &key);
            }

            self.db.write(batch).map_err(|e| {
                StorageError::Database(format!("Failed to delete expired locks: {}", e))
            })?;
        }

        Ok(count)
    }
}

#[cfg(feature = "rocksdb")]
impl Debug for RocksDbLockManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksDbLockManager")
            .field("locks_cf", &self.locks_cf)
            .finish()
    }
}