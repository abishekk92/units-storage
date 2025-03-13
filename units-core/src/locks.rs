use serde::{Deserialize, Serialize};
use std::iter::Iterator;
use crate::id::UnitsObjectId;

/// Type of lock held on an object
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LockType {
    /// Shared read lock
    Read,
    /// Exclusive write lock
    Write,
}

/// Information about a lock held on an object
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockInfo {
    /// The ID of the object being locked
    pub object_id: UnitsObjectId,

    /// The type of lock held (read or write)
    pub lock_type: LockType,

    /// The hash of the transaction that holds this lock
    pub transaction_hash: [u8; 32],

    /// When the lock was acquired (Unix timestamp)
    pub acquired_at: u64,

    /// Optional timeout for the lock (in milliseconds)
    pub timeout_ms: Option<u64>,
}

/// Iterator for traversing lock information
pub trait UnitsLockIterator<E>: Iterator<Item = Result<LockInfo, E>> {}

/// The access intent for an instruction on a TokenizedObject
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccessIntent {
    /// Read-only access to the object
    Read,
    /// Read-write access to the object
    Write,
}

/// Persistent lock manager that stores lock information in the storage
pub trait PersistentLockManager: std::fmt::Debug {
    /// Error type used for lock manager operations
    type Error: std::fmt::Display;

    /// Acquire a lock on an object for a transaction
    ///
    /// # Parameters
    /// * `object_id` - The ID of the object to lock
    /// * `lock_type` - The type of lock to acquire (read or write)
    /// * `transaction_hash` - The hash of the transaction acquiring the lock
    /// * `timeout_ms` - Optional timeout for the lock (in milliseconds)
    ///
    /// # Returns
    /// * `Ok(true)` - Lock was successfully acquired
    /// * `Err` - An error occurred, including if the lock could not be acquired due to conflict 
    ///           (implementations should return a LockConflictError or similar)
    fn acquire_lock(
        &self,
        object_id: &UnitsObjectId,
        lock_type: LockType,
        transaction_hash: &[u8; 32],
        timeout_ms: Option<u64>,
    ) -> Result<bool, Self::Error>;

    /// Release a lock on an object for a transaction
    ///
    /// # Parameters
    /// * `object_id` - The ID of the object to unlock
    /// * `transaction_hash` - The hash of the transaction releasing the lock
    ///
    /// # Returns
    /// * `Ok(true)` - Lock was successfully released
    /// * `Ok(false)` - Lock was not found or belongs to a different transaction
    /// * `Err` - An error occurred while trying to release the lock
    fn release_lock(
        &self,
        object_id: &UnitsObjectId,
        transaction_hash: &[u8; 32],
    ) -> Result<bool, Self::Error>;

    /// Check if a lock exists and get its information
    ///
    /// # Parameters
    /// * `object_id` - The ID of the object to check
    ///
    /// # Returns
    /// * `Some(LockInfo)` - Object is locked, returns lock information
    /// * `None` - Object is not locked
    fn get_lock_info(&self, object_id: &UnitsObjectId) -> Result<Option<LockInfo>, Self::Error>;

    /// Check if a transaction can acquire a lock on an object
    ///
    /// # Parameters
    /// * `object_id` - The ID of the object to check
    /// * `intent` - The access intent (Read or Write)
    /// * `transaction_hash` - The hash of the transaction checking lock availability
    ///
    /// # Returns
    /// * `true` - Lock can be acquired
    /// * `false` - Lock cannot be acquired
    fn can_acquire_lock(
        &self,
        object_id: &UnitsObjectId,
        intent: AccessIntent,
        transaction_hash: &[u8; 32],
    ) -> Result<bool, Self::Error>;

    /// Release all locks held by a transaction
    ///
    /// # Parameters
    /// * `transaction_hash` - The hash of the transaction to release locks for
    ///
    /// # Returns
    /// * `Ok(count)` - Number of locks released
    /// * `Err` - An error occurred while trying to release locks
    fn release_transaction_locks(
        &self,
        transaction_hash: &[u8; 32],
    ) -> Result<usize, Self::Error>;

    /// Get all locks held by a transaction
    ///
    /// # Parameters
    /// * `transaction_hash` - The hash of the transaction to get locks for
    ///
    /// # Returns
    /// An iterator over all locks held by the transaction
    fn get_transaction_locks(
        &self,
        transaction_hash: &[u8; 32],
    ) -> Box<dyn UnitsLockIterator<Self::Error> + '_>;

    /// Get all locks on an object
    ///
    /// # Parameters
    /// * `object_id` - The ID of the object to get locks for
    ///
    /// # Returns
    /// An iterator over all locks on the object
    fn get_object_locks(&self, object_id: &UnitsObjectId) -> Box<dyn UnitsLockIterator<Self::Error> + '_>;

    /// Check for expired locks and release them
    ///
    /// # Returns
    /// * `Ok(count)` - Number of expired locks released
    /// * `Err` - An error occurred while trying to release expired locks
    fn cleanup_expired_locks(&self) -> Result<usize, Self::Error>;
}

/// Guard that holds a lock on an object and releases it when dropped
#[derive(Debug)]
pub struct ObjectLockGuard<'a, M: PersistentLockManager> {
    /// ID of the object that is locked
    object_id: UnitsObjectId,

    /// Type of lock held
    lock_type: LockType,

    /// Hash of the transaction that holds the lock
    transaction_hash: [u8; 32],

    /// Reference to the lock manager
    lock_manager: Option<&'a M>,

    /// Whether the lock has been explicitly released
    released: bool,
}

impl<'a, M: PersistentLockManager> ObjectLockGuard<'a, M> {
    /// Create a new lock guard with a lock manager
    pub fn new(
        object_id: UnitsObjectId,
        lock_type: LockType,
        transaction_hash: [u8; 32],
        lock_manager: &'a M,
    ) -> Result<Self, M::Error> {
        // Try to acquire the lock using the persistent lock manager
        // If the lock can't be acquired, the implementation should return an appropriate error
        let acquired = lock_manager.acquire_lock(&object_id, lock_type, &transaction_hash, None)?;
        
        if !acquired {
            // This should not happen if implementations follow the API contract
            panic!("Lock manager returned false without an error");
        }
        
        Ok(Self {
            object_id,
            lock_type,
            transaction_hash,
            lock_manager: Some(lock_manager),
            released: false,
        })
    }

    /// Create a new in-memory lock guard (for testing only)
    #[cfg(test)]
    pub fn new_in_memory(
        object_id: UnitsObjectId,
        lock_type: LockType,
        transaction_hash: [u8; 32],
    ) -> Self {
        // For testing, we create a lock without a manager
        Self {
            object_id,
            lock_type,
            transaction_hash,
            lock_manager: None,
            released: false,
        }
    }

    /// Explicitly release the lock before the guard is dropped
    pub fn release(&mut self) -> Result<bool, M::Error> {
        if self.released {
            return Ok(false); // Already released
        }

        if let Some(manager) = self.lock_manager {
            match manager.release_lock(&self.object_id, &self.transaction_hash) {
                Ok(released) => {
                    self.released = true;
                    Ok(released)
                }
                Err(e) => Err(e),
            }
        } else {
            // In-memory lock, just mark as released
            self.released = true;
            Ok(true)
        }
    }

    /// Get the object ID
    pub fn object_id(&self) -> &UnitsObjectId {
        &self.object_id
    }

    /// Get the lock type
    pub fn lock_type(&self) -> LockType {
        self.lock_type
    }

    /// Get the transaction hash
    pub fn transaction_hash(&self) -> &[u8; 32] {
        &self.transaction_hash
    }
}

impl<'a, M: PersistentLockManager> Drop for ObjectLockGuard<'a, M> {
    fn drop(&mut self) {
        if !self.released {
            if let Some(manager) = self.lock_manager {
                // Try to release the lock, ignore any errors since we're dropping
                let _ = manager.release_lock(&self.object_id, &self.transaction_hash);
            }
            // If no manager, nothing to do for in-memory locks
        }
    }
}

impl AccessIntent {
    /// Acquire the respective lock on the object
    ///
    /// For Read intent, acquires a shared read lock
    /// For Write intent, acquires an exclusive write lock
    ///
    /// # Parameters
    /// * `object_id` - The ID of the object to lock
    /// * `transaction_hash` - The hash of the transaction acquiring the lock
    /// * `lock_manager` - The persistent lock manager to use
    ///
    /// # Returns
    /// A lock guard that will be released when dropped, or an error if the lock couldn't be acquired
    pub fn acquire_lock<'a, M: PersistentLockManager>(
        &self,
        object_id: &UnitsObjectId,
        transaction_hash: &[u8; 32],
        lock_manager: &'a M,
    ) -> Result<ObjectLockGuard<'a, M>, M::Error> {
        // Convert AccessIntent to LockType
        let lock_type = match self {
            AccessIntent::Read => LockType::Read,
            AccessIntent::Write => LockType::Write,
        };

        ObjectLockGuard::new(*object_id, lock_type, *transaction_hash, lock_manager)
    }

    /// Create an in-memory lock (for testing only)
    #[cfg(test)]
    pub fn create_in_memory_lock<M: PersistentLockManager>(
        &self,
        object_id: &UnitsObjectId,
        transaction_hash: &[u8; 32],
    ) -> ObjectLockGuard<'static, M> {
        let lock_type = match self {
            AccessIntent::Read => LockType::Read,
            AccessIntent::Write => LockType::Write,
        };

        ObjectLockGuard::new_in_memory(*object_id, lock_type, *transaction_hash)
    }
}