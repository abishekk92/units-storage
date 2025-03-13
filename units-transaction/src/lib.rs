use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use units_core::id::UnitsObjectId;
use units_core::objects::TokenizedObject;
use units_proofs::{SlotNumber, TokenizedObjectProof};
use units_scheduler::{
    AccessIntent, ObjectLockGuard, PersistentLockManager, TransactionHash,
};

// Re-export types from scheduler to avoid breaking changes
pub use units_scheduler::ConflictResult;
pub use units_scheduler::AccessIntent;
pub use units_scheduler::TransactionHash;

/// Represents the commitment level of a transaction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitmentLevel {
    /// Transaction is in-flight/processing and can be rolled back
    Processing,
    /// Transaction has been committed and cannot be rolled back
    Committed,
    /// Transaction has failed and cannot be executed again
    Failed,
}

impl Default for CommitmentLevel {
    fn default() -> Self {
        CommitmentLevel::Processing
    }
}


/// A structure representing an instruction within a transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instruction {
    /// The binary representation of the instruction
    pub data: Vec<u8>,

    /// The objects this instruction intends to access and their access intents
    pub object_intents: Vec<(UnitsObjectId, AccessIntent)>,
}

impl Instruction {
    /// Acquire all locks needed for this instruction
    ///
    /// This acquires locks for all objects according to their access intents.
    /// Read intents acquire shared read locks, while write intents acquire exclusive write locks.
    ///
    /// # Parameters
    /// * `transaction_hash` - The hash of the transaction acquiring the locks
    /// * `lock_manager` - The persistent lock manager to use
    ///
    /// # Returns
    /// A vector of lock guards or errors for each lock attempt
    pub fn acquire_locks<'a, M: PersistentLockManager>(
        &self,
        transaction_hash: &TransactionHash,
        lock_manager: &'a M,
    ) -> Vec<Result<ObjectLockGuard<'a, M>, M::Error>> {
        self.object_intents
            .iter()
            .map(|(object_id, intent)| {
                intent.acquire_lock(object_id, transaction_hash, lock_manager)
            })
            .collect()
    }

    /// Check if all locks needed for this instruction can be acquired
    ///
    /// # Parameters
    /// * `transaction_hash` - The hash of the transaction checking lock availability
    /// * `lock_manager` - The persistent lock manager to use
    ///
    /// # Returns
    /// True if all locks can be acquired, false otherwise
    pub fn can_acquire_locks<M: PersistentLockManager>(
        &self,
        transaction_hash: &TransactionHash,
        lock_manager: &M,
    ) -> Result<bool, M::Error> {
        for (object_id, intent) in &self.object_intents {
            match lock_manager.can_acquire_lock(object_id, *intent, transaction_hash) {
                Ok(false) => return Ok(false),
                Err(e) => return Err(e),
                _ => continue,
            }
        }

        Ok(true)
    }

    /// Create in-memory locks for testing
    #[cfg(test)]
    pub fn create_in_memory_locks<M: PersistentLockManager>(
        &self,
        transaction_hash: &TransactionHash,
    ) -> Vec<ObjectLockGuard<'static, M>> {
        self.object_intents
            .iter()
            .map(|(object_id, intent)| intent.create_in_memory_lock(object_id, transaction_hash))
            .collect()
    }
}

/// Transaction that contains multiple instructions to be executed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    /// List of instructions to be executed as part of this transaction
    pub instructions: Vec<Instruction>,

    /// The hash of the transaction
    pub hash: TransactionHash,

    /// The commitment level of this transaction
    pub commitment_level: CommitmentLevel,
}

impl Transaction {
    /// Create a new transaction with a Processing commitment level
    pub fn new(instructions: Vec<Instruction>, hash: TransactionHash) -> Self {
        Self {
            instructions,
            hash,
            commitment_level: CommitmentLevel::Processing,
        }
    }

    /// Mark the transaction as committed
    pub fn commit(&mut self) {
        self.commitment_level = CommitmentLevel::Committed;
    }

    /// Mark the transaction as failed
    pub fn fail(&mut self) {
        self.commitment_level = CommitmentLevel::Failed;
    }

    /// Check if the transaction can be rolled back
    pub fn can_rollback(&self) -> bool {
        self.commitment_level == CommitmentLevel::Processing
    }

    /// Acquire all locks needed for this transaction
    ///
    /// This acquires locks for all objects according to their access intents in all instructions.
    /// Read intents acquire shared read locks, while write intents acquire exclusive write locks.
    ///
    /// # Parameters
    /// * `lock_manager` - The persistent lock manager to use
    ///
    /// # Returns
    /// A result containing a vector of lock guards if all locks were acquired successfully,
    /// or an error if any lock could not be acquired
    pub fn acquire_locks<'a, M: PersistentLockManager>(
        &self,
        lock_manager: &'a M,
    ) -> Result<Vec<ObjectLockGuard<'a, M>>, M::Error> {
        let mut locks = Vec::new();
        let mut locked_objects = std::collections::HashMap::new();

        for instruction in &self.instructions {
            for (object_id, intent) in &instruction.object_intents {
                // Check if we've already locked this object
                if let Some(existing_intent) = locked_objects.get(object_id) {
                    // If we already have a write lock, or if we only need a read lock, skip
                    if *existing_intent == AccessIntent::Write || *intent == AccessIntent::Read {
                        continue;
                    }
                    // Otherwise we had a read lock but need a write lock - upgrade needed

                    // Release the read lock first (find it in our locks and remove it)
                    if let Some(position) = locks
                        .iter()
                        .position(|lock: &ObjectLockGuard<'a, M>| lock.object_id() == object_id)
                    {
                        let mut lock = locks.remove(position);
                        if let Err(e) = lock.release() {
                            // If we can't release the read lock, we can't upgrade
                            return Err(e);
                        }
                    }
                }

                // Acquire the lock
                match intent.acquire_lock(object_id, &self.hash, lock_manager) {
                    Ok(lock) => {
                        locks.push(lock);
                        locked_objects.insert(*object_id, *intent);
                    }
                    Err(e) => {
                        // Release all locks we've acquired so far
                        for lock in &mut locks {
                            let _ = lock.release(); // Ignore errors during cleanup
                        }
                        return Err(e);
                    }
                }
            }
        }

        Ok(locks)
    }

    /// Execute the transaction with automatic lock acquisition and release
    ///
    /// This is a convenience method that:
    /// 1. Acquires all needed locks
    /// 2. Calls the provided execution function
    /// 3. Releases all locks when done
    ///
    /// # Parameters
    /// * `lock_manager` - The persistent lock manager to use
    /// * `exec_fn` - Function that receives a reference to this transaction and performs execution
    ///
    /// # Returns
    /// A result containing the result of the execution function if successful,
    /// or an error if any lock could not be acquired
    pub fn execute_with_locks<'a, M: PersistentLockManager, F, R>(
        &self,
        lock_manager: &'a M,
        exec_fn: F,
    ) -> Result<R, M::Error>
    where
        F: FnOnce(&Self) -> R,
    {
        // Acquire all locks
        let _locks = self.acquire_locks(lock_manager)?;

        // Execute the transaction
        let result = exec_fn(self);

        // Locks are automatically released when _locks goes out of scope
        Ok(result)
    }

    /// Create in-memory locks for testing
    #[cfg(test)]
    pub fn create_in_memory_locks<M: PersistentLockManager>(&self) -> Vec<ObjectLockGuard<'static, M>> {
        let mut locks = Vec::new();
        let mut locked_objects = std::collections::HashMap::new();

        for instruction in &self.instructions {
            for (object_id, intent) in &instruction.object_intents {
                // Check if we've already locked this object
                if let Some(existing_intent) = locked_objects.get(object_id) {
                    // If we already have a write lock, or if we only need a read lock, skip
                    if *existing_intent == AccessIntent::Write || *intent == AccessIntent::Read {
                        continue;
                    }
                }

                // Create in-memory lock
                locks.push(intent.create_in_memory_lock::<M>(object_id, &self.hash));
                // Remember what we locked
                locked_objects.insert(*object_id, *intent);
            }
        }

        locks
    }

    /// Check if all locks needed for this transaction can be acquired
    ///
    /// # Parameters
    /// * `lock_manager` - The persistent lock manager to use
    ///
    /// # Returns
    /// True if all locks can be acquired, false otherwise
    pub fn can_acquire_all_locks<M: PersistentLockManager>(
        &self,
        lock_manager: &M,
    ) -> Result<bool, M::Error> {
        let mut locked_objects = std::collections::HashMap::new();

        for instruction in &self.instructions {
            for (object_id, intent) in &instruction.object_intents {
                // Check if we've already checked this object
                if let Some(existing_intent) = locked_objects.get(object_id) {
                    // If we already have a write lock, or if we only need a read lock, skip
                    if *existing_intent == AccessIntent::Write || *intent == AccessIntent::Read {
                        continue;
                    }
                    // Otherwise we need to upgrade from read to write - check if we can
                }

                // Check if we can acquire the lock
                match lock_manager.can_acquire_lock(object_id, *intent, &self.hash) {
                    Ok(false) => return Ok(false),
                    Err(e) => return Err(e),
                    Ok(true) => {
                        // Remember what we checked
                        locked_objects.insert(*object_id, *intent);
                    }
                }
            }
        }

        Ok(true)
    }
}

/// Represents the before and after state of an object in a transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionEffect {
    /// The transaction that caused this effect
    pub transaction_hash: TransactionHash,

    /// The ID of the object affected
    pub object_id: UnitsObjectId,

    /// The state of the object before the transaction (None if object was created)
    pub before_image: Option<TokenizedObject>,

    /// The state of the object after the transaction (None if object was deleted)
    pub after_image: Option<TokenizedObject>,
}

/// A receipt of a processed transaction, containing all proofs of object modifications
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionReceipt {
    /// The hash of the transaction that was executed
    pub transaction_hash: TransactionHash,

    /// The slot in which this transaction was processed
    pub slot: SlotNumber,

    /// Map of object IDs to their state proofs after the transaction
    pub object_proofs: HashMap<UnitsObjectId, TokenizedObjectProof>,

    /// Whether the transaction was executed successfully
    pub success: bool,

    /// Timestamp when the transaction was processed
    pub timestamp: u64,

    /// The commitment level of this transaction
    pub commitment_level: CommitmentLevel,

    /// Any error message from the execution (if not successful)
    pub error_message: Option<String>,

    /// Effects track the before and after state of objects for easier rollback
    pub effects: Vec<TransactionEffect>,
}

impl TransactionReceipt {
    /// Create a new transaction receipt
    pub fn new(
        transaction_hash: TransactionHash,
        slot: SlotNumber,
        success: bool,
        timestamp: u64,
    ) -> Self {
        Self {
            transaction_hash,
            slot,
            object_proofs: HashMap::new(),
            success,
            timestamp,
            commitment_level: if success {
                CommitmentLevel::Committed
            } else {
                CommitmentLevel::Failed
            },
            error_message: None,
            effects: Vec::new(),
        }
    }

    /// Create a new transaction receipt with a specific commitment level
    pub fn with_commitment_level(
        transaction_hash: TransactionHash,
        slot: SlotNumber,
        success: bool,
        timestamp: u64,
        commitment_level: CommitmentLevel,
    ) -> Self {
        Self {
            transaction_hash,
            slot,
            object_proofs: HashMap::new(),
            success,
            timestamp,
            commitment_level,
            error_message: None,
            effects: Vec::new(),
        }
    }

    /// Add an object proof to the receipt
    pub fn add_proof(&mut self, object_id: UnitsObjectId, proof: TokenizedObjectProof) {
        self.object_proofs.insert(object_id, proof);
    }

    /// Add a transaction effect to the receipt
    pub fn add_effect(&mut self, effect: TransactionEffect) {
        self.effects.push(effect);
    }

    /// Set an error message (used when transaction fails)
    pub fn set_error(&mut self, error: String) {
        self.success = false;
        self.commitment_level = CommitmentLevel::Failed;
        self.error_message = Some(error);
    }

    /// Mark the transaction as committed
    pub fn commit(&mut self) {
        self.commitment_level = CommitmentLevel::Committed;
    }

    /// Mark the transaction as failed
    pub fn fail(&mut self) {
        self.success = false;
        self.commitment_level = CommitmentLevel::Failed;
    }

    /// Check if the transaction can be rolled back
    pub fn can_rollback(&self) -> bool {
        self.commitment_level == CommitmentLevel::Processing
    }

    /// Get the number of objects modified by this transaction
    pub fn object_count(&self) -> usize {
        self.object_proofs.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use units_core::id::UnitsObjectId;
    use units_proofs::{SlotNumber, TokenizedObjectProof};

    #[test]
    fn test_transaction_receipt_creation() {
        // Create a transaction receipt
        let transaction_hash = [1u8; 32];
        let slot = 42;
        let success = true;
        let timestamp = 123456789;

        let mut receipt = TransactionReceipt::new(transaction_hash, slot, success, timestamp);

        // Verify the receipt fields
        assert_eq!(receipt.transaction_hash, transaction_hash);
        assert_eq!(receipt.slot, slot);
        assert_eq!(receipt.success, success);
        assert_eq!(receipt.timestamp, timestamp);
        assert_eq!(receipt.object_count(), 0);
        assert_eq!(receipt.effects.len(), 0);

        // Add some object proofs
        let object_id1 = UnitsObjectId::unique_id_for_tests();
        let object_id2 = UnitsObjectId::unique_id_for_tests();

        let proof1 = TokenizedObjectProof {
            object_id: object_id1,
            slot,
            object_hash: [1u8; 32],
            prev_proof_hash: None,
            transaction_hash: Some(transaction_hash),
            proof_data: vec![1, 2, 3],
        };

        let proof2 = TokenizedObjectProof {
            object_id: object_id2,
            slot,
            object_hash: [2u8; 32],
            prev_proof_hash: None,
            transaction_hash: Some(transaction_hash),
            proof_data: vec![4, 5, 6],
        };

        receipt.add_proof(object_id1, proof1.clone());
        receipt.add_proof(object_id2, proof2.clone());

        // Verify the proofs were added
        assert_eq!(receipt.object_count(), 2);

        // Check if objects exist in the collection
        assert!(receipt.object_proofs.contains_key(&object_id1));

        // Test setting an error
        let error_msg = "Transaction failed".to_string();
        receipt.set_error(error_msg.clone());

        assert_eq!(receipt.success, false);
        assert_eq!(receipt.error_message, Some(error_msg));
    }

    #[test]
    fn test_access_intent_locking() {
        // Create test objects
        let object_id1 = UnitsObjectId::unique_id_for_tests();
        let object_id2 = UnitsObjectId::unique_id_for_tests();
        let transaction_hash = [1u8; 32];

        // Test basic intent lock acquisition
        let read_intent = AccessIntent::Read;
        let write_intent = AccessIntent::Write;

        // Create in-memory locks for testing
        let read_lock = read_intent.create_in_memory_lock(&object_id1, &transaction_hash);
        let write_lock = write_intent.create_in_memory_lock(&object_id2, &transaction_hash);

        // Check lock types
        assert_eq!(read_lock.lock_type(), LockType::Read);
        assert_eq!(write_lock.lock_type(), LockType::Write);

        // Check object IDs
        assert_eq!(*read_lock.object_id(), object_id1);
        assert_eq!(*write_lock.object_id(), object_id2);
    }

    #[test]
    fn test_instruction_locking() {
        // Create test objects
        let object_id1 = UnitsObjectId::unique_id_for_tests();
        let object_id2 = UnitsObjectId::unique_id_for_tests();
        let transaction_hash = [1u8; 32];

        // Create an instruction with multiple intents
        let instruction = Instruction {
            data: vec![1, 2, 3],
            object_intents: vec![
                (object_id1, AccessIntent::Read),
                (object_id2, AccessIntent::Write),
            ],
        };

        // Create in-memory locks for testing
        let locks = instruction.create_in_memory_locks(&transaction_hash);

        // Verify we got two locks
        assert_eq!(locks.len(), 2);

        // Verify lock types
        let mut read_locks = 0;
        let mut write_locks = 0;

        for lock in &locks {
            match lock.lock_type() {
                LockType::Read => read_locks += 1,
                LockType::Write => write_locks += 1,
            }
        }

        assert_eq!(read_locks, 1);
        assert_eq!(write_locks, 1);
    }

    #[test]
    fn test_transaction_locking() {
        // Create test objects
        let object_id1 = UnitsObjectId::unique_id_for_tests();
        let object_id2 = UnitsObjectId::unique_id_for_tests();
        let object_id3 = UnitsObjectId::unique_id_for_tests();

        // Create a transaction with multiple instructions
        let instr1 = Instruction {
            data: vec![1, 2, 3],
            object_intents: vec![
                (object_id1, AccessIntent::Read),
                (object_id2, AccessIntent::Write),
            ],
        };

        let instr2 = Instruction {
            data: vec![4, 5, 6],
            object_intents: vec![
                (object_id1, AccessIntent::Read), // Same as instr1, should be de-duped
                (object_id3, AccessIntent::Write),
            ],
        };

        let transaction = Transaction::new(vec![instr1, instr2], [1u8; 32]);

        // Create in-memory locks for testing
        let locks = transaction.create_in_memory_locks();

        // Verify we got 3 locks (not 4) because we deduplicate
        assert_eq!(locks.len(), 3);
    }

    #[test]
    fn test_lock_upgrade() {
        // Create a mock persistent lock manager for testing
        #[derive(Debug)]
        struct MockLockManager {
            acquired_locks: std::cell::RefCell<Vec<(UnitsObjectId, LockType, TransactionHash)>>,
        }

        impl MockLockManager {
            fn new() -> Self {
                Self {
                    acquired_locks: std::cell::RefCell::new(Vec::new()),
                }
            }
        }

        impl PersistentLockManager for MockLockManager {
            fn acquire_lock(
                &self,
                object_id: &UnitsObjectId,
                lock_type: LockType,
                transaction_hash: &TransactionHash,
                _timeout_ms: Option<u64>,
            ) -> Result<bool, String> {
                // Check if this object is already locked
                let locks = self.acquired_locks.borrow();

                // If it's already locked with Write by someone else, we can't lock it
                let conflict = locks.iter().any(|(id, lt, tx)| {
                    *id == *object_id && *lt == LockType::Write && *tx != *transaction_hash
                });

                if conflict {
                    return Ok(false);
                }

                // If we're trying to get a Write lock, check if anyone else has any lock
                if lock_type == LockType::Write {
                    let conflict = locks
                        .iter()
                        .any(|(id, _, tx)| *id == *object_id && *tx != *transaction_hash);

                    if conflict {
                        return Ok(false);
                    }
                }

                // No conflicts, we can acquire the lock
                drop(locks); // Release the borrow
                self.acquired_locks
                    .borrow_mut()
                    .push((*object_id, lock_type, *transaction_hash));
                Ok(true)
            }

            fn release_lock(
                &self,
                object_id: &UnitsObjectId,
                transaction_hash: &TransactionHash,
            ) -> Result<bool, String> {
                let mut locks = self.acquired_locks.borrow_mut();
                let original_len = locks.len();

                // Remove all locks on this object by this transaction
                locks.retain(|(id, _, tx)| !(*id == *object_id && *tx == *transaction_hash));

                Ok(locks.len() < original_len)
            }

            fn get_lock_info(&self, object_id: &UnitsObjectId) -> Result<Option<LockInfo>, String> {
                let locks = self.acquired_locks.borrow();

                // Find the first lock on this object
                for (id, lock_type, tx) in locks.iter() {
                    if *id == *object_id {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();

                        return Ok(Some(LockInfo {
                            object_id: *id,
                            lock_type: *lock_type,
                            transaction_hash: *tx,
                            acquired_at: now,
                            timeout_ms: None,
                        }));
                    }
                }

                Ok(None)
            }

            fn can_acquire_lock(
                &self,
                object_id: &UnitsObjectId,
                intent: AccessIntent,
                transaction_hash: &TransactionHash,
            ) -> Result<bool, String> {
                let lock_type = match intent {
                    AccessIntent::Read => LockType::Read,
                    AccessIntent::Write => LockType::Write,
                };

                // Use the same logic as acquire_lock but don't actually acquire
                let locks = self.acquired_locks.borrow();

                // If it's already locked with Write by someone else, we can't lock it
                let conflict = locks.iter().any(|(id, lt, tx)| {
                    *id == *object_id && *lt == LockType::Write && *tx != *transaction_hash
                });

                if conflict {
                    return Ok(false);
                }

                // If we're trying to get a Write lock, check if anyone else has any lock
                if lock_type == LockType::Write {
                    let conflict = locks
                        .iter()
                        .any(|(id, _, tx)| *id == *object_id && *tx != *transaction_hash);

                    if conflict {
                        return Ok(false);
                    }
                }

                Ok(true)
            }

            fn release_transaction_locks(
                &self,
                transaction_hash: &TransactionHash,
            ) -> Result<usize, String> {
                let mut locks = self.acquired_locks.borrow_mut();
                let original_len = locks.len();

                // Remove all locks by this transaction
                locks.retain(|(_, _, tx)| *tx != *transaction_hash);

                Ok(original_len - locks.len())
            }

            fn get_transaction_locks(
                &self,
                _transaction_hash: &TransactionHash,
            ) -> Box<dyn UnitsLockIterator + '_> {
                // Not needed for this test
                unimplemented!()
            }

            fn get_object_locks(
                &self,
                _object_id: &UnitsObjectId,
            ) -> Box<dyn UnitsLockIterator + '_> {
                // Not needed for this test
                unimplemented!()
            }

            fn cleanup_expired_locks(&self) -> Result<usize, String> {
                // Not needed for this test
                Ok(0)
            }
        }

        // Create test object
        let object_id = UnitsObjectId::unique_id_for_tests();

        // Create a transaction with an object that needs lock upgrade
        let instr1 = Instruction {
            data: vec![1, 2, 3],
            object_intents: vec![
                (object_id, AccessIntent::Read), // First we read
            ],
        };

        let instr2 = Instruction {
            data: vec![4, 5, 6],
            object_intents: vec![
                (object_id, AccessIntent::Write), // Then we write, needs upgrade
            ],
        };

        let transaction = Transaction::new(vec![instr1, instr2], [1u8; 32]);

        // Create the lock manager
        let lock_manager = MockLockManager::new();

        // Acquire locks (this should include lock upgrade)
        let locks = transaction.acquire_locks(&lock_manager).unwrap();

        // We should have one lock - after upgrade, it should be a write lock
        assert_eq!(locks.len(), 1);
        assert_eq!(locks[0].lock_type(), LockType::Write);
    }

    #[test]
    fn test_persistent_locking() {
        // Create a mock persistent lock manager for testing
        #[derive(Debug)]
        struct MockLockManager {
            acquired_locks: std::cell::RefCell<Vec<(UnitsObjectId, LockType, TransactionHash)>>,
        }

        impl MockLockManager {
            fn new() -> Self {
                Self {
                    acquired_locks: std::cell::RefCell::new(Vec::new()),
                }
            }
        }

        impl PersistentLockManager for MockLockManager {
            fn acquire_lock(
                &self,
                object_id: &UnitsObjectId,
                lock_type: LockType,
                transaction_hash: &TransactionHash,
                _timeout_ms: Option<u64>,
            ) -> Result<bool, String> {
                // Check if this object is already locked
                let locks = self.acquired_locks.borrow();

                // If it's already locked with Write by someone else, we can't lock it
                let conflict = locks.iter().any(|(id, lt, tx)| {
                    *id == *object_id && *lt == LockType::Write && *tx != *transaction_hash
                });

                if conflict {
                    return Ok(false);
                }

                // If we're trying to get a Write lock, check if anyone else has any lock
                if lock_type == LockType::Write {
                    let conflict = locks
                        .iter()
                        .any(|(id, _, tx)| *id == *object_id && *tx != *transaction_hash);

                    if conflict {
                        return Ok(false);
                    }
                }

                // No conflicts, we can acquire the lock
                drop(locks); // Release the borrow
                self.acquired_locks
                    .borrow_mut()
                    .push((*object_id, lock_type, *transaction_hash));
                Ok(true)
            }

            fn release_lock(
                &self,
                object_id: &UnitsObjectId,
                transaction_hash: &TransactionHash,
            ) -> Result<bool, String> {
                let mut locks = self.acquired_locks.borrow_mut();
                let original_len = locks.len();

                // Remove all locks on this object by this transaction
                locks.retain(|(id, _, tx)| !(*id == *object_id && *tx == *transaction_hash));

                Ok(locks.len() < original_len)
            }

            fn get_lock_info(&self, object_id: &UnitsObjectId) -> Result<Option<LockInfo>, String> {
                let locks = self.acquired_locks.borrow();

                // Find the first lock on this object
                for (id, lock_type, tx) in locks.iter() {
                    if *id == *object_id {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();

                        return Ok(Some(LockInfo {
                            object_id: *id,
                            lock_type: *lock_type,
                            transaction_hash: *tx,
                            acquired_at: now,
                            timeout_ms: None,
                        }));
                    }
                }

                Ok(None)
            }

            fn can_acquire_lock(
                &self,
                object_id: &UnitsObjectId,
                intent: AccessIntent,
                transaction_hash: &TransactionHash,
            ) -> Result<bool, String> {
                let lock_type = match intent {
                    AccessIntent::Read => LockType::Read,
                    AccessIntent::Write => LockType::Write,
                };

                // Use the same logic as acquire_lock but don't actually acquire
                let locks = self.acquired_locks.borrow();

                // If it's already locked with Write by someone else, we can't lock it
                let conflict = locks.iter().any(|(id, lt, tx)| {
                    *id == *object_id && *lt == LockType::Write && *tx != *transaction_hash
                });

                if conflict {
                    return Ok(false);
                }

                // If we're trying to get a Write lock, check if anyone else has any lock
                if lock_type == LockType::Write {
                    let conflict = locks
                        .iter()
                        .any(|(id, _, tx)| *id == *object_id && *tx != *transaction_hash);

                    if conflict {
                        return Ok(false);
                    }
                }

                Ok(true)
            }

            fn release_transaction_locks(
                &self,
                transaction_hash: &TransactionHash,
            ) -> Result<usize, String> {
                let mut locks = self.acquired_locks.borrow_mut();
                let original_len = locks.len();

                // Remove all locks by this transaction
                locks.retain(|(_, _, tx)| *tx != *transaction_hash);

                Ok(original_len - locks.len())
            }

            fn get_transaction_locks(
                &self,
                _transaction_hash: &TransactionHash,
            ) -> Box<dyn UnitsLockIterator + '_> {
                // Not needed for this test
                unimplemented!()
            }

            fn get_object_locks(
                &self,
                _object_id: &UnitsObjectId,
            ) -> Box<dyn UnitsLockIterator + '_> {
                // Not needed for this test
                unimplemented!()
            }

            fn cleanup_expired_locks(&self) -> Result<usize, String> {
                // Not needed for this test
                Ok(0)
            }
        }

        // Create test objects
        let object_id1 = UnitsObjectId::unique_id_for_tests();
        let object_id2 = UnitsObjectId::unique_id_for_tests();

        // Create a transaction
        let tx1_hash = [1u8; 32];
        let instr = Instruction {
            data: vec![1, 2, 3],
            object_intents: vec![
                (object_id1, AccessIntent::Read),
                (object_id2, AccessIntent::Write),
            ],
        };

        let tx1 = Transaction::new(vec![instr], tx1_hash);

        // Create the lock manager
        let lock_manager = MockLockManager::new();

        // Test acquiring locks for a transaction
        let locks = tx1.acquire_locks(&lock_manager).unwrap();
        assert_eq!(locks.len(), 2);

        // Verify locks were acquired in the manager
        assert_eq!(lock_manager.acquired_locks.borrow().len(), 2);

        // Test conflict detection with a second transaction
        let tx2_hash = [2u8; 32];
        let instr2 = Instruction {
            data: vec![4, 5, 6],
            object_intents: vec![
                (object_id1, AccessIntent::Read), // Should not conflict (shared read)
                (object_id2, AccessIntent::Write), // Should conflict (existing write)
            ],
        };

        let tx2 = Transaction::new(vec![instr2], tx2_hash);

        // Check if tx2 can acquire all locks (should fail due to conflict on object2)
        assert_eq!(tx2.can_acquire_all_locks(&lock_manager).unwrap(), false);

        // Test transaction with only read on object1 (should succeed)
        let tx3_hash = [3u8; 32];
        let instr3 = Instruction {
            data: vec![7, 8, 9],
            object_intents: vec![
                (object_id1, AccessIntent::Read), // Should not conflict (shared read)
            ],
        };

        let tx3 = Transaction::new(vec![instr3], tx3_hash);

        // Check if tx3 can acquire all locks (should succeed)
        assert_eq!(tx3.can_acquire_all_locks(&lock_manager).unwrap(), true);

        // Actually acquire locks for tx3
        let tx3_locks = tx3.acquire_locks(&lock_manager).unwrap();
        assert_eq!(tx3_locks.len(), 1);

        // Now release all locks for tx1
        for mut lock in locks {
            lock.release().unwrap();
        }

        // Now tx2 should be able to acquire all locks
        assert_eq!(tx2.can_acquire_all_locks(&lock_manager).unwrap(), true);

        // Test execute_with_locks
        let result = tx3
            .execute_with_locks(&lock_manager, |tx| {
                // We could access the objects here, they're locked
                tx.instructions.len()
            })
            .unwrap();

        assert_eq!(result, 1);
    }
}
