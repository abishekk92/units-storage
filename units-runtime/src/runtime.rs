use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::iter::Iterator;
use units_core::error::StorageError;
use units_core::id::UnitsObjectId;
use units_core::objects::TokenizedObject;
use units_proofs::{SlotNumber, TokenizedObjectProof};

// Import types from units-transaction
pub use units_transaction::{
    AccessIntent, CommitmentLevel, ConflictResult, Instruction, Transaction, TransactionHash,
};

/// Iterator for traversing transaction receipts in storage
pub trait UnitsReceiptIterator: Iterator<Item = Result<TransactionReceipt, StorageError>> {}

/// Storage interface for transaction receipts
pub trait TransactionReceiptStorage {
    /// Store a transaction receipt
    ///
    /// # Parameters
    /// * `receipt` - The transaction receipt to store
    ///
    /// # Returns
    /// Ok(()) if successful, Err otherwise
    fn store_receipt(&self, receipt: &TransactionReceipt) -> Result<(), StorageError>;

    /// Get a transaction receipt by transaction hash
    ///
    /// # Parameters
    /// * `hash` - The transaction hash to get the receipt for
    ///
    /// # Returns
    /// Some(receipt) if found, None otherwise
    fn get_receipt(&self, hash: &[u8; 32]) -> Result<Option<TransactionReceipt>, StorageError>;

    /// Get all transaction receipts for a specific object
    ///
    /// # Parameters
    /// * `id` - The ID of the object to get receipts for
    ///
    /// # Returns
    /// An iterator that yields all receipts that affected this object
    fn get_receipts_for_object(&self, id: &UnitsObjectId) -> Box<dyn UnitsReceiptIterator + '_>;

    /// Get all transaction receipts in a specific slot
    ///
    /// # Parameters
    /// * `slot` - The slot to get receipts for
    ///
    /// # Returns
    /// An iterator that yields all receipts in this slot
    fn get_receipts_in_slot(&self, slot: SlotNumber) -> Box<dyn UnitsReceiptIterator + '_>;
}

/// Result of a transaction execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionResult {
    /// The hash of the transaction that was executed
    pub transaction_hash: TransactionHash,

    /// Whether the transaction was executed successfully
    pub success: bool,

    /// Any error message from the execution
    pub error_message: Option<String>,
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

// Conversion functions no longer needed as we've consolidated to a single TransactionReceipt type

/// Runtime for executing transactions that modify TokenizedObjects
pub trait Runtime {
    /// Check for potential conflicts with pending or recent transactions
    ///
    /// This allows detecting conflicts before executing a transaction.
    /// Implementations should use a conflict checker from the units-scheduler crate.
    ///
    /// # Parameters
    /// * `transaction` - The transaction to check for conflicts
    ///
    /// # Returns
    /// A ConflictResult indicating whether conflicts were detected
    fn check_conflicts(&self, _transaction: &Transaction) -> Result<ConflictResult, String> {
        // Default implementation assumes no conflicts
        // Real implementations should use a ConflictChecker from units-scheduler
        Ok(ConflictResult::NoConflict)
    }

    /// Execute a transaction and return a transaction receipt with proofs
    fn execute_transaction(&self, transaction: Transaction) -> TransactionReceipt;

    /// Try to execute a transaction with conflict checking
    ///
    /// This method first checks for conflicts and only executes the transaction
    /// if no conflicts are detected.
    ///
    /// # Parameters
    /// * `transaction` - The transaction to execute
    ///
    /// # Returns
    /// Either a receipt or a conflict error
    fn try_execute_transaction(
        &self,
        transaction: Transaction,
    ) -> Result<TransactionReceipt, ConflictResult> {
        // Check for conflicts
        match self.check_conflicts(&transaction) {
            Ok(ConflictResult::NoConflict) | Ok(ConflictResult::ReadOnly) => {
                // No conflicts, execute the transaction
                Ok(self.execute_transaction(transaction))
            }
            Ok(conflict) => {
                // Conflicts detected, return them
                Err(conflict)
            }
            Err(_) => {
                // Error checking conflicts, use a default error
                Err(ConflictResult::Conflict(vec![]))
            }
        }
    }

    /// Rollback a previously executed transaction by reverting objects to their state
    /// before the transaction was executed. This only works for transactions with a
    /// Processing commitment level.
    ///
    /// # Parameters
    /// * `transaction_hash` - The hash of the transaction to rollback
    ///
    /// # Returns
    /// True if the rollback was successful, error message otherwise
    fn rollback_transaction(&self, _transaction_hash: &TransactionHash) -> Result<bool, String> {
        // Default implementation returns an error since not all runtimes support rollback
        Err("Transaction rollback not supported by this runtime".to_string())
    }

    /// Update the commitment level of a transaction
    ///
    /// # Parameters
    /// * `transaction_hash` - The hash of the transaction to update
    /// * `commitment_level` - The new commitment level
    ///
    /// # Returns
    /// Ok(()) if successful, Err with error message otherwise
    fn update_commitment_level(
        &self,
        _transaction_hash: &TransactionHash,
        _commitment_level: CommitmentLevel,
    ) -> Result<(), String> {
        // Default implementation returns an error
        Err("Updating commitment level not supported by this runtime".to_string())
    }

    /// Commit a transaction, making its changes permanent and preventing rollback
    ///
    /// # Parameters
    /// * `transaction_hash` - The hash of the transaction to commit
    ///
    /// # Returns
    /// Ok(()) if successful, Err with error message otherwise
    fn commit_transaction(&self, transaction_hash: &TransactionHash) -> Result<(), String> {
        self.update_commitment_level(transaction_hash, CommitmentLevel::Committed)
    }

    /// Mark a transaction as failed
    ///
    /// # Parameters
    /// * `transaction_hash` - The hash of the transaction to mark as failed
    ///
    /// # Returns
    /// Ok(()) if successful, Err with error message otherwise
    fn fail_transaction(&self, transaction_hash: &TransactionHash) -> Result<(), String> {
        self.update_commitment_level(transaction_hash, CommitmentLevel::Failed)
    }

    /// Get a transaction by its hash
    fn get_transaction(&self, hash: &TransactionHash) -> Option<Transaction>;

    /// Get a transaction receipt by the transaction hash
    fn get_transaction_receipt(&self, hash: &TransactionHash) -> Option<TransactionReceipt>;
}

/// Mock implementation of the Runtime trait for testing purposes
pub struct MockRuntime {
    /// Store of transactions by their hash
    transactions: HashMap<TransactionHash, Transaction>,
    /// Store of transaction receipts by transaction hash
    receipts: HashMap<TransactionHash, TransactionReceipt>,
    /// Current slot for transaction processing
    current_slot: SlotNumber,
}

impl MockRuntime {
    /// Create a new MockRuntime
    pub fn new() -> Self {
        Self {
            transactions: HashMap::new(),
            receipts: HashMap::new(),
            current_slot: 0,
        }
    }

    /// Add a transaction to the mock runtime's transaction store
    pub fn add_transaction(&mut self, transaction: Transaction) {
        self.transactions.insert(transaction.hash, transaction);
    }

    /// Add a transaction receipt to the mock runtime's receipt store
    pub fn add_receipt(&mut self, receipt: TransactionReceipt) {
        self.receipts.insert(receipt.transaction_hash, receipt);
    }

    /// Get the current timestamp for transaction processing
    fn current_timestamp(&self) -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    /// Set the current slot for testing purposes
    pub fn set_slot(&mut self, slot: SlotNumber) {
        self.current_slot = slot;
    }
}

impl Runtime for MockRuntime {
    fn check_conflicts(&self, transaction: &Transaction) -> Result<ConflictResult, String> {
        use units_scheduler::{BasicConflictChecker, ConflictChecker};

        // Get recent transactions for testing (last 10)
        let recent_transactions: Vec<_> = self.transactions.values().cloned().collect();
        let recent_transactions = recent_transactions
            .iter()
            .rev()
            .take(10)
            .cloned()
            .collect::<Vec<_>>();

        // Use the BasicConflictChecker from units-scheduler
        let checker = BasicConflictChecker::new();
        checker.check_conflicts(transaction, &recent_transactions)
    }

    fn execute_transaction(&self, transaction: Transaction) -> TransactionReceipt {
        // In a mock implementation, we just pretend all transactions succeed
        let mut mock = self.clone();
        mock.add_transaction(transaction.clone());

        // Create a transaction receipt for this transaction with Processing commitment level
        let receipt = TransactionReceipt::with_commitment_level(
            transaction.hash,
            self.current_slot,
            true, // Success
            self.current_timestamp(),
            CommitmentLevel::Processing, // Start as Processing to allow rollback
        );

        // In a real implementation, we would:
        // 1. Process each instruction in the transaction
        // 2. Update objects based on the instructions
        // 3. Generate proofs for each modified object
        // 4. Add those proofs to the receipt
        // 5. Set commitment level to Processing initially regardless of success

        // Store the receipt
        mock.add_receipt(receipt.clone());

        receipt
    }

    fn rollback_transaction(&self, transaction_hash: &TransactionHash) -> Result<bool, String> {
        // Check if the transaction exists
        let transaction = match self.get_transaction(transaction_hash) {
            Some(tx) => tx,
            None => return Err("Transaction not found".to_string()),
        };

        // Check if we have a receipt for this transaction
        match self.get_transaction_receipt(transaction_hash) {
            Some(receipt) => receipt,
            None => return Err("Transaction receipt not found".to_string()),
        };

        // Check if the transaction can be rolled back based on its commitment level
        if !transaction.can_rollback() {
            return Err(format!(
                "Cannot rollback transaction with commitment level {:?}. Only Processing transactions can be rolled back.",
                transaction.commitment_level
            ));
        }

        let mut mock = self.clone();

        // In a real implementation:
        // 1. For each object affected by the transaction (found in receipt.object_proofs),
        //    we would retrieve its state from before the transaction was executed
        // 2. Restore each object to its previous state
        // 3. Remove the transaction's effects from the system

        // Mark the transaction as failed (it was rolled back)
        if let Some(mut tx) = mock.transactions.get_mut(transaction_hash).cloned() {
            tx.fail();
            mock.transactions.insert(*transaction_hash, tx);
        }

        if let Some(mut rcpt) = mock.receipts.get_mut(transaction_hash).cloned() {
            rcpt.fail();
            rcpt.commitment_level = CommitmentLevel::Failed;
            mock.receipts.insert(*transaction_hash, rcpt);
        }

        // For a real implementation, we'd use storage history to restore previous versions
        // of the objects, but for the mock we'll just mark the transaction as failed

        Ok(true)
    }

    fn update_commitment_level(
        &self,
        transaction_hash: &TransactionHash,
        commitment_level: CommitmentLevel,
    ) -> Result<(), String> {
        let mut mock = self.clone();

        // Update transaction commitment level
        if let Some(mut tx) = mock.transactions.get_mut(transaction_hash).cloned() {
            // Check if this is a valid state transition
            match (tx.commitment_level, commitment_level) {
                // Can move from Processing to any state
                (CommitmentLevel::Processing, _) => {}
                // Cannot change from Committed or Failed
                (CommitmentLevel::Committed, _) => {
                    return Err(
                        "Cannot change commitment level of a Committed transaction".to_string()
                    );
                }
                (CommitmentLevel::Failed, _) => {
                    return Err(
                        "Cannot change commitment level of a Failed transaction".to_string()
                    );
                }
            }

            tx.commitment_level = commitment_level;
            mock.transactions.insert(*transaction_hash, tx);
        } else {
            return Err("Transaction not found".to_string());
        }

        // Update receipt commitment level
        if let Some(mut receipt) = mock.receipts.get_mut(transaction_hash).cloned() {
            receipt.commitment_level = commitment_level;
            if commitment_level == CommitmentLevel::Failed {
                receipt.success = false;
            }
            mock.receipts.insert(*transaction_hash, receipt);
        }

        Ok(())
    }

    fn get_transaction(&self, hash: &TransactionHash) -> Option<Transaction> {
        self.transactions.get(hash).cloned()
    }

    fn get_transaction_receipt(&self, hash: &TransactionHash) -> Option<TransactionReceipt> {
        self.receipts.get(hash).cloned()
    }
}

impl Clone for MockRuntime {
    fn clone(&self) -> Self {
        Self {
            transactions: self.transactions.clone(),
            receipts: self.receipts.clone(),
            current_slot: self.current_slot,
        }
    }
}

/// In-memory implementation of transaction receipt storage for testing
pub struct InMemoryReceiptStorage {
    // Mapping from transaction hash to receipt
    receipts_by_hash: Mutex<HashMap<[u8; 32], TransactionReceipt>>,

    // Mapping from object ID to set of transaction hashes that affected it
    receipts_by_object: Mutex<HashMap<UnitsObjectId, HashSet<[u8; 32]>>>,

    // Mapping from slot to set of transaction hashes in that slot
    receipts_by_slot: Mutex<HashMap<SlotNumber, HashSet<[u8; 32]>>>,
}

impl InMemoryReceiptStorage {
    /// Create a new in-memory receipt storage
    pub fn new() -> Self {
        Self {
            receipts_by_hash: Mutex::new(HashMap::new()),
            receipts_by_object: Mutex::new(HashMap::new()),
            receipts_by_slot: Mutex::new(HashMap::new()),
        }
    }
}

/// Iterator implementation for receipts
pub struct InMemoryReceiptIterator {
    receipts: Vec<TransactionReceipt>,
    current_index: usize,
}

impl Iterator for InMemoryReceiptIterator {
    type Item = Result<TransactionReceipt, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_index < self.receipts.len() {
            let receipt = self.receipts[self.current_index].clone();
            self.current_index += 1;
            Some(Ok(receipt))
        } else {
            None
        }
    }
}

impl UnitsReceiptIterator for InMemoryReceiptIterator {}

impl TransactionReceiptStorage for InMemoryReceiptStorage {
    fn store_receipt(&self, receipt: &TransactionReceipt) -> Result<(), StorageError> {
        // Store the receipt by transaction hash
        {
            let mut receipts_by_hash = self.receipts_by_hash.lock().unwrap();
            receipts_by_hash.insert(receipt.transaction_hash, receipt.clone());
        }

        // Index the receipt by objects it affected
        {
            let mut receipts_by_object = self.receipts_by_object.lock().unwrap();
            for object_id in receipt.object_proofs.keys() {
                let entry = receipts_by_object
                    .entry(*object_id)
                    .or_insert_with(HashSet::new);
                entry.insert(receipt.transaction_hash);
            }
        }

        // Index the receipt by slot
        {
            let mut receipts_by_slot = self.receipts_by_slot.lock().unwrap();
            let entry = receipts_by_slot
                .entry(receipt.slot)
                .or_insert_with(HashSet::new);
            entry.insert(receipt.transaction_hash);
        }

        Ok(())
    }

    fn get_receipt(&self, hash: &[u8; 32]) -> Result<Option<TransactionReceipt>, StorageError> {
        let receipts_by_hash = self.receipts_by_hash.lock().unwrap();

        if let Some(receipt) = receipts_by_hash.get(hash) {
            Ok(Some(receipt.clone()))
        } else {
            Ok(None)
        }
    }

    fn get_receipts_for_object(&self, id: &UnitsObjectId) -> Box<dyn UnitsReceiptIterator + '_> {
        // Get all transaction hashes for this object
        let transaction_hashes = {
            let receipts_by_object = self.receipts_by_object.lock().unwrap();
            match receipts_by_object.get(id) {
                Some(hashes) => hashes.clone(),
                None => HashSet::new(),
            }
        };

        // Get all receipts for these transaction hashes
        let mut receipts = Vec::new();
        {
            let receipts_by_hash = self.receipts_by_hash.lock().unwrap();
            for hash in transaction_hashes {
                if let Some(receipt) = receipts_by_hash.get(&hash) {
                    receipts.push(receipt.clone());
                }
            }
        }

        // Sort receipts by slot (most recent first)
        receipts.sort_by(|a, b| b.slot.cmp(&a.slot));

        Box::new(InMemoryReceiptIterator {
            receipts,
            current_index: 0,
        })
    }

    fn get_receipts_in_slot(&self, slot: SlotNumber) -> Box<dyn UnitsReceiptIterator + '_> {
        // Get all transaction hashes for this slot
        let transaction_hashes = {
            let receipts_by_slot = self.receipts_by_slot.lock().unwrap();
            match receipts_by_slot.get(&slot) {
                Some(hashes) => hashes.clone(),
                None => HashSet::new(),
            }
        };

        // Get all receipts for these transaction hashes
        let mut receipts = Vec::new();
        {
            let receipts_by_hash = self.receipts_by_hash.lock().unwrap();
            for hash in transaction_hashes {
                if let Some(receipt) = receipts_by_hash.get(&hash) {
                    receipts.push(receipt.clone());
                }
            }
        }

        // Sort receipts by timestamp (most recent first)
        receipts.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

        Box::new(InMemoryReceiptIterator {
            receipts,
            current_index: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use units_core::id::UnitsObjectId;

    #[test]
    fn test_transaction_receipt_creation() {
        // Create a transaction receipt
        let transaction_hash = [1u8; 32];
        let slot = 42;
        let success = true;
        let timestamp = 123456789;

        let mut receipt =
            TransactionReceipt::new(transaction_hash, slot, success, timestamp);

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
}
