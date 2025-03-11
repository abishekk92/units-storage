use serde::{Serialize, Deserialize};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use crate::error::StorageError;
use crate::id::UnitsObjectId;
use crate::proofs::{SlotNumber, TokenizedObjectProof};
use crate::storage_traits::{TransactionReceiptStorage, UnitsReceiptIterator};

/// A transaction hash uniquely identifies a transaction in the system
pub type TransactionHash = [u8; 32];

/// The result of a transaction conflict check
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictResult {
    /// No conflicts detected, transaction can proceed
    NoConflict,
    /// Conflicts detected with these transaction hashes
    Conflict(Vec<TransactionHash>),
    /// Read-only transaction, no conflict possible
    ReadOnly,
}

/// The access intent for an instruction on a TokenizedObject
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccessIntent {
    /// Read-only access to the object
    Read,
    /// Read-write access to the object
    Write,
}

/// A structure representing an instruction within a transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instruction {
    /// The binary representation of the instruction
    pub data: Vec<u8>,
    
    /// The objects this instruction intends to access and their access intents
    pub object_intents: Vec<(UnitsObjectId, AccessIntent)>,
}

/// Transaction that contains multiple instructions to be executed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    /// List of instructions to be executed as part of this transaction
    pub instructions: Vec<Instruction>,
    
    /// The hash of the transaction
    pub hash: TransactionHash,
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
    
    /// Any error message from the execution (if not successful)
    pub error_message: Option<String>,
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
            error_message: None,
        }
    }
    
    /// Add an object proof to the receipt
    pub fn add_proof(&mut self, object_id: UnitsObjectId, proof: TokenizedObjectProof) {
        self.object_proofs.insert(object_id, proof);
    }
    
    /// Set an error message (used when transaction fails)
    pub fn set_error(&mut self, error: String) {
        self.success = false;
        self.error_message = Some(error);
    }
    
    /// Get the number of objects modified by this transaction
    pub fn object_count(&self) -> usize {
        self.object_proofs.len()
    }
}

/// Runtime for executing transactions that modify TokenizedObjects
pub trait Runtime {
    /// Check for potential conflicts with pending or recent transactions
    ///
    /// This allows detecting conflicts before executing a transaction.
    /// Implementations can use various strategies to detect conflicts.
    ///
    /// # Parameters
    /// * `transaction` - The transaction to check for conflicts
    ///
    /// # Returns
    /// A ConflictResult indicating whether conflicts were detected
    fn check_conflicts(&self, transaction: &Transaction) -> Result<ConflictResult, String> {
        // Default implementation checks if transaction is read-only
        if transaction.instructions.iter().all(|i| {
            i.object_intents.iter().all(|(_, intent)| *intent == AccessIntent::Read)
        }) {
            return Ok(ConflictResult::ReadOnly);
        }
        
        // By default, assume no conflicts if all objects are being read
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
    fn try_execute_transaction(&self, transaction: Transaction) 
        -> Result<TransactionReceipt, ConflictResult> 
    {
        // Check for conflicts
        match self.check_conflicts(&transaction) {
            Ok(ConflictResult::NoConflict) | Ok(ConflictResult::ReadOnly) => {
                // No conflicts, execute the transaction
                Ok(self.execute_transaction(transaction))
            },
            Ok(conflict) => {
                // Conflicts detected, return them
                Err(conflict)
            },
            Err(_) => {
                // Error checking conflicts, use a default error
                Err(ConflictResult::Conflict(vec![]))
            }
        }
    }
    
    /// Rollback a previously executed transaction
    ///
    /// # Parameters
    /// * `transaction_hash` - The hash of the transaction to rollback
    ///
    /// # Returns
    /// A receipt for the rollback transaction if successful
    fn rollback_transaction(&self, _transaction_hash: &TransactionHash) 
        -> Result<TransactionReceipt, String>
    {
        // Default implementation returns an error since not all runtimes support rollback
        Err("Transaction rollback not supported by this runtime".to_string())
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
        // For the mock, we'll implement a simple conflict detection mechanism
        // that checks if any objects with write intents have been modified in recent transactions
        
        // Extract object IDs with write intent from this transaction
        let mut write_objects = HashSet::new();
        for instruction in &transaction.instructions {
            for (obj_id, intent) in &instruction.object_intents {
                if *intent == AccessIntent::Write {
                    write_objects.insert(*obj_id);
                }
            }
        }
        
        // If it's a read-only transaction, no conflicts are possible
        if write_objects.is_empty() {
            return Ok(ConflictResult::ReadOnly);
        }
        
        // Check for conflicts with recent transactions
        let mut conflicts = Vec::new();
        
        // Check the last 10 transactions (arbitrary number for testing)
        let recent_transactions: Vec<_> = self.transactions.values().cloned().collect();
        
        for other_tx in recent_transactions.iter().rev().take(10) {
            // Skip checking against itself
            if other_tx.hash == transaction.hash {
                continue;
            }
            
            // Check for overlapping write intents
            for instruction in &other_tx.instructions {
                for (obj_id, intent) in &instruction.object_intents {
                    if *intent == AccessIntent::Write && write_objects.contains(obj_id) {
                        conflicts.push(other_tx.hash);
                        break;
                    }
                }
                if conflicts.contains(&other_tx.hash) {
                    break;
                }
            }
        }
        
        if conflicts.is_empty() {
            Ok(ConflictResult::NoConflict)
        } else {
            Ok(ConflictResult::Conflict(conflicts))
        }
    }
    
    fn execute_transaction(&self, transaction: Transaction) -> TransactionReceipt {
        // In a mock implementation, we just pretend all transactions succeed
        let mut mock = self.clone();
        mock.add_transaction(transaction.clone());
        
        // Create a transaction receipt for this transaction
        let receipt = TransactionReceipt::new(
            transaction.hash,
            self.current_slot,
            true,
            self.current_timestamp()
        );
        
        // In a real implementation, we would:
        // 1. Process each instruction in the transaction
        // 2. Update objects based on the instructions
        // 3. Generate proofs for each modified object
        // 4. Add those proofs to the receipt
        
        // Store the receipt
        mock.add_receipt(receipt.clone());
        
        receipt
    }
    
    fn rollback_transaction(&self, transaction_hash: &TransactionHash) 
        -> Result<TransactionReceipt, String> 
    {
        // Check if the transaction exists
        let _transaction = match self.get_transaction(transaction_hash) {
            Some(tx) => tx,
            None => return Err("Transaction not found".to_string()),
        };
        
        // Check if we have a receipt for this transaction
        let _original_receipt = match self.get_transaction_receipt(transaction_hash) {
            Some(receipt) => receipt,
            None => return Err("Transaction receipt not found".to_string()),
        };
        
        // Create a new "rollback" transaction
        let mut rollback_hash = [0u8; 32];
        rollback_hash.copy_from_slice(&[0xFF; 32]); // Placeholder rollback hash
        
        // In a real implementation, we would:
        // 1. Look up the previous state of all affected objects
        // 2. Create a transaction that reverts to the previous state
        // 3. Execute that transaction
        
        // For the mock, just create a receipt indicating rollback
        let rollback_receipt = TransactionReceipt::new(
            rollback_hash,
            self.current_slot,
            true,
            self.current_timestamp()
        );
        
        // Add the rollback transaction and receipt
        let mut mock = self.clone();
        mock.add_receipt(rollback_receipt.clone());
        
        Ok(rollback_receipt)
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
                let entry = receipts_by_object.entry(*object_id).or_insert_with(HashSet::new);
                entry.insert(receipt.transaction_hash);
            }
        }
        
        // Index the receipt by slot
        {
            let mut receipts_by_slot = self.receipts_by_slot.lock().unwrap();
            let entry = receipts_by_slot.entry(receipt.slot).or_insert_with(HashSet::new);
            entry.insert(receipt.transaction_hash);
        }
        
        Ok(())
    }
    
    fn get_receipt(&self, hash: &[u8; 32]) -> Result<Option<TransactionReceipt>, StorageError> {
        let receipts_by_hash = self.receipts_by_hash.lock().unwrap();
        Ok(receipts_by_hash.get(hash).cloned())
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
    use crate::id::tests::unique_id;

    #[test]
    fn test_transaction_receipt_creation() {
        // Create a transaction receipt
        let transaction_hash = [1u8; 32];
        let slot = 42;
        let success = true;
        let timestamp = 123456789;
        
        let mut receipt = TransactionReceipt::new(
            transaction_hash,
            slot,
            success,
            timestamp
        );
        
        // Verify the receipt fields
        assert_eq!(receipt.transaction_hash, transaction_hash);
        assert_eq!(receipt.slot, slot);
        assert_eq!(receipt.success, success);
        assert_eq!(receipt.timestamp, timestamp);
        assert_eq!(receipt.object_count(), 0);
        
        // Add some object proofs
        let object_id1 = unique_id();
        let object_id2 = unique_id();
        
        let proof1 = TokenizedObjectProof {
            proof: vec![1, 2, 3],
            slot,
            prev_proof_hash: None,
            transaction_hash: Some(transaction_hash),
        };
        
        let proof2 = TokenizedObjectProof {
            proof: vec![4, 5, 6],
            slot,
            prev_proof_hash: None,
            transaction_hash: Some(transaction_hash),
        };
        
        receipt.add_proof(object_id1, proof1.clone());
        receipt.add_proof(object_id2, proof2.clone());
        
        // Verify the proofs were added
        assert_eq!(receipt.object_count(), 2);
        assert_eq!(receipt.object_proofs.get(&object_id1).unwrap(), &proof1);
        assert_eq!(receipt.object_proofs.get(&object_id2).unwrap(), &proof2);
        
        // Test setting an error
        let error_msg = "Transaction failed".to_string();
        receipt.set_error(error_msg.clone());
        
        assert_eq!(receipt.success, false);
        assert_eq!(receipt.error_message, Some(error_msg));
    }
    
    #[test]
    fn test_in_memory_receipt_storage() {
        // Create a receipt storage
        let storage = InMemoryReceiptStorage::new();
        
        // Create some test data
        let transaction_hash1 = [1u8; 32];
        let transaction_hash2 = [2u8; 32];
        let slot1 = 42;
        let slot2 = 43;
        let object_id1 = unique_id();
        let object_id2 = unique_id();
        
        // Create a receipt
        let mut receipt1 = TransactionReceipt::new(
            transaction_hash1,
            slot1,
            true,
            123456789
        );
        
        // Add some object proofs
        receipt1.add_proof(object_id1, TokenizedObjectProof {
            proof: vec![1, 2, 3],
            slot: slot1,
            prev_proof_hash: None,
            transaction_hash: Some(transaction_hash1),
        });
        
        // Create another receipt
        let mut receipt2 = TransactionReceipt::new(
            transaction_hash2,
            slot2,
            true,
            123456790
        );
        
        // Add some object proofs
        receipt2.add_proof(object_id1, TokenizedObjectProof {
            proof: vec![4, 5, 6],
            slot: slot2,
            prev_proof_hash: None,
            transaction_hash: Some(transaction_hash2),
        });
        
        receipt2.add_proof(object_id2, TokenizedObjectProof {
            proof: vec![7, 8, 9],
            slot: slot2,
            prev_proof_hash: None,
            transaction_hash: Some(transaction_hash2),
        });
        
        // Store the receipts
        storage.store_receipt(&receipt1).unwrap();
        storage.store_receipt(&receipt2).unwrap();
        
        // Test get_receipt
        let retrieved1 = storage.get_receipt(&transaction_hash1).unwrap().unwrap();
        let retrieved2 = storage.get_receipt(&transaction_hash2).unwrap().unwrap();
        
        assert_eq!(retrieved1.transaction_hash, receipt1.transaction_hash);
        assert_eq!(retrieved1.slot, receipt1.slot);
        assert_eq!(retrieved1.object_count(), receipt1.object_count());
        
        assert_eq!(retrieved2.transaction_hash, receipt2.transaction_hash);
        assert_eq!(retrieved2.slot, receipt2.slot);
        assert_eq!(retrieved2.object_count(), receipt2.object_count());
        
        // Test get_receipts_for_object
        let receipts_for_obj1: Vec<_> = storage.get_receipts_for_object(&object_id1)
            .map(|r| r.unwrap())
            .collect();
            
        assert_eq!(receipts_for_obj1.len(), 2);
        
        // The results should be ordered by slot (most recent first)
        assert_eq!(receipts_for_obj1[0].slot, slot2);
        assert_eq!(receipts_for_obj1[1].slot, slot1);
        
        // Test get_receipts_in_slot
        let receipts_in_slot1: Vec<_> = storage.get_receipts_in_slot(slot1)
            .map(|r| r.unwrap())
            .collect();
            
        assert_eq!(receipts_in_slot1.len(), 1);
        assert_eq!(receipts_in_slot1[0].transaction_hash, transaction_hash1);
        
        let receipts_in_slot2: Vec<_> = storage.get_receipts_in_slot(slot2)
            .map(|r| r.unwrap())
            .collect();
            
        assert_eq!(receipts_in_slot2.len(), 1);
        assert_eq!(receipts_in_slot2[0].transaction_hash, transaction_hash2);
    }
}