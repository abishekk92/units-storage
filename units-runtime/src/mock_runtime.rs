use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use units_core::error::StorageError;
use units_core::id::UnitsObjectId;
use units_core::scheduler::{BasicConflictChecker, ConflictChecker};
use units_core::transaction::{CommitmentLevel, ConflictResult, Transaction, TransactionHash, TransactionReceipt};
use units_proofs::SlotNumber;

use crate::runtime::Runtime;
use units_storage_impl::storage_traits::{TransactionReceiptStorage, UnitsReceiptIterator};

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
        // Get recent transactions for testing (last 10)
        let recent_transactions: Vec<_> = self.transactions.values().cloned().collect();
        let recent_transactions = recent_transactions
            .iter()
            .rev()
            .take(10)
            .cloned()
            .collect::<Vec<_>>();

        // Use the BasicConflictChecker from units-core
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

    fn get_receipts_for_object(
        &self,
        id: &UnitsObjectId,
    ) -> Box<dyn UnitsReceiptIterator + '_> {
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