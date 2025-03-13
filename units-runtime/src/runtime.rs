// Import types from units-transaction
pub use units_transaction::{
    AccessIntent, CommitmentLevel, ConflictResult, Instruction, Transaction, TransactionEffect,
    TransactionHash, TransactionReceipt,
};

// Moved UnitsReceiptIterator and TransactionReceiptStorage to units-storage-impl::storage_traits

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};
    use std::sync::Mutex;
    use units_core::error::StorageError;
    use units_core::id::UnitsObjectId;
    use units_proofs::{SlotNumber, TokenizedObjectProof};
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
}
