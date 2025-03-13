// Import types from units-core
pub use units_core::id::UnitsObjectId;
pub use units_core::locks::AccessIntent;
pub use units_core::objects::TokenizedObject;
pub use units_core::transaction::{
    CommitmentLevel, ConflictResult, Instruction, InstructionType, Transaction, TransactionEffect,
    TransactionHash, TransactionReceipt,
};
use crate::runtime_backend::{RuntimeRegistry, ExecutionError};
use std::collections::HashMap;

// Moved UnitsReceiptIterator and TransactionReceiptStorage to units-storage-impl::storage_traits

/// Runtime for executing transactions that modify TokenizedObjects
pub trait Runtime {
    /// Get the runtime registry used by this runtime
    fn registry(&self) -> &RuntimeRegistry;
    /// Check for potential conflicts with pending or recent transactions
    ///
    /// This allows detecting conflicts before executing a transaction.
    /// Implementations should use a conflict checker from units-core.
    ///
    /// # Parameters
    /// * `transaction` - The transaction to check for conflicts
    ///
    /// # Returns
    /// A ConflictResult indicating whether conflicts were detected
    fn check_conflicts(&self, _transaction: &Transaction) -> Result<ConflictResult, String> {
        // Default implementation assumes no conflicts
        // Real implementations should use a ConflictChecker from units-core
        Ok(ConflictResult::NoConflict)
    }

    /// Execute a single instruction with the appropriate runtime backend
    ///
    /// This method fetches the objects referenced by the instruction, builds the execution context,
    /// selects the appropriate runtime backend based on the instruction type, and executes
    /// the instruction.
    ///
    /// # Parameters
    /// * `instruction` - The instruction to execute
    /// * `transaction_hash` - The hash of the transaction containing this instruction
    /// * `parameters` - Additional runtime parameters for the instruction
    ///
    /// # Returns
    /// A map of object IDs to their updated state after execution
    fn execute_instruction(
        &self,
        _instruction: &Instruction,
        _transaction_hash: &TransactionHash,
        _parameters: HashMap<String, String>,
    ) -> Result<HashMap<UnitsObjectId, TokenizedObject>, ExecutionError> {
        // This is just a placeholder - implementations must override this method
        Err(ExecutionError::ExecutionFailed(
            "Default execute_instruction not implemented".to_string()
        ))
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
    fn rollback_transaction(&self, _transaction_hash: &TransactionHash) -> Result<bool, String>;

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
    use units_core::id::UnitsObjectId;

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

        // In the new implementation, proofs are stored as serialized bytes
        let proof1_data = vec![1, 2, 3];
        let proof2_data = vec![4, 5, 6];

        receipt.add_proof(object_id1, proof1_data.clone());
        receipt.add_proof(object_id2, proof2_data.clone());

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
