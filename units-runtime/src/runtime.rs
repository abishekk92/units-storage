use crate::runtime_backend::{ExecutionError, InstructionContext, RuntimeBackendManager};
use std::collections::HashMap;
use units_core::error::RuntimeError;
use units_core::id::UnitsObjectId;
use units_core::objects::UnitsObject;
use units_core::transaction::{
    CommitmentLevel, ConflictResult, Instruction, Transaction, TransactionHash, TransactionReceipt,
};

/// Runtime for executing transactions and programs in the UNITS system
pub trait Runtime {
    /// Get the runtime backend manager used by this runtime
    fn backend_manager(&self) -> &RuntimeBackendManager;

    //--------------------------------------------------------------------------
    // TRANSACTION EXECUTION
    //--------------------------------------------------------------------------

    /// Execute a transaction and return a transaction receipt with proofs
    fn execute_transaction(&self, transaction: Transaction) -> TransactionReceipt;

    /// Try to execute a transaction with conflict checking
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
            Ok(conflict) => Err(conflict),
            Err(_) => Err(ConflictResult::Conflict(vec![])),
        }
    }

    /// Check for potential conflicts with pending or recent transactions
    fn check_conflicts(&self, _transaction: &Transaction) -> Result<ConflictResult, RuntimeError> {
        // Default implementation assumes no conflicts
        Ok(ConflictResult::NoConflict)
    }

    //--------------------------------------------------------------------------
    // PROGRAM EXECUTION
    //--------------------------------------------------------------------------

    /// Execute a program call instruction
    fn execute_program_call(
        &self,
        program_id: &UnitsObjectId,
        args: &[u8],
        transaction_hash: &TransactionHash,
        objects: HashMap<UnitsObjectId, UnitsObject>,
        parameters: HashMap<String, String>,
    ) -> Result<HashMap<UnitsObjectId, UnitsObject>, ExecutionError> {
        // Create an instruction
        let instruction = Instruction::new(
            args.to_vec(),
            self.backend_manager().default_runtime_type(),
            vec![],
            *program_id,
        );

        // Create execution context
        let context = InstructionContext {
            transaction_hash,
            objects,
            parameters,
            program_id: Some(*program_id),
            entrypoint: None,
        };

        // Execute the program
        self.backend_manager()
            .execute_program_call(program_id, &instruction, context)
    }

    //--------------------------------------------------------------------------
    // TRANSACTION MANAGEMENT
    //--------------------------------------------------------------------------

    /// Get a transaction by its hash
    fn get_transaction(&self, hash: &TransactionHash) -> Option<Transaction>;

    /// Get a transaction receipt by its hash
    fn get_transaction_receipt(&self, hash: &TransactionHash) -> Option<TransactionReceipt>;

    /// Rollback a previously executed transaction
    fn rollback_transaction(&self, transaction_hash: &TransactionHash) -> Result<bool, RuntimeError>;

    /// Update a transaction's commitment level
    fn update_commitment_level(
        &self,
        _transaction_hash: &TransactionHash,
        _commitment_level: CommitmentLevel,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unimplemented("Updating commitment level not supported".to_string()))
    }

    /// Commit a transaction
    fn commit_transaction(&self, transaction_hash: &TransactionHash) -> Result<(), RuntimeError> {
        self.update_commitment_level(transaction_hash, CommitmentLevel::Committed)
    }

    /// Mark a transaction as failed
    fn fail_transaction(&self, transaction_hash: &TransactionHash) -> Result<(), RuntimeError> {
        self.update_commitment_level(transaction_hash, CommitmentLevel::Failed)
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
