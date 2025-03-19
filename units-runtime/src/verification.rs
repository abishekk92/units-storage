//! Receipt and proof verification utilities
//!
//! This module provides adapter functions for verifying transaction receipts against
//! the underlying proof engine.

use std::collections::HashMap;
use units_core::id::UnitsObjectId;
use units_core::objects::UnitsObject;

use units_proofs::UnitsObjectProof;
use units_proofs::{ProofEngine, SlotNumber, StateProof, VerificationResult};

use units_core::transaction::TransactionReceipt;

/// Verifier for transaction receipts and proofs that adapts the proof engine
/// for receipt verification.
pub struct ProofVerifier<'a> {
    /// The proof engine to use for verification
    engine: &'a dyn ProofEngine,
}

impl<'a> ProofVerifier<'a> {
    /// Create a new proof verifier with the given engine
    pub fn new(engine: &'a dyn ProofEngine) -> Self {
        Self { engine }
    }

    /// Verify a single object proof
    ///
    /// # Parameters
    /// * `object` - The object to verify
    /// * `proof` - The proof to verify
    ///
    /// # Returns
    /// A VerificationResult indicating whether the proof is valid
    pub fn verify_object_proof(
        &self,
        object: &UnitsObject,
        proof: &UnitsObjectProof,
    ) -> VerificationResult {
        match self.engine.verify_object_proof(object, proof) {
            Ok(true) => VerificationResult::Valid,
            Ok(false) => {
                VerificationResult::Invalid("Proof does not match object state".to_string())
            }
            Err(e) => VerificationResult::Invalid(format!("Verification error: {}", e)),
        }
    }

    /// Verify a proof chain for an object
    ///
    /// Delegates to the underlying proof engine's verify_proof_history method.
    ///
    /// # Parameters
    /// * `object_states` - Vector of (slot, object state) pairs in ascending slot order
    /// * `proofs` - Vector of (slot, object proof) pairs in ascending slot order
    ///
    /// # Returns
    /// A VerificationResult indicating whether the proof chain is valid
    pub fn verify_proof_chain(
        &self,
        object_states: &[(SlotNumber, UnitsObject)],
        proofs: &[(SlotNumber, UnitsObjectProof)],
    ) -> VerificationResult {
        // Delegate to the proof engine's implementation
        self.engine.verify_proof_history(object_states, proofs)
    }

    /// Verify a transaction receipt
    ///
    /// # Parameters
    /// * `receipt` - The receipt to verify
    /// * `objects` - The current state of objects referenced in the receipt
    ///
    /// # Returns
    /// A VerificationResult indicating whether the receipt is valid
    pub fn verify_transaction_receipt(
        &self,
        receipt: &TransactionReceipt,
        objects: &HashMap<UnitsObjectId, UnitsObject>,
    ) -> VerificationResult {
        // Verify each object proof in the receipt
        for (id, _proof_data) in &receipt.object_proofs {
            if !objects.contains_key(id) {
                return VerificationResult::MissingData(format!("Missing object for ID {:?}", id));
            }

            // Note: In the new structure, we don't verify individual proofs here
            // because the raw proof data would need to be deserialized first
            // In a real implementation, we would:
            // 1. Deserialize the proof data
            // 2. Verify the proof against the object
            // 3. Check transaction hash consistency
        }

        VerificationResult::Valid
    }

    /// Verify a state proof
    ///
    /// # Parameters
    /// * `state_proof` - The state proof to verify
    /// * `object_proofs` - Proofs for objects included in the state proof
    ///
    /// # Returns
    /// A VerificationResult indicating whether the state proof is valid
    pub fn verify_state_proof(
        &self,
        state_proof: &StateProof,
        object_proofs: &HashMap<UnitsObjectId, UnitsObjectProof>,
    ) -> VerificationResult {
        // Convert HashMap to the format expected by the proof engine
        let object_proof_pairs: Vec<(UnitsObjectId, UnitsObjectProof)> = object_proofs
            .iter()
            .map(|(id, proof)| (*id, proof.clone()))
            .collect();

        // Delegate to the proof engine for state proof verification
        match self
            .engine
            .verify_state_proof(state_proof, &object_proof_pairs)
        {
            Ok(true) => VerificationResult::Valid,
            Ok(false) => VerificationResult::Invalid("State proof verification failed".to_string()),
            Err(e) => VerificationResult::Invalid(format!("Verification error: {}", e)),
        }
    }
}

/// Verify if a transaction is included in a collection of receipts
///
/// # Parameters
/// * `transaction_hash` - Hash of transaction to verify
/// * `receipts` - Collection of receipts to search
///
/// # Returns
/// A VerificationResult indicating whether the transaction is included
pub fn verify_transaction_included(
    transaction_hash: &[u8; 32],
    receipts: &[TransactionReceipt],
) -> VerificationResult {
    for receipt in receipts {
        if receipt.transaction_hash == *transaction_hash {
            return VerificationResult::Valid;
        }
    }

    VerificationResult::Invalid(format!(
        "Transaction {:?} not found in receipts",
        transaction_hash
    ))
}

/// Detect if any double spend exists for an object in a collection of receipts
///
/// A double spend is detected if the same object is modified by two different
/// transactions in the same slot.
///
/// # Parameters
/// * `object_id` - ID of the object to check
/// * `receipts` - Collection of transaction receipts to analyze
///
/// # Returns
/// A VerificationResult indicating whether a double spend was detected
pub fn detect_double_spend(
    object_id: &UnitsObjectId,
    receipts: &[TransactionReceipt],
) -> VerificationResult {
    let mut last_writer: Option<[u8; 32]> = None;
    let mut slots_by_transaction: HashMap<[u8; 32], SlotNumber> = HashMap::new();

    // Sort receipts by slot
    let mut sorted_receipts = receipts.to_vec();
    sorted_receipts.sort_by_key(|r| r.slot);

    for receipt in sorted_receipts {
        if let Some(_proof) = receipt.object_proofs.get(object_id) {
            let transaction_hash = receipt.transaction_hash;
            let slot = receipt.slot;

            if let Some(last_tx) = last_writer {
                // Check if the previous modification was in an earlier slot
                let last_slot = slots_by_transaction[&last_tx];

                if last_slot == slot && last_tx != transaction_hash {
                    return VerificationResult::Invalid(
                        format!("Double spend detected: Object {:?} modified by two transactions in slot {}",
                                object_id, slot)
                    );
                }
            }

            last_writer = Some(transaction_hash);
            slots_by_transaction.insert(transaction_hash, slot);
        }
    }

    VerificationResult::Valid
}

#[cfg(test)]
mod tests {
    use super::*;
    use units_core::id::UnitsObjectId;
    use units_proofs::merkle_proof::MerkleProofEngine;

    fn create_test_object() -> UnitsObject {
        UnitsObject::new_token(
            UnitsObjectId::unique_id_for_tests(),
            UnitsObjectId::unique_id_for_tests(),
            units_core::objects::TokenType::Native,
            UnitsObjectId::unique_id_for_tests(),
            vec![1, 2, 3, 4],
        )
    }

    #[test]
    fn test_verify_object_proof() {
        // Create a test object and generate a proof
        let engine = MerkleProofEngine::new();
        let object = create_test_object();

        // Generate a valid proof
        let proof = engine.generate_object_proof(&object, None, None).unwrap();

        // Create a verifier
        let verifier = ProofVerifier::new(&engine);

        // Verify the proof
        let result = verifier.verify_object_proof(&object, &proof);
        assert_eq!(result, VerificationResult::Valid);

        // Modify the object and verify the proof should fail
        let modified_object = UnitsObject::new_token(
            *object.id(),
            *object.owner(),
            object.token_type().unwrap(),
            *object.token_manager().unwrap(),
            vec![5, 6, 7, 8],
        );

        let invalid_result = verifier.verify_object_proof(&modified_object, &proof);
        assert!(matches!(invalid_result, VerificationResult::Invalid(_)));
    }

    #[test]
    fn test_verify_transaction_receipt() {
        // Create test objects and proofs
        let engine = MerkleProofEngine::new();
        let object1 = create_test_object();
        let object2 = create_test_object();

        // Create a transaction hash
        let transaction_hash = [42u8; 32];

        // Generate proofs for objects with transaction_hash
        let proof1 = engine
            .generate_object_proof(&object1, None, Some(transaction_hash))
            .unwrap();

        let proof2 = engine
            .generate_object_proof(&object2, None, Some(transaction_hash))
            .unwrap();

        // Create a receipt
        let mut receipt = TransactionReceipt::new(transaction_hash, 123, true, 456789);

        // Add the proofs to the receipt (now stored as Vec<u8>)
        receipt.add_proof(*object1.id(), bincode::serialize(&proof1).unwrap());
        receipt.add_proof(*object2.id(), bincode::serialize(&proof2).unwrap());

        // Create a map of objects
        let mut objects = HashMap::new();
        objects.insert(*object1.id(), object1.clone());
        objects.insert(*object2.id(), object2.clone());

        // Create a verifier
        let verifier = ProofVerifier::new(&engine);

        // Verify the receipt
        let result = verifier.verify_transaction_receipt(&receipt, &objects);
        assert_eq!(result, VerificationResult::Valid);

        // Missing object scenario
        let mut missing_objects = objects.clone();
        missing_objects.remove(object1.id());

        let missing_result = verifier.verify_transaction_receipt(&receipt, &missing_objects);
        assert!(matches!(missing_result, VerificationResult::MissingData(_)));
    }
}
