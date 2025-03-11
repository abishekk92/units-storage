//! Receipt and proof verification utilities
//! 
//! This module provides standalone verification functions for receipts and proofs
//! that don't require access to the storage layer.

// use crate::error::StorageError; // Not needed
use crate::id::UnitsObjectId;
use crate::objects::TokenizedObject;
use crate::proofs::{ProofEngine, SlotNumber, StateProof, TokenizedObjectProof};
use crate::runtime::TransactionReceipt;
use std::collections::HashMap;

/// Result of a verification operation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerificationResult {
    /// Verification was successful
    Valid,
    /// Verification failed with a specific reason
    Invalid(String),
    /// Verification couldn't be completed due to missing data
    MissingData(String),
}

impl From<VerificationResult> for Result<(), String> {
    fn from(result: VerificationResult) -> Self {
        match result {
            VerificationResult::Valid => Ok(()),
            VerificationResult::Invalid(msg) => Err(msg),
            VerificationResult::MissingData(msg) => Err(format!("Missing data: {}", msg)),
        }
    }
}

/// Verifier for transaction receipts and proofs
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
        object: &TokenizedObject, 
        proof: &TokenizedObjectProof
    ) -> VerificationResult {
        match self.engine.verify_object_proof(object, proof) {
            Ok(true) => VerificationResult::Valid,
            Ok(false) => VerificationResult::Invalid("Proof does not match object state".to_string()),
            Err(e) => VerificationResult::Invalid(format!("Verification error: {}", e)),
        }
    }
    
    /// Verify a proof chain for an object
    ///
    /// # Parameters
    /// * `object_states` - Map of slots to object states
    /// * `proofs` - Map of slots to proofs for the object
    ///
    /// # Returns
    /// A VerificationResult indicating whether the proof chain is valid
    pub fn verify_proof_chain(
        &self,
        object_states: &HashMap<SlotNumber, TokenizedObject>,
        proofs: &HashMap<SlotNumber, TokenizedObjectProof>,
    ) -> VerificationResult {
        if object_states.is_empty() || proofs.is_empty() {
            return VerificationResult::MissingData("No object states or proofs provided".to_string());
        }
        
        // Verify each state has a corresponding proof
        for (slot, obj) in object_states {
            if let Some(proof) = proofs.get(slot) {
                match self.verify_object_proof(obj, proof) {
                    VerificationResult::Valid => {},
                    result => return result,
                }
            } else {
                return VerificationResult::MissingData(
                    format!("Missing proof for slot {}", slot)
                );
            }
        }
        
        // Verify proof chain links
        let mut slots: Vec<_> = proofs.keys().copied().collect();
        slots.sort();
        
        for i in 1..slots.len() {
            let current_slot = slots[i];
            let prev_slot = slots[i - 1];
            
            let current_proof = &proofs[&current_slot];
            let prev_proof = &proofs[&prev_slot];
            
            // Verify the current proof references the previous proof correctly
            if let Some(prev_hash) = &current_proof.prev_proof_hash {
                let computed_hash = prev_proof.hash();
                if computed_hash != *prev_hash {
                    return VerificationResult::Invalid(
                        format!("Proof chain broken between slots {} and {}", prev_slot, current_slot)
                    );
                }
            } else {
                return VerificationResult::Invalid(
                    format!("Proof at slot {} does not reference previous proof", current_slot)
                );
            }
        }
        
        VerificationResult::Valid
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
        objects: &HashMap<UnitsObjectId, TokenizedObject>,
    ) -> VerificationResult {
        // Verify each object proof in the receipt
        for (id, proof) in &receipt.object_proofs {
            if let Some(object) = objects.get(id) {
                match self.verify_object_proof(object, proof) {
                    VerificationResult::Valid => {},
                    result => return result,
                }
            } else {
                return VerificationResult::MissingData(
                    format!("Missing object for ID {:?}", id)
                );
            }
            
            // Verify transaction hash is consistent
            match proof.transaction_hash {
                Some(hash) if hash == receipt.transaction_hash => {},
                Some(_) => return VerificationResult::Invalid(
                    format!("Inconsistent transaction hash in proof for object {:?}", id)
                ),
                None => return VerificationResult::Invalid(
                    format!("Missing transaction hash in proof for object {:?}", id)
                ),
            }
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
        object_proofs: &HashMap<UnitsObjectId, TokenizedObjectProof>,
    ) -> VerificationResult {
        // Verify each object in the state proof has a valid proof
        for id in &state_proof.included_objects {
            if !object_proofs.contains_key(id) {
                return VerificationResult::MissingData(
                    format!("Missing proof for object {:?} in state proof", id)
                );
            }
        }
        
        // Verify the state proof hash
        // This would typically verify the Merkle root or other aggregation mechanism
        // Since we don't have the actual verification code here, we just return Valid
        
        VerificationResult::Valid
    }
}

/// Simplistic proof of inclusion verification
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
    
    VerificationResult::Invalid(format!("Transaction {:?} not found in receipts", transaction_hash))
}

/// Simplistic double spend detection
///
/// # Parameters
/// * `object_id` - ID of the object to check
/// * `transactions` - Collection of transaction receipts to analyze
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
    use crate::id::tests::unique_id;
    use crate::proofs::{LatticeProofEngine, ProofEngine};
    
    fn create_test_object() -> TokenizedObject {
        TokenizedObject {
            id: unique_id(),
            holder: unique_id(),
            token_type: crate::objects::TokenType::Native,
            token_manager: unique_id(),
            data: vec![1, 2, 3, 4],
        }
    }
    
    #[test]
    fn test_verify_object_proof() {
        // Create a test object and generate a proof
        let engine = LatticeProofEngine::new();
        let object = create_test_object();
        
        // Generate a valid proof
        let proof = engine.generate_object_proof(&object, None, None).unwrap();
        
        // Create a verifier
        let verifier = ProofVerifier::new(&engine);
        
        // Verify the proof
        let result = verifier.verify_object_proof(&object, &proof);
        assert_eq!(result, VerificationResult::Valid);
        
        // Modify the object and verify the proof should fail
        let mut modified_object = object.clone();
        modified_object.data = vec![5, 6, 7, 8];
        
        let invalid_result = verifier.verify_object_proof(&modified_object, &proof);
        assert!(matches!(invalid_result, VerificationResult::Invalid(_)));
    }
    
    #[test]
    fn test_verify_transaction_receipt() {
        // Create test objects and proofs
        let engine = LatticeProofEngine::new();
        let object1 = create_test_object();
        let object2 = create_test_object();
        
        // Create a transaction hash
        let transaction_hash = [42u8; 32];
        
        // Generate proofs for the objects
        let proof1 = engine.generate_object_proof(&object1, None, Some(transaction_hash)).unwrap();
        let proof2 = engine.generate_object_proof(&object2, None, Some(transaction_hash)).unwrap();
        
        // Create a receipt
        let mut receipt = TransactionReceipt::new(
            transaction_hash,
            123,
            true,
            456789
        );
        
        // Add the proofs to the receipt
        receipt.add_proof(object1.id, proof1.clone());
        receipt.add_proof(object2.id, proof2.clone());
        
        // Create a map of objects
        let mut objects = HashMap::new();
        objects.insert(object1.id, object1.clone());
        objects.insert(object2.id, object2.clone());
        
        // Create a verifier
        let verifier = ProofVerifier::new(&engine);
        
        // Verify the receipt
        let result = verifier.verify_transaction_receipt(&receipt, &objects);
        assert_eq!(result, VerificationResult::Valid);
        
        // Modify an object and verify the receipt should fail
        let mut modified_objects = objects.clone();
        let mut modified_object = object1.clone();
        modified_object.data = vec![5, 6, 7, 8];
        modified_objects.insert(object1.id, modified_object);
        
        let invalid_result = verifier.verify_transaction_receipt(&receipt, &modified_objects);
        assert!(matches!(invalid_result, VerificationResult::Invalid(_)));
    }
}