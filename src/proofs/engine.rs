use crate::error::StorageError;
use crate::id::UnitsObjectId;
use crate::objects::TokenizedObject;
use crate::proofs::{SlotNumber, StateProof, TokenizedObjectProof};
use crate::verification::VerificationResult;

/// Interface for different proof engine implementations
/// 
/// A ProofEngine provides cryptographic mechanisms to generate and verify proofs
/// for tokenized objects in the UNITS system. These proofs ensure:
/// 
/// 1. Data integrity - objects have not been tampered with
/// 2. State consistency - the full state of all objects is valid
/// 3. Verifiable history - changes to objects can be verified
pub trait ProofEngine {
    /// Generate a cryptographic proof for a tokenized object
    /// 
    /// This creates a proof that commits to the current state of the object.
    /// The proof can later be verified to ensure the object has not been modified.
    ///
    /// # Parameters
    /// * `object` - The tokenized object to generate a proof for
    /// * `prev_proof` - The previous proof for this object, if any
    /// * `transaction_hash` - Hash of the transaction that led to this state change, if any
    /// 
    /// # Returns
    /// A cryptographic proof that commits to the object's current state
    /// and links to its previous state through the prev_proof_hash
    fn generate_object_proof(
        &self, 
        object: &TokenizedObject,
        prev_proof: Option<&TokenizedObjectProof>,
        transaction_hash: Option<[u8; 32]>
    ) -> Result<TokenizedObjectProof, StorageError>;
    
    /// Verify that a proof correctly commits to an object's state
    /// 
    /// # Parameters
    /// * `object` - The tokenized object to verify the proof against
    /// * `proof` - The proof to verify
    /// 
    /// # Returns
    /// `true` if the proof is valid for the given object, `false` otherwise
    fn verify_object_proof(
        &self, 
        object: &TokenizedObject, 
        proof: &TokenizedObjectProof
    ) -> Result<bool, StorageError>;
    
    /// Verify the chain of proofs for an object
    /// 
    /// This verifies that a sequence of proofs forms a valid chain,
    /// with each proof correctly linking to its predecessor.
    ///
    /// # Parameters
    /// * `object` - The current tokenized object
    /// * `proof` - The current proof for the object
    /// * `prev_proof` - The previous proof in the chain
    /// 
    /// # Returns
    /// `true` if the chain is valid, `false` otherwise
    fn verify_proof_chain(
        &self,
        object: &TokenizedObject,
        proof: &TokenizedObjectProof,
        prev_proof: &TokenizedObjectProof
    ) -> Result<bool, StorageError>;
    
    /// Generate a state proof from a collection of object proofs
    /// 
    /// This creates an aggregated proof that commits to the state of multiple objects.
    /// State proofs are used to verify the collective state of the system.
    ///
    /// # Parameters
    /// * `object_proofs` - A list of object IDs and their associated proofs
    /// * `prev_state_proof` - The previous state proof, if any
    /// * `slot` - The slot number for which to generate the proof
    /// 
    /// # Returns
    /// A cryptographic proof that commits to the state of all provided objects
    fn generate_state_proof(
        &self, 
        object_proofs: &[(UnitsObjectId, TokenizedObjectProof)],
        prev_state_proof: Option<&StateProof>,
        slot: SlotNumber
    ) -> Result<StateProof, StorageError>;
    
    /// Verify that a state proof correctly commits to a collection of object proofs
    /// 
    /// # Parameters
    /// * `state_proof` - The state proof to verify
    /// * `object_proofs` - The list of object IDs and their proofs that should be committed to
    /// 
    /// # Returns
    /// `true` if the state proof is valid for all the given objects, `false` otherwise
    fn verify_state_proof(
        &self,
        state_proof: &StateProof,
        object_proofs: &[(UnitsObjectId, TokenizedObjectProof)],
    ) -> Result<bool, StorageError>;
    
    /// Verify a chain of state proofs
    /// 
    /// This verifies that two consecutive state proofs form a valid chain,
    /// with the newer proof correctly linking to the previous one.
    ///
    /// # Parameters
    /// * `state_proof` - The current state proof
    /// * `prev_state_proof` - The previous state proof in the chain
    /// 
    /// # Returns
    /// `true` if the chain is valid, `false` otherwise
    fn verify_state_proof_chain(
        &self,
        state_proof: &StateProof,
        prev_state_proof: &StateProof
    ) -> Result<bool, StorageError>;
    
    /// Verify a proof history for an object
    /// 
    /// This verifies a sequence of object states and their corresponding proofs,
    /// ensuring that each proof is valid and they form a valid chain.
    ///
    /// # Parameters
    /// * `object_states` - Vector of (slot, object state) pairs in ascending slot order
    /// * `proofs` - Vector of (slot, proof) pairs in ascending slot order
    /// 
    /// # Returns
    /// A VerificationResult indicating whether the proof chain is valid
    fn verify_proof_history(
        &self,
        object_states: &[(SlotNumber, TokenizedObject)],
        proofs: &[(SlotNumber, TokenizedObjectProof)]
    ) -> VerificationResult {
        // Default implementation delegates to the LatticeProofEngine implementation
        if object_states.is_empty() || proofs.is_empty() {
            return VerificationResult::MissingData("No object states or proofs provided".to_string());
        }
        
        // Verify each state has a corresponding proof
        for (slot, obj) in object_states {
            // Find matching proof for this slot
            let matching_proof = proofs.iter().find(|(proof_slot, _)| proof_slot == slot);
            
            if let Some((_, proof)) = matching_proof {
                match self.verify_object_proof(obj, proof) {
                    Ok(true) => {},
                    Ok(false) => return VerificationResult::Invalid(
                        format!("Proof verification failed for slot {}", slot)
                    ),
                    Err(e) => return VerificationResult::Invalid(
                        format!("Proof verification error at slot {}: {}", slot, e)
                    ),
                }
            } else {
                return VerificationResult::MissingData(
                    format!("Missing proof for slot {}", slot)
                );
            }
        }
        
        // Verify proof chain links - since proofs are ordered by slot, we can just iterate sequentially
        for i in 1..proofs.len() {
            let (current_slot, current_proof) = &proofs[i];
            let (prev_slot, prev_proof) = &proofs[i - 1];
            
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
}