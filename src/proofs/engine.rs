use crate::id::UnitsObjectId;
use crate::objects::TokenizedObject;
use crate::proofs::{StateProof, TokenizedObjectProof};

/// Interface for different proof engine implementations
pub trait ProofEngine {
    /// Generate a proof for a tokenized object
    fn generate_object_proof(&self, object: &TokenizedObject) -> TokenizedObjectProof;
    
    /// Verify a proof for a tokenized object
    fn verify_object_proof(&self, object: &TokenizedObject, proof: &TokenizedObjectProof) -> bool;
    
    /// Generate a state proof from a collection of object proofs
    fn generate_state_proof(&self, object_proofs: &[(UnitsObjectId, TokenizedObjectProof)]) -> StateProof;
    
    /// Verify that a state proof is valid for a collection of object proofs
    fn verify_state_proof(
        &self,
        state_proof: &StateProof,
        object_proofs: &[(UnitsObjectId, TokenizedObjectProof)],
    ) -> bool;
}