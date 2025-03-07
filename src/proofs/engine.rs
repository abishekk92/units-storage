use crate::error::StorageError;
use crate::id::UnitsObjectId;
use crate::objects::TokenizedObject;
use crate::proofs::{StateProof, TokenizedObjectProof};

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
    /// 
    /// # Returns
    /// A cryptographic proof that commits to the object's current state
    fn generate_object_proof(&self, object: &TokenizedObject) -> Result<TokenizedObjectProof, StorageError>;
    
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
    
    /// Generate a state proof from a collection of object proofs
    /// 
    /// This creates an aggregated proof that commits to the state of multiple objects.
    /// State proofs are used to verify the collective state of the system.
    ///
    /// # Parameters
    /// * `object_proofs` - A list of object IDs and their associated proofs
    /// 
    /// # Returns
    /// A cryptographic proof that commits to the state of all provided objects
    fn generate_state_proof(
        &self, 
        object_proofs: &[(UnitsObjectId, TokenizedObjectProof)]
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
}