use crate::id::UnitsObjectId;
use crate::objects::TokenizedObject;
use crate::proofs::{StateProof, TokenizedObjectProof};

/// Iterator for traversing objects in storage
pub trait UnitsStorageIterator {
    /// Get the next object from the iterator
    fn next(&mut self) -> Option<TokenizedObject>;
}

/// Engine for creating and verifying proofs
pub trait UnitsStorageProofEngine {
    /// Generate a state proof representing the current state of all objects
    fn generate_state_proof(&self) -> StateProof;
    
    /// Get the proof for a specific object
    fn get_proof(&self, id: &UnitsObjectId) -> Option<TokenizedObjectProof>;
    
    /// Verify a proof for a specific object
    fn verify_proof(&self, id: &UnitsObjectId, proof: &TokenizedObjectProof) -> bool;
}

/// Main storage interface for UNITS objects
pub trait UnitsStorage: UnitsStorageProofEngine {
    /// Get an object by its ID
    fn get(&self, id: &UnitsObjectId) -> Option<TokenizedObject>;
    
    /// Store an object
    fn set(&self, object: &TokenizedObject) -> Result<(), String>;
    
    /// Create an iterator to scan through all objects
    fn scan(&self) -> Box<dyn UnitsStorageIterator + '_>;
    
    /// Delete an object by its ID
    fn delete(&self, id: &UnitsObjectId) -> Result<(), String>;
}
