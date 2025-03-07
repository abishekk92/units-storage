use crate::error::StorageError;
use crate::id::UnitsObjectId;
use crate::objects::TokenizedObject;
use crate::proofs::{ProofEngine, StateProof, TokenizedObjectProof};
use std::iter::Iterator;

/// Iterator for traversing objects in storage
pub trait UnitsStorageIterator: Iterator<Item = Result<TokenizedObject, StorageError>> {}

/// Engine for creating and verifying proofs
pub trait UnitsStorageProofEngine {
    /// Get the proof engine used by this storage
    fn proof_engine(&self) -> &dyn ProofEngine;

    /// Generate a state proof representing the current state of all objects
    ///
    /// # Returns
    /// A `StateProof` that cryptographically commits to the current state of all objects
    fn generate_state_proof(&self) -> Result<StateProof, StorageError>;

    /// Get the proof for a specific object
    ///
    /// # Parameters
    /// * `id` - The ID of the object to get the proof for
    ///
    /// # Returns
    /// Some(proof) if the object exists, None otherwise
    fn get_proof(&self, id: &UnitsObjectId) -> Result<Option<TokenizedObjectProof>, StorageError>;

    /// Verify a proof for a specific object
    ///
    /// # Parameters
    /// * `id` - The ID of the object the proof is for
    /// * `proof` - The proof to verify
    ///
    /// # Returns
    // TODO
    // THINK: Is it verify a proof against the object or verify an object proof against the state
    // proof?
    // How are we going to store the state proof and object proof per slot?
    // Definitely not the right approach
    fn verify_proof(
        &self,
        id: &UnitsObjectId,
        proof: &TokenizedObjectProof,
    ) -> Result<bool, StorageError>;
}

/// Main storage interface for UNITS objects
pub trait UnitsStorage: UnitsStorageProofEngine {
    /// Get an object by its ID
    ///
    /// # Parameters
    /// * `id` - The ID of the object to retrieve
    ///
    /// # Returns
    /// Some(object) if found, None otherwise
    fn get(&self, id: &UnitsObjectId) -> Result<Option<TokenizedObject>, StorageError>;

    /// Store an object
    ///
    /// # Parameters
    /// * `object` - The tokenized object to store
    ///
    /// # Returns
    /// Ok(()) if successful, Err with the error otherwise
    fn set(&self, object: &TokenizedObject) -> Result<(), StorageError>;

    /// Create an iterator to scan through all objects
    ///
    /// # Returns
    /// A boxed iterator that yields `TokenizedObject`s
    fn scan(&self) -> Box<dyn UnitsStorageIterator + '_>;

    /// Delete an object by its ID
    ///
    /// # Parameters
    /// * `id` - The ID of the object to delete
    ///
    /// # Returns
    /// Ok(()) if successful, Err with the error otherwise
    fn delete(&self, id: &UnitsObjectId) -> Result<(), StorageError>;
}
