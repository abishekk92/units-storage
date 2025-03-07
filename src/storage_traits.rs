use crate::error::StorageError;
use crate::id::UnitsObjectId;
use crate::objects::TokenizedObject;
use crate::proofs::{ProofEngine, SlotNumber, StateProof, TokenizedObjectProof};
use serde::{Deserialize, Serialize};
use std::iter::Iterator;
use std::path::Path;

/// Iterator for traversing objects in storage
pub trait UnitsStorageIterator: Iterator<Item = Result<TokenizedObject, StorageError>> {}

/// Iterator for traversing object proofs in storage
pub trait UnitsProofIterator: Iterator<Item = Result<(SlotNumber, TokenizedObjectProof), StorageError>> {}

/// Iterator for traversing state proofs in storage
pub trait UnitsStateProofIterator: Iterator<Item = Result<StateProof, StorageError>> {}

/// A write-ahead log entry for an object update
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WALEntry {
    /// The object being updated
    pub object: TokenizedObject,
    
    /// The slot in which this update occurred
    pub slot: SlotNumber,
    
    /// The proof generated for this update
    pub proof: TokenizedObjectProof,
    
    /// Timestamp of when this update was recorded
    pub timestamp: u64,
}

/// Write-ahead log for durably recording all updates before they're committed to storage
pub trait UnitsWriteAheadLog {
    /// Initialize the write-ahead log
    ///
    /// # Parameters
    /// * `path` - The file path for the write-ahead log
    ///
    /// # Returns
    /// Ok(()) if successful, Err otherwise
    fn init(&self, path: &Path) -> Result<(), StorageError>;
    
    /// Record an object update in the write-ahead log
    ///
    /// # Parameters
    /// * `object` - The object being updated
    /// * `proof` - The proof for this update
    ///
    /// # Returns
    /// Ok(()) if successful, Err otherwise
    fn record_update(
        &self, 
        object: &TokenizedObject, 
        proof: &TokenizedObjectProof
    ) -> Result<(), StorageError>;
    
    /// Record a state proof in the write-ahead log
    ///
    /// # Parameters
    /// * `state_proof` - The state proof to record
    ///
    /// # Returns
    /// Ok(()) if successful, Err otherwise
    fn record_state_proof(&self, state_proof: &StateProof) -> Result<(), StorageError>;
    
    /// Get an iterator over all WAL entries
    ///
    /// # Returns
    /// An iterator that yields WALEntry instances
    fn iterate_entries(&self) -> Box<dyn Iterator<Item = Result<WALEntry, StorageError>> + '_>;
}

/// Engine for creating and verifying proofs
pub trait UnitsStorageProofEngine {
    /// Get the proof engine used by this storage
    fn proof_engine(&self) -> &dyn ProofEngine;

    /// Generate a state proof representing the current state of all objects
    ///
    /// # Parameters
    /// * `slot` - Optional slot number to use for the proof (defaults to current slot)
    ///
    /// # Returns
    /// A `StateProof` that cryptographically commits to the current state of all objects
    fn generate_state_proof(&self, slot: Option<SlotNumber>) -> Result<StateProof, StorageError>;

    /// Get the most recent proof for a specific object
    ///
    /// # Parameters
    /// * `id` - The ID of the object to get the proof for
    ///
    /// # Returns
    /// Some(proof) if the object exists, None otherwise
    fn get_proof(&self, id: &UnitsObjectId) -> Result<Option<TokenizedObjectProof>, StorageError>;
    
    /// Get all historical proofs for a specific object
    ///
    /// # Parameters
    /// * `id` - The ID of the object to get proofs for
    ///
    /// # Returns
    /// An iterator that yields (slot, proof) pairs for each historical state
    fn get_proof_history(&self, id: &UnitsObjectId) -> Box<dyn UnitsProofIterator + '_>;
    
    /// Get a specific historical proof for an object
    ///
    /// # Parameters
    /// * `id` - The ID of the object to get the proof for
    /// * `slot` - The slot number for which to retrieve the proof
    ///
    /// # Returns
    /// Some(proof) if a proof exists for that object at that slot, None otherwise
    fn get_proof_at_slot(
        &self,
        id: &UnitsObjectId,
        slot: SlotNumber
    ) -> Result<Option<TokenizedObjectProof>, StorageError>;
    
    /// Get all state proofs
    ///
    /// # Returns
    /// An iterator that yields all state proofs ordered by slot
    fn get_state_proofs(&self) -> Box<dyn UnitsStateProofIterator + '_>;
    
    /// Get a state proof for a specific slot
    ///
    /// # Parameters
    /// * `slot` - The slot number for which to retrieve the state proof
    ///
    /// # Returns
    /// Some(proof) if a state proof exists for that slot, None otherwise
    fn get_state_proof_at_slot(&self, slot: SlotNumber) -> Result<Option<StateProof>, StorageError>;

    /// Verify a proof for a specific object
    ///
    /// # Parameters
    /// * `id` - The ID of the object the proof is for
    /// * `proof` - The proof to verify
    ///
    /// # Returns
    /// `true` if the proof is valid for the object, `false` otherwise
    fn verify_proof(
        &self,
        id: &UnitsObjectId,
        proof: &TokenizedObjectProof,
    ) -> Result<bool, StorageError>;
    
    /// Verify a proof chain for a specific object
    ///
    /// # Parameters
    /// * `id` - The ID of the object
    /// * `start_slot` - The starting slot for verification
    /// * `end_slot` - The ending slot for verification
    ///
    /// # Returns
    /// `true` if the proof chain is valid, `false` otherwise
    fn verify_proof_chain(
        &self,
        id: &UnitsObjectId,
        start_slot: SlotNumber,
        end_slot: SlotNumber
    ) -> Result<bool, StorageError>;
}

/// Main storage interface for UNITS objects
pub trait UnitsStorage: UnitsStorageProofEngine + UnitsWriteAheadLog {
    /// Get an object by its ID
    ///
    /// # Parameters
    /// * `id` - The ID of the object to retrieve
    ///
    /// # Returns
    /// Some(object) if found, None otherwise
    fn get(&self, id: &UnitsObjectId) -> Result<Option<TokenizedObject>, StorageError>;
    
    /// Get an object at a specific historical slot
    ///
    /// # Parameters
    /// * `id` - The ID of the object to retrieve
    /// * `slot` - The slot number at which to retrieve the object
    ///
    /// # Returns
    /// Some(object) if found at that slot, None otherwise
    fn get_at_slot(
        &self, 
        id: &UnitsObjectId, 
        slot: SlotNumber
    ) -> Result<Option<TokenizedObject>, StorageError>;

    /// Store an object
    ///
    /// # Parameters
    /// * `object` - The tokenized object to store
    ///
    /// # Returns
    /// The generated proof for this update
    fn set(&self, object: &TokenizedObject) -> Result<TokenizedObjectProof, StorageError>;

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
    /// The generated proof for this deletion
    fn delete(&self, id: &UnitsObjectId) -> Result<TokenizedObjectProof, StorageError>;
    
    /// Generate a state proof for the current slot and store it
    ///
    /// # Returns
    /// The generated state proof
    fn generate_and_store_state_proof(&self) -> Result<StateProof, StorageError>;
}
