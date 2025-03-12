use units_core::error::StorageError;
use units_core::id::UnitsObjectId;
use units_core::objects::TokenizedObject;
use units_proofs::{ProofEngine, SlotNumber, StateProof, TokenizedObjectProof};
use serde::{Deserialize, Serialize};

/// A temporary TransactionReceipt implementation until the units-runtime crate is fully functional
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionReceipt {
    pub transaction_hash: [u8; 32],
    pub slot: SlotNumber,
    pub success: bool,
    pub timestamp: u64,
    pub proofs: HashMap<UnitsObjectId, TokenizedObjectProof>,
}

impl TransactionReceipt {
    pub fn new(transaction_hash: [u8; 32], slot: SlotNumber, success: bool, timestamp: u64) -> Self {
        Self {
            transaction_hash,
            slot,
            success,
            timestamp,
            proofs: HashMap::new(),
        }
    }

    pub fn add_proof(&mut self, id: UnitsObjectId, proof: TokenizedObjectProof) {
        self.proofs.insert(id, proof);
    }
}
use std::collections::HashMap;
use std::iter::Iterator;
use std::path::Path;

/// Iterator for traversing objects in storage
pub trait UnitsStorageIterator: Iterator<Item = Result<TokenizedObject, StorageError>> {}

/// Iterator for traversing object proofs in storage
pub trait UnitsProofIterator:
    Iterator<Item = Result<(SlotNumber, TokenizedObjectProof), StorageError>>
{
}

/// Iterator for traversing state proofs in storage
pub trait UnitsStateProofIterator: Iterator<Item = Result<StateProof, StorageError>> {}

/// Iterator for traversing transaction receipts in storage
pub trait UnitsReceiptIterator: Iterator<Item = Result<TransactionReceipt, StorageError>> {}

/// Storage interface for transaction receipts
pub trait TransactionReceiptStorage {
    /// Store a transaction receipt
    ///
    /// # Parameters
    /// * `receipt` - The transaction receipt to store
    ///
    /// # Returns
    /// Ok(()) if successful, Err otherwise
    fn store_receipt(&self, receipt: &TransactionReceipt) -> Result<(), StorageError>;
    
    /// Get a transaction receipt by transaction hash
    ///
    /// # Parameters
    /// * `hash` - The transaction hash to get the receipt for
    ///
    /// # Returns
    /// Some(receipt) if found, None otherwise
    fn get_receipt(&self, hash: &[u8; 32]) -> Result<Option<TransactionReceipt>, StorageError>;
    
    /// Get all transaction receipts for a specific object
    ///
    /// # Parameters
    /// * `id` - The ID of the object to get receipts for
    ///
    /// # Returns
    /// An iterator that yields all receipts that affected this object
    fn get_receipts_for_object(&self, id: &UnitsObjectId) -> Box<dyn UnitsReceiptIterator + '_>;
    
    /// Get all transaction receipts in a specific slot
    ///
    /// # Parameters
    /// * `slot` - The slot to get receipts for
    ///
    /// # Returns
    /// An iterator that yields all receipts in this slot
    fn get_receipts_in_slot(&self, slot: SlotNumber) -> Box<dyn UnitsReceiptIterator + '_>;
}
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

    /// Hash of the transaction that led to this update, if any
    pub transaction_hash: Option<[u8; 32]>,
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
    /// * `transaction_hash` - Hash of the transaction that led to this update, if any
    ///
    /// # Returns
    /// Ok(()) if successful, Err otherwise
    fn record_update(
        &self,
        object: &TokenizedObject,
        proof: &TokenizedObjectProof,
        transaction_hash: Option<[u8; 32]>,
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
        slot: SlotNumber,
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
    fn get_state_proof_at_slot(&self, slot: SlotNumber)
        -> Result<Option<StateProof>, StorageError>;

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
    /// Verifies that all proofs for an object between the given slots form a valid chain,
    /// with each proof correctly linking to its predecessor through cryptographic hashes.
    ///
    /// # Parameters
    /// * `id` - The ID of the object
    /// * `start_slot` - The starting slot for verification
    /// * `end_slot` - The ending slot for verification
    ///
    /// # Performance
    /// This method is optimized for sequentially increasing slot numbers, using vectors
    /// instead of hash maps for better performance.
    ///
    /// # Returns
    /// `true` if the proof chain is valid, `false` otherwise
    fn verify_proof_chain(
        &self,
        id: &UnitsObjectId,
        start_slot: SlotNumber,
        end_slot: SlotNumber,
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
        slot: SlotNumber,
    ) -> Result<Option<TokenizedObject>, StorageError>;
    
    /// Get object state history between slots
    ///
    /// # Parameters
    /// * `id` - The ID of the object to retrieve history for
    /// * `start_slot` - The starting slot (inclusive)
    /// * `end_slot` - The ending slot (inclusive)
    ///
    /// # Returns
    /// A map of slot numbers to objects representing the state at each slot where changes occurred
    fn get_history(
        &self,
        id: &UnitsObjectId,
        start_slot: SlotNumber,
        end_slot: SlotNumber,
    ) -> Result<HashMap<SlotNumber, TokenizedObject>, StorageError> {
        // Default implementation that uses get_proof_history and reconstructs objects
        let mut history = HashMap::new();
        
        // Get all proofs for this object between the slots
        let proofs: Vec<_> = self.get_proof_history(id)
            .map(|r| r.unwrap())
            .filter(|(slot, _)| *slot >= start_slot && *slot <= end_slot)
            .collect();
            
        if proofs.is_empty() {
            return Ok(history);
        }
        
        // For each slot with a proof, reconstruct the object at that time
        for (slot, _) in proofs {
            if let Some(obj) = self.get_at_slot(id, slot)? {
                history.insert(slot, obj);
            }
        }
        
        Ok(history)
    }
    
    /// Get all transactions that affected an object
    ///
    /// # Parameters
    /// * `id` - The ID of the object to get transaction history for
    /// * `start_slot` - Optional starting slot for filtering (inclusive)
    /// * `end_slot` - Optional ending slot for filtering (inclusive)
    ///
    /// # Returns
    /// A vector of transaction hashes that affected this object, ordered by slot
    fn get_transaction_history(
        &self,
        id: &UnitsObjectId,
        start_slot: Option<SlotNumber>,
        end_slot: Option<SlotNumber>,
    ) -> Result<Vec<[u8; 32]>, StorageError> {
        // Default implementation that uses get_proof_history
        let mut transactions = Vec::new();
        
        // Get all proofs for this object
        let proofs: Vec<_> = self.get_proof_history(id)
            .map(|r| r.unwrap())
            .filter(|(slot, _)| {
                if let Some(start) = start_slot {
                    if *slot < start {
                        return false;
                    }
                }
                if let Some(end) = end_slot {
                    if *slot > end {
                        return false;
                    }
                }
                true
            })
            .collect();
            
        // Extract transaction hashes from proofs
        for (_, proof) in proofs {
            if let Some(hash) = proof.transaction_hash {
                if !transactions.contains(&hash) {
                    transactions.push(hash);
                }
            }
        }
        
        Ok(transactions)
    }

    /// Store an object
    ///
    /// # Parameters
    /// * `object` - The tokenized object to store
    /// * `transaction_hash` - Hash of the transaction that led to this update, if any
    ///
    /// # Returns
    /// The generated proof for this update
    fn set(
        &self,
        object: &TokenizedObject,
        transaction_hash: Option<[u8; 32]>,
    ) -> Result<TokenizedObjectProof, StorageError>;
    
    /// Store multiple objects in a single transaction
    ///
    /// # Parameters
    /// * `objects` - The tokenized objects to store
    /// * `transaction_hash` - Hash of the transaction that led to these updates
    ///
    /// # Returns
    /// A map of object IDs to their generated proofs
    fn set_batch(
        &self,
        objects: &[TokenizedObject],
        transaction_hash: [u8; 32],
    ) -> Result<HashMap<UnitsObjectId, TokenizedObjectProof>, StorageError> {
        // Default implementation that calls set() for each object
        let mut proofs = HashMap::new();
        
        for object in objects {
            let proof = self.set(object, Some(transaction_hash))?;
            proofs.insert(object.id, proof);
        }
        
        Ok(proofs)
    }

    /// Create an iterator to scan through all objects
    ///
    /// # Returns
    /// A boxed iterator that yields `TokenizedObject`s
    fn scan(&self) -> Box<dyn UnitsStorageIterator + '_>;

    /// Delete an object by its ID
    ///
    /// # Parameters
    /// * `id` - The ID of the object to delete
    /// * `transaction_hash` - Hash of the transaction that led to this deletion, if any
    ///
    /// # Returns
    /// The generated proof for this deletion
    fn delete(
        &self,
        id: &UnitsObjectId,
        transaction_hash: Option<[u8; 32]>,
    ) -> Result<TokenizedObjectProof, StorageError>;
    
    /// Delete multiple objects in a single transaction
    ///
    /// # Parameters
    /// * `ids` - The IDs of the objects to delete
    /// * `transaction_hash` - Hash of the transaction that led to these deletions
    ///
    /// # Returns
    /// A map of object IDs to their generated proofs
    fn delete_batch(
        &self,
        ids: &[UnitsObjectId],
        transaction_hash: [u8; 32],
    ) -> Result<HashMap<UnitsObjectId, TokenizedObjectProof>, StorageError> {
        // Default implementation that calls delete() for each object
        let mut proofs = HashMap::new();
        
        for id in ids {
            let proof = self.delete(id, Some(transaction_hash))?;
            proofs.insert(*id, proof);
        }
        
        Ok(proofs)
    }
    
    /// Execute a transaction and generate a receipt
    ///
    /// # Parameters
    /// * `objects_to_store` - The tokenized objects to store
    /// * `objects_to_delete` - The IDs of objects to delete
    /// * `transaction_hash` - Hash of the transaction being executed
    /// * `slot` - The slot in which this transaction is being executed
    ///
    /// # Returns
    /// A transaction receipt containing all proofs
    fn execute_transaction_batch(
        &self,
        objects_to_store: &[TokenizedObject],
        objects_to_delete: &[UnitsObjectId],
        transaction_hash: [u8; 32],
        slot: SlotNumber,
    ) -> Result<TransactionReceipt, StorageError> {
        // Get the current timestamp
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
            
        // Create a receipt
        let mut receipt = TransactionReceipt::new(
            transaction_hash,
            slot,
            true, // Assume success initially
            timestamp
        );
        
        // Store all objects
        if !objects_to_store.is_empty() {
            let store_proofs = self.set_batch(objects_to_store, transaction_hash)?;
            for (id, proof) in store_proofs {
                receipt.add_proof(id, proof);
            }
        }
        
        // Delete all objects
        if !objects_to_delete.is_empty() {
            let delete_proofs = self.delete_batch(objects_to_delete, transaction_hash)?;
            for (id, proof) in delete_proofs {
                receipt.add_proof(id, proof);
            }
        }
        
        Ok(receipt)
    }

    /// Generate a state proof for the current slot and store it
    ///
    /// # Returns
    /// The generated state proof
    fn generate_and_store_state_proof(&self) -> Result<StateProof, StorageError>;
    
    /// Compact historical data up to a specific slot
    ///
    /// This operation reduces the storage footprint by pruning historical data
    /// while maintaining the ability to verify the integrity of the current state.
    ///
    /// # Parameters
    /// * `before_slot` - Compact history before this slot (exclusive)
    /// * `preserve_state_proofs` - Whether to preserve state proofs during compaction
    ///
    /// # Returns
    /// The number of objects compacted
    fn compact_history(
        &self,
        _before_slot: SlotNumber,
        _preserve_state_proofs: bool,
    ) -> Result<usize, StorageError> {
        // Default implementation that does nothing
        // Storage backends should override this with an efficient implementation
        Ok(0)
    }
    
    /// Get storage statistics about history size
    ///
    /// # Returns
    /// A map of statistics about the storage
    fn get_history_stats(&self) -> Result<HashMap<String, u64>, StorageError> {
        // Default implementation that returns empty stats
        // Storage backends should override this with actual statistics
        let mut stats = HashMap::new();
        stats.insert("total_objects".to_string(), 0);
        stats.insert("total_proofs".to_string(), 0);
        stats.insert("total_state_proofs".to_string(), 0);
        stats.insert("oldest_slot".to_string(), 0);
        stats.insert("newest_slot".to_string(), 0);
        Ok(stats)
    }
}
