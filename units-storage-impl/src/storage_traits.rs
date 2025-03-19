use serde::{Deserialize, Serialize};
use units_core::error::StorageError;
use units_core::id::UnitsObjectId;
use units_core::locks::PersistentLockManager;
use units_core::objects::UnitsObject;
use units_core::transaction::{CommitmentLevel, TransactionEffect, TransactionReceipt};
use units_proofs::{ProofEngine, SlotNumber, StateProof, UnitsObjectProof};

use std::collections::HashMap;
use std::iter::Iterator;
use std::path::Path;

//==============================================================================
// ITERATORS
//==============================================================================

/// Iterator for traversing objects in storage
pub trait UnitsStorageIterator: Iterator<Item = Result<UnitsObject, StorageError>> {}

/// Iterator for traversing object proofs in storage
pub trait UnitsProofIterator:
    Iterator<Item = Result<(SlotNumber, UnitsObjectProof), StorageError>>
{
}

/// Iterator for traversing state proofs in storage
pub trait UnitsStateProofIterator: Iterator<Item = Result<StateProof, StorageError>> {}

/// Iterator for traversing transaction receipts in storage
pub trait UnitsReceiptIterator: Iterator<Item = Result<TransactionReceipt, StorageError>> {}

//==============================================================================
// WRITE-AHEAD LOG
//==============================================================================

/// A write-ahead log entry for an object update
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WALEntry {
    /// The object being updated
    pub object: UnitsObject,
    /// The slot in which this update occurred
    pub slot: SlotNumber,
    /// The proof generated for this update
    pub proof: UnitsObjectProof,
    /// Timestamp of when this update was recorded
    pub timestamp: u64,
    /// Hash of the transaction that led to this update, if any
    pub transaction_hash: Option<[u8; 32]>,
}

/// Write-ahead log for durably recording all updates before they're committed to storage
pub trait UnitsWriteAheadLog {
    /// Initialize the write-ahead log
    fn init(&self, path: &Path) -> Result<(), StorageError>;

    /// Record an object update in the write-ahead log
    fn record_update(
        &self,
        object: &UnitsObject,
        proof: &UnitsObjectProof,
        transaction_hash: Option<[u8; 32]>,
    ) -> Result<(), StorageError>;

    /// Record a state proof in the write-ahead log
    fn record_state_proof(&self, state_proof: &StateProof) -> Result<(), StorageError>;

    /// Get an iterator over all WAL entries
    fn iterate_entries(&self) -> Box<dyn Iterator<Item = Result<WALEntry, StorageError>> + '_>;
}

//==============================================================================
// PROOF ENGINE
//==============================================================================

/// Engine for creating and verifying cryptographic proofs
pub trait UnitsStorageProofEngine {
    /// Get the proof engine used by this storage
    fn proof_engine(&self) -> &dyn ProofEngine;

    /// Generate a state proof for the current state of all objects
    fn generate_state_proof(&self, slot: Option<SlotNumber>) -> Result<StateProof, StorageError>;

    /// Get the most recent proof for a specific object
    fn get_proof(&self, id: &UnitsObjectId) -> Result<Option<UnitsObjectProof>, StorageError>;

    /// Get all historical proofs for a specific object
    fn get_proof_history(&self, id: &UnitsObjectId) -> Box<dyn UnitsProofIterator + '_>;

    /// Get a specific historical proof for an object
    fn get_proof_at_slot(
        &self,
        id: &UnitsObjectId,
        slot: SlotNumber,
    ) -> Result<Option<UnitsObjectProof>, StorageError>;

    /// Get all state proofs
    fn get_state_proofs(&self) -> Box<dyn UnitsStateProofIterator + '_>;

    /// Get a state proof for a specific slot
    fn get_state_proof_at_slot(
        &self, 
        slot: SlotNumber
    ) -> Result<Option<StateProof>, StorageError>;

    /// Verify a proof for a specific object
    fn verify_proof(
        &self,
        id: &UnitsObjectId,
        proof: &UnitsObjectProof,
    ) -> Result<bool, StorageError>;

    /// Verify a proof chain for a specific object
    fn verify_proof_chain(
        &self,
        id: &UnitsObjectId,
        start_slot: SlotNumber,
        end_slot: SlotNumber,
    ) -> Result<bool, StorageError>;
}

//==============================================================================
// TRANSACTION RECEIPTS
//==============================================================================

/// Storage interface for transaction receipts
pub trait TransactionReceiptStorage {
    /// Store a transaction receipt
    fn store_receipt(&self, receipt: &TransactionReceipt) -> Result<(), StorageError>;

    /// Get a transaction receipt by transaction hash
    fn get_receipt(&self, hash: &[u8; 32]) -> Result<Option<TransactionReceipt>, StorageError>;

    /// Get all transaction receipts for a specific object
    fn get_receipts_for_object(&self, id: &UnitsObjectId) -> Box<dyn UnitsReceiptIterator + '_>;

    /// Get all transaction receipts in a specific slot
    fn get_receipts_in_slot(&self, slot: SlotNumber) -> Box<dyn UnitsReceiptIterator + '_>;
}

//==============================================================================
// MAIN STORAGE INTERFACE
//==============================================================================

/// Main storage interface for UNITS objects
pub trait UnitsStorage: UnitsStorageProofEngine + UnitsWriteAheadLog {
    /// Get the lock manager for this storage
    fn lock_manager(&self) -> &dyn PersistentLockManager<Error = StorageError>;

    //--------------------------------------------------------------------------
    // BASIC OPERATIONS
    //--------------------------------------------------------------------------

    /// Get an object by its ID
    fn get(&self, id: &UnitsObjectId) -> Result<Option<UnitsObject>, StorageError>;

    /// Get an object at a specific historical slot
    fn get_at_slot(
        &self,
        id: &UnitsObjectId,
        slot: SlotNumber,
    ) -> Result<Option<UnitsObject>, StorageError>;

    /// Store an object
    fn set(
        &self,
        object: &UnitsObject,
        transaction_hash: Option<[u8; 32]>,
    ) -> Result<UnitsObjectProof, StorageError>;

    /// Delete an object by its ID
    fn delete(
        &self,
        id: &UnitsObjectId,
        transaction_hash: Option<[u8; 32]>,
    ) -> Result<UnitsObjectProof, StorageError>;

    /// Create an iterator to scan through all objects
    fn scan(&self) -> Box<dyn UnitsStorageIterator + '_>;
    
    /// Generate a state proof for the current slot and store it
    fn generate_and_store_state_proof(&self) -> Result<StateProof, StorageError>;

    //--------------------------------------------------------------------------
    // BATCH OPERATIONS
    //--------------------------------------------------------------------------

    /// Store multiple objects in a single transaction
    fn set_batch(
        &self,
        objects: &[UnitsObject],
        transaction_hash: [u8; 32],
    ) -> Result<HashMap<UnitsObjectId, UnitsObjectProof>, StorageError> {
        // Default implementation that calls set() for each object
        let mut proofs = HashMap::new();
        for object in objects {
            let proof = self.set(object, Some(transaction_hash))?;
            proofs.insert(*object.id(), proof);
        }
        Ok(proofs)
    }

    /// Delete multiple objects in a single transaction
    fn delete_batch(
        &self,
        ids: &[UnitsObjectId],
        transaction_hash: [u8; 32],
    ) -> Result<HashMap<UnitsObjectId, UnitsObjectProof>, StorageError> {
        // Default implementation that calls delete() for each object
        let mut proofs = HashMap::new();
        for id in ids {
            let proof = self.delete(id, Some(transaction_hash))?;
            proofs.insert(*id, proof);
        }
        Ok(proofs)
    }

    //--------------------------------------------------------------------------
    // HISTORY OPERATIONS
    //--------------------------------------------------------------------------

    /// Get object state history between slots
    fn get_history(
        &self,
        id: &UnitsObjectId,
        start_slot: SlotNumber,
        end_slot: SlotNumber,
    ) -> Result<HashMap<SlotNumber, UnitsObject>, StorageError> {
        let mut history = HashMap::new();
        let proofs: Vec<_> = self
            .get_proof_history(id)
            .map(|r| r.unwrap())
            .filter(|(slot, _)| *slot >= start_slot && *slot <= end_slot)
            .collect();

        if proofs.is_empty() {
            return Ok(history);
        }

        for (slot, _) in proofs {
            if let Some(obj) = self.get_at_slot(id, slot)? {
                history.insert(slot, obj);
            }
        }
        Ok(history)
    }

    /// Get all transactions that affected an object
    fn get_transaction_history(
        &self,
        id: &UnitsObjectId,
        start_slot: Option<SlotNumber>,
        end_slot: Option<SlotNumber>,
    ) -> Result<Vec<[u8; 32]>, StorageError> {
        let mut transactions = Vec::new();
        let proofs: Vec<_> = self
            .get_proof_history(id)
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

        for (_, proof) in proofs {
            if let Some(hash) = proof.transaction_hash {
                if !transactions.contains(&hash) {
                    transactions.push(hash);
                }
            }
        }
        Ok(transactions)
    }

    /// Compact historical data up to a specific slot
    fn compact_history(
        &self,
        _before_slot: SlotNumber,
        _preserve_state_proofs: bool,
    ) -> Result<usize, StorageError> {
        // Default implementation that does nothing
        Ok(0)
    }

    /// Get storage statistics about history size
    fn get_history_stats(&self) -> Result<HashMap<String, u64>, StorageError> {
        // Default implementation that returns empty stats
        let mut stats = HashMap::new();
        stats.insert("total_objects".to_string(), 0);
        stats.insert("total_proofs".to_string(), 0);
        stats.insert("total_state_proofs".to_string(), 0);
        stats.insert("oldest_slot".to_string(), 0);
        stats.insert("newest_slot".to_string(), 0);
        Ok(stats)
    }

    //--------------------------------------------------------------------------
    // TRANSACTION OPERATIONS
    //--------------------------------------------------------------------------
    
    /// Execute a transaction and generate a receipt
    fn execute_transaction_batch(
        &self,
        objects_to_store: &[UnitsObject],
        objects_to_delete: &[UnitsObjectId],
        transaction_hash: [u8; 32],
        slot: SlotNumber,
    ) -> Result<TransactionReceipt, StorageError> {
        // Get the current timestamp
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Create a receipt with Processing commitment level
        let mut receipt = TransactionReceipt::with_commitment_level(
            transaction_hash,
            slot,
            true, // Assume success initially
            timestamp,
            CommitmentLevel::Processing,
        );

        // For each object to store, retrieve its current state for the before image
        for object in objects_to_store {
            let before_image = self.get(object.id())?;
            let effect = TransactionEffect {
                transaction_hash,
                object_id: *object.id(),
                before_image,
                after_image: Some(object.clone()),
            };
            receipt.add_effect(effect);
        }

        // For each object to delete, retrieve its current state for the before image
        for &object_id in objects_to_delete {
            let before_image = self.get(&object_id)?;
            if let Some(before) = before_image {
                let effect = TransactionEffect {
                    transaction_hash,
                    object_id,
                    before_image: Some(before),
                    after_image: None, // Object will be deleted
                };
                receipt.add_effect(effect);
            }
        }

        // Apply changes
        for object in objects_to_store {
            self.set(object, Some(transaction_hash))?;
        }

        for &id in objects_to_delete {
            self.delete(&id, Some(transaction_hash))?;
        }

        Ok(receipt)
    }

    /// Get a transaction receipt by its hash
    fn get_transaction_receipt(
        &self,
        _transaction_hash: &[u8; 32],
    ) -> Result<Option<TransactionReceipt>, StorageError> {
        // Default implementation returns None
        Ok(None)
    }

    /// Update a transaction's commitment level
    fn update_transaction_commitment(
        &self,
        _transaction_hash: &[u8; 32],
        _commitment_level: CommitmentLevel,
    ) -> Result<(), StorageError> {
        Err(StorageError::Unimplemented(
            "Updating transaction commitment level not implemented by this storage".to_string(),
        ))
    }

    /// Commit a transaction, making its changes permanent
    fn commit_transaction(&self, transaction_hash: &[u8; 32]) -> Result<(), StorageError> {
        // Get the transaction receipt
        let mut receipt = match self.get_transaction_receipt(transaction_hash)? {
            Some(r) => r,
            None => return Err(StorageError::TransactionNotFound(*transaction_hash)),
        };

        // Skip if already committed
        if receipt.commitment_level == CommitmentLevel::Committed {
            return Ok(());
        }

        // Verify transaction is in Processing state
        if receipt.commitment_level != CommitmentLevel::Processing {
            return Err(StorageError::InvalidOperation(format!(
                "Cannot commit transaction with commitment level {:?}. Only Processing transactions can be committed.",
                receipt.commitment_level
            )));
        }

        // Clone effects to avoid borrowing issues
        let effects = receipt.effects.clone();

        // Generate proofs for all effects
        for effect in effects {
            if let Some(after) = &effect.after_image {
                // Object was created or modified
                let proof = self.set(after, Some(*transaction_hash))?;
                let proof_bytes = bincode::serialize(&proof)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                receipt.add_proof(effect.object_id, proof_bytes);
            } else if effect.before_image.is_some() {
                // Object was deleted
                let proof = self.delete(&effect.object_id, Some(*transaction_hash))?;
                let proof_bytes = bincode::serialize(&proof)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                receipt.add_proof(effect.object_id, proof_bytes);
            }
        }

        // Mark as committed
        receipt.commit();

        // Update the receipt in storage
        self.update_transaction_commitment(transaction_hash, CommitmentLevel::Committed)?;

        Ok(())
    }

    /// Rollback a transaction, reverting all changes
    fn rollback_transaction(&self, transaction_hash: &[u8; 32]) -> Result<bool, StorageError> {
        // Get the transaction receipt
        let receipt = match self.get_transaction_receipt(transaction_hash)? {
            Some(r) => r,
            None => return Err(StorageError::TransactionNotFound(*transaction_hash)),
        };

        // Check if the transaction can be rolled back
        if !receipt.can_rollback() {
            return Err(StorageError::InvalidOperation(format!(
                "Cannot rollback transaction with commitment level {:?}. Only Processing transactions can be rolled back.",
                receipt.commitment_level
            )));
        }

        // Restore the before image for each effect
        for effect in receipt.effects {
            if let Some(before_image) = effect.before_image {
                // Object existed before transaction, restore its state
                self.set(&before_image, None)?;
            } else {
                // Object was created in this transaction, delete it
                self.delete(&effect.object_id, None)?;
            }
        }

        // Mark the transaction as failed
        self.update_transaction_commitment(transaction_hash, CommitmentLevel::Failed)?;

        Ok(true)
    }

    /// Mark a transaction as failed
    fn fail_transaction(&self, transaction_hash: &[u8; 32]) -> Result<(), StorageError> {
        self.update_transaction_commitment(transaction_hash, CommitmentLevel::Failed)
    }
}
