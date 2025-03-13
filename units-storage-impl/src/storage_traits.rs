use serde::{Deserialize, Serialize};
use units_core::error::StorageError;
use units_core::id::UnitsObjectId;
use units_core::locks::{AccessIntent, LockInfo, LockType};
use units_core::objects::TokenizedObject;
use units_core::transaction::{CommitmentLevel, TransactionEffect, TransactionReceipt};
use units_proofs::{ProofEngine, SlotNumber, StateProof, TokenizedObjectProof};

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

// Moved to units-runtime::runtime
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
    /// Acquire a lock on an object for a transaction
    fn acquire_object_lock(
        &self,
        _object_id: &UnitsObjectId,
        _lock_type: LockType,
        _transaction_hash: &[u8; 32],
        _timeout_ms: Option<u64>,
    ) -> Result<bool, StorageError> {
        // Default implementation returns unimplemented
        Err(StorageError::Unimplemented(
            "Acquiring object locks not implemented by this storage".to_string(),
        ))
    }

    /// Release a lock on an object for a transaction
    fn release_object_lock(
        &self,
        _object_id: &UnitsObjectId,
        _transaction_hash: &[u8; 32],
    ) -> Result<bool, StorageError> {
        // Default implementation returns unimplemented
        Err(StorageError::Unimplemented(
            "Releasing object locks not implemented by this storage".to_string(),
        ))
    }

    /// Check if a lock exists and get its information
    fn get_object_lock_info(
        &self,
        _object_id: &UnitsObjectId,
    ) -> Result<Option<LockInfo>, StorageError> {
        // Default implementation returns unimplemented
        Err(StorageError::Unimplemented(
            "Getting object lock info not implemented by this storage".to_string(),
        ))
    }

    /// Check if a transaction can acquire a lock on an object
    fn can_acquire_object_lock(
        &self,
        _object_id: &UnitsObjectId,
        _intent: AccessIntent,
        _transaction_hash: &[u8; 32],
    ) -> Result<bool, StorageError> {
        // Default implementation returns unimplemented
        Err(StorageError::Unimplemented(
            "Checking object lock availability not implemented by this storage".to_string(),
        ))
    }

    /// Release all locks held by a transaction
    fn release_all_transaction_locks(
        &self,
        _transaction_hash: &[u8; 32],
    ) -> Result<usize, StorageError> {
        // Default implementation returns unimplemented
        Err(StorageError::Unimplemented(
            "Releasing transaction locks not implemented by this storage".to_string(),
        ))
    }

    /// Get all locks held by a transaction
    fn get_all_transaction_locks(
        &self,
        _transaction_hash: &[u8; 32],
    ) -> UnitsStorageLockIterator<'_> {
        // Default implementation returns an empty iterator
        Box::new(EmptyLockIterator::<StorageError>::new())
    }

    /// Get all locks on an object
    fn get_all_object_locks(&self, _object_id: &UnitsObjectId) -> UnitsStorageLockIterator<'_> {
        // Default implementation returns an empty iterator
        Box::new(EmptyLockIterator::<StorageError>::new())
    }

    /// Check for expired locks and release them
    fn cleanup_expired_object_locks(&self) -> Result<usize, StorageError> {
        // Default implementation returns unimplemented
        Err(StorageError::Unimplemented(
            "Cleaning up expired locks not implemented by this storage".to_string(),
        ))
    }

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
        let proofs: Vec<_> = self
            .get_proof_history(id)
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

    /// Execute a transaction and generate a receipt.
    /// The transaction starts in the Processing commitment level and can be rolled back
    /// until it is explicitly committed.
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

        // Create a receipt with Processing commitment level to allow for rollbacks
        let mut receipt = TransactionReceipt::with_commitment_level(
            transaction_hash,
            slot,
            true, // Assume success initially
            timestamp,
            CommitmentLevel::Processing, // Start as Processing to allow rollback
        );

        // For each object to store, retrieve its current state (if any) for the before image
        for object in objects_to_store {
            let before_image = self.get(&object.id)?;

            // Create a transaction effect
            let effect = TransactionEffect {
                transaction_hash,
                object_id: object.id,
                before_image,
                after_image: Some(object.clone()),
            };

            receipt.add_effect(effect);
        }

        // For each object to delete, retrieve its current state for the before image
        for &object_id in objects_to_delete {
            let before_image = self.get(&object_id)?;

            // Only create an effect if the object exists
            if let Some(before) = before_image {
                // Create a transaction effect
                let effect = TransactionEffect {
                    transaction_hash,
                    object_id,
                    before_image: Some(before),
                    after_image: None, // Object will be deleted
                };

                receipt.add_effect(effect);
            }
        }

        // Apply the changes but only generate proofs if committed

        // Store all objects
        if !objects_to_store.is_empty() {
            // For Processing level transactions, we still need to apply the changes
            // without generating proofs in the receipt (they'll be generated at commit time)
            for object in objects_to_store {
                self.set(object, Some(transaction_hash))?;
            }
        }

        // Delete all objects
        if !objects_to_delete.is_empty() {
            // For Processing level transactions, apply the changes without
            // generating proofs in the receipt
            for &id in objects_to_delete {
                self.delete(&id, Some(transaction_hash))?;
            }
        }

        // Note: The transaction is executed but NOT committed.
        // The caller must explicitly commit the transaction to make it permanent
        // by calling commit_transaction() on the Runtime.

        Ok(receipt)
    }

    /// Internal method to get transaction history for objects
    /// This functionality is provided by default in Transaction Receipt Storage
    ///
    /// # Parameters
    /// * `transaction_hash` - The hash of the transaction to check
    ///
    /// # Returns
    /// Optional TransactionReceipt
    fn get_transaction_receipt(
        &self,
        _transaction_hash: &[u8; 32],
    ) -> Result<Option<TransactionReceipt>, StorageError> {
        // Default implementation returns None
        // Implementations should override this with proper storage-backed implementation
        Ok(None)
    }

    /// Rollback a transaction, reverting all changes made by the transaction
    /// This only works for transactions with a Processing commitment level
    ///
    /// # Parameters
    /// * `transaction_hash` - The hash of the transaction to rollback
    ///
    /// # Returns
    /// Ok(true) if successful, Err otherwise
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

    /// Update a transaction's commitment level
    ///
    /// # Parameters
    /// * `transaction_hash` - The hash of the transaction to update
    /// * `commitment_level` - The new commitment level
    ///
    /// # Returns
    /// Ok(()) if successful, Err otherwise
    fn update_transaction_commitment(
        &self,
        _transaction_hash: &[u8; 32],
        _commitment_level: CommitmentLevel,
    ) -> Result<(), StorageError> {
        // Default implementation returns unimplemented error
        Err(StorageError::Unimplemented(
            "Updating transaction commitment level not implemented by this storage".to_string(),
        ))
    }

    /// Commit a transaction, making its changes permanent and preventing rollback.
    /// This is when proofs are actually generated and stored.
    ///
    /// # Parameters
    /// * `transaction_hash` - The hash of the transaction to commit
    ///
    /// # Returns
    /// Ok(()) if successful, Err otherwise
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
                // Convert TokenizedObjectProof to Vec<u8> for storage
                let proof_bytes = bincode::serialize(&proof)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                receipt.add_proof(effect.object_id, proof_bytes);
            } else if effect.before_image.is_some() {
                // Object was deleted
                let proof = self.delete(&effect.object_id, Some(*transaction_hash))?;
                // Convert TokenizedObjectProof to Vec<u8> for storage
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

    /// Mark a transaction as failed
    ///
    /// # Parameters
    /// * `transaction_hash` - The hash of the transaction to mark as failed
    ///
    /// # Returns
    /// Ok(()) if successful, Err otherwise
    fn fail_transaction(&self, transaction_hash: &[u8; 32]) -> Result<(), StorageError> {
        self.update_transaction_commitment(transaction_hash, CommitmentLevel::Failed)
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

// We don't need to implement PersistentLockManager for UnitsStorage directly
// Instead, specific implementations of UnitsStorage will implement PersistentLockManager
// This avoids the orphan rule issues

/// Helper type for a lock iterator with StorageError
pub type UnitsStorageLockIterator<'a> =
    Box<dyn Iterator<Item = Result<LockInfo, StorageError>> + 'a>;

/// Wrapper struct for empty lock iterator
pub struct EmptyLockIterator<E>(std::marker::PhantomData<E>);

impl<E> EmptyLockIterator<E> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<E> Iterator for EmptyLockIterator<E> {
    type Item = Result<LockInfo, E>;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

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
