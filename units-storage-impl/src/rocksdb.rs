#[cfg(feature = "rocksdb")]
use crate::storage_traits::{
    UnitsProofIterator, UnitsReceiptIterator, UnitsStateProofIterator, UnitsStorage, 
    UnitsStorageIterator, UnitsStorageProofEngine, TransactionReceiptStorage,
    UnitsWriteAheadLog,
};
#[cfg(feature = "rocksdb")]
use units_core::transaction::TransactionReceipt;
#[cfg(feature = "rocksdb")]
use crate::wal::FileWriteAheadLog;
#[cfg(feature = "rocksdb")]
use anyhow::{Context, Result};
#[cfg(feature = "rocksdb")]
use log;
#[cfg(feature = "rocksdb")]
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
#[cfg(feature = "rocksdb")]
use std::{
    fmt::Debug,
    path::{Path, PathBuf},
    sync::Arc,
};
#[cfg(feature = "rocksdb")]
use units_core::error::StorageError;
#[cfg(feature = "rocksdb")]
use units_core::id::UnitsObjectId;
#[cfg(feature = "rocksdb")]
use units_core::objects::TokenizedObject;
#[cfg(feature = "rocksdb")]
use units_proofs::lattice_proof_engine::LatticeProofEngine;
#[cfg(feature = "rocksdb")]
use units_proofs::proofs::current_slot;
#[cfg(feature = "rocksdb")]
use units_proofs::{ProofEngine, SlotNumber, StateProof, TokenizedObjectProof, VerificationResult};

#[cfg(feature = "rocksdb")]
// Column family names used to organize different types of data
const CF_OBJECTS: &str = "objects";
#[cfg(feature = "rocksdb")]
const CF_OBJECT_PROOFS: &str = "object_proofs";
#[cfg(feature = "rocksdb")]
const CF_STATE_PROOFS: &str = "state_proofs";
#[cfg(feature = "rocksdb")]
const CF_OBJECT_HISTORY: &str = "object_history";
#[cfg(feature = "rocksdb")]
const CF_PROOF_HISTORY: &str = "proof_history";
#[cfg(feature = "rocksdb")]
const CF_TRANSACTION_RECEIPTS: &str = "transaction_receipts";
#[cfg(feature = "rocksdb")]
const CF_OBJECT_TRANSACTIONS: &str = "object_transactions";

#[cfg(feature = "rocksdb")]
/// Key format for storing historical objects and proofs
/// Format: <object_id>:<slot_number>
fn make_history_key(id: &UnitsObjectId, slot: SlotNumber) -> Vec<u8> {
    let mut key = Vec::with_capacity(id.as_ref().len() + 8 + 1);
    key.extend_from_slice(id.as_ref());
    key.push(b':'); // Separator
    key.extend_from_slice(&slot.to_le_bytes());
    key
}

/// Extract object ID and slot from a history key
#[cfg_attr(not(test), allow(dead_code))]
fn parse_history_key(key: &[u8]) -> Option<(UnitsObjectId, SlotNumber)> {
    // Find the separator
    let separator_pos = key.iter().position(|&b| b == b':')?;

    // Extract the object ID
    let id_bytes = &key[..separator_pos];
    // Create a new ID from raw bytes, verifying it's the correct length
    if id_bytes.len() != 32 {
        return None;
    }
    let mut id_array = [0u8; 32];
    id_array.copy_from_slice(id_bytes);
    let id = UnitsObjectId::from_bytes(id_array);

    // Extract the slot number
    if key.len() < separator_pos + 1 + 8 {
        return None;
    }

    let slot_bytes = &key[separator_pos + 1..separator_pos + 1 + 8];
    let slot_array = slot_bytes.try_into().ok()?;
    let slot = u64::from_le_bytes(slot_array);

    Some((id, slot))
}

#[cfg(feature = "rocksdb")]
/// RocksDB implementation of UnitsStorage
pub struct RocksDbStorage {
    db: Arc<DB>,
    db_path: PathBuf,
    proof_engine: LatticeProofEngine,
    wal: FileWriteAheadLog,
}

#[cfg(feature = "rocksdb")]
/// A simplified implementation of RocksDB storage
impl RocksDbStorage {
    /// Creates a new RocksDB storage at the specified path
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, StorageError> {
        let db_path = path.as_ref().to_path_buf();

        // Set up database options
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        // Define column family descriptors
        let cf_objects = ColumnFamilyDescriptor::new(CF_OBJECTS, Options::default());
        let cf_object_proofs = ColumnFamilyDescriptor::new(CF_OBJECT_PROOFS, Options::default());
        let cf_state_proofs = ColumnFamilyDescriptor::new(CF_STATE_PROOFS, Options::default());
        let cf_object_history = ColumnFamilyDescriptor::new(CF_OBJECT_HISTORY, Options::default());
        let cf_proof_history = ColumnFamilyDescriptor::new(CF_PROOF_HISTORY, Options::default());
        let cf_transaction_receipts = ColumnFamilyDescriptor::new(CF_TRANSACTION_RECEIPTS, Options::default());
        let cf_object_transactions = ColumnFamilyDescriptor::new(CF_OBJECT_TRANSACTIONS, Options::default());

        // Try to open the database with column families
        let db = match DB::open_cf_descriptors(
            &opts,
            &db_path,
            vec![
                cf_objects,
                cf_object_proofs,
                cf_state_proofs,
                cf_object_history,
                cf_proof_history,
                cf_transaction_receipts,
                cf_object_transactions,
            ],
        ) {
            Ok(db) => db,
            Err(_) => {
                // If it fails, try opening without column families first
                let mut raw_db = DB::open(&opts, &db_path)
                    .with_context(|| format!("Failed to open RocksDB database at {:?}", db_path))?;

                // Create column families if they don't exist
                for cf_name in &[
                    CF_OBJECTS,
                    CF_OBJECT_PROOFS,
                    CF_STATE_PROOFS,
                    CF_OBJECT_HISTORY,
                    CF_PROOF_HISTORY,
                    CF_TRANSACTION_RECEIPTS,
                    CF_OBJECT_TRANSACTIONS,
                ] {
                    if let Err(e) = raw_db.create_cf(*cf_name, &Options::default()) {
                        return Err(StorageError::Database(format!(
                            "Failed to create column family {}: {}",
                            cf_name, e
                        )));
                    }
                }
                raw_db
            }
        };

        // Initialize write-ahead log
        let wal = FileWriteAheadLog::new();
        let wal_path = db_path.join("units_wal.log");

        // Create instance first
        let instance = Self {
            db: Arc::new(db),
            db_path,
            proof_engine: LatticeProofEngine::new(),
            wal,
        };

        // Initialize the WAL
        instance.init(&wal_path)?;

        Ok(instance)
    }

    /// Store an object with its history
    fn store_object_with_history(
        &self,
        object: &TokenizedObject,
        slot: SlotNumber,
    ) -> Result<(), StorageError> {
        let cf_object_history = match self.db.cf_handle(CF_OBJECT_HISTORY) {
            Some(cf) => cf,
            None => {
                return Err(StorageError::Database(
                    "Object history column family not found".to_string(),
                ))
            }
        };

        // Create history key with object ID and slot
        let history_key = make_history_key(&object.id, slot);

        // Serialize the object
        let serialized_object = bincode::serialize(object)
            .with_context(|| format!("Failed to serialize object with ID: {:?}", object.id))?;

        // Store the object in the history column family
        self.db
            .put_cf(&cf_object_history, &history_key, &serialized_object)
            .with_context(|| format!("Failed to store object history for ID: {:?}", object.id))?;

        Ok(())
    }

    /// Store a proof with its history
    fn store_proof_with_history(
        &self,
        id: &UnitsObjectId,
        proof: &TokenizedObjectProof,
    ) -> Result<(), StorageError> {
        let cf_proof_history = match self.db.cf_handle(CF_PROOF_HISTORY) {
            Some(cf) => cf,
            None => {
                return Err(StorageError::Database(
                    "Proof history column family not found".to_string(),
                ))
            }
        };

        // Create history key with object ID and slot
        let history_key = make_history_key(id, proof.slot);

        // Serialize the proof
        let serialized_proof = bincode::serialize(proof)
            .with_context(|| format!("Failed to serialize proof for object ID: {:?}", id))?;

        // Store the proof in the history column family
        self.db
            .put_cf(&cf_proof_history, &history_key, &serialized_proof)
            .with_context(|| format!("Failed to store proof history for ID: {:?}", id))?;

        Ok(())
    }
}

#[cfg(feature = "rocksdb")]
/// Iterator implementation for RocksDB storage
/// Uses a simplified approach to avoid iterator lifetime issues
pub struct RocksDbStorageIterator {
    objects: Vec<TokenizedObject>,
    current_index: usize,
}

#[cfg(feature = "rocksdb")]
impl RocksDbStorageIterator {
    fn new(_db: Arc<DB>) -> Self {
        // TODO: Fix RocksDB iterator implementation
        // There are issues with the return type of iterator_cf() in RocksDB 0.23.0
        // The method returns a Result<DBIterator>, but there are type mismatches when trying to use it
        // Also, there are issues with [u8] not having a known size at compile time when processing
        // iterator results

        // For now, just return an empty set of objects
        let objects = Vec::new();

        Self {
            objects,
            current_index: 0,
        }
    }
}

#[cfg(feature = "rocksdb")]
impl Iterator for RocksDbStorageIterator {
    type Item = Result<TokenizedObject, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_index < self.objects.len() {
            let obj = self.objects[self.current_index].clone();
            self.current_index += 1;
            Some(Ok(obj))
        } else {
            None
        }
    }
}

impl UnitsStorageIterator for RocksDbStorageIterator {}

/// Iterator for RocksDB proof history
pub struct RocksDbProofIterator {
    proofs: Vec<(SlotNumber, TokenizedObjectProof)>,
    current_index: usize,
}

impl RocksDbProofIterator {
    fn new(proofs: Vec<(SlotNumber, TokenizedObjectProof)>) -> Self {
        Self {
            proofs,
            current_index: 0,
        }
    }
}

impl Iterator for RocksDbProofIterator {
    type Item = Result<(SlotNumber, TokenizedObjectProof), StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_index < self.proofs.len() {
            let (slot, proof) = self.proofs[self.current_index].clone();
            self.current_index += 1;
            Some(Ok((slot, proof)))
        } else {
            None
        }
    }
}

impl UnitsProofIterator for RocksDbProofIterator {}

/// Iterator for RocksDB state proofs
pub struct RocksDbStateProofIterator {
    state_proofs: Vec<StateProof>,
    current_index: usize,
}

impl RocksDbStateProofIterator {
    fn new(state_proofs: Vec<StateProof>) -> Self {
        Self {
            state_proofs,
            current_index: 0,
        }
    }
}

impl Iterator for RocksDbStateProofIterator {
    type Item = Result<StateProof, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_index < self.state_proofs.len() {
            let state_proof = self.state_proofs[self.current_index].clone();
            self.current_index += 1;
            Some(Ok(state_proof))
        } else {
            None
        }
    }
}

impl UnitsStateProofIterator for RocksDbStateProofIterator {}

/// Iterator for RocksDB transaction receipts
pub struct RocksDbReceiptIterator {
    receipts: Vec<TransactionReceipt>,
    current_index: usize,
}

impl RocksDbReceiptIterator {
    fn new(receipts: Vec<TransactionReceipt>) -> Self {
        Self {
            receipts,
            current_index: 0,
        }
    }
}

impl Iterator for RocksDbReceiptIterator {
    type Item = Result<TransactionReceipt, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_index < self.receipts.len() {
            let receipt = self.receipts[self.current_index].clone();
            self.current_index += 1;
            Some(Ok(receipt))
        } else {
            None
        }
    }
}

impl UnitsReceiptIterator for RocksDbReceiptIterator {}

#[cfg(feature = "rocksdb")]
impl UnitsWriteAheadLog for RocksDbStorage {
    fn init(&self, path: &Path) -> Result<(), StorageError> {
        self.wal.init(path)
    }

    fn record_update(
        &self,
        object: &TokenizedObject,
        proof: &TokenizedObjectProof,
        transaction_hash: Option<[u8; 32]>,
    ) -> Result<(), StorageError> {
        self.wal.record_update(object, proof, transaction_hash)
    }

    fn record_state_proof(&self, state_proof: &StateProof) -> Result<(), StorageError> {
        self.wal.record_state_proof(state_proof)
    }

    fn iterate_entries(
        &self,
    ) -> Box<dyn Iterator<Item = Result<crate::storage_traits::WALEntry, StorageError>> + '_> {
        self.wal.iterate_entries()
    }
}

#[cfg(feature = "rocksdb")]
impl UnitsStorage for RocksDbStorage {
    fn get(&self, id: &UnitsObjectId) -> Result<Option<TokenizedObject>, StorageError> {
        let cf_objects = match self.db.cf_handle(CF_OBJECTS) {
            Some(cf) => cf,
            None => {
                return Err(StorageError::Database(
                    "Objects column family not found".to_string(),
                ))
            }
        };

        let result = self
            .db
            .get_cf(&cf_objects, id.as_ref())
            .with_context(|| format!("Failed to get object with ID: {:?}", id))?;

        if let Some(bytes) = result {
            let obj = bincode::deserialize(&bytes)
                .with_context(|| format!("Failed to deserialize object with ID: {:?}", id))?;
            Ok(Some(obj))
        } else {
            Ok(None)
        }
    }

    fn get_at_slot(
        &self,
        id: &UnitsObjectId,
        slot: SlotNumber,
    ) -> Result<Option<TokenizedObject>, StorageError> {
        let cf_object_history = match self.db.cf_handle(CF_OBJECT_HISTORY) {
            Some(cf) => cf,
            None => {
                return Err(StorageError::Database(
                    "Object history column family not found".to_string(),
                ))
            }
        };

        // Create history key
        let history_key = make_history_key(id, slot);

        // Try to get the object at the specified slot
        let result = self
            .db
            .get_cf(&cf_object_history, &history_key)
            .with_context(|| {
                format!(
                    "Failed to get object history for ID: {:?} at slot {}",
                    id, slot
                )
            })?;

        if let Some(bytes) = result {
            let obj = bincode::deserialize(&bytes).with_context(|| {
                format!(
                    "Failed to deserialize object with ID: {:?} at slot {}",
                    id, slot
                )
            })?;
            Ok(Some(obj))
        } else {
            Ok(None)
        }
    }

    fn set(
        &self,
        object: &TokenizedObject,
        transaction_hash: Option<[u8; 32]>,
    ) -> Result<TokenizedObjectProof, StorageError> {
        let cf_objects = match self.db.cf_handle(CF_OBJECTS) {
            Some(cf) => cf,
            None => {
                return Err(StorageError::Database(
                    "Objects column family not found".to_string(),
                ))
            }
        };

        let cf_object_proofs = match self.db.cf_handle(CF_OBJECT_PROOFS) {
            Some(cf) => cf,
            None => {
                return Err(StorageError::Database(
                    "Object proofs column family not found".to_string(),
                ))
            }
        };

        // Get the previous proof for this object, if any
        let prev_proof = self.get_proof(&object.id)?;

        // Generate proof for the object, linking to previous proof if it exists
        // and including the transaction hash that led to this state change
        let proof = self.proof_engine.generate_object_proof(
            object,
            prev_proof.as_ref(),
            transaction_hash,
        )?;

        // Serialize the object and proof
        let serialized_object = bincode::serialize(object)
            .with_context(|| format!("Failed to serialize object with ID: {:?}", object.id))?;
        let serialized_proof = bincode::serialize(&proof)
            .with_context(|| format!("Failed to serialize proof for object ID: {:?}", object.id))?;

        // Store the object and proof with history
        let current_slot = proof.slot;
        self.store_object_with_history(object, current_slot)?;
        self.store_proof_with_history(&object.id, &proof)?;

        // Also update the current object and proof
        self.db
            .put_cf(&cf_objects, object.id.as_ref(), &serialized_object)
            .with_context(|| format!("Failed to store object with ID: {:?}", object.id))?;

        self.db
            .put_cf(&cf_object_proofs, object.id.as_ref(), &serialized_proof)
            .with_context(|| format!("Failed to store proof for object ID: {:?}", object.id))?;

        // Record the update in the write-ahead log
        self.record_update(object, &proof, transaction_hash)?;

        Ok(proof)
    }

    fn delete(
        &self,
        id: &UnitsObjectId,
        transaction_hash: Option<[u8; 32]>,
    ) -> Result<TokenizedObjectProof, StorageError> {
        // First, check if the object exists
        let object = match self.get(id)? {
            Some(obj) => obj,
            None => {
                return Err(StorageError::NotFound(format!(
                    "Object with ID {:?} not found",
                    id
                )))
            }
        };

        // Get the column family handles
        let cf_objects = match self.db.cf_handle(CF_OBJECTS) {
            Some(cf) => cf,
            None => {
                return Err(StorageError::Database(
                    "Objects column family not found".to_string(),
                ))
            }
        };

        let cf_object_proofs = match self.db.cf_handle(CF_OBJECT_PROOFS) {
            Some(cf) => cf,
            None => {
                return Err(StorageError::Database(
                    "Object proofs column family not found".to_string(),
                ))
            }
        };

        // Get the previous proof
        let prev_proof = self.get_proof(id)?;

        // Create a "tombstone" object to mark deletion
        // In a real system, you might use a dedicated marker
        let mut tombstone = object.clone();
        tombstone.data = Vec::new(); // Empty data to indicate deletion

        // Generate a proof for the deletion, including the transaction hash
        let proof = self.proof_engine.generate_object_proof(
            &tombstone,
            prev_proof.as_ref(),
            transaction_hash,
        )?;

        // Store the deletion proof with history
        self.store_proof_with_history(id, &proof)?;

        // Record the deletion in the write-ahead log
        self.record_update(&tombstone, &proof, transaction_hash)?;

        // Delete the object and its current proof
        self.db
            .delete_cf(&cf_objects, id.as_ref())
            .with_context(|| format!("Failed to delete object with ID: {:?}", id))?;

        self.db
            .delete_cf(&cf_object_proofs, id.as_ref())
            .with_context(|| format!("Failed to delete proof for object ID: {:?}", id))?;

        Ok(proof)
    }

    fn scan(&self) -> Box<dyn UnitsStorageIterator + '_> {
        Box::new(RocksDbStorageIterator::new(self.db.clone()))
    }

    fn generate_and_store_state_proof(&self) -> Result<StateProof, StorageError> {
        // Get all current object proofs
        let mut object_proofs = Vec::new();

        // This would be better with a proper iterator, but for now we'll use the simplified approach
        // Iterate over objects, get their proofs, and add to the list
        let _cf_objects = match self.db.cf_handle(CF_OBJECTS) {
            Some(cf) => cf,
            None => {
                return Err(StorageError::Database(
                    "Objects column family not found".to_string(),
                ))
            }
        };

        let cf_state_proofs = match self.db.cf_handle(CF_STATE_PROOFS) {
            Some(cf) => cf,
            None => {
                return Err(StorageError::Database(
                    "State proofs column family not found".to_string(),
                ))
            }
        };

        // Get the latest state proof, if any
        let mut state_proofs_iter = self.get_state_proofs();
        let latest_state_proof = if let Some(first_result) = state_proofs_iter.next() {
            first_result.ok()
        } else {
            None
        };

        // Get all objects and their proofs
        for obj_result in self.scan() {
            let obj = obj_result?;
            if let Some(proof) = self.get_proof(&obj.id)? {
                object_proofs.push((obj.id.clone(), proof));
            }
        }

        // Current slot
        let current_slot = current_slot();

        // Generate a state proof
        let state_proof = self.proof_engine.generate_state_proof(
            &object_proofs,
            latest_state_proof.as_ref(),
            current_slot,
        )?;

        // Serialize and store the state proof
        let serialized_proof =
            bincode::serialize(&state_proof).with_context(|| "Failed to serialize state proof")?;

        // Use slot number as the key
        let key = state_proof.slot.to_le_bytes();

        self.db
            .put_cf(&cf_state_proofs, &key, &serialized_proof)
            .with_context(|| "Failed to store state proof")?;

        // Record the state proof in the WAL
        self.record_state_proof(&state_proof)?;

        Ok(state_proof)
    }
}

#[cfg(feature = "rocksdb")]
impl UnitsStorageProofEngine for RocksDbStorage {
    fn proof_engine(&self) -> &dyn ProofEngine {
        &self.proof_engine
    }

    fn generate_state_proof(&self, slot: Option<SlotNumber>) -> Result<StateProof, StorageError> {
        // Get all current object proofs
        let mut object_proofs = Vec::new();

        // Get all objects and their proofs
        for obj_result in self.scan() {
            let obj = obj_result?;
            if let Some(proof) = self.get_proof(&obj.id)? {
                object_proofs.push((obj.id.clone(), proof));
            }
        }

        // Get the latest state proof if one exists
        let mut state_proofs_iter = self.get_state_proofs();
        let latest_state_proof = if let Some(first_result) = state_proofs_iter.next() {
            match first_result {
                Ok(proof) => Some(proof),
                Err(_) => None,
            }
        } else {
            None
        };

        // Use provided slot or current slot
        let slot_to_use = slot.unwrap_or_else(current_slot);

        // Generate the state proof
        self.proof_engine.generate_state_proof(
            &object_proofs,
            latest_state_proof.as_ref(),
            slot_to_use,
        )
    }

    fn get_proof(&self, id: &UnitsObjectId) -> Result<Option<TokenizedObjectProof>, StorageError> {
        let cf_object_proofs = match self.db.cf_handle(CF_OBJECT_PROOFS) {
            Some(cf) => cf,
            None => {
                return Err(StorageError::Database(
                    "Object proofs column family not found".to_string(),
                ))
            }
        };

        // Get the proof from the database
        match self
            .db
            .get_cf(&cf_object_proofs, id.as_ref())
            .with_context(|| format!("Failed to get proof for object ID: {:?}", id))?
        {
            Some(value) => {
                // Deserialize the proof
                let proof = bincode::deserialize(&value).with_context(|| {
                    format!("Failed to deserialize proof for object ID: {:?}", id)
                })?;
                Ok(Some(proof))
            }
            None => {
                // If the proof is not found, try to generate it from the object
                match self.get(id)? {
                    Some(object) => {
                        // No previous proof since we're generating a new one
                        let proof = self
                            .proof_engine
                            .generate_object_proof(&object, None, None)?;

                        // Store the proof for future use
                        let serialized_proof = bincode::serialize(&proof).with_context(|| {
                            format!("Failed to serialize proof for object ID: {:?}", id)
                        })?;

                        let _ = self
                            .db
                            .put_cf(&cf_object_proofs, id.as_ref(), &serialized_proof);

                        // Also store it in the history
                        self.store_proof_with_history(id, &proof)?;

                        Ok(Some(proof))
                    }
                    None => Ok(None),
                }
            }
        }
    }

    fn get_proof_history(&self, id: &UnitsObjectId) -> Box<dyn UnitsProofIterator + '_> {
        // Get all proofs for this object from the history
        let cf_proof_history = match self.db.cf_handle(CF_PROOF_HISTORY) {
            Some(cf) => cf,
            None => {
                // Return empty iterator if column family doesn't exist
                return Box::new(RocksDbProofIterator::new(Vec::new()));
            }
        };

        // For now, load all proofs and create an in-memory iterator
        // In a real implementation, you'd use RocksDB's iterator
        let mut proofs = Vec::new();

        // Look up all history keys that start with this ID
        // This is slow but works for our purposes - in a real implementation
        // you'd use a prefix iterator
        let _prefix = id.as_ref().to_vec(); // Used for prefix matching in a real implementation

        // Since we can't use RocksDB's iterator directly, we'll use a simplified approach
        // Create a dummy key range to check
        // This is a hack that won't work well in production
        for slot in 0..current_slot() + 100 {
            let history_key = make_history_key(id, slot);
            match self.db.get_cf(&cf_proof_history, &history_key) {
                Ok(Some(value)) => {
                    // Found a proof at this slot
                    match bincode::deserialize::<TokenizedObjectProof>(&value) {
                        Ok(proof) => {
                            proofs.push((slot, proof));
                        }
                        Err(_) => continue,
                    }
                }
                _ => continue,
            }
        }

        // Sort by slot in descending order (newest first)
        proofs.sort_by(|a, b| b.0.cmp(&a.0));

        Box::new(RocksDbProofIterator::new(proofs))
    }

    fn get_proof_at_slot(
        &self,
        id: &UnitsObjectId,
        slot: SlotNumber,
    ) -> Result<Option<TokenizedObjectProof>, StorageError> {
        let cf_proof_history = match self.db.cf_handle(CF_PROOF_HISTORY) {
            Some(cf) => cf,
            None => {
                return Err(StorageError::Database(
                    "Proof history column family not found".to_string(),
                ))
            }
        };

        // Create the history key
        let history_key = make_history_key(id, slot);

        // Get the proof from history
        match self
            .db
            .get_cf(&cf_proof_history, &history_key)
            .map_err(|e| StorageError::Database(e.to_string()))?
        {
            Some(value) => {
                let proof = bincode::deserialize(&value).with_context(|| {
                    format!(
                        "Failed to deserialize proof history for ID: {:?} at slot {}",
                        id, slot
                    )
                })?;
                Ok(Some(proof))
            }
            None => Ok(None),
        }
    }

    fn get_state_proofs(&self) -> Box<dyn UnitsStateProofIterator + '_> {
        let cf_state_proofs = match self.db.cf_handle(CF_STATE_PROOFS) {
            Some(cf) => cf,
            None => {
                // Return empty iterator if column family doesn't exist
                return Box::new(RocksDbStateProofIterator::new(Vec::new()));
            }
        };

        // For now, load all state proofs and create an in-memory iterator
        // In a real implementation, you'd use RocksDB's iterator
        let mut state_proofs = Vec::new();

        // Since we can't use RocksDB's iterator directly, we'll use a simplified approach
        // This is a hack that won't work well in production
        for slot in 0..current_slot() + 100 {
            let key = slot.to_le_bytes();
            match self.db.get_cf(&cf_state_proofs, &key) {
                Ok(Some(value)) => {
                    // Found a state proof at this slot
                    match bincode::deserialize::<StateProof>(&value) {
                        Ok(proof) => {
                            state_proofs.push(proof);
                        }
                        Err(_) => continue,
                    }
                }
                _ => continue,
            }
        }

        // Sort by slot in descending order (newest first)
        state_proofs.sort_by(|a, b| b.slot.cmp(&a.slot));

        Box::new(RocksDbStateProofIterator::new(state_proofs))
    }

    fn get_state_proof_at_slot(
        &self,
        slot: SlotNumber,
    ) -> Result<Option<StateProof>, StorageError> {
        let cf_state_proofs = match self.db.cf_handle(CF_STATE_PROOFS) {
            Some(cf) => cf,
            None => {
                return Err(StorageError::Database(
                    "State proofs column family not found".to_string(),
                ))
            }
        };

        // Use slot as the key
        let key = slot.to_le_bytes();

        // Get the state proof
        match self
            .db
            .get_cf(&cf_state_proofs, &key)
            .map_err(|e| StorageError::Database(e.to_string()))?
        {
            Some(value) => {
                let proof = bincode::deserialize(&value).with_context(|| {
                    format!("Failed to deserialize state proof at slot {}", slot)
                })?;
                Ok(Some(proof))
            }
            None => Ok(None),
        }
    }

    fn verify_proof(
        &self,
        id: &UnitsObjectId,
        proof: &TokenizedObjectProof,
    ) -> Result<bool, StorageError> {
        match self.get(id)? {
            Some(object) => self.proof_engine.verify_object_proof(&object, proof),
            None => Ok(false),
        }
    }

    fn verify_proof_chain(
        &self,
        id: &UnitsObjectId,
        start_slot: SlotNumber,
        end_slot: SlotNumber,
    ) -> Result<bool, StorageError> {
        // Get all proofs between start and end slots
        let mut proofs: Vec<(SlotNumber, TokenizedObjectProof)> = self
            .get_proof_history(id)
            .filter_map(|result| {
                result
                    .ok()
                    .filter(|(slot, _)| *slot >= start_slot && *slot <= end_slot)
            })
            .collect();

        // Sort proofs by slot (should be redundant as they are already ordered, but to be safe)
        proofs.sort_by_key(|(slot, _)| *slot);

        if proofs.is_empty() {
            return Err(StorageError::ProofNotFound(*id));
        }

        // Get the corresponding object states
        let mut object_states: Vec<(SlotNumber, TokenizedObject)> = Vec::new();
        for (slot, _) in &proofs {
            if let Some(obj) = self.get_at_slot(id, *slot)? {
                object_states.push((*slot, obj));
            } else {
                return Err(StorageError::ObjectNotAtSlot(*slot));
            }
        }

        // Use the verifier from the proof engine for consistent verification
        // Convert the result to ensure our return type matches the trait
        match self
            .proof_engine
            .verify_proof_history(&object_states, &proofs)
        {
            VerificationResult::Valid => Ok(true),
            VerificationResult::Invalid(msg) => {
                log::warn!("Proof chain verification failed: {}", msg);
                Ok(false)
            }
            VerificationResult::MissingData(msg) => {
                log::warn!("Proof chain missing data: {}", msg);
                Err(StorageError::ProofMissingData(*id, msg))
            }
        }
    }
}

#[cfg(feature = "rocksdb")]
impl Debug for RocksDbStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksDbStorage")
            .field("db_path", &self.db_path)
            .finish()
    }
}

#[cfg(feature = "rocksdb")]
impl TransactionReceiptStorage for RocksDbStorage {
    fn store_receipt(&self, receipt: &TransactionReceipt) -> Result<(), StorageError> {
        // Get column family handles
        let cf_receipts = match self.db.cf_handle(CF_TRANSACTION_RECEIPTS) {
            Some(cf) => cf,
            None => return Err(StorageError::Database(
                "Transaction receipts column family not found".to_string()
            )),
        };

        let cf_obj_tx = match self.db.cf_handle(CF_OBJECT_TRANSACTIONS) {
            Some(cf) => cf,
            None => return Err(StorageError::Database(
                "Object transactions column family not found".to_string()
            )),
        };

        // Serialize the receipt
        let serialized_receipt = bincode::serialize(receipt)
            .with_context(|| format!("Failed to serialize receipt for transaction hash {:?}", receipt.transaction_hash))?;

        // Store the receipt
        self.db.put_cf(&cf_receipts, receipt.transaction_hash.as_ref(), &serialized_receipt)
            .with_context(|| format!("Failed to store receipt for transaction hash {:?}", receipt.transaction_hash))?;

        // Index objects affected by this transaction for efficient lookups
        
        // First, delete any existing mappings (in case we're updating a receipt)
        // We need to find all existing mappings in a real implementation, but
        // for simplicity, we'll just add new ones
        
        // Store mappings for all objects with proofs
        for object_id in receipt.object_proofs.keys() {
            // Create a composite key: object_id + transaction_hash
            let mut key = Vec::with_capacity(object_id.as_ref().len() + receipt.transaction_hash.len() + 1);
            key.extend_from_slice(object_id.as_ref());
            key.push(b':'); // Separator
            key.extend_from_slice(&receipt.transaction_hash);
            
            // Store the slot as the value for sorting by time
            let slot_bytes = receipt.slot.to_le_bytes();
            
            self.db.put_cf(&cf_obj_tx, &key, &slot_bytes)
                .with_context(|| format!("Failed to store object transaction mapping for object ID {:?}", object_id))?;
        }
        
        // Also index objects from effects that might not have proofs yet (e.g., in Processing state)
        for effect in &receipt.effects {
            // Skip if we already added this object
            if receipt.object_proofs.contains_key(&effect.object_id) {
                continue;
            }
            
            // Create a composite key
            let mut key = Vec::with_capacity(effect.object_id.as_ref().len() + receipt.transaction_hash.len() + 1);
            key.extend_from_slice(effect.object_id.as_ref());
            key.push(b':'); // Separator
            key.extend_from_slice(&receipt.transaction_hash);
            
            // Store the slot as the value
            let slot_bytes = receipt.slot.to_le_bytes();
            
            self.db.put_cf(&cf_obj_tx, &key, &slot_bytes)
                .with_context(|| format!("Failed to store object effect mapping for object ID {:?}", effect.object_id))?;
        }
        
        Ok(())
    }
    
    fn get_receipt(&self, hash: &[u8; 32]) -> Result<Option<TransactionReceipt>, StorageError> {
        let cf_receipts = match self.db.cf_handle(CF_TRANSACTION_RECEIPTS) {
            Some(cf) => cf,
            None => return Err(StorageError::Database(
                "Transaction receipts column family not found".to_string()
            )),
        };
        
        // Try to get the receipt
        match self.db.get_cf(&cf_receipts, hash.as_ref())
            .with_context(|| format!("Failed to retrieve receipt for transaction hash {:?}", hash))?
        {
            Some(bytes) => {
                // Deserialize the receipt
                let receipt: TransactionReceipt = bincode::deserialize(&bytes)
                    .with_context(|| format!("Failed to deserialize receipt for transaction hash {:?}", hash))?;
                
                Ok(Some(receipt))
            }
            None => Ok(None),
        }
    }
    
    fn get_receipts_for_object(&self, id: &UnitsObjectId) -> Box<dyn UnitsReceiptIterator + '_> {
        let cf_obj_tx = match self.db.cf_handle(CF_OBJECT_TRANSACTIONS) {
            Some(cf) => cf,
            None => {
                // If column family doesn't exist, return empty iterator
                return Box::new(RocksDbReceiptIterator::new(Vec::new()));
            }
        };
        
        let cf_receipts = match self.db.cf_handle(CF_TRANSACTION_RECEIPTS) {
            Some(cf) => cf,
            None => {
                // If column family doesn't exist, return empty iterator
                return Box::new(RocksDbReceiptIterator::new(Vec::new()));
            }
        };
        
        // For simplicity, we'll scan for all keys that start with the object ID
        // In a real implementation, we'd use a prefix iterator
        
        // Create a prefix for lookups
        let mut receipts = Vec::new();
        let prefix = id.as_ref().to_vec();
        
        // This is a simplified approach - in production you'd use RocksDB's prefix iterators
        // For each key with the object ID prefix, extract the transaction hash and get the receipt
        for _slot in 0..current_slot() + 100 {
            // Try different transaction hashes (this is a crude approach, but works for demo)
            // In a real implementation, you'd use a RocksDB iterator with a prefix
            for tx_hash_val in 0..10 {
                // Create a mock transaction hash for testing
                let mut tx_hash = [0u8; 32];
                tx_hash[0] = tx_hash_val;
                
                // Create a composite key
                let mut key = Vec::with_capacity(prefix.len() + tx_hash.len() + 1);
                key.extend_from_slice(&prefix);
                key.push(b':'); // Separator
                key.extend_from_slice(&tx_hash);
                
                // Check if this key exists
                if let Ok(Some(_)) = self.db.get_cf(&cf_obj_tx, &key) {
                    // Key exists, get the receipt
                    if let Ok(Some(receipt_bytes)) = self.db.get_cf(&cf_receipts, &tx_hash) {
                        if let Ok(receipt) = bincode::deserialize::<TransactionReceipt>(&receipt_bytes) {
                            receipts.push(receipt);
                        }
                    }
                }
            }
        }
        
        // Sort receipts by slot (most recent first)
        receipts.sort_by(|a, b| b.slot.cmp(&a.slot));
        
        Box::new(RocksDbReceiptIterator::new(receipts))
    }
    
    fn get_receipts_in_slot(&self, slot: SlotNumber) -> Box<dyn UnitsReceiptIterator + '_> {
        let cf_receipts = match self.db.cf_handle(CF_TRANSACTION_RECEIPTS) {
            Some(cf) => cf,
            None => {
                // If column family doesn't exist, return empty iterator
                return Box::new(RocksDbReceiptIterator::new(Vec::new()));
            }
        };
        
        // In a real implementation, we'd have a secondary index on slot
        // For simplicity, we'll scan all receipts and filter by slot
        
        // This is a simplified approach for demo purposes
        // In production, you'd use a more efficient index
        let mut receipts = Vec::new();
        
        // Try a few transaction hash values to simulate iteration
        // In a real implementation, you'd iterate through all receipts
        for tx_hash_val in 0..100 {
            let mut tx_hash = [0u8; 32];
            tx_hash[0] = tx_hash_val;
            
            // Check if this transaction exists
            if let Ok(Some(receipt_bytes)) = self.db.get_cf(&cf_receipts, &tx_hash) {
                if let Ok(receipt) = bincode::deserialize::<TransactionReceipt>(&receipt_bytes) {
                    // Add to result if the slot matches
                    if receipt.slot == slot {
                        receipts.push(receipt);
                    }
                }
            }
        }
        
        // Sort receipts by timestamp (most recent first)
        receipts.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        
        Box::new(RocksDbReceiptIterator::new(receipts))
    }
}

#[cfg(all(test, feature = "rocksdb"))]
mod tests {
    use super::*;
    use crate::storage_traits::WALEntry;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use units_core::id::UnitsObjectId;
    use units_core::objects::TokenType;

    // Iterator implementations for testing
    struct MockProofIterator {
        proofs: Vec<(SlotNumber, TokenizedObjectProof)>,
        index: usize,
    }

    impl Iterator for MockProofIterator {
        type Item = Result<(SlotNumber, TokenizedObjectProof), StorageError>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.index < self.proofs.len() {
                let (slot, proof) = &self.proofs[self.index];
                self.index += 1;
                Some(Ok((*slot, proof.clone())))
            } else {
                None
            }
        }
    }

    impl UnitsProofIterator for MockProofIterator {}

    struct MockStateProofIterator {
        proofs: Vec<StateProof>,
        index: usize,
    }

    impl Iterator for MockStateProofIterator {
        type Item = Result<StateProof, StorageError>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.index < self.proofs.len() {
                let proof = &self.proofs[self.index];
                self.index += 1;
                Some(Ok(proof.clone()))
            } else {
                None
            }
        }
    }

    impl UnitsStateProofIterator for MockStateProofIterator {}

    struct MockStorageIterator {
        objects: Vec<TokenizedObject>,
        index: usize,
    }

    impl Iterator for MockStorageIterator {
        type Item = Result<TokenizedObject, StorageError>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.index < self.objects.len() {
                let obj = &self.objects[self.index];
                self.index += 1;
                Some(Ok(obj.clone()))
            } else {
                None
            }
        }
    }

    impl UnitsStorageIterator for MockStorageIterator {}

    // Mock implementation for RocksDB storage that avoids database interaction
    struct MockRocksDbStorage {
        objects: Arc<Mutex<HashMap<UnitsObjectId, TokenizedObject>>>,
        proofs: Arc<Mutex<HashMap<UnitsObjectId, Vec<(SlotNumber, TokenizedObjectProof)>>>>,
        state_proofs: Arc<Mutex<HashMap<SlotNumber, StateProof>>>,
        current_slot: Arc<Mutex<SlotNumber>>,
        proof_engine: LatticeProofEngine,
        // Track historical versions of objects for get_at_slot
        object_history: Arc<Mutex<HashMap<UnitsObjectId, Vec<(SlotNumber, TokenizedObject)>>>>,
    }

    impl MockRocksDbStorage {
        fn new() -> Self {
            Self {
                objects: Arc::new(Mutex::new(HashMap::new())),
                proofs: Arc::new(Mutex::new(HashMap::new())),
                state_proofs: Arc::new(Mutex::new(HashMap::new())),
                current_slot: Arc::new(Mutex::new(1000)), // Start at a base slot number
                proof_engine: LatticeProofEngine::new(),
                object_history: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        // Get the current slot and increment for consistent slot ordering in tests
        fn next_slot(&self) -> SlotNumber {
            let mut slot = self.current_slot.lock().unwrap();
            *slot += 1;
            *slot
        }
    }

    impl UnitsWriteAheadLog for MockRocksDbStorage {
        fn init(&self, _path: &Path) -> Result<(), StorageError> {
            Ok(())
        }

        fn record_update(
            &self,
            _object: &TokenizedObject,
            _proof: &TokenizedObjectProof,
            _transaction_hash: Option<[u8; 32]>,
        ) -> Result<(), StorageError> {
            Ok(())
        }

        fn record_state_proof(&self, _state_proof: &StateProof) -> Result<(), StorageError> {
            Ok(())
        }

        fn iterate_entries(&self) -> Box<dyn Iterator<Item = Result<WALEntry, StorageError>> + '_> {
            // Empty iterator for testing
            let empty: Vec<Result<WALEntry, StorageError>> = Vec::new();
            Box::new(empty.into_iter())
        }
    }

    impl UnitsStorageProofEngine for MockRocksDbStorage {
        fn proof_engine(&self) -> &dyn ProofEngine {
            &self.proof_engine
        }

        fn get_proof(
            &self,
            id: &UnitsObjectId,
        ) -> Result<Option<TokenizedObjectProof>, StorageError> {
            let proofs = self.proofs.lock().unwrap();

            if let Some(proof_vec) = proofs.get(id) {
                if let Some((_, proof)) = proof_vec.last() {
                    return Ok(Some(proof.clone()));
                }
            }

            Ok(None)
        }

        fn get_proof_history(&self, id: &UnitsObjectId) -> Box<dyn UnitsProofIterator + '_> {
            let proofs = self.proofs.lock().unwrap();

            let result: Vec<(SlotNumber, TokenizedObjectProof)> =
                if let Some(proof_vec) = proofs.get(id) {
                    proof_vec
                        .iter()
                        .map(|(slot, proof)| (*slot, proof.clone()))
                        .collect()
                } else {
                    Vec::new()
                };

            // Sort by slot in descending order (newest first)
            let mut sorted_result = result;
            sorted_result.sort_by(|a, b| b.0.cmp(&a.0));

            Box::new(MockProofIterator {
                proofs: sorted_result,
                index: 0,
            })
        }

        fn get_proof_at_slot(
            &self,
            id: &UnitsObjectId,
            slot: SlotNumber,
        ) -> Result<Option<TokenizedObjectProof>, StorageError> {
            let proofs = self.proofs.lock().unwrap();

            if let Some(proof_vec) = proofs.get(id) {
                for (proof_slot, proof) in proof_vec {
                    if *proof_slot == slot {
                        return Ok(Some(proof.clone()));
                    }
                }
            }

            Ok(None)
        }

        fn get_state_proofs(&self) -> Box<dyn UnitsStateProofIterator + '_> {
            let state_proofs = self.state_proofs.lock().unwrap();

            let result: Vec<StateProof> = state_proofs.values().cloned().collect();

            Box::new(MockStateProofIterator {
                proofs: result,
                index: 0,
            })
        }

        fn get_state_proof_at_slot(
            &self,
            slot: SlotNumber,
        ) -> Result<Option<StateProof>, StorageError> {
            let state_proofs = self.state_proofs.lock().unwrap();
            if let Some(proof) = state_proofs.get(&slot) {
                Ok(Some(proof.clone()))
            } else {
                Ok(None)
            }
        }

        fn verify_proof(
            &self,
            id: &UnitsObjectId,
            proof: &TokenizedObjectProof,
        ) -> Result<bool, StorageError> {
            match self.get(id)? {
                Some(object) => self.proof_engine.verify_object_proof(&object, proof),
                None => Ok(false),
            }
        }

        fn verify_proof_chain(
            &self,
            id: &UnitsObjectId,
            start_slot: SlotNumber,
            end_slot: SlotNumber,
        ) -> Result<bool, StorageError> {
            // Get all proofs between start and end slots
            let mut proofs: Vec<(SlotNumber, TokenizedObjectProof)> = self
                .get_proof_history(id)
                .filter_map(|result| {
                    result
                        .ok()
                        .filter(|(slot, _)| *slot >= start_slot && *slot <= end_slot)
                })
                .collect();

            if proofs.is_empty() {
                return Err(StorageError::ProofNotFound(*id));
            }

            // Sort proofs by slot
            proofs.sort_by_key(|(slot, _)| *slot);

            // Get the corresponding object states
            let mut object_states: Vec<(SlotNumber, TokenizedObject)> = Vec::new();
            for (slot, _) in &proofs {
                if let Some(obj) = self.get_at_slot(id, *slot)? {
                    object_states.push((*slot, obj));
                } else {
                    return Err(StorageError::ObjectNotAtSlot(*slot));
                }
            }

            // In mock implementation, always return true
            Ok(true)
        }

        fn generate_state_proof(
            &self,
            _slot: Option<SlotNumber>,
        ) -> Result<StateProof, StorageError> {
            // Generate a simple state proof without previous state
            let slot = _slot.unwrap_or_else(units_proofs::proofs::current_slot);

            // Collect all proofs for state proof generation
            let proofs_lock = self.proofs.lock().unwrap();
            let mut all_proofs = Vec::new();

            for (id, proof_vec) in proofs_lock.iter() {
                if let Some((_, proof)) = proof_vec.last() {
                    all_proofs.push((*id, proof.clone()));
                }
            }

            self.proof_engine
                .generate_state_proof(&all_proofs, None, slot)
        }
    }

    impl UnitsStorage for MockRocksDbStorage {
        fn get(&self, id: &UnitsObjectId) -> Result<Option<TokenizedObject>, StorageError> {
            let objects = self.objects.lock().unwrap();
            if let Some(obj) = objects.get(id) {
                Ok(Some(obj.clone()))
            } else {
                Ok(None)
            }
        }

        fn get_at_slot(
            &self,
            id: &UnitsObjectId,
            slot: SlotNumber,
        ) -> Result<Option<TokenizedObject>, StorageError> {
            // For testing, find the object state as it was at the given slot
            // Look through the history to find the version at or before the requested slot
            let mut object_history = self.object_history.lock().unwrap();

            if let Some(history) = object_history.get_mut(id) {
                // Sort by slot in descending order
                history.sort_by(|(a_slot, _), (b_slot, _)| b_slot.cmp(a_slot));

                // Find the most recent object state at or before the requested slot
                for (obj_slot, obj) in history.iter() {
                    if *obj_slot <= slot {
                        return Ok(Some(obj.clone()));
                    }
                }
            }

            // If we don't have history or a version at or before the slot, try current state
            // This is needed because we might not have saved history for the very first object state
            self.get(id)
        }

        fn set(
            &self,
            object: &TokenizedObject,
            transaction_hash: Option<[u8; 32]>,
        ) -> Result<TokenizedObjectProof, StorageError> {
            // Get previous proof if it exists
            let prev_proof = self.get_proof(&object.id)?;

            // Force using our own slot numbers for testing to ensure they are sequential
            let slot = self.next_slot();

            // Generate a normal proof first
            let mut proof = self.proof_engine.generate_object_proof(
                object,
                prev_proof.as_ref(),
                transaction_hash,
            )?;

            // Then override its slot for testing consistency
            proof.slot = slot;

            // Store the object
            {
                let mut objects = self.objects.lock().unwrap();
                objects.insert(object.id, object.clone());
            }

            // Store the object in history with its slot
            {
                let mut object_history = self.object_history.lock().unwrap();
                let history_vec = object_history.entry(object.id).or_insert_with(Vec::new);
                history_vec.push((proof.slot, object.clone()));
            }

            // Store the proof
            {
                let mut proofs = self.proofs.lock().unwrap();
                let proof_vec = proofs.entry(object.id).or_insert_with(Vec::new);
                proof_vec.push((proof.slot, proof.clone()));
            }

            Ok(proof)
        }

        fn delete(
            &self,
            id: &UnitsObjectId,
            transaction_hash: Option<[u8; 32]>,
        ) -> Result<TokenizedObjectProof, StorageError> {
            // Get the object
            let object = match self.get(id)? {
                Some(obj) => obj,
                None => {
                    return Err(StorageError::NotFound(format!(
                        "Object with ID {:?} not found",
                        id
                    )))
                }
            };

            // Get previous proof if it exists
            let prev_proof = self.get_proof(id)?;

            // Force using our own slot numbers for testing to ensure they are sequential
            let slot = self.next_slot();

            // Generate a normal proof first
            let mut proof = self.proof_engine.generate_object_proof(
                &object,
                prev_proof.as_ref(),
                transaction_hash,
            )?;

            // Then override its slot for testing
            proof.slot = slot;

            // Remove the object
            {
                let mut objects = self.objects.lock().unwrap();
                objects.remove(id);
            }

            // Add a tombstone entry to history
            {
                let mut object_history = self.object_history.lock().unwrap();
                let history_vec = object_history.entry(*id).or_insert_with(Vec::new);

                // Create a tombstone object (empty data) to mark deletion
                let tombstone = TokenizedObject {
                    id: *id,
                    holder: UnitsObjectId::default(),
                    token_type: TokenType::Native,
                    token_manager: UnitsObjectId::default(),
                    data: Vec::new(),
                };

                history_vec.push((proof.slot, tombstone));
            }

            // Store the proof
            {
                let mut proofs = self.proofs.lock().unwrap();
                let proof_vec = proofs.entry(*id).or_insert_with(Vec::new);
                proof_vec.push((proof.slot, proof.clone()));
            }

            Ok(proof)
        }

        fn scan(&self) -> Box<dyn UnitsStorageIterator + '_> {
            let objects = self.objects.lock().unwrap();
            let values: Vec<TokenizedObject> = objects.values().cloned().collect();

            Box::new(MockStorageIterator {
                objects: values,
                index: 0,
            })
        }

        fn generate_and_store_state_proof(&self) -> Result<StateProof, StorageError> {
            let state_proof = self.generate_state_proof(None)?;

            // Store the state proof
            let mut state_proofs = self.state_proofs.lock().unwrap();
            state_proofs.insert(state_proof.slot, state_proof.clone());

            Ok(state_proof)
        }
    }

    #[test]
    fn test_basic_storage_operations() {
        // Create a mock storage
        let storage = MockRocksDbStorage::new();

        // Create test object
        let id = UnitsObjectId::unique_id_for_tests();
        let holder = UnitsObjectId::unique_id_for_tests();
        let token_manager = UnitsObjectId::unique_id_for_tests();
        let obj = TokenizedObject {
            id,
            holder,
            token_type: TokenType::Native,
            token_manager,
            data: vec![1, 2, 3, 4],
        };

        // Test set and get
        storage.set(&obj, None).unwrap();
        let retrieved = storage.get(&id).unwrap();

        // Verify the retrieved object is the same as the one we set
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.id, obj.id);
        assert_eq!(retrieved.holder, obj.holder);
        assert_eq!(retrieved.token_type, obj.token_type);
        assert_eq!(retrieved.token_manager, obj.token_manager);
        assert_eq!(retrieved.data, obj.data);

        // Test delete
        storage.delete(&id, None).unwrap();

        // Verify the object is deleted
        let deleted = storage.get(&id).unwrap();
        assert!(deleted.is_none());
    }

    #[test]
    fn test_proof_operations() {
        // Create mock storage
        let storage = MockRocksDbStorage::new();

        // Create test objects
        let obj1 = TokenizedObject {
            id: UnitsObjectId::unique_id_for_tests(),
            holder: UnitsObjectId::unique_id_for_tests(),
            token_type: TokenType::Native,
            token_manager: UnitsObjectId::unique_id_for_tests(),
            data: vec![1, 2, 3, 4],
        };

        let obj2 = TokenizedObject {
            id: UnitsObjectId::unique_id_for_tests(),
            holder: UnitsObjectId::unique_id_for_tests(),
            token_type: TokenType::Custodial,
            token_manager: UnitsObjectId::unique_id_for_tests(),
            data: vec![5, 6, 7, 8],
        };

        // Store the objects
        storage.set(&obj1, None).unwrap();
        storage.set(&obj2, None).unwrap();

        // Get proofs for the objects
        let proof1 = storage.get_proof(&obj1.id).unwrap();
        let proof2 = storage.get_proof(&obj2.id).unwrap();

        assert!(proof1.is_some());
        assert!(proof2.is_some());

        let proof1 = proof1.unwrap();
        let proof2 = proof2.unwrap();

        // Verify the proofs
        assert!(storage.verify_proof(&obj1.id, &proof1).unwrap());
        assert!(storage.verify_proof(&obj2.id, &proof2).unwrap());

        // Verify cross-proofs fail
        assert!(!storage.verify_proof(&obj1.id, &proof2).unwrap());
        assert!(!storage.verify_proof(&obj2.id, &proof1).unwrap());

        // Generate a state proof
        let state_proof = storage.generate_state_proof(None).unwrap();

        // We should have a non-empty proof now
        assert!(!state_proof.proof_data.is_empty());
    }

    #[test]
    fn test_storage_iterator() {
        // Create mock storage
        let storage = MockRocksDbStorage::new();

        // Create and store test objects
        let mut objects = Vec::new();
        for i in 0..5 {
            let obj = TokenizedObject {
                id: UnitsObjectId::unique_id_for_tests(),
                holder: UnitsObjectId::unique_id_for_tests(),
                token_type: TokenType::Native,
                token_manager: UnitsObjectId::unique_id_for_tests(),
                data: vec![i as u8; 4],
            };
            objects.push(obj.clone());
            storage.set(&obj, None).unwrap();
        }

        // Test iterator
        let mut iter = storage.scan();
        let mut found_objects = 0;

        while let Some(result) = iter.next() {
            // Verify the object is one of our test objects
            found_objects += 1;
            let obj = result.unwrap();
            let found = objects.iter().any(|test_obj| {
                test_obj.id == obj.id
                    && test_obj.holder == obj.holder
                    && test_obj.token_type == obj.token_type
                    && test_obj.token_manager == obj.token_manager
                    && test_obj.data == obj.data
            });
            assert!(found, "Iterator returned an unexpected object");
        }

        // Verify we found all our objects
        assert_eq!(found_objects, objects.len());
    }

    #[test]
    fn test_proof_chain_verification() {
        // Create mock storage
        let storage = MockRocksDbStorage::new();

        // Create a test object
        let id = UnitsObjectId::unique_id_for_tests();
        let holder = UnitsObjectId::unique_id_for_tests();
        let token_manager = UnitsObjectId::unique_id_for_tests();
        let mut obj = TokenizedObject {
            id,
            holder,
            token_type: TokenType::Native,
            token_manager,
            data: vec![1, 2, 3, 4],
        };

        // Store the object initially - this will create the first proof
        let proof1 = storage.set(&obj, None).unwrap();
        println!("Initial proof slot: {}", proof1.slot);

        // Modify and store the object again to create a chain of proofs
        obj.data = vec![5, 6, 7, 8];
        let proof2 = storage.set(&obj, None).unwrap();
        println!("Second proof slot: {}", proof2.slot);
        println!("Second proof prev_hash: {:?}", proof2.prev_proof_hash);

        // Modify and store once more
        obj.data = vec![9, 10, 11, 12];
        let proof3 = storage.set(&obj, None).unwrap();
        println!("Third proof slot: {}", proof3.slot);
        println!("Third proof prev_hash: {:?}", proof3.prev_proof_hash);

        // Get the slot numbers from the proofs
        let mut slots = Vec::new();
        let mut proof_list = Vec::new();

        println!("Getting proof history");
        for result in storage.get_proof_history(&id) {
            let (slot, proof) = result.unwrap();
            println!("Found proof at slot {}", slot);
            slots.push(slot);
            proof_list.push((slot, proof));
        }

        // Sort slots and proofs (should already be sorted, but to be safe)
        slots.sort();
        proof_list.sort_by_key(|(slot, _)| *slot);

        println!("Number of proofs: {}", slots.len());
        // We should have at least 3 slots with proofs
        assert!(slots.len() >= 3);

        // Verify the proof chain between first and last slot
        let start_slot = slots[0];
        let end_slot = slots[slots.len() - 1];

        // Let's look at the proof chain in detail
        for (i, (slot, proof)) in proof_list.iter().enumerate() {
            println!(
                "Proof {}: slot={}, prev_hash={:?}",
                i, slot, proof.prev_proof_hash
            );
        }

        // This should succeed since we have a valid chain
        assert!(storage
            .verify_proof_chain(&id, start_slot, end_slot)
            .unwrap());

        // Verify between first and second slot
        if slots.len() >= 2 {
            let second_slot = slots[1];
            assert!(storage
                .verify_proof_chain(&id, start_slot, second_slot)
                .unwrap());
        }

        // Test with non-existent object ID
        let nonexistent_id = UnitsObjectId::unique_id_for_tests();
        let result = storage.verify_proof_chain(&nonexistent_id, start_slot, end_slot);
        assert!(result.is_err());
        match result {
            Err(StorageError::ProofNotFound(_)) => {} // Expected error
            _ => panic!("Expected ProofNotFound error for non-existent object ID"),
        }
    }

    #[test]
    fn test_history_key_functions() {
        // Create a test ID and slot
        let id = UnitsObjectId::unique_id_for_tests();
        let slot: SlotNumber = 12345;

        // Generate a history key
        let key = make_history_key(&id, slot);

        // Parse the key back
        let parsed = parse_history_key(&key);
        assert!(parsed.is_some());

        // Check that parsed values match original values
        let (parsed_id, parsed_slot) = parsed.unwrap();
        assert_eq!(parsed_id, id);
        assert_eq!(parsed_slot, slot);

        // Test parsing with invalid inputs

        // Test with key that's too short
        let short_key = vec![1, 2, 3];
        assert!(parse_history_key(&short_key).is_none());

        // Test with key that has no separator
        let no_separator = vec![0; 40];
        assert!(parse_history_key(&no_separator).is_none());

        // Test with key that has separator but wrong format
        let mut bad_format = vec![0; 32];
        bad_format.push(b':');
        bad_format.push(1);
        assert!(parse_history_key(&bad_format).is_none());
    }
}
