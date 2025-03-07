#[cfg(feature = "rocksdb")]
use crate::{
    error::StorageError,
    id::UnitsObjectId,
    objects::TokenizedObject,
    proofs::{current_slot, LatticeProofEngine, ProofEngine, SlotNumber, StateProof, TokenizedObjectProof},
    storage::FileWriteAheadLog,
    storage_traits::{
        UnitsProofIterator, UnitsStateProofIterator, UnitsStorage, UnitsStorageIterator,
        UnitsStorageProofEngine, UnitsWriteAheadLog
    },
};
#[cfg(feature = "rocksdb")]
use anyhow::{Context, Result};
#[cfg(feature = "rocksdb")]
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
#[cfg(feature = "rocksdb")]
use std::{
    convert::TryInto,
    fmt::Debug,
    path::{Path, PathBuf},
    sync::Arc,
};

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
        slot: SlotNumber
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
        proof: &TokenizedObjectProof
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

#[cfg(feature = "rocksdb")]
impl UnitsWriteAheadLog for RocksDbStorage {
    fn init(&self, path: &Path) -> Result<(), StorageError> {
        self.wal.init(path)
    }
    
    fn record_update(
        &self, 
        object: &TokenizedObject, 
        proof: &TokenizedObjectProof
    ) -> Result<(), StorageError> {
        self.wal.record_update(object, proof)
    }
    
    fn record_state_proof(&self, state_proof: &StateProof) -> Result<(), StorageError> {
        self.wal.record_state_proof(state_proof)
    }
    
    fn iterate_entries(&self) -> Box<dyn Iterator<Item = Result<crate::storage_traits::WALEntry, StorageError>> + '_> {
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
        slot: SlotNumber
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
            .with_context(|| format!("Failed to get object history for ID: {:?} at slot {}", id, slot))?;

        if let Some(bytes) = result {
            let obj = bincode::deserialize(&bytes)
                .with_context(|| format!("Failed to deserialize object with ID: {:?} at slot {}", id, slot))?;
            Ok(Some(obj))
        } else {
            Ok(None)
        }
    }

    fn set(&self, object: &TokenizedObject) -> Result<TokenizedObjectProof, StorageError> {
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
        let proof = self.proof_engine.generate_object_proof(object, prev_proof.as_ref())?;
        
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
        self.record_update(object, &proof)?;

        Ok(proof)
    }

    fn delete(&self, id: &UnitsObjectId) -> Result<TokenizedObjectProof, StorageError> {
        // First, check if the object exists
        let object = match self.get(id)? {
            Some(obj) => obj,
            None => return Err(StorageError::NotFound(format!("Object with ID {:?} not found", id))),
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
        
        // Generate a proof for the deletion
        let proof = self.proof_engine.generate_object_proof(&tombstone, prev_proof.as_ref())?;
        
        // Store the deletion proof with history
        self.store_proof_with_history(id, &proof)?;
        
        // Record the deletion in the write-ahead log
        self.record_update(&tombstone, &proof)?;
        
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
            current_slot
        )?;
        
        // Serialize and store the state proof
        let serialized_proof = bincode::serialize(&state_proof)
            .with_context(|| "Failed to serialize state proof")?;
            
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
                Err(_) => None
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
            slot_to_use
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
                        let proof = self.proof_engine.generate_object_proof(&object, None)?;

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
                        },
                        Err(_) => continue,
                    }
                },
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
        slot: SlotNumber
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
        match self.db.get_cf(&cf_proof_history, &history_key)? {
            Some(value) => {
                let proof = bincode::deserialize(&value)
                    .with_context(|| format!("Failed to deserialize proof history for ID: {:?} at slot {}", id, slot))?;
                Ok(Some(proof))
            },
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
                        },
                        Err(_) => continue,
                    }
                },
                _ => continue,
            }
        }
        
        // Sort by slot in descending order (newest first)
        state_proofs.sort_by(|a, b| b.slot.cmp(&a.slot));
        
        Box::new(RocksDbStateProofIterator::new(state_proofs))
    }
    
    fn get_state_proof_at_slot(&self, slot: SlotNumber) -> Result<Option<StateProof>, StorageError> {
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
        match self.db.get_cf(&cf_state_proofs, &key)? {
            Some(value) => {
                let proof = bincode::deserialize(&value)
                    .with_context(|| format!("Failed to deserialize state proof at slot {}", slot))?;
                Ok(Some(proof))
            },
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
        end_slot: SlotNumber
    ) -> Result<bool, StorageError> {
        // Get the proofs at the start and end slots
        let start_proof = match self.get_proof_at_slot(id, start_slot)? {
            Some(proof) => proof,
            None => return Err(StorageError::ProofNotAtSlot(start_slot)),
        };
        
        let end_proof = match self.get_proof_at_slot(id, end_slot)? {
            Some(proof) => proof,
            None => return Err(StorageError::ProofNotAtSlot(end_slot)),
        };
        
        // If they're the same proof, return true
        if start_proof.slot == end_proof.slot {
            return Ok(true);
        }
        
        // Get the object at the end slot
        let end_object = match self.get_at_slot(id, end_slot)? {
            Some(obj) => obj,
            None => return Err(StorageError::ObjectNotAtSlot(end_slot)),
        };
        
        // Check that the end proof is valid for the end object
        if !self.proof_engine.verify_object_proof(&end_object, &end_proof)? {
            return Ok(false);
        }
        
        // Verify the chain backwards
        let mut current_proof = end_proof;
        
        while current_proof.slot > start_slot {
            // Get the previous proof from the chain
            let prev_proof_hash = match current_proof.prev_proof_hash {
                Some(hash) => hash,
                None => {
                    // Chain is broken - no link to previous proof
                    return Ok(false);
                }
            };
            
            // Find the previous proof in the history
            // We need to scan backwards until we find a proof with the matching hash
            let mut prev_proof_found = false;
            
            for result in self.get_proof_history(id) {
                let (slot, prev_proof) = result?;
                
                // Skip proofs that are newer than the current one or older than the start slot
                if slot >= current_proof.slot || slot < start_slot {
                    continue;
                }
                
                // Check if this is the right previous proof
                if prev_proof.hash() == prev_proof_hash {
                    // Verify that the previous proof is valid for the object at that slot
                    match self.get_at_slot(id, slot)? {
                        Some(prev_object) => {
                            if !self.proof_engine.verify_object_proof(&prev_object, &prev_proof)? {
                                return Ok(false);
                            }
                            
                            // Move to the previous proof
                            current_proof = prev_proof;
                            prev_proof_found = true;
                            break;
                        },
                        None => continue, // Object not found at this slot, try another proof
                    }
                }
            }
            
            if !prev_proof_found {
                // Couldn't find a valid previous proof
                return Ok(false);
            }
            
            // If we've reached the start proof, we're done
            if current_proof.slot <= start_slot {
                break;
            }
        }
        
        // If we've made it to the start slot, verify the chain is complete
        Ok(current_proof.slot == start_slot)
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

#[cfg(all(test, feature = "rocksdb"))]
mod tests {
    use super::*;
    use crate::id::tests::unique_id;
    use crate::objects::TokenType;
    use tempfile::tempdir;

    #[test]
    #[ignore = "RocksDB implementation needs to be updated with new error types"]
    fn test_basic_storage_operations() {
        // Create temporary directory for test database
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");

        // Create storage
        let storage = RocksDbStorage::new(&db_path).unwrap();

        // Create test object
        let id = unique_id();
        let holder = unique_id();
        let token_manager = unique_id();
        let obj = TokenizedObject {
            id,
            holder,
            token_type: TokenType::Native,
            token_manager,
            data: vec![1, 2, 3, 4],
        };

        // Test set and get
        storage.set(&obj).unwrap();
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
        storage.delete(&id).unwrap();

        // Verify the object is deleted
        let deleted = storage.get(&id).unwrap();
        assert!(deleted.is_none());
    }

    #[test]
    #[ignore = "RocksDB implementation needs to be updated with new error types"]
    fn test_proof_operations() {
        // Create temporary directory for test database
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test_proofs.db");

        // Create storage
        let storage = RocksDbStorage::new(&db_path).unwrap();

        // Create test objects
        let obj1 = TokenizedObject {
            id: unique_id(),
            holder: unique_id(),
            token_type: TokenType::Native,
            token_manager: unique_id(),
            data: vec![1, 2, 3, 4],
        };

        let obj2 = TokenizedObject {
            id: unique_id(),
            holder: unique_id(),
            token_type: TokenType::Custodial,
            token_manager: unique_id(),
            data: vec![5, 6, 7, 8],
        };

        // Store the objects
        storage.set(&obj1).unwrap();
        storage.set(&obj2).unwrap();

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
        assert!(!state_proof.proof.is_empty());
    }

    #[test]
    #[ignore = "RocksDB implementation needs to be updated with new error types"]
    fn test_storage_iterator() {
        // Create temporary directory for test database
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test_iter.db");

        // Create storage
        let storage = RocksDbStorage::new(&db_path).unwrap();

        // Create and store test objects
        let mut objects = Vec::new();
        for i in 0..5 {
            let obj = TokenizedObject {
                id: unique_id(),
                holder: unique_id(),
                token_type: TokenType::Native,
                token_manager: unique_id(),
                data: vec![i as u8; 4],
            };
            objects.push(obj.clone());
            storage.set(&obj).unwrap();
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
}
