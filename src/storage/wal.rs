use crate::error::StorageError;
use crate::objects::TokenizedObject;
use crate::proofs::{StateProof, TokenizedObjectProof};
use crate::storage_traits::{UnitsWriteAheadLog, WALEntry};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use bincode;
use serde::{Deserialize, Serialize};

/// Entry type in the WAL
#[derive(Debug, Clone, Serialize, Deserialize)]
enum WALEntryType {
    /// Update to an object's state
    ObjectUpdate(WALEntry),
    
    /// State proof for a slot
    StateProof(StateProof),
}

/// A basic file-based write-ahead log implementation
pub struct FileWriteAheadLog {
    /// Path to the WAL file
    path: Arc<Mutex<PathBuf>>,
    
    /// File handle for writing
    file: Arc<Mutex<Option<BufWriter<File>>>>,
}

impl FileWriteAheadLog {
    /// Create a new file-based WAL
    pub fn new() -> Self {
        Self {
            path: Arc::new(Mutex::new(PathBuf::new())),
            file: Arc::new(Mutex::new(None)),
        }
    }
    
    /// Get the current timestamp in milliseconds
    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

impl UnitsWriteAheadLog for FileWriteAheadLog {
    fn init(&self, path: &Path) -> Result<(), StorageError> {
        let mut file_guard = self.file.lock().map_err(|e| {
            StorageError::WAL(format!("Failed to acquire lock: {}", e))
        })?;
        
        // Create or open the WAL file
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)
            .map_err(|e| StorageError::WAL(format!("Failed to open WAL file: {}", e)))?;
            
        let writer = BufWriter::new(file);
        
        // Store the file writer
        *file_guard = Some(writer);
        
        // Store the path
        let mut path_guard = self.path.lock().map_err(|e| {
            StorageError::WAL(format!("Failed to acquire path lock: {}", e))
        })?;
        *path_guard = path.to_path_buf();
        
        Ok(())
    }
    
    fn record_update(
        &self, 
        object: &TokenizedObject, 
        proof: &TokenizedObjectProof
    ) -> Result<(), StorageError> {
        let mut file_guard = self.file.lock().map_err(|e| {
            StorageError::WAL(format!("Failed to acquire lock: {}", e))
        })?;
        
        let file = file_guard.as_mut().ok_or_else(|| {
            StorageError::WAL("WAL has not been initialized".to_string())
        })?;
        
        // Create the WAL entry
        let entry = WALEntry {
            object: object.clone(),
            slot: proof.slot,
            proof: proof.clone(),
            timestamp: Self::current_timestamp(),
        };
        
        // Serialize the entry
        let wal_entry = WALEntryType::ObjectUpdate(entry);
        let serialized = bincode::serialize(&wal_entry)?;
        
        // Write the entry length and data
        let entry_len = serialized.len() as u64;
        file.write_all(&entry_len.to_le_bytes())?;
        file.write_all(&serialized)?;
        file.flush()?;
        
        Ok(())
    }
    
    fn record_state_proof(&self, state_proof: &StateProof) -> Result<(), StorageError> {
        let mut file_guard = self.file.lock().map_err(|e| {
            StorageError::WAL(format!("Failed to acquire lock: {}", e))
        })?;
        
        let file = file_guard.as_mut().ok_or_else(|| {
            StorageError::WAL("WAL has not been initialized".to_string())
        })?;
        
        // Create the WAL entry
        let wal_entry = WALEntryType::StateProof(state_proof.clone());
        let serialized = bincode::serialize(&wal_entry)?;
        
        // Write the entry length and data
        let entry_len = serialized.len() as u64;
        file.write_all(&entry_len.to_le_bytes())?;
        file.write_all(&serialized)?;
        file.flush()?;
        
        Ok(())
    }
    
    fn iterate_entries(&self) -> Box<dyn Iterator<Item = Result<WALEntry, StorageError>> + '_> {
        // Get the path
        let path_guard = match self.path.lock() {
            Ok(guard) => guard,
            Err(_) => return Box::new(std::iter::empty()),
        };
        let path = path_guard.clone();
        drop(path_guard);
        
        // Create a new file reader
        let result = File::open(&path).and_then(|file| {
            Ok(WALEntryIterator {
                reader: BufReader::new(file),
            })
        });
        
        match result {
            Ok(iterator) => Box::new(iterator),
            Err(_) => {
                // Return an empty iterator if we can't open the file
                Box::new(std::iter::empty::<Result<WALEntry, StorageError>>())
            }
        }
    }
}

/// Iterator over WAL entries
struct WALEntryIterator {
    reader: BufReader<File>,
}

impl Iterator for WALEntryIterator {
    type Item = Result<WALEntry, StorageError>;
    
    fn next(&mut self) -> Option<Self::Item> {
        // Read the entry length
        let mut len_buf = [0u8; 8];
        match self.reader.read_exact(&mut len_buf) {
            Ok(_) => {},
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // End of file
                return None;
            },
            Err(e) => {
                return Some(Err(StorageError::from(e)));
            }
        }
        
        let entry_len = u64::from_le_bytes(len_buf);
        
        // Read the entry data
        let mut entry_data = vec![0u8; entry_len as usize];
        if let Err(e) = self.reader.read_exact(&mut entry_data) {
            return Some(Err(StorageError::from(e)));
        }
        
        // Deserialize the entry
        let entry_type: Result<WALEntryType, _> = bincode::deserialize(&entry_data);
        match entry_type {
            Ok(WALEntryType::ObjectUpdate(entry)) => Some(Ok(entry)),
            Ok(WALEntryType::StateProof(_)) => {
                // Skip state proofs - this iterator only returns object updates
                self.next()
            },
            Err(e) => Some(Err(StorageError::from(e))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::UnitsObjectId;
    use crate::objects::{TokenType, TokenizedObject};
    use crate::proofs::current_slot;
    use tempfile::tempdir;
    
    // Helper to create a test object
    fn create_test_object() -> TokenizedObject {
        let id = UnitsObjectId::random();
        let holder = UnitsObjectId::random();
        let token_manager = UnitsObjectId::random();
        
        TokenizedObject {
            id,
            holder,
            token_type: TokenType::Native,
            token_manager,
            data: vec![1, 2, 3, 4],
        }
    }
    
    // Helper to create a test proof
    fn create_test_proof() -> TokenizedObjectProof {
        TokenizedObjectProof {
            proof: vec![5, 6, 7, 8],
            slot: current_slot(),
            prev_proof_hash: None,
        }
    }
    
    // Helper to create a test state proof
    fn create_test_state_proof() -> StateProof {
        StateProof {
            proof: vec![9, 10, 11, 12],
            slot: current_slot(),
            prev_proof_hash: None,
            included_objects: vec![UnitsObjectId::random()],
        }
    }
    
    #[test]
    fn test_wal_object_updates() {
        // Create a temporary directory for the WAL
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path().join("test.wal");
        
        // Create a WAL and initialize it
        let wal = FileWriteAheadLog::new();
        wal.init(&wal_path).unwrap();
        
        // Create some test objects and proofs
        let obj1 = create_test_object();
        let proof1 = create_test_proof();
        
        let obj2 = create_test_object();
        let proof2 = create_test_proof();
        
        // Record updates
        wal.record_update(&obj1, &proof1).unwrap();
        wal.record_update(&obj2, &proof2).unwrap();
        
        // Iterate over the entries
        let entries: Vec<_> = wal.iterate_entries().collect::<Result<Vec<_>, _>>().unwrap();
        
        // Verify the results
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].object.id, obj1.id);
        assert_eq!(entries[1].object.id, obj2.id);
    }
    
    #[test]
    fn test_wal_state_proofs() {
        // Create a temporary directory for the WAL
        let temp_dir = tempdir().unwrap();
        let wal_path = temp_dir.path().join("test.wal");
        
        // Create a WAL and initialize it
        let wal = FileWriteAheadLog::new();
        wal.init(&wal_path).unwrap();
        
        // Create a test object, proof, and state proof
        let obj = create_test_object();
        let proof = create_test_proof();
        let state_proof = create_test_state_proof();
        
        // Record updates
        wal.record_update(&obj, &proof).unwrap();
        wal.record_state_proof(&state_proof).unwrap();
        
        // Iterate over the entries
        let entries: Vec<_> = wal.iterate_entries().collect::<Result<Vec<_>, _>>().unwrap();
        
        // Verify the results
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].object.id, obj.id);
    }
}