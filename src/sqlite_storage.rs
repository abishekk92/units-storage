use crate::{
    id::UnitsObjectId,
    objects::{TokenType, TokenizedObject},
    proofs::{StateProof, TokenizedObjectProof},
    storage::{UnitsStorage, UnitsStorageIterator, UnitsStorageProofEngine},
};
use rusqlite::{params, Connection, Result as SqlResult};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

/// A simple SQLite-based implementation of the UnitsStorage interface.
/// This provides a concrete example of how to implement storage for UNITS objects.
pub struct SqliteStorage {
    conn: Arc<Mutex<Connection>>,
    db_path: PathBuf,
}

/// Iterator implementation for SQLite storage
pub struct SqliteStorageIterator {
    conn: Arc<Mutex<Connection>>,
    current_index: i64,
}

impl SqliteStorage {
    /// Creates a new SQLite storage instance
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, String> {
        let db_path = path.as_ref().to_path_buf();
        
        // Open or create the database
        let conn = match Connection::open(&db_path) {
            Ok(conn) => conn,
            Err(e) => return Err(format!("Failed to open SQLite database: {}", e)),
        };

        // Initialize the database schema
        if let Err(e) = Self::initialize_schema(&conn) {
            return Err(format!("Failed to initialize database schema: {}", e));
        }

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            db_path,
        })
    }

    /// Creates the necessary tables in the database
    fn initialize_schema(conn: &Connection) -> SqlResult<()> {
        conn.execute(
            "CREATE TABLE IF NOT EXISTS objects (
                id BLOB PRIMARY KEY,
                holder BLOB NOT NULL,
                token_type INTEGER NOT NULL,
                token_manager BLOB NOT NULL,
                data BLOB
            )",
            [],
        )?;
        
        // Table for storing object proofs
        conn.execute(
            "CREATE TABLE IF NOT EXISTS object_proofs (
                object_id BLOB PRIMARY KEY,
                proof BLOB NOT NULL
            )",
            [],
        )?;
        
        // Table for storing state proofs
        conn.execute(
            "CREATE TABLE IF NOT EXISTS state_proofs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER NOT NULL,
                proof BLOB NOT NULL
            )",
            [],
        )?;
        
        Ok(())
    }
    
    /// Convert TokenType enum to integer for storage
    fn token_type_to_int(token_type: &TokenType) -> i64 {
        match token_type {
            TokenType::Native => 0,
            TokenType::Custodial => 1,
            TokenType::Proxy => 2,
        }
    }
    
    /// Convert integer from storage to TokenType enum
    fn int_to_token_type(value: i64) -> Result<TokenType, String> {
        match value {
            0 => Ok(TokenType::Native),
            1 => Ok(TokenType::Custodial),
            2 => Ok(TokenType::Proxy),
            _ => Err(format!("Invalid token type value: {}", value)),
        }
    }
}

impl UnitsStorage for SqliteStorage {
    fn get(&self, id: &UnitsObjectId) -> Option<TokenizedObject> {
        let conn = match self.conn.lock() {
            Ok(conn) => conn,
            Err(_) => return None, // Lock poisoned
        };
        
        let query = "SELECT id, holder, token_type, token_manager, data FROM objects WHERE id = ?";
        
        conn.query_row(query, params![id.as_ref()], |row| {
            let id_blob: Vec<u8> = row.get(0)?;
            let holder_blob: Vec<u8> = row.get(1)?;
            let token_type_int: i64 = row.get(2)?;
            let token_manager_blob: Vec<u8> = row.get(3)?;
            let data: Vec<u8> = row.get(4).unwrap_or_else(|_| Vec::new());
            
            // Convert to UnitsObjectId
            let mut id_array = [0u8; 32];
            let mut holder_array = [0u8; 32];
            let mut token_manager_array = [0u8; 32];
            
            if id_blob.len() == 32 {
                id_array.copy_from_slice(&id_blob);
            }
            
            if holder_blob.len() == 32 {
                holder_array.copy_from_slice(&holder_blob);
            }
            
            if token_manager_blob.len() == 32 {
                token_manager_array.copy_from_slice(&token_manager_blob);
            }
            
            // Convert token type
            let token_type = match SqliteStorage::int_to_token_type(token_type_int) {
                Ok(tt) => tt,
                Err(_) => TokenType::Native, // Default to Native if invalid
            };
            
            Ok(TokenizedObject {
                id: UnitsObjectId::new(id_array),
                holder: UnitsObjectId::new(holder_array),
                token_type,
                token_manager: UnitsObjectId::new(token_manager_array),
                data,
            })
        }).ok()
    }

    fn set(&self, object: &TokenizedObject) -> Result<(), String> {
        let conn = match self.conn.lock() {
            Ok(conn) => conn,
            Err(_) => return Err("Failed to acquire database lock".to_string()),
        };
        
        let token_type_int = Self::token_type_to_int(&object.token_type);
        
        let query = "INSERT OR REPLACE INTO objects (id, holder, token_type, token_manager, data) VALUES (?, ?, ?, ?, ?)";
        
        match conn.execute(
            query,
            params![
                object.id.as_ref(),
                object.holder.as_ref(),
                token_type_int,
                object.token_manager.as_ref(),
                &object.data,
            ],
        ) {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("Failed to store object: {}", e)),
        }
    }

    fn delete(&self, id: &UnitsObjectId) -> Result<(), String> {
        let mut conn = match self.conn.lock() {
            Ok(conn) => conn,
            Err(_) => return Err("Failed to acquire database lock".to_string()),
        };
        
        // Use a transaction to ensure consistency
        let tx = match conn.transaction() {
            Ok(tx) => tx,
            Err(e) => return Err(format!("Failed to start transaction: {}", e)),
        };
        
        // Delete from object_proofs
        if let Err(e) = tx.execute(
            "DELETE FROM object_proofs WHERE object_id = ?",
            params![id.as_ref()],
        ) {
            return Err(format!("Failed to delete object proofs: {}", e));
        }
        
        // Delete from objects
        if let Err(e) = tx.execute(
            "DELETE FROM objects WHERE id = ?",
            params![id.as_ref()],
        ) {
            return Err(format!("Failed to delete object: {}", e));
        }
        
        // Commit transaction
        if let Err(e) = tx.commit() {
            return Err(format!("Failed to commit transaction: {}", e));
        }
        
        Ok(())
    }

    fn scan(&self) -> Box<dyn UnitsStorageIterator> {
        // Return an iterator that will scan through all objects
        Box::new(SqliteStorageIterator {
            conn: self.conn.clone(),
            current_index: 0,
        })
    }
}

impl UnitsStorageProofEngine for SqliteStorage {
    fn generate_state_proof(&self) -> StateProof {
        let conn = match self.conn.lock() {
            Ok(conn) => conn,
            Err(_) => return StateProof { proof: Vec::new() },
        };
        
        // In a real implementation, this would:
        // 1. Generate a cryptographic proof based on all objects in storage
        // 2. Store the proof in the state_proofs table with a timestamp
        // 3. Return the generated proof
        
        // For this example, we just return a dummy proof
        let dummy_proof = vec![0, 1, 2, 3, 4];
        
        // Store the proof for future reference
        let _ = conn.execute(
            "INSERT INTO state_proofs (timestamp, proof) VALUES (?, ?)",
            params![
                chrono::Utc::now().timestamp(),
                &dummy_proof,
            ],
        );
        
        StateProof { proof: dummy_proof }
    }

    fn get_proof(&self, id: &UnitsObjectId) -> Option<TokenizedObjectProof> {
        let conn = match self.conn.lock() {
            Ok(conn) => conn,
            Err(_) => return None,
        };
        
        let query = "SELECT proof FROM object_proofs WHERE object_id = ?";
        
        match conn.query_row(query, params![id.as_ref()], |row| {
            let proof: Vec<u8> = row.get(0)?;
            Ok(TokenizedObjectProof { proof })
        }) {
            Ok(proof) => Some(proof),
            Err(_) => None,
        }
    }

    fn verify_proof(&self, _id: &UnitsObjectId, _proof: &TokenizedObjectProof) -> bool {
        // In a real implementation, this would:
        // 1. Verify the cryptographic proof against the current state
        // 2. Return true if valid, false otherwise
        
        // For this example, always return true
        true
    }
}

impl UnitsStorageIterator for SqliteStorageIterator {
    fn next(&mut self) -> Option<TokenizedObject> {
        let conn = match self.conn.lock() {
            Ok(conn) => conn,
            Err(_) => return None, // Lock poisoned
        };
        
        // Query a single object at the current index
        let query = "SELECT id, holder, token_type, token_manager, data FROM objects LIMIT 1 OFFSET ?";
        
        let result = conn.query_row(query, params![self.current_index], |row| {
            let id_blob: Vec<u8> = row.get(0)?;
            let holder_blob: Vec<u8> = row.get(1)?;
            let token_type_int: i64 = row.get(2)?;
            let token_manager_blob: Vec<u8> = row.get(3)?;
            let data: Vec<u8> = row.get(4).unwrap_or_else(|_| Vec::new());
            
            // Convert to UnitsObjectId
            let mut id_array = [0u8; 32];
            let mut holder_array = [0u8; 32];
            let mut token_manager_array = [0u8; 32];
            
            if id_blob.len() == 32 {
                id_array.copy_from_slice(&id_blob);
            }
            
            if holder_blob.len() == 32 {
                holder_array.copy_from_slice(&holder_blob);
            }
            
            if token_manager_blob.len() == 32 {
                token_manager_array.copy_from_slice(&token_manager_blob);
            }
            
            // Convert token type
            let token_type = match SqliteStorage::int_to_token_type(token_type_int) {
                Ok(tt) => tt,
                Err(_) => TokenType::Native, // Default to Native if invalid
            };
            
            Ok(TokenizedObject {
                id: UnitsObjectId::new(id_array),
                holder: UnitsObjectId::new(holder_array),
                token_type,
                token_manager: UnitsObjectId::new(token_manager_array),
                data,
            })
        });
        
        // Increment the index for the next call
        self.current_index += 1;
        
        result.ok()
    }
}

impl std::fmt::Debug for SqliteStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteStorage")
            .field("db_path", &self.db_path)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    
    #[test]
    fn test_basic_storage_operations() {
        // Create temporary directory for test database
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        
        // Create storage
        let storage = SqliteStorage::new(&db_path).unwrap();
        
        // Create test object
        let id = UnitsObjectId::default();
        let holder = UnitsObjectId::default();
        let token_manager = UnitsObjectId::default();
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
        
        assert_eq!(retrieved.id, obj.id);
        assert_eq!(retrieved.holder, obj.holder);
        assert_eq!(retrieved.token_type, obj.token_type);
        assert_eq!(retrieved.token_manager, obj.token_manager);
        assert_eq!(retrieved.data, obj.data);
        
        // Test delete
        storage.delete(&id).unwrap();
        assert!(storage.get(&id).is_none());
    }
    
    #[test]
    fn test_scan_operations() {
        // Create temporary directory for test database
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test_scan.db");
        
        // Create storage
        let storage = SqliteStorage::new(&db_path).unwrap();
        
        // Add multiple objects
        for i in 0..5 {
            let mut id_bytes = [0u8; 32];
            id_bytes[0] = i;
            let id = UnitsObjectId::new(id_bytes);
            
            let obj = TokenizedObject {
                id,
                holder: UnitsObjectId::default(),
                token_type: TokenType::Native,
                token_manager: UnitsObjectId::default(),
                data: vec![i, i+1, i+2],
            };
            
            storage.set(&obj).unwrap();
        }
        
        // Test scan
        let mut iterator = storage.scan();
        let mut count = 0;
        
        while let Some(_) = iterator.next() {
            count += 1;
        }
        
        assert_eq!(count, 5);
    }
    
    #[test]
    fn test_proof_operations() {
        // Create temporary directory for test database
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test_proofs.db");
        
        // Create storage
        let storage = SqliteStorage::new(&db_path).unwrap();
        
        // Generate a state proof
        let proof = storage.generate_state_proof();
        assert!(!proof.proof.is_empty());
        
        // For a complete implementation, we would also test
        // storing and retrieving object proofs
    }
}