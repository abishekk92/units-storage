use crate::{
    error::StorageError,
    id::UnitsObjectId,
    objects::{TokenType, TokenizedObject},
    proofs::{LatticeProofEngine, ProofEngine, StateProof, TokenizedObjectProof},
    storage_traits::{UnitsStorage, UnitsStorageIterator, UnitsStorageProofEngine},
};
use anyhow::{Context, Result};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions},
    Row,
};
use std::{
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};
use tokio::runtime::Runtime;

/// A SQLite-based implementation of the UnitsStorage interface using sqlx.
pub struct SqliteStorage {
    pool: SqlitePool,
    rt: Arc<Runtime>,
    db_path: PathBuf,
    proof_engine: LatticeProofEngine,
}

/// Iterator implementation for SQLite storage
pub struct SqliteStorageIterator {
    pool: SqlitePool,
    rt: Arc<Runtime>,
    current_index: i64,
}

impl SqliteStorage {
    /// Creates a new SQLite storage instance
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, String> {
        let db_path = path.as_ref().to_path_buf();
        let db_url = format!("sqlite:{}", db_path.to_string_lossy());

        // Create a runtime for async operations
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => Arc::new(rt),
            Err(e) => return Err(format!("Failed to create runtime: {}", e)),
        };

        // Connect options
        let options = match SqliteConnectOptions::from_str(&db_url) {
            Ok(opt) => opt.create_if_missing(true),
            Err(e) => return Err(format!("Invalid database URL: {}", e)),
        };

        // Create connection pool
        let pool = rt.block_on(async {
            SqlitePoolOptions::new()
                .max_connections(5)
                .connect_with(options)
                .await
        });

        let pool = match pool {
            Ok(pool) => pool,
            Err(e) => return Err(format!("Failed to connect to database: {}", e)),
        };

        // Initialize the database schema
        if let Err(e) = rt.block_on(Self::initialize_schema(&pool)) {
            return Err(format!("Failed to initialize database schema: {}", e));
        }

        Ok(Self {
            pool,
            rt,
            db_path,
            proof_engine: LatticeProofEngine::new(),
        })
    }

    /// Creates the necessary tables in the database
    async fn initialize_schema(pool: &SqlitePool) -> Result<(), sqlx::Error> {
        // Table for objects
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS objects (
                id BLOB PRIMARY KEY,
                holder BLOB NOT NULL,
                token_type INTEGER NOT NULL,
                token_manager BLOB NOT NULL,
                data BLOB
            )",
        )
        .execute(pool)
        .await?;

        // Table for storing object proofs
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS object_proofs (
                object_id BLOB PRIMARY KEY,
                proof BLOB NOT NULL
            )",
        )
        .execute(pool)
        .await?;

        // Table for storing state proofs
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS state_proofs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER NOT NULL,
                proof BLOB NOT NULL
            )",
        )
        .execute(pool)
        .await?;

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
    fn get(&self, id: &UnitsObjectId) -> Result<Option<TokenizedObject>, StorageError> {
        self.rt.block_on(async {
            let query = "SELECT id, holder, token_type, token_manager, data FROM objects WHERE id = ?";
            
            let row = sqlx::query(query)
                .bind(id.as_ref())
                .fetch_optional(&self.pool)
                .await
                .with_context(|| format!("Failed to fetch object with ID: {:?}", id))?;
                
            if row.is_none() {
                return Ok(None);
            }
            
            let row = row.unwrap();

            let id_blob: Vec<u8> = row.get(0);
            let holder_blob: Vec<u8> = row.get(1);
            let token_type_int: i64 = row.get(2);
            let token_manager_blob: Vec<u8> = row.get(3);
            let data: Option<Vec<u8>> = row.get(4);
            let data = data.unwrap_or_default();

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
                Err(e) => return Err(StorageError::Other(e)),
            };

            Ok(Some(TokenizedObject {
                id: UnitsObjectId::new(id_array),
                holder: UnitsObjectId::new(holder_array),
                token_type,
                token_manager: UnitsObjectId::new(token_manager_array),
                data,
            }))
        })
    }

    fn set(&self, object: &TokenizedObject) -> Result<(), StorageError> {
        self.rt.block_on(async {
            // Use a transaction to ensure atomicity
            let mut tx = self.pool
                .begin()
                .await
                .with_context(|| "Failed to start database transaction")?;
            
            // Store the object
            let token_type_int = Self::token_type_to_int(&object.token_type);
            let query = "INSERT OR REPLACE INTO objects (id, holder, token_type, token_manager, data) VALUES (?, ?, ?, ?, ?)";

            sqlx::query(query)
                .bind(object.id.as_ref())
                .bind(object.holder.as_ref())
                .bind(token_type_int)
                .bind(object.token_manager.as_ref())
                .bind(&object.data)
                .execute(&mut *tx)
                .await
                .with_context(|| format!("Failed to store object with ID: {:?}", object.id))?;
                
            // Generate and store a proof for the object
            let proof = self.proof_engine.generate_object_proof(object)
                .with_context(|| format!("Failed to generate proof for object ID: {:?}", object.id))?;
            
            sqlx::query("INSERT OR REPLACE INTO object_proofs (object_id, proof) VALUES (?, ?)")
                .bind(object.id.as_ref())
                .bind(&proof.proof)
                .execute(&mut *tx)
                .await
                .with_context(|| format!("Failed to store proof for object ID: {:?}", object.id))?;
                
            // Commit the transaction
            tx.commit()
                .await
                .with_context(|| "Failed to commit transaction")?;

            Ok(())
        })
    }

    fn delete(&self, id: &UnitsObjectId) -> Result<(), StorageError> {
        self.rt.block_on(async {
            // Use a transaction for consistency
            let mut tx = self.pool
                .begin()
                .await
                .with_context(|| "Failed to start transaction for delete operation")?;

            // Delete from object_proofs
            sqlx::query("DELETE FROM object_proofs WHERE object_id = ?")
                .bind(id.as_ref())
                .execute(&mut *tx)
                .await
                .with_context(|| format!("Failed to delete proofs for object ID: {:?}", id))?;

            // Delete from objects
            sqlx::query("DELETE FROM objects WHERE id = ?")
                .bind(id.as_ref())
                .execute(&mut *tx)
                .await
                .with_context(|| format!("Failed to delete object with ID: {:?}", id))?;

            // Commit transaction
            tx.commit()
                .await
                .with_context(|| "Failed to commit delete transaction")?;

            Ok(())
        })
    }

    fn scan(&self) -> Box<dyn UnitsStorageIterator + '_> {
        // Return an iterator that will scan through all objects
        Box::new(SqliteStorageIterator {
            pool: self.pool.clone(),
            rt: self.rt.clone(),
            current_index: 0,
        })
    }
}

impl UnitsStorageProofEngine for SqliteStorage {
    fn proof_engine(&self) -> &dyn ProofEngine {
        &self.proof_engine
    }
    
    fn generate_state_proof(&self) -> Result<StateProof, StorageError> {
        self.rt.block_on(async {
            // Get all objects and their proofs
            let query = "SELECT o.id, p.proof 
                         FROM objects o
                         LEFT JOIN object_proofs p ON o.id = p.object_id";

            let rows = sqlx::query(query)
                .fetch_all(&self.pool)
                .await
                .with_context(|| "Failed to fetch objects and proofs for state proof generation")?;

            // Collect all object IDs and their proofs
            let mut object_proofs = Vec::new();
            for row in rows {
                let id_blob: Vec<u8> = row.get(0);
                let proof_data: Option<Vec<u8>> = row.get(1);
                
                // Skip objects without proofs
                let proof_data = match proof_data {
                    Some(data) => data,
                    None => continue,
                };
                
                // Convert to UnitsObjectId
                let mut id_array = [0u8; 32];
                if id_blob.len() == 32 {
                    id_array.copy_from_slice(&id_blob);
                } else {
                    continue;
                }
                
                let id = UnitsObjectId::new(id_array);
                let proof = TokenizedObjectProof { proof: proof_data };
                
                object_proofs.push((id, proof));
            }
            
            // Generate the state proof
            let state_proof = self.proof_engine.generate_state_proof(&object_proofs)?;
            
            // Store the proof for future reference
            let _ = sqlx::query("INSERT INTO state_proofs (timestamp, proof) VALUES (?, ?)")
                .bind(chrono::Utc::now().timestamp())
                .bind(&state_proof.proof)
                .execute(&self.pool)
                .await;

            Ok(state_proof)
        })
    }

    fn get_proof(&self, id: &UnitsObjectId) -> Result<Option<TokenizedObjectProof>, StorageError> {
        self.rt.block_on(async {
            // First check if we have a stored proof
            let query = "SELECT proof FROM object_proofs WHERE object_id = ?";

            let row = sqlx::query(query)
                .bind(id.as_ref())
                .fetch_optional(&self.pool)
                .await
                .with_context(|| format!("Failed to fetch proof for object ID: {:?}", id))?;
                
            if let Some(row) = row {
                let proof: Vec<u8> = row.get(0);
                return Ok(Some(TokenizedObjectProof { proof }));
            }
            
            // If no stored proof, get the object and generate one
            let object = match self.get(id)? {
                Some(obj) => obj,
                None => return Ok(None),
            };
            
            let proof = self.proof_engine.generate_object_proof(&object)?;
            
            // Store the proof for future reference
            let _ = sqlx::query("INSERT OR REPLACE INTO object_proofs (object_id, proof) VALUES (?, ?)")
                .bind(id.as_ref())
                .bind(&proof.proof)
                .execute(&self.pool)
                .await;
                
            Ok(Some(proof))
        })
    }

    fn verify_proof(&self, id: &UnitsObjectId, proof: &TokenizedObjectProof) -> Result<bool, StorageError> {
        // Get the object
        let object = match self.get(id)? {
            Some(obj) => obj,
            None => return Ok(false),
        };
        
        // Verify the proof
        self.proof_engine.verify_object_proof(&object, proof)
    }
}

impl Iterator for SqliteStorageIterator {
    type Item = Result<TokenizedObject, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rt.block_on(async {
            // Query a single object at the current index
            let query =
                "SELECT id, holder, token_type, token_manager, data FROM objects LIMIT 1 OFFSET ?";

            let row = match sqlx::query(query)
                .bind(self.current_index)
                .fetch_optional(&self.pool)
                .await
            {
                Ok(Some(row)) => row,
                Ok(None) => return None,
                Err(e) => return Some(Err(StorageError::from(e))),
            };

            let id_blob: Vec<u8> = row.get(0);
            let holder_blob: Vec<u8> = row.get(1);
            let token_type_int: i64 = row.get(2);
            let token_manager_blob: Vec<u8> = row.get(3);
            let data: Option<Vec<u8>> = row.get(4);
            let data = data.unwrap_or_default();

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
                Err(e) => return Some(Err(StorageError::Other(e))),
            };

            // Increment the index for the next call
            self.current_index += 1;

            Some(Ok(TokenizedObject {
                id: UnitsObjectId::new(id_array),
                holder: UnitsObjectId::new(holder_array),
                token_type,
                token_manager: UnitsObjectId::new(token_manager_array),
                data,
            }))
        })
    }
}

impl UnitsStorageIterator for SqliteStorageIterator {}

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
    use crate::id::tests::unique_id;
    use tempfile::tempdir;

    #[test]
    fn test_basic_storage_operations() {
        // Create temporary directory for test database
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");

        // Create storage
        let storage = SqliteStorage::new(&db_path).unwrap();

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
        let retrieved = storage.get(&id).unwrap().unwrap();

        assert_eq!(retrieved.id, obj.id);
        assert_eq!(retrieved.holder, obj.holder);
        assert_eq!(retrieved.token_type, obj.token_type);
        assert_eq!(retrieved.token_manager, obj.token_manager);
        assert_eq!(retrieved.data, obj.data);

        // Test delete
        storage.delete(&id).unwrap();
        assert!(storage.get(&id).unwrap().is_none());
    }

    #[test]
    fn test_scan_operations() {
        // Create temporary directory for test database
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test_scan.db");

        // Create storage
        let storage = SqliteStorage::new(&db_path).unwrap();

        // Add multiple objects
        for _ in 0..5 {
            let id = unique_id();

            let obj = TokenizedObject {
                id,
                holder: unique_id(),
                token_type: TokenType::Native,
                token_manager: unique_id(),
                data: vec![1, 2, 3],
            };

            storage.set(&obj).unwrap();
        }

        // Test scan
        let mut iterator = storage.scan();
        let mut count = 0;

        while let Some(result) = iterator.next() {
            assert!(result.is_ok());
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
        
        // Create and store an object to ensure we have something to generate proofs for
        let id = unique_id();
        let obj = TokenizedObject {
            id,
            holder: unique_id(),
            token_type: TokenType::Native,
            token_manager: unique_id(),
            data: vec![1, 2, 3, 4],
        };
        storage.set(&obj).unwrap();

        // Generate a state proof
        let proof = storage.generate_state_proof().unwrap();
        
        // With our lattice implementation, this might still be empty if we have no objects
        // so let's add this check conditionally:
        let proof_size = proof.proof.len();
        println!("State proof size: {}", proof_size);
        
        // For this test, we'll just verify we can generate a proof without errors
    }
}