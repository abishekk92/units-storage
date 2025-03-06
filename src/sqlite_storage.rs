use crate::{
    id::UnitsObjectId,
    objects::{TokenType, TokenizedObject},
    proofs::{StateProof, TokenizedObjectProof},
    storage::{UnitsStorage, UnitsStorageIterator, UnitsStorageProofEngine},
};
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
    fn get(&self, id: &UnitsObjectId) -> Option<TokenizedObject> {
        self.rt.block_on(async {
            let query = "SELECT id, holder, token_type, token_manager, data FROM objects WHERE id = ?";
            
            let row = sqlx::query(query)
                .bind(id.as_ref())
                .fetch_optional(&self.pool)
                .await
                .ok()?;
            
            let row = match row {
                Some(row) => row,
                None => return None,
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
                Err(_) => TokenType::Native, // Default to Native if invalid
            };

            Some(TokenizedObject {
                id: UnitsObjectId::new(id_array),
                holder: UnitsObjectId::new(holder_array),
                token_type,
                token_manager: UnitsObjectId::new(token_manager_array),
                data,
            })
        })
    }

    fn set(&self, object: &TokenizedObject) -> Result<(), String> {
        self.rt.block_on(async {
            let token_type_int = Self::token_type_to_int(&object.token_type);

            let query = "INSERT OR REPLACE INTO objects (id, holder, token_type, token_manager, data) VALUES (?, ?, ?, ?, ?)";

            sqlx::query(query)
                .bind(object.id.as_ref())
                .bind(object.holder.as_ref())
                .bind(token_type_int)
                .bind(object.token_manager.as_ref())
                .bind(&object.data)
                .execute(&self.pool)
                .await
                .map_err(|e| format!("Failed to store object: {}", e))?;

            Ok(())
        })
    }

    fn delete(&self, id: &UnitsObjectId) -> Result<(), String> {
        self.rt.block_on(async {
            // Use a transaction for consistency
            let mut tx = self.pool
                .begin()
                .await
                .map_err(|e| format!("Failed to start transaction: {}", e))?;

            // Delete from object_proofs
            sqlx::query("DELETE FROM object_proofs WHERE object_id = ?")
                .bind(id.as_ref())
                .execute(&mut *tx)
                .await
                .map_err(|e| format!("Failed to delete object proofs: {}", e))?;

            // Delete from objects
            sqlx::query("DELETE FROM objects WHERE id = ?")
                .bind(id.as_ref())
                .execute(&mut *tx)
                .await
                .map_err(|e| format!("Failed to delete object: {}", e))?;

            // Commit transaction
            tx.commit()
                .await
                .map_err(|e| format!("Failed to commit transaction: {}", e))?;

            Ok(())
        })
    }

    fn scan(&self) -> Box<dyn UnitsStorageIterator> {
        // Return an iterator that will scan through all objects
        Box::new(SqliteStorageIterator {
            pool: self.pool.clone(),
            rt: self.rt.clone(),
            current_index: 0,
        })
    }
}

impl UnitsStorageProofEngine for SqliteStorage {
    fn generate_state_proof(&self) -> StateProof {
        self.rt.block_on(async {
            // In a real implementation, this would:
            // 1. Generate a cryptographic proof based on all objects in storage
            // 2. Store the proof in the state_proofs table with a timestamp
            // 3. Return the generated proof

            // For this example, we just return a dummy proof
            let dummy_proof = vec![0, 1, 2, 3, 4];

            // Store the proof for future reference
            let _ = sqlx::query("INSERT INTO state_proofs (timestamp, proof) VALUES (?, ?)")
                .bind(chrono::Utc::now().timestamp())
                .bind(&dummy_proof)
                .execute(&self.pool)
                .await;

            StateProof { proof: dummy_proof }
        })
    }

    fn get_proof(&self, id: &UnitsObjectId) -> Option<TokenizedObjectProof> {
        self.rt.block_on(async {
            let query = "SELECT proof FROM object_proofs WHERE object_id = ?";

            let row = sqlx::query(query)
                .bind(id.as_ref())
                .fetch_optional(&self.pool)
                .await
                .ok()?;

            let row = match row {
                Some(row) => row,
                None => return None,
            };

            let proof: Vec<u8> = row.get(0);
            Some(TokenizedObjectProof { proof })
        })
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
        self.rt.block_on(async {
            // Query a single object at the current index
            let query =
                "SELECT id, holder, token_type, token_manager, data FROM objects LIMIT 1 OFFSET ?";

            let row = sqlx::query(query)
                .bind(self.current_index)
                .fetch_optional(&self.pool)
                .await
                .ok()?;

            let row = match row {
                Some(row) => row,
                None => return None,
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
                Err(_) => TokenType::Native, // Default to Native if invalid
            };

            // Increment the index for the next call
            self.current_index += 1;

            Some(TokenizedObject {
                id: UnitsObjectId::new(id_array),
                holder: UnitsObjectId::new(holder_array),
                token_type,
                token_manager: UnitsObjectId::new(token_manager_array),
                data,
            })
        })
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