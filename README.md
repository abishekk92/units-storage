# UNITS Storage

A versatile storage system for the Universal Information Tokenization System (UNITS).

## Overview

UNITS is a component of Finternet that provides a way to tokenize and manage objects. This crate implements storage backends for UNITS objects, which can be persisted and retrieved using various storage engines.

## Features

- **Storage Trait**: A unified interface for different storage backends
- **TokenizedObject**: Core data structure with cryptographic features
- **Proofs**: State and object proofs for verification
- **Multiple Backends**: 
  - SQLite implementation (default)
  - RocksDB implementation (optional)

## Usage

Add the following to your `Cargo.toml`:

```toml
[dependencies]
units-storage = "0.1.0"
```

By default, the SQLite storage backend is enabled. If you want to use RocksDB:

```toml
[dependencies]
units-storage = { version = "0.1.0", features = ["rocksdb"] }
```

To use both backends:

```toml
[dependencies]
units-storage = { version = "0.1.0", features = ["all"] }
```

## Examples

### Basic Usage

```rust
use units_storage::{UnitsObjectId, TokenizedObject, TokenType, SqliteStorage, UnitsStorage};
use std::path::Path;

// Create a storage instance
let storage = SqliteStorage::new(Path::new("./my_database.db")).unwrap();

// Create an object
let id = UnitsObjectId::default();
let obj = TokenizedObject {
    id,
    holder: UnitsObjectId::default(),
    token_type: TokenType::Native,
    token_manager: UnitsObjectId::default(),
    data: vec![1, 2, 3, 4],
};

// Store the object
storage.set(&obj).unwrap();

// Retrieve the object
if let Some(retrieved) = storage.get(&id) {
    println!("Retrieved object: {:?}", retrieved);
}

// Delete the object
storage.delete(&id).unwrap();
```

### Scanning Objects

```rust
// Iterate over all objects
let mut iterator = storage.scan();
while let Some(obj) = iterator.next() {
    println!("Found object: {:?}", obj);
}
```

### Proofs

```rust
// Generate a state proof
let proof = storage.generate_state_proof();
println!("State proof: {:?}", proof);

// Get a proof for a specific object
if let Some(obj_proof) = storage.get_proof(&id) {
    // Verify the proof
    if storage.verify_proof(&id, &obj_proof) {
        println!("Proof verified!");
    }
}
```

## License

MIT License. See [LICENSE](LICENSE) for details.