# UNITS Storage

A versatile storage system for the Universal Information Tokenization System (UNITS).

## Overview

UNITS is a component of Finternet that provides a way to tokenize and manage objects. This crate implements storage backends for UNITS objects, which can be persisted and retrieved using various storage engines.

## Features

- **Storage Trait**: A unified interface for different storage backends
- **TokenizedObject**: Core data structure with cryptographic features
- **Verifiable History**: Cryptographic proof chains that link object states over time
- **Write-Ahead Log**: Durable logging of all state changes for reliability
- **Slot-Based Versioning**: Historical tracking of objects and their proofs
- **Multiple Backends**: 
  - SQLite implementation (default)
  - RocksDB implementation (optional)

## Architecture

### Proof System

The proof system is designed to provide cryptographic guarantees about object states and their history:

1. **Object Proofs**: Each TokenizedObject has a proof that:
   - Commits to the current state of the object
   - Links to the previous state through the `prev_proof_hash`
   - Is tracked with a `slot` number to organize time

2. **State Proofs**: Aggregate multiple object proofs to commit to system state:
   - Track which objects are included in each state proof
   - Link to previous state proofs, creating a chain of state transitions
   - Provide a way to verify the collective state at a point in time

3. **Proof Chains**: Verify historical transitions of objects:
   - Any proof can be traced back to its ancestors
   - Each transition is cryptographically verified
   - Invalid transitions break the chain, ensuring data integrity

### Storage Layers

The storage system is organized into distinct layers:

1. **Write-Ahead Log (WAL)**: 
   - Durable log of all state changes before they're committed
   - Provides crash recovery and audit capabilities
   - Records both object updates and state proofs

2. **Key-Value Store**:
   - Current state of all objects and their proofs
   - Optimized for fast reads and updates
   - Multiple backend implementations (SQLite, RocksDB)

3. **Historical State**:
   - Versioned history of all objects and proofs
   - Organized by slot number for time-based access
   - Enables verification of past states and transitions

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
let id = UnitsObjectId::random();
let holder = UnitsObjectId::random();
let token_manager = UnitsObjectId::random();
let obj = TokenizedObject {
    id,
    holder,
    token_type: TokenType::Native,
    token_manager,
    data: vec![1, 2, 3, 4],
};

// Store the object and get its proof
let proof = storage.set(&obj).unwrap();
println!("Object proof: {:?}", proof);

// Retrieve the object
if let Some(retrieved) = storage.get(&id).unwrap() {
    println!("Retrieved object: {:?}", retrieved);
}

// Delete the object and get the deletion proof
let deletion_proof = storage.delete(&id).unwrap();
println!("Deletion proof: {:?}", deletion_proof);
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
let state_proof = storage.generate_and_store_state_proof().unwrap();
println!("State proof: {:?}", state_proof);

// Get the current proof for a specific object
if let Some(obj_proof) = storage.get_proof(&id).unwrap() {
    // Verify the proof
    if storage.verify_proof(&id, &obj_proof).unwrap() {
        println!("Proof verified!");
    }
}

// Get an object's state at a specific historical slot
let historical_slot = 12345;
if let Some(historical_obj) = storage.get_at_slot(&id, historical_slot).unwrap() {
    println!("Object at slot {}: {:?}", historical_slot, historical_obj);
}

// Get an object's proof at a specific historical slot
if let Some(historical_proof) = storage.get_proof_at_slot(&id, historical_slot).unwrap() {
    println!("Proof at slot {}: {:?}", historical_slot, historical_proof);
}

// Verify a chain of proofs between two slots
if storage.verify_proof_chain(&id, 12340, 12350).unwrap() {
    println!("Proof chain is valid!");
}

// Iterate through an object's proof history
let mut history = storage.get_proof_history(&id);
while let Some(Ok((slot, proof))) = history.next() {
    println!("Found proof at slot {}: {:?}", slot, proof);
}

// Get all state proofs
let mut state_proofs = storage.get_state_proofs();
while let Some(Ok(proof)) = state_proofs.next() {
    println!("State proof at slot {}: {:?}", proof.slot, proof);
}
```

## License

MIT License. See [LICENSE](LICENSE) for details.