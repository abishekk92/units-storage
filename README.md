# UNITS (Universal Information Tokenization System)

A modular storage and runtime system for the Universal Information Tokenization System (UNITS).

## Overview

UNITS is a component of Finternet that provides a way to tokenize and manage objects. This workspace implements the full UNITS stack, organized into logical crates that work together.

## Workspace Structure

The project is organized as a Cargo workspace with the following crates:

- **units-core**: Core data structures and fundamental types
  - UnitsObjectId
  - TokenizedObject
  - Basic error types

- **units-proofs**: Cryptographic proof systems
  - Merkle Proofs
  - Lattice Proofs
  - Proof Engines
  - State Proofs

- **units-storage-impl**: Storage backends
  - Storage Traits
  - SQLite Implementation
  - RocksDB Implementation
  - Write-Ahead Log

- **units-runtime**: Runtime and verification
  - Object Runtime
  - Proof Verification
  - Transaction Processing
  - Transaction Commitment Levels

- **units**: Convenience wrapper crate that re-exports all components

## Features

- **Storage Trait**: A unified interface for different storage backends
- **TokenizedObject**: Core data structure with cryptographic features
- **Verifiable History**: Cryptographic proof chains that link object states over time
- **Write-Ahead Log**: Durable logging of all state changes for reliability
- **Slot-Based Versioning**: Historical tracking of objects and their proofs
- **Transaction Commitment Levels**: Support for processing, committed, and failed transaction states
- **Multiple Backends**:
  - SQLite implementation (default)
  - RocksDB implementation (optional)

## Architecture

For detailed architecture documentation, see [ARCHITECTURE.md](ARCHITECTURE.md).

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
units = "0.1.0"  # For the complete package

# Or use specific components:
units-core = "0.1.0"
units-proofs = "0.1.0"
units-storage-impl = { version = "0.1.0", features = ["sqlite"] }
units-runtime = "0.1.0"
```

By default, the SQLite storage backend is enabled. If you want to use RocksDB:

```toml
[dependencies]
units-storage-impl = { version = "0.1.0", features = ["rocksdb"] }
```

To use both backends:

```toml
[dependencies]
units-storage-impl = { version = "0.1.0", features = ["all"] }
```

## Examples

### Basic Usage

```rust
use units::{UnitsObjectId, TokenizedObject, TokenType};
use units::SqliteStorage;  // With sqlite feature enabled
use units::UnitsStorage;
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

### Transaction Processing with Commitment Levels

```rust
use units::{Transaction, Instruction, CommitmentLevel, AccessIntent};
use units::runtime::Runtime;

// Create instructions for a transaction
let instruction = Instruction {
    data: vec![/* instruction data */],
    object_intents: vec![(object_id, AccessIntent::Write)]
};

// Create a transaction (starts with Processing commitment level)
let mut transaction = Transaction::new(vec![instruction], transaction_hash);

// Execute the transaction
let result = runtime.execute_transaction(&transaction).unwrap();

if result.success {
    // Mark the transaction as committed when ready
    transaction.commit();

    // Or if there's an issue, mark it as failed
    // transaction.fail();

    // Check if a transaction can be rolled back
    if transaction.can_rollback() {
        // Roll back operations if needed
    }
}

// Transaction receipts also capture commitment levels
let receipt = runtime.get_transaction_receipt(&transaction.hash).unwrap();
println!("Transaction commitment level: {:?}", receipt.commitment_level);
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
