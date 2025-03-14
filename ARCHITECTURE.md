# UNITS Architecture

This document provides a detailed overview of the UNITS (Universal Information Tokenization System) architecture, design principles, and component interactions.

## System Overview

UNITS is a modular storage and runtime system that provides a way to tokenize and manage objects within the Finternet ecosystem. The system implements a complete stack for object tokenization with cryptographic verification, transactional processing, and historical state management.

### Core Design Principles

1. **Object-Centric**: The system revolves around TokenizedObjects as the fundamental unit of state.
2. **Cryptographic Verification**: All state transitions are cryptographically verified.
3. **Temporal Organization**: Time is divided into slots for organizing state transitions.
4. **Historical Traceability**: Complete history of all objects is maintained and verifiable.
5. **Modular Components**: The system is built from loosely coupled, replaceable components.

## Core Components

### 1. Tokenized Objects

The foundation of UNITS is the TokenizedObject, which represents a tokenized piece of information:

- **UnitsObjectId**: A 32-byte public key that uniquely identifies the object
- **Holder**: The entity that controls the object (can be a user or another object)
- **Token Type**: Can be Native, Custodial, or Proxy
- **Token Manager**: The entity responsible for managing the token
- **Data**: Arbitrary payload data associated with the object

Key characteristics:
- Objects can only be modified by their holder or by a controller program in the case of IDs that are not on the curve
- State changes generate cryptographic proofs that commit to both previous and new states

### 2. Storage System

The storage layer is responsible for persisting objects and their historical states:

#### Components:

- **Storage Trait**: Unified interface implemented by all storage backends
- **Storage Backends**:
  - **SQLite Implementation**: Default, suitable for most use cases
  - **RocksDB Implementation**: Optional, optimized for high-performance scenarios
- **Write-Ahead Log (WAL)**: Records all state changes before they're committed
  - Provides durability and crash recovery
  - Enables audit capability for all state transitions
- **Historical State Manager**: Maintains versioned history of all objects
  - Objects are versioned by slot number
  - Enables verification of past states and transitions

#### Key Operations:

- **Object Storage**: CRUD operations on TokenizedObjects
- **Proof Management**: Storage and retrieval of object and state proofs
- **Historical Access**: Retrieval of objects and proofs at specific slots
- **Scanning**: Iteration through objects and proofs

### 3. Proof System

The proof system provides cryptographic guarantees about object states and their history:

#### Components:

- **Object Proofs**: Cryptographic commitments to object states
  - Commit to the current state of the object
  - Link to previous state via `prev_proof_hash`
  - Include slot number for temporal organization
- **State Proofs**: Aggregate multiple object proofs
  - Commit to system state at a specific slot
  - Track which objects are included
  - Link to previous state proofs, creating verification chains
- **Proof Engines**: Implementations of different proof systems
  - **Merkle Proof Engine**: Tree-based proofs for efficient verification
  - **Lattice Proof Engine**: Alternative proof system with different properties

#### Key Operations:

- **Proof Generation**: Creating proofs for object state changes
- **Proof Verification**: Verifying the validity of individual proofs
- **Chain Verification**: Ensuring the integrity of proof chains between slots
- **State Proof Aggregation**: Combining object proofs into state proofs

### 4. Runtime and Execution

The runtime system handles the execution of transactions and instructions:

#### Components:

- **Runtime**: Core execution environment
  - Manages transaction processing
  - Handles commitment levels
  - Dispatches instructions to appropriate handlers
- **Runtime Backends**:
  - **WebAssembly**: Using Wasmtime for executing WebAssembly modules
  - **Host Environment**: Provides system functions to executed code
- **Transaction Processor**: Manages transaction lifecycle
  - Processing → Committed/Failed states
  - Rollback capability for failed transactions

#### Key Operations:

- **Transaction Execution**: Processing transactions with multiple instructions
- **Commitment Management**: Tracking and updating transaction commitment levels
- **Receipt Generation**: Creating receipts for executed transactions
- **Runtime Verification**: Ensuring the validity of executed code

### 5. Concurrency Control

The system manages concurrent access to objects through lock management:

#### Components:

- **Lock Manager**: Manages locks on objects
  - Prevents conflicting access to objects
  - Supports various lock types (read, write, exclusive)
- **Persistent Lock Manager**: Ensures locks survive process restarts
- **Conflict Detection**: Identifies and prevents conflicting transactions

#### Key Operations:

- **Lock Acquisition**: Requesting and obtaining locks on objects
- **Lock Release**: Releasing locks when operations complete
- **Deadlock Detection**: Identifying and resolving potential deadlocks
- **Transaction Scheduling**: Ordering transactions to minimize conflicts

## Data Flow and System Interactions

### Object Lifecycle

1. **Object Creation**:
   - Client creates a TokenizedObject
   - Object is submitted to storage
   - Storage generates initial proof
   - Object is persisted in the current state

2. **Object Modification**:
   - Client submits modification transaction
   - System verifies holder has permission
   - Lock is acquired on the object
   - Modification is applied
   - New proof is generated linking to previous proof
   - Updated object and proof are persisted

3. **Object Deletion**:
   - Client submits deletion transaction
   - System verifies holder has permission
   - Lock is acquired on the object
   - Deletion proof is generated
   - Object is marked as deleted

### Transaction Processing

1. **Transaction Submission**:
   - Client submits transaction with instructions
   - System validates transaction format
   - Transaction enters 'Processing' commitment level

2. **Instruction Execution**:
   - System acquires locks on involved objects
   - Instructions are executed
   - Objects are modified accordingly
   - Proofs are generated for all changes

3. **Transaction Commitment**:
   - If successful, transaction moves to 'Committed' level
   - If failed, transaction moves to 'Failed' level
   - Locks are released
   - Transaction receipt is generated
   - State is persisted
   - WAL records are updated

### Proof Generation and Verification

1. **Object Proof Generation**:
   - Each object change triggers proof generation
   - Proof commits to new state and links to previous proof
   - Proof includes slot number for temporal tracking

2. **State Proof Generation**:
   - At slot boundaries, state proofs are generated
   - State proof aggregates all object proofs from the slot
   - State proof links to previous state proof

3. **Verification Process**:
   - Verify individual object proofs
   - Verify proof chains for specific objects
   - Verify state proofs for overall system integrity

## Architectural Features

### Slot-Based Time

The system divides time into discrete slots:
- Each slot represents a fixed time period (configurable)
- Object changes and proofs are organized by slots
- State proofs are generated at slot boundaries
- Historical access is provided by slot number
- This enables:
  - Temporal organization of state changes
  - Consistent points for state proof generation
  - Well-defined reference points for historical access

### Transaction Commitment Levels

Transactions progress through commitment levels:
- **Processing**: Initial state, changes not finalized
- **Committed**: Changes are final and persisted
- **Failed**: Transaction failed, changes are rolled back

This enables:
- Atomic transactions across multiple objects
- Proper handling of failures
- Appropriate client feedback
- Audit trail of transaction outcomes

### Proof Chains and Verification

The system maintains cryptographic chains of proofs:
- Each proof links to its predecessor
- State proofs link object proofs together
- Complete verification is possible from any point

This enables:
- Cryptographic verification of historical states
- Detection of tampering with object history
- Proving that current state evolved correctly from genesis

## Cross-Cutting Concerns

### Error Handling

- All operations return Results with specific error types
- Errors propagate upward with context
- Failures are properly tracked and reported
- Transaction rollback handles partial failures

### Performance Considerations

- Storage backends can be selected based on performance needs
- Lock manager minimizes contention
- Proof generation is optimized for common operations
- Verification can be performed incrementally

### Security

- All object mutations require holder verification
- Cryptographic proofs prevent tampering
- Controller programs have limited capabilities
- Historical state is immutable

## Future Directions

- Cross-object transaction support
- Distributed consensus integration
- Sharding for horizontal scaling
- Enhanced privacy features
- More efficient proof systems

## Conclusion

The UNITS architecture provides a comprehensive system for tokenizing and managing objects with strong guarantees about state transitions and history. The modular design allows for extension and adaptation to various use cases while maintaining the core principles of cryptographic verification and historical traceability.