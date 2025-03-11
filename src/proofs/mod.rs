pub mod engine;
pub mod lattice_hash;
pub mod lattice_proof_engine;
pub mod merkle_proof;

use crate::id::UnitsObjectId;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// Slot number representing a specific time interval
///
/// Slots are used to organize time into discrete intervals.
/// Each slot contains a set of state changes that occurred during that interval.
pub type SlotNumber = u64;

/// Get the current slot number based on system time
///
/// This uses a fixed slot length to determine the current slot.
/// In a production system, this would be based on a network-wide consensus mechanism.
pub fn current_slot() -> SlotNumber {
    // For testing, use a simple timestamp-based slot numbering
    // In production, this would be derived from the network's consensus mechanism
    const SLOT_LENGTH_MS: u64 = 1000; // 1 second per slot
    
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
        
    now / SLOT_LENGTH_MS
}

/// A cryptographic proof that commits to the state of a single TokenizedObject
///
/// This proof enables verification that an object exists and has not been tampered with.
/// Different proof engine implementations may use different cryptographic techniques
/// (merkle proofs, lattice-based proofs, etc.) to generate these proofs.
///
/// The proof now includes a reference to the previous state through its hash,
/// forming a chain of state transitions that can be verified.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenizedObjectProof {
    /// The binary representation of the cryptographic proof
    pub proof: Vec<u8>,
    
    /// Slot in which this proof was created
    pub slot: SlotNumber,
    
    /// Hash of the previous proof for this object, if any
    /// This forms a hash chain of state transitions
    pub prev_proof_hash: Option<[u8; 32]>,
    
    /// Hash of the transaction that created this proof
    /// This enables tracking which transaction led to a state change
    pub transaction_hash: Option<[u8; 32]>,
}

impl TokenizedObjectProof {
    /// Create a new object proof with the given proof data, previous proof, and transaction hash
    pub fn new(
        proof: Vec<u8>, 
        prev_proof: Option<&TokenizedObjectProof>,
        transaction_hash: Option<[u8; 32]>
    ) -> Self {
        // Calculate the hash of the previous proof if it exists
        let prev_proof_hash = prev_proof.map(|p| {
            // Use blake3 for consistency with our other hashing
            let mut hasher = blake3::Hasher::new();
            hasher.update(&p.proof);
            if let Some(hash) = &p.prev_proof_hash {
                hasher.update(hash);
            }
            hasher.update(&p.slot.to_le_bytes());
            if let Some(hash) = &p.transaction_hash {
                hasher.update(hash);
            }
            *hasher.finalize().as_bytes()
        });
        
        Self {
            proof,
            slot: current_slot(),
            prev_proof_hash,
            transaction_hash,
        }
    }
    
    /// Calculate the hash of this proof for chaining
    pub fn hash(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&self.proof);
        if let Some(hash) = &self.prev_proof_hash {
            hasher.update(hash);
        }
        hasher.update(&self.slot.to_le_bytes());
        if let Some(hash) = &self.transaction_hash {
            hasher.update(hash);
        }
        *hasher.finalize().as_bytes()
    }
}

/// A cryptographic proof that commits to the state of multiple objects
///
/// State proofs aggregate multiple TokenizedObjectProofs to provide a compact
/// representation of the entire system state at a point in time. These are used
/// to verify the collective state of all objects in the system.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateProof {
    /// The binary representation of the state proof
    pub proof: Vec<u8>,
    
    /// Slot in which this state proof was created
    pub slot: SlotNumber,
    
    /// Hash of the previous state proof, if any
    /// This forms a hash chain of slot state transitions
    pub prev_proof_hash: Option<[u8; 32]>,
    
    /// List of object IDs included in this state proof
    pub included_objects: Vec<UnitsObjectId>,
}

impl StateProof {
    /// Create a new state proof with the given proof data and previous state proof
    pub fn new(
        proof: Vec<u8>, 
        included_objects: Vec<UnitsObjectId>,
        prev_proof: Option<&StateProof>
    ) -> Self {
        // Calculate the hash of the previous proof if it exists
        let prev_proof_hash = prev_proof.map(|p| {
            let mut hasher = blake3::Hasher::new();
            hasher.update(&p.proof);
            if let Some(hash) = &p.prev_proof_hash {
                hasher.update(hash);
            }
            hasher.update(&p.slot.to_le_bytes());
            *hasher.finalize().as_bytes()
        });
        
        Self {
            proof,
            slot: current_slot(),
            prev_proof_hash,
            included_objects,
        }
    }
    
    /// Calculate the hash of this state proof for chaining
    pub fn hash(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&self.proof);
        if let Some(hash) = &self.prev_proof_hash {
            hasher.update(hash);
        }
        hasher.update(&self.slot.to_le_bytes());
        *hasher.finalize().as_bytes()
    }
}

// Re-export key types
pub use engine::ProofEngine;
pub use lattice_hash::{LatticeHash, LatticeHashParams};
pub use lattice_proof_engine::LatticeProofEngine;
pub use merkle_proof::{MerkleProofEngine, MerkleTree};