pub mod engine;
pub mod lattice_hash;
pub mod lattice_proof_engine;
pub mod merkle_proof;

use serde::{Deserialize, Serialize};

/// A cryptographic proof that commits to the state of a single TokenizedObject
///
/// This proof enables verification that an object exists and has not been tampered with.
/// Different proof engine implementations may use different cryptographic techniques
/// (merkle proofs, lattice-based proofs, etc.) to generate these proofs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenizedObjectProof {
    /// The binary representation of the cryptographic proof
    pub proof: Vec<u8>,
}

/// A cryptographic proof that commits to the state of multiple objects
///
/// State proofs aggregate multiple TokenizedObjectProofs to provide a compact
/// representation of the entire system state at a point in time. These are used
/// to verify the collective state of all objects in the system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateProof {
    /// The binary representation of the state proof
    pub proof: Vec<u8>,
}

// Re-export key types
pub use engine::ProofEngine;
pub use lattice_hash::{LatticeHash, LatticeHashParams};
pub use lattice_proof_engine::LatticeProofEngine;
pub use merkle_proof::{MerkleProofEngine, MerkleTree};