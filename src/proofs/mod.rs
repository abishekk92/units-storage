pub mod engine;
pub mod lattice_hash;
pub mod lattice_proof_engine;
pub mod merkle_proof;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenizedObjectProof {
    pub proof: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateProof {
    pub proof: Vec<u8>,
}

// Re-export key types
pub use engine::ProofEngine;
pub use lattice_hash::{LatticeHash, LatticeHashParams};
pub use lattice_proof_engine::LatticeProofEngine;
pub use merkle_proof::{MerkleProofEngine, MerkleTree};