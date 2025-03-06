pub mod engine;
pub mod lattice_hash;
pub mod lattice_proof_engine;

#[derive(Debug, Clone)]
pub struct TokenizedObjectProof {
    pub proof: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct StateProof {
    pub proof: Vec<u8>,
}

// Re-export key types
pub use engine::ProofEngine;
pub use lattice_hash::{LatticeHash, LatticeHashParams};
pub use lattice_proof_engine::LatticeProofEngine;