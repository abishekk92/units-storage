pub mod engine;
pub mod lattice_hash;
pub mod lattice_proof_engine;
pub mod merkle_proof;

// Re-export the main types for convenience
pub use engine::{ProofEngine, SlotNumber, StateProof, UnitsObjectProof, VerificationResult};

// Helper functions
pub mod proofs {
    use crate::engine::SlotNumber;
    use std::time::{SystemTime, UNIX_EPOCH};

    /// Get the current slot number based on system time
    /// In a production system, this would use a synchronized clock
    pub fn current_slot() -> SlotNumber {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        now
    }
}
