use serde::{Deserialize, Serialize};
use units_core::error::StorageError;
use units_core::id::UnitsObjectId;
use units_core::objects::UnitsObject;

/// Slot number type (represents points in time)
pub type SlotNumber = u64;

/// A cryptographic proof for a UNITS object
///
/// This proof commits to the state of a UnitsObject at a particular slot,
/// and optionally links to a previous proof to form a chain of state changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnitsObjectProof {
    /// The UnitsObjectId this proof is for
    pub object_id: UnitsObjectId,

    /// The slot number when this proof was created
    pub slot: SlotNumber,

    /// Hash of the object state this proof commits to
    pub object_hash: [u8; 32],

    /// Optional hash of the previous proof for this object
    /// If None, this is the first proof for the object
    pub prev_proof_hash: Option<[u8; 32]>,

    /// Optional hash of the transaction that led to this state change
    pub transaction_hash: Option<[u8; 32]>,

    /// Cryptographic data that authenticates this proof
    /// The format depends on the specific proof implementation
    pub proof_data: Vec<u8>,
}

impl UnitsObjectProof {
    /// Creates a new UnitsObjectProof with the given data
    pub fn new(
        proof_data: Vec<u8>,
        prev_proof: Option<&UnitsObjectProof>,
        transaction_hash: Option<[u8; 32]>,
    ) -> Self {
        let prev_proof_hash = prev_proof.map(|p| p.hash());
        let slot = SlotNumber::default(); // Will be replaced by proper slot logic
        let object_id = UnitsObjectId::default(); // This should be set by the caller
        let object_hash = [0u8; 32]; // This should be set by the caller

        Self {
            object_id,
            slot,
            object_hash,
            prev_proof_hash,
            transaction_hash,
            proof_data,
        }
    }

    /// Computes the hash of this proof
    /// Used to link proofs in a chain
    pub fn hash(&self) -> [u8; 32] {
        use sha2::{Digest, Sha256};

        let mut hasher = Sha256::new();
        hasher.update(self.object_id.bytes());
        hasher.update(self.slot.to_le_bytes());
        hasher.update(self.object_hash);

        if let Some(prev_hash) = self.prev_proof_hash {
            hasher.update(prev_hash);
        }

        if let Some(tx_hash) = self.transaction_hash {
            hasher.update(tx_hash);
        }

        hasher.update(&self.proof_data);

        let result = hasher.finalize();
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&result);
        hash
    }
}

/// A state proof represents the aggregated state of multiple objects at a specific slot
///
/// State proofs commit to the collective state of the system at a point in time,
/// and form a chain that can be used to verify the evolution of the system state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateProof {
    /// The slot number this state proof is for
    pub slot: SlotNumber,

    /// The hash of the previous state proof, if any
    pub prev_state_proof_hash: Option<[u8; 32]>,

    /// List of object IDs included in this state proof
    pub object_ids: Vec<UnitsObjectId>,

    /// Cryptographic data that authenticates this proof
    /// The format depends on the specific proof implementation
    pub proof_data: Vec<u8>,
}

impl StateProof {
    /// Creates a new StateProof with the given data
    pub fn new(
        proof_data: Vec<u8>,
        object_ids: Vec<UnitsObjectId>,
        prev_state_proof: Option<&StateProof>,
    ) -> Self {
        let slot = SlotNumber::default(); // Will be replaced by proper slot logic
        let prev_state_proof_hash = prev_state_proof.map(|p| p.hash());

        Self {
            slot,
            prev_state_proof_hash,
            object_ids,
            proof_data,
        }
    }

    /// Computes the hash of this state proof
    /// Used to link proofs in a chain
    pub fn hash(&self) -> [u8; 32] {
        use sha2::{Digest, Sha256};

        let mut hasher = Sha256::new();
        hasher.update(self.slot.to_le_bytes());

        if let Some(prev_hash) = self.prev_state_proof_hash {
            hasher.update(prev_hash);
        }

        // Hash all object IDs in a deterministic order
        let mut object_ids = self.object_ids.clone();
        object_ids.sort(); // Ensure deterministic ordering
        for id in object_ids {
            hasher.update(id.bytes());
        }

        hasher.update(&self.proof_data);

        let result = hasher.finalize();
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&result);
        hash
    }
}

/// Represents the result of verifying a proof chain
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerificationResult {
    /// The proof chain is valid
    Valid,

    /// The proof chain is invalid for the specified reason
    Invalid(String),

    /// Missing data needed to complete verification
    MissingData(String),
}

/// Interface for different proof engine implementations
///
/// A ProofEngine provides cryptographic mechanisms to generate and verify proofs
/// for tokenized objects in the UNITS system. These proofs ensure:
///
/// 1. Data integrity - objects have not been tampered with
/// 2. State consistency - the full state of all objects is valid
/// 3. Verifiable history - changes to objects can be verified
///
/// # Note on Proof Chain Verification
/// This trait provides two methods for proof chain verification:
/// - `verify_proof_chain`: Verifies a single link in a chain between two adjacent proofs
/// - `verify_proof_history`: Verifies an entire history of proofs for optimal performance
pub trait ProofEngine {
    /// Generate a cryptographic proof for a UNITS object
    ///
    /// This creates a proof that commits to the current state of the object.
    /// The proof can later be verified to ensure the object has not been modified.
    ///
    /// # Parameters
    /// * `object` - The UnitsObject to generate a proof for
    /// * `prev_proof` - The previous proof for this object, if any
    /// * `transaction_hash` - Hash of the transaction that led to this state change, if any
    ///
    /// # Returns
    /// A cryptographic proof that commits to the object's current state
    /// and links to its previous state through the prev_proof_hash
    fn generate_object_proof(
        &self,
        object: &UnitsObject,
        prev_proof: Option<&UnitsObjectProof>,
        transaction_hash: Option<[u8; 32]>,
    ) -> Result<UnitsObjectProof, StorageError>;

    /// Verify that a proof correctly commits to an object's state
    ///
    /// # Parameters
    /// * `object` - The UnitsObject to verify the proof against
    /// * `proof` - The proof to verify
    ///
    /// # Returns
    /// `true` if the proof is valid for the given object, `false` otherwise
    fn verify_object_proof(
        &self,
        object: &UnitsObject,
        proof: &UnitsObjectProof,
    ) -> Result<bool, StorageError>;

    /// Verify two adjacent proofs in a chain
    ///
    /// This verifies that two proofs form a valid chain link,
    /// with the current proof correctly referencing its predecessor.
    /// For verifying an entire chain of proofs, use `verify_proof_history` instead.
    ///
    /// # Parameters
    /// * `object` - The current UnitsObject
    /// * `proof` - The current proof for the object
    /// * `prev_proof` - The previous proof in the chain
    ///
    /// # Returns
    /// `true` if the link between proofs is valid, `false` otherwise
    fn verify_proof_chain(
        &self,
        object: &UnitsObject,
        proof: &UnitsObjectProof,
        prev_proof: &UnitsObjectProof,
    ) -> Result<bool, StorageError>;

    /// Generate a state proof from a collection of object proofs
    ///
    /// This creates an aggregated proof that commits to the state of multiple objects.
    /// State proofs are used to verify the collective state of the system.
    ///
    /// # Parameters
    /// * `object_proofs` - A list of object IDs and their associated proofs
    /// * `prev_state_proof` - The previous state proof, if any
    /// * `slot` - The slot number for which to generate the proof
    ///
    /// # Returns
    /// A cryptographic proof that commits to the state of all provided objects
    fn generate_state_proof(
        &self,
        object_proofs: &[(UnitsObjectId, UnitsObjectProof)],
        prev_state_proof: Option<&StateProof>,
        slot: SlotNumber,
    ) -> Result<StateProof, StorageError>;

    /// Verify that a state proof correctly commits to a collection of object proofs
    ///
    /// # Parameters
    /// * `state_proof` - The state proof to verify
    /// * `object_proofs` - The list of object IDs and their proofs that should be committed to
    ///
    /// # Returns
    /// `true` if the state proof is valid for all the given objects, `false` otherwise
    fn verify_state_proof(
        &self,
        state_proof: &StateProof,
        object_proofs: &[(UnitsObjectId, UnitsObjectProof)],
    ) -> Result<bool, StorageError>;

    /// Verify a chain of state proofs
    ///
    /// This verifies that two consecutive state proofs form a valid chain,
    /// with the newer proof correctly linking to the previous one.
    ///
    /// # Parameters
    /// * `state_proof` - The current state proof
    /// * `prev_state_proof` - The previous state proof in the chain
    ///
    /// # Returns
    /// `true` if the chain is valid, `false` otherwise
    fn verify_state_proof_chain(
        &self,
        state_proof: &StateProof,
        prev_state_proof: &StateProof,
    ) -> Result<bool, StorageError>;

    /// Verify an entire history of proofs for an object
    ///
    /// This verifies a sequence of object states and their corresponding proofs,
    /// ensuring that each proof is valid and they form a valid chain. This is more
    /// efficient than calling `verify_proof_chain` on each adjacent pair of proofs
    /// because it can verify the entire chain at once.
    ///
    /// # Parameters
    /// * `object_states` - Vector of (slot, object state) pairs in ascending slot order
    /// * `proofs` - Vector of (slot, proof) pairs in ascending slot order
    ///
    /// # Performance
    /// This method is optimized for sequentially increasing slot numbers, using vectors
    /// instead of hash maps for better performance with large proof chains.
    ///
    /// # Returns
    /// A VerificationResult indicating whether the proof chain is valid
    fn verify_proof_history(
        &self,
        object_states: &[(SlotNumber, UnitsObject)],
        proofs: &[(SlotNumber, UnitsObjectProof)],
    ) -> VerificationResult {
        // Default implementation delegates to the LatticeProofEngine implementation
        if object_states.is_empty() || proofs.is_empty() {
            return VerificationResult::MissingData(
                "No object states or proofs provided".to_string(),
            );
        }

        // Verify each state has a corresponding proof
        for (slot, obj) in object_states {
            // Find matching proof for this slot
            let matching_proof = proofs.iter().find(|(proof_slot, _)| proof_slot == slot);

            if let Some((_, proof)) = matching_proof {
                match self.verify_object_proof(obj, proof) {
                    Ok(true) => {}
                    Ok(false) => {
                        return VerificationResult::Invalid(format!(
                            "Proof verification failed for slot {}",
                            slot
                        ))
                    }
                    Err(e) => {
                        return VerificationResult::Invalid(format!(
                            "Proof verification error at slot {}: {}",
                            slot, e
                        ))
                    }
                }
            } else {
                return VerificationResult::MissingData(format!("Missing proof for slot {}", slot));
            }
        }

        // Verify proof chain links - since proofs are ordered by slot, we can just iterate sequentially
        for i in 1..proofs.len() {
            let (current_slot, current_proof) = &proofs[i];
            let (prev_slot, prev_proof) = &proofs[i - 1];

            // Verify the current proof references the previous proof correctly
            if let Some(prev_hash) = &current_proof.prev_proof_hash {
                let computed_hash = prev_proof.hash();
                if computed_hash != *prev_hash {
                    return VerificationResult::Invalid(format!(
                        "Proof chain broken between slots {} and {}",
                        prev_slot, current_slot
                    ));
                }
            } else {
                return VerificationResult::Invalid(format!(
                    "Proof at slot {} does not reference previous proof",
                    current_slot
                ));
            }
        }

        VerificationResult::Valid
    }
}
