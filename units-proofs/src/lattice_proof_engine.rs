use crate::engine::{
    ProofEngine, SlotNumber, StateProof, TokenizedObjectProof, VerificationResult,
};
use crate::lattice_hash::{LatticeHash, LatticeHashParams};
use units_core::error::StorageError;
use units_core::id::UnitsObjectId;
use units_core::objects::TokenizedObject;

/// A proof engine implementation based on lattice homomorphic hashing
pub struct LatticeProofEngine {
    hasher: LatticeHash,
}

impl LatticeProofEngine {
    /// Create a new lattice proof engine with default parameters
    pub fn new() -> Self {
        Self {
            hasher: LatticeHash::new(),
        }
    }

    /// Calculate a deterministic hash of an object
    fn object_hash(&self, object: &TokenizedObject) -> [u8; 32] {
        use sha2::{Digest, Sha256};

        let mut hasher = Sha256::new();
        hasher.update(object.id().bytes());
        hasher.update(object.holder().bytes());
        hasher.update(object.token_manager.bytes());

        hasher.update(&[match object.token_type {
            units_core::objects::TokenType::Native => 0,
            units_core::objects::TokenType::Custodial => 1,
            units_core::objects::TokenType::Proxy => 2,
        }]);

        hasher.update(object.data());

        let result = hasher.finalize();
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&result);
        hash
    }

    /// Verify a chain of proofs
    ///
    /// # Parameters
    /// * `object_states` - Vector of (slot, object state) pairs in ascending slot order
    /// * `proofs` - Vector of (slot, proof) pairs in ascending slot order
    ///
    /// # Returns
    /// A VerificationResult indicating whether the proof chain is valid
    pub fn verify_proof_chain_internal(
        &self,
        object_states: &[(SlotNumber, TokenizedObject)],
        proofs: &[(SlotNumber, TokenizedObjectProof)],
    ) -> VerificationResult {
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

    /// Create a new lattice proof engine with custom parameters
    pub fn with_params(params: LatticeHashParams) -> Self {
        Self {
            hasher: LatticeHash::with_params(params),
        }
    }
}

impl ProofEngine for LatticeProofEngine {
    fn generate_object_proof(
        &self,
        object: &TokenizedObject,
        prev_proof: Option<&TokenizedObjectProof>,
        transaction_hash: Option<[u8; 32]>,
    ) -> Result<TokenizedObjectProof, StorageError> {
        // Calculate object hash
        let object_hash = self.object_hash(object);

        // Hash the object and create a proof
        let hash = self.hasher.hash(object);
        let proof_data = self.hasher.create_proof(&hash);

        // Create the proof with the previous hash if available
        // and include the transaction hash that led to this state change
        let prev_proof_hash = prev_proof.map(|p| p.hash());

        // Get current slot from system clock
        let slot = crate::engine::SlotNumber::default(); // Will be replaced by proper slot logic

        let proof = TokenizedObjectProof {
            object_id: object.id().clone(),
            slot,
            object_hash,
            prev_proof_hash,
            transaction_hash,
            proof_data,
        };

        Ok(proof)
    }

    fn verify_object_proof(
        &self,
        object: &TokenizedObject,
        proof: &TokenizedObjectProof,
    ) -> Result<bool, StorageError> {
        Ok(self.hasher.verify(object, &proof.proof_data))
    }

    fn verify_proof_chain(
        &self,
        object: &TokenizedObject,
        proof: &TokenizedObjectProof,
        prev_proof: &TokenizedObjectProof,
    ) -> Result<bool, StorageError> {
        // First verify the current proof for the object
        if !self.verify_object_proof(object, proof)? {
            return Ok(false);
        }

        // Then verify that the current proof correctly links to the previous proof
        if let Some(ref prev_hash) = proof.prev_proof_hash {
            let computed_prev_hash = prev_proof.hash();
            if computed_prev_hash != *prev_hash {
                return Ok(false);
            }
            // The proof is valid and links correctly
            Ok(true)
        } else {
            // The current proof doesn't link to anything, but claims to be in a chain
            Ok(false)
        }
    }

    fn generate_state_proof(
        &self,
        object_proofs: &[(UnitsObjectId, TokenizedObjectProof)],
        prev_state_proof: Option<&StateProof>,
        _slot: SlotNumber,
    ) -> Result<StateProof, StorageError> {
        // Extract just the proofs
        let proofs: Vec<&Vec<u8>> = object_proofs
            .iter()
            .map(|(_, proof)| &proof.proof_data)
            .collect();

        // If there are no proofs, return an empty state proof
        if proofs.is_empty() {
            return Ok(StateProof {
                slot: _slot,
                prev_state_proof_hash: prev_state_proof.map(|p| p.hash()),
                object_ids: Vec::new(),
                proof_data: Vec::new(),
            });
        }

        // Start with the first hash
        let mut result_hash = Vec::new();
        let n = self.hasher.params.n;

        // Convert the first proof to a hash
        if let Some(first_proof) = proofs.first() {
            let mut hash = vec![0u64; n];
            for i in 0..n {
                let start = i * 8;
                if start + 8 > first_proof.len() {
                    // Invalid proof length
                    return Ok(StateProof::new(Vec::new(), Vec::new(), prev_state_proof));
                }

                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&first_proof[start..start + 8]);
                hash[i] = u64::from_le_bytes(bytes);
            }
            result_hash = hash;
        }

        // Combine all the remaining hashes
        for proof in proofs.iter().skip(1) {
            let mut hash = vec![0u64; n];
            for i in 0..n {
                let start = i * 8;
                if start + 8 > proof.len() {
                    // Invalid proof length
                    continue;
                }

                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&proof[start..start + 8]);
                hash[i] = u64::from_le_bytes(bytes);
            }

            // Combine the hash with the result hash
            for i in 0..n {
                result_hash[i] = (result_hash[i] + hash[i]) % self.hasher.params.q;
            }
        }

        // Create the state proof
        let mut state_proof_data = Vec::with_capacity(n * 8);
        for &value in &result_hash {
            state_proof_data.extend_from_slice(&value.to_le_bytes());
        }

        // Extract the object IDs
        let included_objects = object_proofs.iter().map(|(id, _)| id.clone()).collect();

        // Create the state proof with link to previous
        let state_proof = StateProof {
            slot: _slot,
            prev_state_proof_hash: prev_state_proof.map(|p| p.hash()),
            object_ids: included_objects,
            proof_data: state_proof_data,
        };

        Ok(state_proof)
    }

    fn verify_state_proof(
        &self,
        state_proof: &StateProof,
        object_proofs: &[(UnitsObjectId, TokenizedObjectProof)],
    ) -> Result<bool, StorageError> {
        // For verification, we need to regenerate the expected state proof
        // Get the IDs that should be included
        let expected_included_ids: Vec<_> = object_proofs.iter().map(|(id, _)| id).collect();

        // Verify that the state proof includes exactly these objects
        if state_proof.object_ids.len() != expected_included_ids.len() {
            return Ok(false);
        }

        for id in expected_included_ids {
            if !state_proof.object_ids.contains(id) {
                return Ok(false);
            }
        }

        // Generate the expected state proof (without prev link/slot which is already in state_proof)
        let temp_state_proof = self.generate_state_proof(object_proofs, None, state_proof.slot)?;

        // Compare just the proof data
        Ok(state_proof.proof_data == temp_state_proof.proof_data)
    }

    fn verify_state_proof_chain(
        &self,
        state_proof: &StateProof,
        prev_state_proof: &StateProof,
    ) -> Result<bool, StorageError> {
        // First verify the state proof itself
        if state_proof.proof_data.is_empty() || prev_state_proof.proof_data.is_empty() {
            return Ok(false);
        }

        // Verify the slots are in order
        if state_proof.slot <= prev_state_proof.slot {
            return Ok(false);
        }

        // Verify the previous hash link
        if let Some(ref prev_hash) = state_proof.prev_state_proof_hash {
            let computed_prev_hash = prev_state_proof.hash();
            return Ok(computed_prev_hash == *prev_hash);
        }

        // No link to previous state proof
        Ok(false)
    }

    fn verify_proof_history(
        &self,
        object_states: &[(SlotNumber, TokenizedObject)],
        proofs: &[(SlotNumber, TokenizedObjectProof)],
    ) -> VerificationResult {
        // Use the internal implementation defined in the struct
        self.verify_proof_chain_internal(object_states, proofs)
    }
}

impl Default for LatticeProofEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use units_core::id::UnitsObjectId;
    use units_core::objects::{TokenType, TokenizedObject};

    #[test]
    fn test_proof_engine_basic() {
        // Create a test object
        let id = UnitsObjectId::unique_id_for_tests();
        let holder = UnitsObjectId::unique_id_for_tests();
        let token_manager = UnitsObjectId::unique_id_for_tests();
        let obj = TokenizedObject::new(
            id,
            holder,
            TokenType::Native,
            token_manager,
            vec![1, 2, 3, 4],
        );

        // Create a proof engine
        let engine = LatticeProofEngine::new();

        // Generate a proof
        let proof = engine.generate_object_proof(&obj, None, None).unwrap();

        // Verify the proof
        assert!(engine.verify_object_proof(&obj, &proof).unwrap());

        // Modify the object and verify the proof fails
        let modified_obj = TokenizedObject::new(
            *obj.id(),
            *obj.holder(),
            obj.token_type,
            obj.token_manager,
            vec![5, 6, 7, 8],
        );

        assert!(!engine.verify_object_proof(&modified_obj, &proof).unwrap());
    }

    #[test]
    fn test_state_proof_generation() {
        use crate::proofs::current_slot;
        // Create multiple test objects
        let obj1 = TokenizedObject::new(
            UnitsObjectId::unique_id_for_tests(),
            UnitsObjectId::unique_id_for_tests(),
            TokenType::Native,
            UnitsObjectId::unique_id_for_tests(),
            vec![1, 2, 3],
        );

        let obj2 = TokenizedObject::new(
            UnitsObjectId::unique_id_for_tests(),
            UnitsObjectId::unique_id_for_tests(),
            TokenType::Custodial,
            UnitsObjectId::unique_id_for_tests(),
            vec![4, 5, 6],
        );

        // Create a proof engine
        let engine = LatticeProofEngine::new();

        // Generate proofs for each object
        let proof1 = engine.generate_object_proof(&obj1, None, None).unwrap();
        let proof2 = engine.generate_object_proof(&obj2, None, None).unwrap();

        // Create a collection of object proofs
        let object_proofs = vec![(*obj1.id(), proof1), (*obj2.id(), proof2)];

        // Generate a state proof
        let state_proof = engine
            .generate_state_proof(&object_proofs, None, current_slot())
            .unwrap();

        // Verify the state proof
        assert!(engine
            .verify_state_proof(&state_proof, &object_proofs)
            .unwrap());

        // Modify the object collection and verify the state proof fails
        let mut modified_proofs = object_proofs.clone();
        modified_proofs.pop(); // Remove one of the proofs

        assert!(!engine
            .verify_state_proof(&state_proof, &modified_proofs)
            .unwrap());
    }
}
