use crate::error::StorageError;
use crate::id::UnitsObjectId;
use crate::objects::TokenizedObject;
use crate::proofs::{
    engine::ProofEngine,
    lattice_hash::{LatticeHash, LatticeHashParams},
    StateProof, TokenizedObjectProof,
};

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

    /// Create a new lattice proof engine with custom parameters
    pub fn with_params(params: LatticeHashParams) -> Self {
        Self {
            hasher: LatticeHash::with_params(params),
        }
    }
}

impl ProofEngine for LatticeProofEngine {
    fn generate_object_proof(&self, object: &TokenizedObject) -> Result<TokenizedObjectProof, StorageError> {
        // Hash the object and create a proof
        let hash = self.hasher.hash(object);
        let proof = self.hasher.create_proof(&hash);
        
        Ok(TokenizedObjectProof { proof })
    }
    
    fn verify_object_proof(
        &self, 
        object: &TokenizedObject, 
        proof: &TokenizedObjectProof
    ) -> Result<bool, StorageError> {
        Ok(self.hasher.verify(object, &proof.proof))
    }
    
    fn generate_state_proof(
        &self, 
        object_proofs: &[(UnitsObjectId, TokenizedObjectProof)]
    ) -> Result<StateProof, StorageError> {
        // Extract just the proofs
        let proofs: Vec<&Vec<u8>> = object_proofs
            .iter()
            .map(|(_, proof)| &proof.proof)
            .collect();
        
        // If there are no proofs, return an empty state proof
        if proofs.is_empty() {
            return Ok(StateProof {
                proof: Vec::new(),
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
                    return Ok(StateProof {
                        proof: Vec::new(),
                    });
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
        let mut state_proof = Vec::with_capacity(n * 8);
        for &value in &result_hash {
            state_proof.extend_from_slice(&value.to_le_bytes());
        }
        
        Ok(StateProof {
            proof: state_proof,
        })
    }
    
    fn verify_state_proof(
        &self,
        state_proof: &StateProof,
        object_proofs: &[(UnitsObjectId, TokenizedObjectProof)],
    ) -> Result<bool, StorageError> {
        // Generate the expected state proof
        let expected_proof = self.generate_state_proof(object_proofs)?;
        
        // Compare the proofs
        Ok(state_proof.proof == expected_proof.proof)
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
    use crate::id::tests::unique_id;
    use crate::objects::{TokenType, TokenizedObject};
    use crate::proofs::engine::ProofEngine;

    #[test]
    fn test_proof_engine_basic() {
        // Create a test object
        let id = unique_id();
        let holder = unique_id();
        let token_manager = unique_id();
        let obj = TokenizedObject {
            id,
            holder,
            token_type: TokenType::Native,
            token_manager,
            data: vec![1, 2, 3, 4],
        };
        
        // Create a proof engine
        let engine = LatticeProofEngine::new();
        
        // Generate a proof
        let proof = engine.generate_object_proof(&obj).unwrap();
        
        // Verify the proof
        assert!(engine.verify_object_proof(&obj, &proof).unwrap());
        
        // Modify the object and verify the proof fails
        let mut modified_obj = obj.clone();
        modified_obj.data = vec![5, 6, 7, 8];
        
        assert!(!engine.verify_object_proof(&modified_obj, &proof).unwrap());
    }
    
    #[test]
    fn test_state_proof_generation() {
        // Create multiple test objects
        let obj1 = TokenizedObject {
            id: unique_id(),
            holder: unique_id(),
            token_type: TokenType::Native,
            token_manager: unique_id(),
            data: vec![1, 2, 3],
        };
        
        let obj2 = TokenizedObject {
            id: unique_id(),
            holder: unique_id(),
            token_type: TokenType::Custodial,
            token_manager: unique_id(),
            data: vec![4, 5, 6],
        };
        
        // Create a proof engine
        let engine = LatticeProofEngine::new();
        
        // Generate proofs for each object
        let proof1 = engine.generate_object_proof(&obj1).unwrap();
        let proof2 = engine.generate_object_proof(&obj2).unwrap();
        
        // Create a collection of object proofs
        let object_proofs = vec![(obj1.id, proof1), (obj2.id, proof2)];
        
        // Generate a state proof
        let state_proof = engine.generate_state_proof(&object_proofs).unwrap();
        
        // Verify the state proof
        assert!(engine.verify_state_proof(&state_proof, &object_proofs).unwrap());
        
        // Modify the object collection and verify the state proof fails
        let mut modified_proofs = object_proofs.clone();
        modified_proofs.pop(); // Remove one of the proofs
        
        assert!(!engine.verify_state_proof(&state_proof, &modified_proofs).unwrap());
    }
}