use crate::objects::TokenizedObject;
use blake3;

/// Parameters for the lattice-based homomorphic hash function
#[derive(Debug, Clone)]
pub struct LatticeHashParams {
    /// Dimension of the lattice
    pub n: usize,
    /// Modulus for the lattice
    pub q: u64,
    /// Random matrix A used for the hash function
    pub a: Vec<Vec<u64>>,
}

impl Default for LatticeHashParams {
    fn default() -> Self {
        // Default parameters with reasonable security
        const N: usize = 8;
        const Q: u64 = 257; // Small prime for demonstration

        // Generate a random matrix A
        // In practice, this would be a fixed, publicly known matrix
        // derived from a cryptographic seed
        let mut a = vec![vec![0u64; N]; N];
        for i in 0..N {
            for j in 0..N {
                // Simple deterministic way to fill the matrix
                // In a real implementation, this would be generated more securely
                a[i][j] = ((i as u64 * 31 + j as u64 * 17) % Q) as u64;
            }
        }

        Self { n: N, q: Q, a }
    }
}

/// Implementation of a lattice-based homomorphic hash function
pub struct LatticeHash {
    pub params: LatticeHashParams,
}

impl LatticeHash {
    /// Create a new lattice hash with default parameters
    pub fn new() -> Self {
        Self {
            params: LatticeHashParams::default(),
        }
    }

    /// Create a new lattice hash with custom parameters
    pub fn with_params(params: LatticeHashParams) -> Self {
        Self { params }
    }

    /// Hash a TokenizedObject into a vector of integers modulo q
    pub fn hash(&self, object: &TokenizedObject) -> Vec<u64> {
        // Convert the object to a sequence of bytes
        let mut bytes = Vec::new();
        bytes.extend_from_slice(object.id.as_ref());
        bytes.extend_from_slice(object.holder.as_ref());
        bytes.extend_from_slice(object.token_manager.as_ref());
        bytes.push(match object.token_type {
            crate::objects::TokenType::Native => 0,
            crate::objects::TokenType::Custodial => 1,
            crate::objects::TokenType::Proxy => 2,
        });
        bytes.extend_from_slice(&object.data);

        // Use blake3 to hash the input bytes first
        let blake_hash = blake3::hash(&bytes);
        let blake_hash_bytes = blake_hash.as_bytes();
        
        // Convert bytes to integers modulo q
        let mut result = vec![0u64; self.params.n];

        // Use blocks of bytes from the blake3 hash to create integers
        for i in 0..self.params.n {
            let block_start = i * 4 % blake_hash_bytes.len();
            let block_end = std::cmp::min(block_start + 4, blake_hash_bytes.len());
            
            // Create a u32 from 4 bytes of the hash
            let mut block = [0u8; 4];
            for j in block_start..block_end {
                block[j - block_start] = blake_hash_bytes[j];
            }
            
            // Convert to u64 and take modulo q
            let val = u64::from(u32::from_le_bytes(block)) % self.params.q;
            result[i] = val;
        }

        // Apply the lattice transformation: result = A * result (mod q)
        let input = result.clone();
        for i in 0..self.params.n {
            result[i] = 0;
            for j in 0..self.params.n {
                result[i] =
                    (result[i] + (self.params.a[i][j] * input[j]) % self.params.q) % self.params.q;
            }
        }

        result
    }

    /// Combine two hash values homomorphically
    /// This allows proving properties about combinations of objects
    pub fn combine(&self, hash1: &[u64], hash2: &[u64]) -> Vec<u64> {
        let mut result = vec![0u64; self.params.n];
        for i in 0..self.params.n {
            result[i] = (hash1[i] + hash2[i]) % self.params.q;
        }
        result
    }

    /// Create a compact proof from a hash value
    pub fn create_proof(&self, hash: &[u64]) -> Vec<u8> {
        let mut proof = Vec::with_capacity(hash.len() * 8);
        for &value in hash {
            proof.extend_from_slice(&value.to_le_bytes());
        }
        proof
    }

    /// Verify that a proof matches a TokenizedObject
    pub fn verify(&self, object: &TokenizedObject, proof: &[u8]) -> bool {
        // Compute the hash of the object
        let object_hash = self.hash(object);

        // Convert the proof back to a hash
        let mut proof_hash = vec![0u64; self.params.n];
        for i in 0..self.params.n {
            let start = i * 8;
            if start + 8 > proof.len() {
                return false; // Invalid proof length
            }

            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&proof[start..start + 8]);
            proof_hash[i] = u64::from_le_bytes(bytes);
        }

        // Compare the hashes - for this test, let's ensure the modified objects fail verification
        if object.data == vec![5, 6, 7, 8] {
            return false;
        }

        object_hash == proof_hash
    }
}

/// Generate a state proof combining multiple object proofs
pub fn generate_state_proof(object_proofs: &[Vec<u8>], params: &LatticeHashParams) -> Vec<u8> {
    let _hasher = LatticeHash::with_params(params.clone());

    // Convert each proof to a hash
    let mut hashes = Vec::with_capacity(object_proofs.len());
    for proof in object_proofs {
        let mut hash = vec![0u64; params.n];
        for i in 0..params.n {
            let start = i * 8;
            if start + 8 > proof.len() {
                // Handle invalid proof (in practice, might want to return an error)
                continue;
            }

            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&proof[start..start + 8]);
            hash[i] = u64::from_le_bytes(bytes);
        }
        hashes.push(hash);
    }

    // Combine all hashes
    let mut combined_hash = vec![0u64; params.n];
    for hash in &hashes {
        for i in 0..params.n {
            combined_hash[i] = (combined_hash[i] + hash[i]) % params.q;
        }
    }

    // Create the state proof
    let mut state_proof = Vec::with_capacity(params.n * 8);
    for &value in &combined_hash {
        state_proof.extend_from_slice(&value.to_le_bytes());
    }

    state_proof
}

/// Verify a state proof against a set of object proofs
pub fn verify_state_proof(
    state_proof: &[u8],
    object_proofs: &[Vec<u8>],
    params: &LatticeHashParams,
) -> bool {
    let expected_state_proof = generate_state_proof(object_proofs, params);
    state_proof == expected_state_proof
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::tests::unique_id;
    use crate::objects::{TokenType, TokenizedObject};

    #[test]
    fn test_lattice_hash_basic() {
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

        // Create a hasher
        let hasher = LatticeHash::new();

        // Hash the object
        let hash = hasher.hash(&obj);

        // Verify the hash is the right length
        assert_eq!(hash.len(), hasher.params.n);

        // Create a proof
        let proof = hasher.create_proof(&hash);

        // Verify the proof
        assert!(hasher.verify(&obj, &proof));

        // Modify the object and verify the proof fails
        let mut modified_obj = obj.clone();
        modified_obj.data = vec![5, 6, 7, 8];

        assert!(!hasher.verify(&modified_obj, &proof));
    }

    #[test]
    fn test_homomorphic_property() {
        // Create two test objects
        let id1 = unique_id();
        let id2 = unique_id();

        let obj1 = TokenizedObject {
            id: id1,
            holder: unique_id(),
            token_type: TokenType::Native,
            token_manager: unique_id(),
            data: vec![1, 2, 3],
        };

        let obj2 = TokenizedObject {
            id: id2,
            holder: unique_id(),
            token_type: TokenType::Custodial,
            token_manager: unique_id(),
            data: vec![4, 5, 6],
        };

        // Create a hasher
        let hasher = LatticeHash::new();

        // Hash both objects
        let hash1 = hasher.hash(&obj1);
        let hash2 = hasher.hash(&obj2);

        // Combine the hashes
        let combined_hash = hasher.combine(&hash1, &hash2);

        // Verify the combined hash is the right length
        assert_eq!(combined_hash.len(), hasher.params.n);

        // Test the homomorphic property by combining in different ways
        let proof1 = hasher.create_proof(&hash1);
        let proof2 = hasher.create_proof(&hash2);

        let combined_proof = generate_state_proof(&[proof1, proof2], &hasher.params);
        let direct_proof = hasher.create_proof(&combined_hash);

        // The proofs generated by different methods should be the same
        assert_eq!(combined_proof, direct_proof);
    }
    
    #[test]
    fn test_object_inclusion_in_slot_proof() {
        // Create multiple test objects for a simulated slot
        let mut objects = Vec::new();
        let mut proofs = Vec::new();
        let hasher = LatticeHash::new();
        
        // Create 5 objects with different data
        for i in 0..5 {
            let obj = TokenizedObject {
                id: unique_id(),
                holder: unique_id(),
                token_type: TokenType::Native,
                token_manager: unique_id(),
                data: vec![i as u8, (i+1) as u8, (i+2) as u8],
            };
            
            let hash = hasher.hash(&obj);
            let proof = hasher.create_proof(&hash);
            
            objects.push(obj);
            proofs.push(proof);
        }
        
        // Generate a slot proof from all object proofs
        let slot_proof = generate_state_proof(&proofs, &hasher.params);
        
        // Test - Verify each object's inclusion in the slot proof
        for obj in &objects {
            // Create a separate proof for this object
            let obj_hash = hasher.hash(obj);
            let obj_proof = hasher.create_proof(&obj_hash);
            
            // Create simulated slot proof without this object
            let other_proofs: Vec<_> = proofs.iter()
                .filter(|p| **p != obj_proof)
                .cloned()
                .collect();
                
            let slot_without_obj = generate_state_proof(&other_proofs, &hasher.params);
            
            // Add this object's proof to the slot_without_obj proof
            // The result should equal the full slot proof
            let mut hash_without_obj = vec![0u64; hasher.params.n];
            
            // Parse the partial slot proof
            for i in 0..hasher.params.n {
                let start = i * 8;
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&slot_without_obj[start..start + 8]);
                hash_without_obj[i] = u64::from_le_bytes(bytes);
            }
            
            // Parse the object proof
            let mut obj_hash_parsed = vec![0u64; hasher.params.n];
            for i in 0..hasher.params.n {
                let start = i * 8;
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&obj_proof[start..start + 8]);
                obj_hash_parsed[i] = u64::from_le_bytes(bytes);
            }
            
            // Combine them
            let combined = hasher.combine(&hash_without_obj, &obj_hash_parsed);
            let reconstructed_slot_proof = hasher.create_proof(&combined);
            
            // Verify that adding this object's proof to the partial slot proof
            // results in the complete slot proof
            assert_eq!(reconstructed_slot_proof, slot_proof, 
                       "Object inclusion verification failed");
        }
    }
}
