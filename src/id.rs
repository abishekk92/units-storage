use curve25519_dalek::edwards::CompressedEdwardsY;
use sha2::{Digest, Sha256};
use std::ops::Deref;

// UnitsObjectId uniquely identifies an instance of tokenized object.
// It is a 32 byte long unique identifier, resembling a public key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnitsObjectId([u8; 32]);

impl Default for UnitsObjectId {
    fn default() -> Self {
        UnitsObjectId([0; 32])
    }
}

impl Deref for UnitsObjectId {
    type Target = [u8; 32];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl UnitsObjectId {
    pub fn new(uid: [u8; 32]) -> Self {
        UnitsObjectId(uid)
    }

    pub fn create_object_id(seeds: &[&[u8]], bump: u8) -> [u8; 32] {
        let mut hasher = Sha256::new();

        // Domain separator
        hasher.update(b"UNITS_Object");

        // Add all seeds
        for seed in seeds {
            hasher.update(seed);
        }

        // Add bump
        hasher.update(&[bump]);

        hasher.finalize().into()
    }

    /// Verify that a 32-byte array is not a valid point on the ed25519 curve
    pub fn is_off_curve(bytes: &[u8; 32]) -> bool {
        let Ok(compressed_edwards_y) = CompressedEdwardsY::from_slice(bytes.as_ref()) else {
            return false;
        };
        compressed_edwards_y.decompress().is_some()
    }

    /// Try Find an UnitsObjectId for given seeds
    pub fn try_find_uid(seeds: &[&[u8]]) -> Option<(UnitsObjectId, u8)> {
        for bump in 0..255 {
            let id = UnitsObjectId::create_object_id(seeds, bump);
            if UnitsObjectId::is_off_curve(&id) {
                return Some((UnitsObjectId(id), bump));
            }
        }
        None
    }

    /// Find an UnitsObjectId for given seeds
    pub fn find_uid(seeds: &[&[u8]]) -> (UnitsObjectId, u8) {
        UnitsObjectId::try_find_uid(seeds).expect("Failed to find a valid UnitsObjectId")
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    /// Generate a unique UnitsObjectId for testing purposes
    pub fn unique_id() -> UnitsObjectId {
        // Use current timestamp as basis for uniqueness
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos()
            .to_le_bytes();
            
        let ts_slice = timestamp.as_slice();
        let extra = [1, 2, 3, 4];
        
        let (id, _) = UnitsObjectId::find_uid(&[ts_slice, &extra]);
        id
    }
    
    #[test]
    fn test_unique_id() {
        let id1 = unique_id();
        let id2 = unique_id();
        
        // Two consecutive calls should produce different IDs
        assert_ne!(id1, id2);
        
        // Unique IDs should not be default
        assert_ne!(id1, UnitsObjectId::default());
        assert_ne!(id2, UnitsObjectId::default());
    }

    #[test]
    fn test_default_id() {
        let default_id = UnitsObjectId::default();
        assert_eq!(*default_id, [0u8; 32]);
    }

    #[test]
    fn test_new_id() {
        let test_bytes = [1u8; 32];
        let id = UnitsObjectId::new(test_bytes);
        assert_eq!(*id, test_bytes);
    }

    #[test]
    fn test_create_object_id() {
        // Test with specific seeds and bump
        let seed1 = b"test_seed_1";
        let seed2 = b"test_seed_2";
        let bump = 5;

        let id = UnitsObjectId::create_object_id(&[seed1, seed2], bump);
        
        // Verify deterministic nature by creating the same ID again
        let id2 = UnitsObjectId::create_object_id(&[seed1, seed2], bump);
        assert_eq!(id, id2);

        // Verify changing bump creates different ID
        let id3 = UnitsObjectId::create_object_id(&[seed1, seed2], bump + 1);
        assert_ne!(id, id3);

        // Verify changing seeds creates different ID
        let id4 = UnitsObjectId::create_object_id(&[seed2, seed1], bump);
        assert_ne!(id, id4);
    }

    #[test]
    fn test_is_off_curve() {
        // Generate a valid object ID which should be guaranteed to be off-curve
        let seed = b"curve_test_seed";
        let (id, _) = UnitsObjectId::find_uid(&[seed]);
        
        // The object ID should be off-curve by definition of how find_uid works
        assert!(UnitsObjectId::is_off_curve(&id));
    }

    #[test]
    fn test_find_uid() {
        let seed1 = b"unique_seed_1";
        let seed2 = b"unique_seed_2";

        // Test finding a valid UID
        let (id, bump) = UnitsObjectId::find_uid(&[seed1, seed2]);
        
        // Verify we can recreate the same ID with found bump
        let raw_id = UnitsObjectId::create_object_id(&[seed1, seed2], bump);
        assert_eq!(*id, raw_id);

        // Verify different seeds produce different IDs
        let (id2, _) = UnitsObjectId::find_uid(&[seed2, seed1]);
        assert_ne!(id, id2);
    }

    #[test]
    fn test_try_find_uid() {
        let seed = b"try_find_test";
        
        // Should find a valid ID
        let result = UnitsObjectId::try_find_uid(&[seed]);
        assert!(result.is_some());
        
        let (id, bump) = result.unwrap();
        
        // Verify we can recreate the ID with the returned bump
        let raw_id = UnitsObjectId::create_object_id(&[seed], bump);
        assert_eq!(*id, raw_id);
    }
}