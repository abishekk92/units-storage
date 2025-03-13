use curve25519_dalek::edwards::CompressedEdwardsY;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;
use std::ops::Deref;

// UnitsObjectId uniquely identifies an instance of tokenized object.
// It is a 32 byte long unique identifier, resembling a public key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UnitsObjectId([u8; 32]);

impl fmt::Display for UnitsObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Format as a hex string with a prefix of the first 6 bytes
        let prefix = hex::encode(&self.0[0..6]);
        write!(f, "obj:{}", prefix)
    }
}

impl Ord for UnitsObjectId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for UnitsObjectId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

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

    /// Create a UnitsObjectId from raw bytes
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        UnitsObjectId(bytes)
    }

    /// Get a reference to the internal bytes
    pub fn bytes(&self) -> &[u8] {
        &self.0
    }

    /// Create a random UnitsObjectId for testing
    pub fn random() -> Self {
        // Generate a random ID using system time
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .to_le_bytes();

        // Use this as a seed to create a unique ID
        let (id, _) = Self::find_uid(&[&now, &[1, 2, 3, 4]]);
        id
    }

    /// Generate a unique UnitsObjectId for testing purposes - exposed for testing in other crates
    pub fn unique_id_for_tests() -> Self {
        // Use current timestamp as basis for uniqueness
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos()
            .to_le_bytes();

        let ts_slice = timestamp.as_slice();
        let extra = [1, 2, 3, 4];

        let (id, _) = UnitsObjectId::find_uid(&[ts_slice, &extra]);
        id
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
    ///
    /// Returns true if the bytes do not represent a valid curve point.
    /// Returns false if the bytes do represent a valid curve point.
    pub fn is_off_curve(bytes: &[u8; 32]) -> bool {
        let Ok(compressed_edwards_y) = CompressedEdwardsY::from_slice(bytes.as_ref()) else {
            return true; // Cannot even parse as a point format, so it's off-curve
        };
        compressed_edwards_y.decompress().is_none() // If we can't decompress it, it's off-curve
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
