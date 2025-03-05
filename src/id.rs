use curve25519_dalek::edwards::CompressedEdwardsY;
use sha2::{Digest, Sha256};
use std::ops::Deref;

// UnitsObjectId uniquely identifies an instance of tokenized object.
// It is a 32 byte long unique identifier, resembling a public key.
#[derive(Debug, Clone, Copy)]
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