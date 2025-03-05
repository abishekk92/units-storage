#![allow(dead_code)]

pub mod id;
pub mod objects;
pub mod proofs;
pub mod storage;

// Re-export the main types for convenience
pub use id::UnitsObjectId;
pub use objects::{TokenType, TokenizedObject};
pub use proofs::{StateProof, TokenizedObjectProof};
pub use storage::{UnitsStorage, UnitsStorageIterator, UnitsStorageProofEngine};

#[cfg(test)]
mod tests {
    use crate::objects::{TokenType, TokenizedObject};
    use crate::UnitsObjectId;

    #[test]
    fn it_works() {
        let token = TokenizedObject {
            data: vec![1, 2, 3, 4],
            token_type: TokenType::Native,
            holder: UnitsObjectId::default(),
            controller_program: UnitsObjectId::default(),
            id: UnitsObjectId::default(),
        };
        println!("{:?}", token);
    }
}
