use crate::id::UnitsObjectId;
use crate::objects::TokenizedObject;
use crate::proofs::{StateProof, TokenizedObjectProof};

pub trait UnitsStorageIterator {
    fn next(&self) -> Option<TokenizedObject>;
}

pub trait UnitsStorageProofEngine: UnitsStorageIterator {
    // fn generate_proof(&self, tokenized_object: TokenizedObject) -> Option<TokenizedObjectProof>;
    fn generate_state_proof(&self) -> Option<StateProof>;
    fn get_proof(&self, id: UnitsObjectId) -> Option<TokenizedObjectProof>;
    fn verify_proof(&self, proof: TokenizedObjectProof) -> Option<bool>;
}

pub trait UnitsStorage: UnitsStorageIterator + UnitsStorageProofEngine {
    fn get(&self, id: UnitsObjectId) -> Option<TokenizedObject>;
    fn set(&self, id: UnitsObjectId, obj: TokenizedObject);
    fn scan(&self) -> impl UnitsStorageIterator;
    fn delete(&self, id: UnitsObjectId) -> Option<bool>;
}