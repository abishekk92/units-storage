pub mod runtime;
pub mod verification;

// Re-export the main types for convenience
pub use runtime::{
    InMemoryReceiptIterator, InMemoryReceiptStorage, MockRuntime, Runtime, TransactionEffect,
    TransactionReceipt,
};
// Re-export moved traits from units-storage-impl
pub use units_storage_impl::storage_traits::{TransactionReceiptStorage, UnitsReceiptIterator};

// Re-export types from units-transaction
pub use units_transaction::{
    AccessIntent, ConflictResult, Instruction, Transaction, TransactionHash,
};

pub use verification::{detect_double_spend, verify_transaction_included, ProofVerifier};

// Re-export VerificationResult from units-proofs
pub use units_proofs::VerificationResult;
