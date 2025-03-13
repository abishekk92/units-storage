pub mod runtime;
pub mod verification;
pub mod mock_runtime;

// Re-export the main types for convenience
pub use runtime::{Runtime, TransactionEffect, TransactionReceipt};
// Re-export moved traits from units-storage-impl
pub use units_storage_impl::storage_traits::{TransactionReceiptStorage, UnitsReceiptIterator};

// Re-export types from units-core
pub use units_core::locks::AccessIntent;
pub use units_core::transaction::{
    ConflictResult, Instruction, Transaction, TransactionHash,
};

pub use verification::{detect_double_spend, verify_transaction_included, ProofVerifier};

// Re-export MockRuntime and InMemoryReceiptStorage for testing
pub use mock_runtime::{MockRuntime, InMemoryReceiptStorage};

// Re-export VerificationResult from units-proofs
pub use units_proofs::VerificationResult;
