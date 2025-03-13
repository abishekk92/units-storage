pub mod runtime;
pub mod verification;

// Re-export the main types for convenience
pub use runtime::{
    TransactionResult,
    RuntimeTransactionReceipt,
    Runtime,
    MockRuntime,
    InMemoryReceiptStorage,
    InMemoryReceiptIterator
};

// Re-export types from units-transaction
pub use units_transaction::{
    AccessIntent,
    Instruction,
    Transaction,
    TransactionHash,
    ConflictResult
};

pub use verification::{
    ProofVerifier,
    verify_transaction_included,
    detect_double_spend
};

// Re-export VerificationResult from units-proofs
pub use units_proofs::VerificationResult;