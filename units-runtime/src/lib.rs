pub mod runtime;
pub mod verification;

// Re-export the main types for convenience
pub use runtime::{
    AccessIntent,
    Instruction,
    Transaction,
    TransactionHash,
    TransactionResult,
    RuntimeTransactionReceipt,
    ConflictResult,
    Runtime,
    MockRuntime,
    InMemoryReceiptStorage,
    InMemoryReceiptIterator
};

pub use verification::{
    VerificationResult,
    ProofVerifier,
    verify_transaction_included,
    detect_double_spend
};