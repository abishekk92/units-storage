pub mod host_environment;
pub mod mock_runtime;
pub mod runtime;
pub mod runtime_backend;
pub mod verification;
pub mod wasmtime_backend;

// Re-export the main types for convenience
pub use runtime::{Runtime, TransactionEffect, TransactionReceipt};
// Re-export moved traits from units-storage-impl
pub use units_storage_impl::storage_traits::{TransactionReceiptStorage, UnitsReceiptIterator};

// Re-export types from units-core
pub use units_core::locks::AccessIntent;
pub use units_core::transaction::{ConflictResult, Instruction, Transaction, TransactionHash};

pub use verification::{detect_double_spend, verify_transaction_included, ProofVerifier};

// Re-export runtime backend types
pub use runtime_backend::{
    EbpfRuntimeBackend, ExecutionError, InstructionContext, InstructionResult, RuntimeBackend,
    RuntimeBackendManager, WasmRuntimeBackend,
};

// Re-export host environment types
pub use host_environment::{
    create_standard_host_environment, HostEnvironment, StandardHostEnvironment,
};

// Re-export Wasmtime backend with feature flag
#[cfg(feature = "wasmtime-backend")]
pub use wasmtime_backend::{create_wasmtime_backend, WasmtimeRuntimeBackend};

// Re-export MockRuntime and InMemoryReceiptStorage for testing
pub use mock_runtime::{InMemoryReceiptStorage, MockRuntime};

// Re-export VerificationResult from units-proofs
pub use units_proofs::VerificationResult;
