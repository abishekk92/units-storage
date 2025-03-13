pub mod conflict;
pub mod lock;

// Re-export the main types for convenience
pub use conflict::{
    ConflictChecker,
    BasicConflictChecker,
    ConflictResult,
    Instruction as SchedulerInstruction,
    Transaction as SchedulerTransaction,
    TransactionHash,
};

pub use lock::{
    AccessIntent,
    LockInfo,
    LockType,
    ObjectLockGuard,
    PersistentLockManager,
    UnitsLockIterator,
};