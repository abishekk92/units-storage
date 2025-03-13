use serde::{Serialize, Deserialize};
use units_core::id::UnitsObjectId;

/// A transaction hash uniquely identifies a transaction in the system
pub type TransactionHash = [u8; 32];

/// Represents the commitment level of a transaction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitmentLevel {
    /// Transaction is in-flight/processing and can be rolled back
    Processing,
    /// Transaction has been committed and cannot be rolled back
    Committed,
    /// Transaction has failed and cannot be executed again
    Failed,
}

impl Default for CommitmentLevel {
    fn default() -> Self {
        CommitmentLevel::Processing
    }
}

/// The result of a transaction conflict check
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictResult {
    /// No conflicts detected, transaction can proceed
    NoConflict,
    /// Conflicts detected with these transaction hashes
    Conflict(Vec<TransactionHash>),
    /// Read-only transaction, no conflict possible
    ReadOnly,
}

/// The access intent for an instruction on a TokenizedObject
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccessIntent {
    /// Read-only access to the object
    Read,
    /// Read-write access to the object
    Write,
}

/// A structure representing an instruction within a transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instruction {
    /// The binary representation of the instruction
    pub data: Vec<u8>,
    
    /// The objects this instruction intends to access and their access intents
    pub object_intents: Vec<(UnitsObjectId, AccessIntent)>,
}

/// Transaction that contains multiple instructions to be executed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    /// List of instructions to be executed as part of this transaction
    pub instructions: Vec<Instruction>,
    
    /// The hash of the transaction
    pub hash: TransactionHash,
    
    /// The commitment level of this transaction
    pub commitment_level: CommitmentLevel,
}

impl Transaction {
    /// Create a new transaction with a Processing commitment level
    pub fn new(instructions: Vec<Instruction>, hash: TransactionHash) -> Self {
        Self {
            instructions,
            hash,
            commitment_level: CommitmentLevel::Processing,
        }
    }
    
    /// Mark the transaction as committed
    pub fn commit(&mut self) {
        self.commitment_level = CommitmentLevel::Committed;
    }
    
    /// Mark the transaction as failed
    pub fn fail(&mut self) {
        self.commitment_level = CommitmentLevel::Failed;
    }
    
    /// Check if the transaction can be rolled back
    pub fn can_rollback(&self) -> bool {
        self.commitment_level == CommitmentLevel::Processing
    }
}