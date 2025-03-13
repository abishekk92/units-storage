use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use units_core::id::UnitsObjectId;
use units_core::objects::TokenizedObject;
use units_proofs::{SlotNumber, TokenizedObjectProof};

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

/// Represents the before and after state of an object in a transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionEffect {
    /// The transaction that caused this effect
    pub transaction_hash: TransactionHash,

    /// The ID of the object affected
    pub object_id: UnitsObjectId,

    /// The state of the object before the transaction (None if object was created)
    pub before_image: Option<TokenizedObject>,

    /// The state of the object after the transaction (None if object was deleted)
    pub after_image: Option<TokenizedObject>,
}

/// A receipt of a processed transaction, containing all proofs of object modifications
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionReceipt {
    /// The hash of the transaction that was executed
    pub transaction_hash: TransactionHash,

    /// The slot in which this transaction was processed
    pub slot: SlotNumber,

    /// Map of object IDs to their state proofs after the transaction
    pub object_proofs: HashMap<UnitsObjectId, TokenizedObjectProof>,

    /// Whether the transaction was executed successfully
    pub success: bool,

    /// Timestamp when the transaction was processed
    pub timestamp: u64,

    /// The commitment level of this transaction
    pub commitment_level: CommitmentLevel,

    /// Any error message from the execution (if not successful)
    pub error_message: Option<String>,

    /// Effects track the before and after state of objects for easier rollback
    pub effects: Vec<TransactionEffect>,
}

impl TransactionReceipt {
    /// Create a new transaction receipt
    pub fn new(
        transaction_hash: TransactionHash,
        slot: SlotNumber,
        success: bool,
        timestamp: u64,
    ) -> Self {
        Self {
            transaction_hash,
            slot,
            object_proofs: HashMap::new(),
            success,
            timestamp,
            commitment_level: if success {
                CommitmentLevel::Committed
            } else {
                CommitmentLevel::Failed
            },
            error_message: None,
            effects: Vec::new(),
        }
    }

    /// Create a new transaction receipt with a specific commitment level
    pub fn with_commitment_level(
        transaction_hash: TransactionHash,
        slot: SlotNumber,
        success: bool,
        timestamp: u64,
        commitment_level: CommitmentLevel,
    ) -> Self {
        Self {
            transaction_hash,
            slot,
            object_proofs: HashMap::new(),
            success,
            timestamp,
            commitment_level,
            error_message: None,
            effects: Vec::new(),
        }
    }

    /// Add an object proof to the receipt
    pub fn add_proof(&mut self, object_id: UnitsObjectId, proof: TokenizedObjectProof) {
        self.object_proofs.insert(object_id, proof);
    }

    /// Add a transaction effect to the receipt
    pub fn add_effect(&mut self, effect: TransactionEffect) {
        self.effects.push(effect);
    }

    /// Set an error message (used when transaction fails)
    pub fn set_error(&mut self, error: String) {
        self.success = false;
        self.commitment_level = CommitmentLevel::Failed;
        self.error_message = Some(error);
    }

    /// Mark the transaction as committed
    pub fn commit(&mut self) {
        self.commitment_level = CommitmentLevel::Committed;
    }

    /// Mark the transaction as failed
    pub fn fail(&mut self) {
        self.success = false;
        self.commitment_level = CommitmentLevel::Failed;
    }

    /// Check if the transaction can be rolled back
    pub fn can_rollback(&self) -> bool {
        self.commitment_level == CommitmentLevel::Processing
    }

    /// Get the number of objects modified by this transaction
    pub fn object_count(&self) -> usize {
        self.object_proofs.len()
    }
}