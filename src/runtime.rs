use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use crate::id::UnitsObjectId;

/// A transaction hash uniquely identifies a transaction in the system
pub type TransactionHash = [u8; 32];

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
}

/// Result of a transaction execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionResult {
    /// The hash of the transaction that was executed
    pub transaction_hash: TransactionHash,
    
    /// Whether the transaction was executed successfully
    pub success: bool,
    
    /// Any error message from the execution
    pub error_message: Option<String>,
}

/// Runtime for executing transactions that modify TokenizedObjects
pub trait Runtime {
    /// Execute a transaction and return the result
    fn execute_transaction(&self, transaction: Transaction) -> TransactionResult;
    
    /// Get a transaction by its hash
    fn get_transaction(&self, hash: &TransactionHash) -> Option<Transaction>;
}

/// Mock implementation of the Runtime trait for testing purposes
pub struct MockRuntime {
    /// Store of transactions by their hash
    transactions: HashMap<TransactionHash, Transaction>,
}

impl MockRuntime {
    /// Create a new MockRuntime
    pub fn new() -> Self {
        Self {
            transactions: HashMap::new(),
        }
    }
    
    /// Add a transaction to the mock runtime's transaction store
    pub fn add_transaction(&mut self, transaction: Transaction) {
        self.transactions.insert(transaction.hash, transaction);
    }
}

impl Runtime for MockRuntime {
    fn execute_transaction(&self, transaction: Transaction) -> TransactionResult {
        // In a mock implementation, we just pretend all transactions succeed
        let mut mock = self.clone();
        mock.add_transaction(transaction.clone());
        
        TransactionResult {
            transaction_hash: transaction.hash,
            success: true,
            error_message: None,
        }
    }
    
    fn get_transaction(&self, hash: &TransactionHash) -> Option<Transaction> {
        self.transactions.get(hash).cloned()
    }
}

impl Clone for MockRuntime {
    fn clone(&self) -> Self {
        Self {
            transactions: self.transactions.clone(),
        }
    }
}