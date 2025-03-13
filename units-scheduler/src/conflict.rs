use std::collections::HashSet;
use units_core::id::UnitsObjectId;
use crate::lock::AccessIntent;

/// Transaction hash type (32-byte array)
pub type TransactionHash = [u8; 32];

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

/// Simple instruction structure for conflict checking
#[derive(Debug, Clone)]
pub struct Instruction {
    /// The objects this instruction intends to access and their access intents
    pub object_intents: Vec<(UnitsObjectId, AccessIntent)>,
}

/// Simple transaction structure for conflict checking
#[derive(Debug, Clone)]
pub struct Transaction {
    /// List of instructions to be executed as part of this transaction
    pub instructions: Vec<Instruction>,
    
    /// The hash of the transaction
    pub hash: TransactionHash,
}

/// Trait for transaction conflict checking
pub trait ConflictChecker {
    /// Check for potential conflicts with pending or recent transactions
    ///
    /// # Parameters
    /// * `transaction` - The transaction to check for conflicts
    /// * `recent_transactions` - Recent transactions to check against
    ///
    /// # Returns
    /// A ConflictResult indicating whether conflicts were detected
    fn check_conflicts(
        &self,
        transaction: &Transaction,
        recent_transactions: &[Transaction],
    ) -> Result<ConflictResult, String>;

    /// Check if a transaction is read-only
    ///
    /// # Parameters
    /// * `transaction` - The transaction to check
    ///
    /// # Returns
    /// True if the transaction is read-only
    fn is_read_only(&self, transaction: &Transaction) -> bool {
        transaction.instructions.iter().all(|i| {
            i.object_intents
                .iter()
                .all(|(_, intent)| *intent == AccessIntent::Read)
        })
    }

    /// Extract object IDs with write intent from a transaction
    ///
    /// # Parameters
    /// * `transaction` - The transaction to analyze
    ///
    /// # Returns
    /// A HashSet of object IDs that the transaction intends to write to
    fn extract_write_objects(&self, transaction: &Transaction) -> HashSet<UnitsObjectId> {
        let mut write_objects = HashSet::new();
        for instruction in &transaction.instructions {
            for (obj_id, intent) in &instruction.object_intents {
                if *intent == AccessIntent::Write {
                    write_objects.insert(*obj_id);
                }
            }
        }
        write_objects
    }
}

/// Basic implementation of the ConflictChecker trait
pub struct BasicConflictChecker;

impl BasicConflictChecker {
    /// Create a new BasicConflictChecker
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for BasicConflictChecker {
    fn default() -> Self {
        Self::new()
    }
}

impl ConflictChecker for BasicConflictChecker {
    fn check_conflicts(
        &self,
        transaction: &Transaction,
        recent_transactions: &[Transaction],
    ) -> Result<ConflictResult, String> {
        // If it's a read-only transaction, no conflicts are possible
        if self.is_read_only(transaction) {
            return Ok(ConflictResult::ReadOnly);
        }

        // Extract object IDs with write intent from this transaction
        let write_objects = self.extract_write_objects(transaction);

        // If there are no write objects, the transaction is read-only
        if write_objects.is_empty() {
            return Ok(ConflictResult::ReadOnly);
        }

        // Check for conflicts with recent transactions
        let mut conflicts = Vec::new();

        for other_tx in recent_transactions {
            // Skip checking against itself
            if other_tx.hash == transaction.hash {
                continue;
            }

            // Check for overlapping write intents
            'outer: for instruction in &other_tx.instructions {
                for (obj_id, intent) in &instruction.object_intents {
                    if *intent == AccessIntent::Write && write_objects.contains(obj_id) {
                        conflicts.push(other_tx.hash);
                        break 'outer;
                    }
                }
            }
        }

        if conflicts.is_empty() {
            Ok(ConflictResult::NoConflict)
        } else {
            Ok(ConflictResult::Conflict(conflicts))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use units_core::id::UnitsObjectId;

    #[test]
    fn test_basic_conflict_checker() {
        let checker = BasicConflictChecker::new();

        // Create test objects
        let object1 = UnitsObjectId::unique_id_for_tests();
        let object2 = UnitsObjectId::unique_id_for_tests();

        // Create a read-only transaction
        let read_tx = Transaction {
            instructions: vec![Instruction {
                object_intents: vec![(object1, AccessIntent::Read), (object2, AccessIntent::Read)],
            }],
            hash: [1u8; 32],
        };

        // Create a transaction that writes to object1
        let write_tx1 = Transaction {
            instructions: vec![Instruction {
                object_intents: vec![(object1, AccessIntent::Write)],
            }],
            hash: [2u8; 32],
        };

        // Create a transaction that writes to object2
        let write_tx2 = Transaction {
            instructions: vec![Instruction {
                object_intents: vec![(object2, AccessIntent::Write)],
            }],
            hash: [3u8; 32],
        };

        // Test read-only transaction
        let result = checker.check_conflicts(&read_tx, &[write_tx1.clone(), write_tx2.clone()]);
        assert_eq!(result, Ok(ConflictResult::ReadOnly));

        // Test write transaction with conflict
        // Create a different transaction that writes to the same object
        let write_tx1_conflicting = Transaction {
            instructions: vec![Instruction {
                object_intents: vec![(object1, AccessIntent::Write)],
            }],
            hash: [4u8; 32],
        };

        let result = checker.check_conflicts(&write_tx1, &[write_tx1_conflicting]);
        println!("Conflict test result: {:?}", result);
        assert!(matches!(result, Ok(ConflictResult::Conflict(_))));

        // Test write transaction with no conflict
        let result = checker.check_conflicts(&write_tx1, &[write_tx2.clone()]);
        assert_eq!(result, Ok(ConflictResult::NoConflict));
    }
}
