use std::collections::HashSet;
use crate::id::UnitsObjectId;
use crate::locks::AccessIntent;
use crate::transaction::{ConflictResult, Transaction};

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