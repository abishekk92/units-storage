use crate::id::UnitsObjectId;
use crate::locks::{AccessIntent, ObjectLockGuard, PersistentLockManager};
use crate::objects::{TokenizedObject, CodeObject, UnitsObject};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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

/// Identifies the runtime environment for program execution
///
/// This represents the type of runtime needed to execute program code.
/// We only support runtimes with proper isolation guarantees.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RuntimeType {
    /// WebAssembly virtual machine (using wasmtime, wasmer, etc.)
    Wasm,
    /// eBPF virtual machine (using rbpf or similar)
    Ebpf,
}

impl Default for RuntimeType {
    fn default() -> Self {
        RuntimeType::Wasm
    }
}

/// Standard entrypoint name used across all runtimes
///
/// Using a consistent entrypoint name across all runtimes ensures
/// that programs can be executed seamlessly regardless of runtime type.
pub const STANDARD_ENTRYPOINT: &str = "main";

/// A structure representing an instruction within a transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instruction {
    /// Parameters for this instruction (arguments, not code)
    pub params: Vec<u8>,

    /// Runtime type for this instruction (added for consistency across backends)
    /// When not explicitly set, it's derived from instruction_type
    #[serde(default = "Instruction::default_runtime_type")]
    pub runtime_type: RuntimeType,

    /// The objects this instruction intends to access and their access intents
    pub object_intents: Vec<(UnitsObjectId, AccessIntent)>,

    /// The ID of the code object to execute
    /// All code execution must come from a verified code object for security
    pub code_object_id: UnitsObjectId,
}

impl Instruction {
    /// Default runtime type derived from instruction_type
    fn default_runtime_type() -> RuntimeType {
        RuntimeType::Wasm
    }

    /// Create a new instruction with the specified runtime type
    pub fn new(
        params: Vec<u8>,
        runtime_type: RuntimeType,
        object_intents: Vec<(UnitsObjectId, AccessIntent)>,
        code_object_id: UnitsObjectId,
    ) -> Self {
        Self {
            params,
            runtime_type,
            object_intents,
            code_object_id,
        }
    }

    /// Get the effective entrypoint for this instruction
    pub fn entrypoint(&self) -> &str {
        STANDARD_ENTRYPOINT
    }

    /// Create a new WebAssembly instruction
    pub fn wasm(
        params: Vec<u8>,
        object_intents: Vec<(UnitsObjectId, AccessIntent)>,
        code_object_id: UnitsObjectId,
    ) -> Self {
        Self::new(params, RuntimeType::Wasm, object_intents, code_object_id)
    }

    /// Create a new eBPF instruction
    pub fn ebpf(
        params: Vec<u8>,
        object_intents: Vec<(UnitsObjectId, AccessIntent)>,
        code_object_id: UnitsObjectId,
    ) -> Self {
        Self::new(params, RuntimeType::Ebpf, object_intents, code_object_id)
    }

    /// Acquire all locks needed for this instruction
    ///
    /// This acquires locks for all objects according to their access intents.
    /// Read intents acquire shared read locks, while write intents acquire exclusive write locks.
    ///
    /// # Parameters
    /// * `transaction_hash` - The hash of the transaction acquiring the locks
    /// * `lock_manager` - The persistent lock manager to use
    ///
    /// # Returns
    /// A vector of lock guards or errors for each lock attempt
    pub fn acquire_locks<'a, M: PersistentLockManager>(
        &self,
        transaction_hash: &TransactionHash,
        lock_manager: &'a M,
    ) -> Vec<Result<ObjectLockGuard<'a, M>, M::Error>> {
        self.object_intents
            .iter()
            .map(|(object_id, intent)| {
                intent.acquire_lock(object_id, transaction_hash, lock_manager)
            })
            .collect()
    }

    /// Check if all locks needed for this instruction can be acquired
    ///
    /// # Parameters
    /// * `transaction_hash` - The hash of the transaction checking lock availability
    /// * `lock_manager` - The persistent lock manager to use
    ///
    /// # Returns
    /// True if all locks can be acquired, false otherwise
    pub fn can_acquire_locks<M: PersistentLockManager>(
        &self,
        transaction_hash: &TransactionHash,
        lock_manager: &M,
    ) -> Result<bool, M::Error> {
        for (object_id, intent) in &self.object_intents {
            match lock_manager.can_acquire_lock(object_id, *intent, transaction_hash) {
                Ok(false) => return Ok(false),
                Err(e) => return Err(e),
                _ => continue,
            }
        }

        Ok(true)
    }

    /// Create in-memory locks for testing
    #[cfg(test)]
    pub fn create_in_memory_locks<M: PersistentLockManager>(
        &self,
        transaction_hash: &TransactionHash,
    ) -> Vec<ObjectLockGuard<'static, M>> {
        self.object_intents
            .iter()
            .map(|(object_id, intent)| {
                intent.create_in_memory_lock::<M>(object_id, transaction_hash)
            })
            .collect()
    }
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

    /// Acquire all locks needed for this transaction
    ///
    /// This acquires locks for all objects according to their access intents in all instructions.
    /// Read intents acquire shared read locks, while write intents acquire exclusive write locks.
    ///
    /// # Parameters
    /// * `lock_manager` - The persistent lock manager to use
    ///
    /// # Returns
    /// A result containing a vector of lock guards if all locks were acquired successfully,
    /// or an error if any lock could not be acquired
    pub fn acquire_locks<'a, M: PersistentLockManager>(
        &self,
        lock_manager: &'a M,
    ) -> Result<Vec<ObjectLockGuard<'a, M>>, M::Error> {
        let mut locks = Vec::new();
        let mut locked_objects = std::collections::HashMap::new();

        for instruction in &self.instructions {
            for (object_id, intent) in &instruction.object_intents {
                // Check if we've already locked this object
                if let Some(existing_intent) = locked_objects.get(object_id) {
                    // If we already have a write lock, or if we only need a read lock, skip
                    if *existing_intent == AccessIntent::Write || *intent == AccessIntent::Read {
                        continue;
                    }
                    // Otherwise we had a read lock but need a write lock - upgrade needed

                    // Release the read lock first (find it in our locks and remove it)
                    if let Some(position) = locks
                        .iter()
                        .position(|lock: &ObjectLockGuard<'a, M>| lock.object_id() == object_id)
                    {
                        let mut lock = locks.remove(position);
                        if let Err(e) = lock.release() {
                            // If we can't release the read lock, we can't upgrade
                            return Err(e);
                        }
                    }
                }

                // Acquire the lock
                match intent.acquire_lock(object_id, &self.hash, lock_manager) {
                    Ok(lock) => {
                        locks.push(lock);
                        locked_objects.insert(*object_id, *intent);
                    }
                    Err(e) => {
                        // Release all locks we've acquired so far
                        for lock in &mut locks {
                            let _ = lock.release(); // Ignore errors during cleanup
                        }
                        return Err(e);
                    }
                }
            }
        }

        Ok(locks)
    }

    /// Execute the transaction with automatic lock acquisition and release
    ///
    /// This is a convenience method that:
    /// 1. Acquires all needed locks
    /// 2. Calls the provided execution function
    /// 3. Releases all locks when done
    ///
    /// # Parameters
    /// * `lock_manager` - The persistent lock manager to use
    /// * `exec_fn` - Function that receives a reference to this transaction and performs execution
    ///
    /// # Returns
    /// A result containing the result of the execution function if successful,
    /// or an error if any lock could not be acquired
    pub fn execute_with_locks<'a, M: PersistentLockManager, F, R>(
        &self,
        lock_manager: &'a M,
        exec_fn: F,
    ) -> Result<R, M::Error>
    where
        F: FnOnce(&Self) -> R,
    {
        // Acquire all locks
        let _locks = self.acquire_locks(lock_manager)?;

        // Execute the transaction
        let result = exec_fn(self);

        // Locks are automatically released when _locks goes out of scope
        Ok(result)
    }

    /// Create in-memory locks for testing
    #[cfg(test)]
    pub fn create_in_memory_locks<M: PersistentLockManager>(
        &self,
    ) -> Vec<ObjectLockGuard<'static, M>> {
        let mut locks = Vec::new();
        let mut locked_objects = std::collections::HashMap::new();

        for instruction in &self.instructions {
            for (object_id, intent) in &instruction.object_intents {
                // Check if we've already locked this object
                if let Some(existing_intent) = locked_objects.get(object_id) {
                    // If we already have a write lock, or if we only need a read lock, skip
                    if *existing_intent == AccessIntent::Write || *intent == AccessIntent::Read {
                        continue;
                    }
                }

                // Create in-memory lock
                locks.push(intent.create_in_memory_lock::<M>(object_id, &self.hash));
                // Remember what we locked
                locked_objects.insert(*object_id, *intent);
            }
        }

        locks
    }

    /// Check if all locks needed for this transaction can be acquired
    ///
    /// # Parameters
    /// * `lock_manager` - The persistent lock manager to use
    ///
    /// # Returns
    /// True if all locks can be acquired, false otherwise
    pub fn can_acquire_all_locks<M: PersistentLockManager>(
        &self,
        lock_manager: &M,
    ) -> Result<bool, M::Error> {
        let mut locked_objects = std::collections::HashMap::new();

        for instruction in &self.instructions {
            for (object_id, intent) in &instruction.object_intents {
                // Check if we've already checked this object
                if let Some(existing_intent) = locked_objects.get(object_id) {
                    // If we already have a write lock, or if we only need a read lock, skip
                    if *existing_intent == AccessIntent::Write || *intent == AccessIntent::Read {
                        continue;
                    }
                    // Otherwise we need to upgrade from read to write - check if we can
                }

                // Check if we can acquire the lock
                match lock_manager.can_acquire_lock(object_id, *intent, &self.hash) {
                    Ok(false) => return Ok(false),
                    Err(e) => return Err(e),
                    Ok(true) => {
                        // Remember what we checked
                        locked_objects.insert(*object_id, *intent);
                    }
                }
            }
        }

        Ok(true)
    }
}

/// Represents the before and after state of a legacy TokenizedObject in a transaction
/// 
/// This is maintained for backward compatibility.
/// New code should use UnifiedObjectEffect instead.
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

/// Represents the before and after state of a units object in a transaction
/// This supports both TokenizedObject and CodeObject for backward compatibility
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ObjectEffect {
    /// Effect on a TokenizedObject (legacy)
    Token(TransactionEffect),
    
    /// Effect on a CodeObject (legacy)
    Code {
        /// The transaction that caused this effect
        transaction_hash: TransactionHash,
        
        /// The ID of the object affected
        object_id: UnitsObjectId,
        
        /// The state of the object before the transaction (None if object was created)
        before_image: Option<CodeObject>,
        
        /// The state of the object after the transaction (None if object was deleted)
        after_image: Option<CodeObject>,
    },
    
    /// Effect on a unified UnitsObject (new model)
    Unified(UnifiedObjectEffect),
}

impl ObjectEffect {
    /// Get the transaction hash for this effect
    pub fn transaction_hash(&self) -> &TransactionHash {
        match self {
            ObjectEffect::Token(effect) => &effect.transaction_hash,
            ObjectEffect::Code { transaction_hash, .. } => transaction_hash,
            ObjectEffect::Unified(effect) => &effect.transaction_hash,
        }
    }
    
    /// Get the object ID for this effect
    pub fn object_id(&self) -> &UnitsObjectId {
        match self {
            ObjectEffect::Token(effect) => &effect.object_id,
            ObjectEffect::Code { object_id, .. } => object_id,
            ObjectEffect::Unified(effect) => &effect.object_id,
        }
    }
    
    /// Convert this effect to a UnifiedObjectEffect
    pub fn to_unified(&self) -> UnifiedObjectEffect {
        match self {
            ObjectEffect::Token(effect) => {
                let before_image = effect.before_image.as_ref().map(|obj| obj.to_units_object());
                let after_image = effect.after_image.as_ref().map(|obj| obj.to_units_object());
                
                UnifiedObjectEffect {
                    transaction_hash: effect.transaction_hash,
                    object_id: effect.object_id,
                    before_image,
                    after_image,
                }
            },
            ObjectEffect::Code { transaction_hash, object_id, before_image, after_image } => {
                let before = before_image.as_ref().map(|obj| obj.to_units_object());
                let after = after_image.as_ref().map(|obj| obj.to_units_object());
                
                UnifiedObjectEffect {
                    transaction_hash: *transaction_hash,
                    object_id: *object_id,
                    before_image: before,
                    after_image: after,
                }
            },
            ObjectEffect::Unified(effect) => effect.clone(),
        }
    }
    
    /// Check if this effect represents an object creation
    pub fn is_creation(&self) -> bool {
        match self {
            ObjectEffect::Token(effect) => effect.before_image.is_none() && effect.after_image.is_some(),
            ObjectEffect::Code { before_image, after_image, .. } => before_image.is_none() && after_image.is_some(),
            ObjectEffect::Unified(effect) => effect.before_image.is_none() && effect.after_image.is_some(),
        }
    }
    
    /// Check if this effect represents an object deletion
    pub fn is_deletion(&self) -> bool {
        match self {
            ObjectEffect::Token(effect) => effect.before_image.is_some() && effect.after_image.is_none(),
            ObjectEffect::Code { before_image, after_image, .. } => before_image.is_some() && after_image.is_none(),
            ObjectEffect::Unified(effect) => effect.before_image.is_some() && effect.after_image.is_none(),
        }
    }
    
    /// Check if this effect represents an object modification
    pub fn is_modification(&self) -> bool {
        match self {
            ObjectEffect::Token(effect) => effect.before_image.is_some() && effect.after_image.is_some(),
            ObjectEffect::Code { before_image, after_image, .. } => before_image.is_some() && after_image.is_some(),
            ObjectEffect::Unified(effect) => effect.before_image.is_some() && effect.after_image.is_some(),
        }
    }
}

/// Represents the before and after state of a unified UnitsObject in a transaction
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UnifiedObjectEffect {
    /// The transaction that caused this effect
    pub transaction_hash: TransactionHash,
    
    /// The ID of the object affected
    pub object_id: UnitsObjectId,
    
    /// The state of the object before the transaction (None if object was created)
    pub before_image: Option<UnitsObject>,
    
    /// The state of the object after the transaction (None if object was deleted)
    pub after_image: Option<UnitsObject>,
}

impl UnifiedObjectEffect {
    /// Create a new effect for object creation
    pub fn new_creation(
        transaction_hash: TransactionHash,
        object: UnitsObject,
    ) -> Self {
        Self {
            transaction_hash,
            object_id: *object.id(),
            before_image: None,
            after_image: Some(object),
        }
    }
    
    /// Create a new effect for object deletion
    pub fn new_deletion(
        transaction_hash: TransactionHash,
        object: UnitsObject,
    ) -> Self {
        Self {
            transaction_hash,
            object_id: *object.id(),
            before_image: Some(object),
            after_image: None,
        }
    }
    
    /// Create a new effect for object modification
    pub fn new_modification(
        transaction_hash: TransactionHash,
        before: UnitsObject,
        after: UnitsObject,
    ) -> Self {
        Self {
            transaction_hash,
            object_id: *after.id(),
            before_image: Some(before),
            after_image: Some(after),
        }
    }
    
    /// Check if this effect represents an object creation
    pub fn is_creation(&self) -> bool {
        self.before_image.is_none() && self.after_image.is_some()
    }
    
    /// Check if this effect represents an object deletion
    pub fn is_deletion(&self) -> bool {
        self.before_image.is_some() && self.after_image.is_none()
    }
    
    /// Check if this effect represents an object modification
    pub fn is_modification(&self) -> bool {
        self.before_image.is_some() && self.after_image.is_some()
    }
    
    /// Convert to legacy TransactionEffect if possible
    pub fn to_legacy_token_effect(&self) -> Option<TransactionEffect> {
        match (&self.before_image, &self.after_image) {
            (Some(before), Some(after)) if before.is_token() && after.is_token() => {
                // Both before and after are tokens - create a modification effect
                let before_obj = self.before_image_as_token()?;
                let after_obj = self.after_image_as_token()?;
                
                Some(TransactionEffect {
                    transaction_hash: self.transaction_hash,
                    object_id: self.object_id,
                    before_image: Some(before_obj),
                    after_image: Some(after_obj),
                })
            },
            (None, Some(after)) if after.is_token() => {
                // Token creation
                let after_obj = self.after_image_as_token()?;
                
                Some(TransactionEffect {
                    transaction_hash: self.transaction_hash,
                    object_id: self.object_id,
                    before_image: None,
                    after_image: Some(after_obj),
                })
            },
            (Some(before), None) if before.is_token() => {
                // Token deletion
                let before_obj = self.before_image_as_token()?;
                
                Some(TransactionEffect {
                    transaction_hash: self.transaction_hash,
                    object_id: self.object_id,
                    before_image: Some(before_obj),
                    after_image: None,
                })
            },
            _ => None,
        }
    }
    
    /// Convert before image to TokenizedObject if possible
    fn before_image_as_token(&self) -> Option<TokenizedObject> {
        if let Some(before) = &self.before_image {
            if before.is_token() {
                if let (Some(token_type), Some(token_manager)) = (before.token_type(), before.token_manager()) {
                    return Some(TokenizedObject::new(
                        *before.id(),
                        *before.owner(),
                        token_type,
                        *token_manager,
                        before.data().to_vec(),
                    ));
                }
            }
        }
        None
    }
    
    /// Convert after image to TokenizedObject if possible
    fn after_image_as_token(&self) -> Option<TokenizedObject> {
        if let Some(after) = &self.after_image {
            if after.is_token() {
                if let (Some(token_type), Some(token_manager)) = (after.token_type(), after.token_manager()) {
                    return Some(TokenizedObject::new(
                        *after.id(),
                        *after.owner(),
                        token_type,
                        *token_manager,
                        after.data().to_vec(),
                    ));
                }
            }
        }
        None
    }
}

/// A receipt of a processed transaction, containing all proofs of object modifications
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionReceipt {
    /// The hash of the transaction that was executed
    pub transaction_hash: TransactionHash,

    /// The slot in which this transaction was processed
    pub slot: u64,

    /// Map of object IDs to their state proofs after the transaction
    /// This is a simplified representation; implementations will use appropriate proof types
    pub object_proofs: HashMap<UnitsObjectId, Vec<u8>>,

    /// Whether the transaction was executed successfully
    pub success: bool,

    /// Timestamp when the transaction was processed
    pub timestamp: u64,

    /// The commitment level of this transaction
    pub commitment_level: CommitmentLevel,

    /// Any error message from the execution (if not successful)
    pub error_message: Option<String>,

    /// Legacy effects for backward compatibility (TokenizedObject only)
    pub effects: Vec<TransactionEffect>,
    
    /// New generic effects that support both TokenizedObject and CodeObject
    #[serde(default)]
    pub object_effects: Vec<ObjectEffect>,
}

impl TransactionReceipt {
    /// Create a new transaction receipt
    pub fn new(
        transaction_hash: TransactionHash,
        slot: u64,
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
            object_effects: Vec::new(),
        }
    }

    /// Create a new transaction receipt with a specific commitment level
    pub fn with_commitment_level(
        transaction_hash: TransactionHash,
        slot: u64,
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
            object_effects: Vec::new(),
        }
    }

    /// Add an object proof to the receipt
    pub fn add_proof(&mut self, object_id: UnitsObjectId, proof: Vec<u8>) {
        self.object_proofs.insert(object_id, proof);
    }

    /// Add a legacy transaction effect to the receipt
    pub fn add_effect(&mut self, effect: TransactionEffect) {
        // Add to both legacy effects and new object_effects for backward compatibility
        self.object_effects.push(ObjectEffect::Token(effect.clone()));
        self.effects.push(effect);
    }

    /// Add a legacy code object effect to the receipt
    pub fn add_code_effect(
        &mut self,
        transaction_hash: TransactionHash,
        object_id: UnitsObjectId,
        before_image: Option<CodeObject>,
        after_image: Option<CodeObject>,
    ) {
        self.object_effects.push(ObjectEffect::Code {
            transaction_hash,
            object_id,
            before_image,
            after_image,
        });
    }

    /// Add a unified object effect to the receipt
    pub fn add_unified_effect(
        &mut self,
        transaction_hash: TransactionHash,
        object_id: UnitsObjectId,
        before_image: Option<UnitsObject>,
        after_image: Option<UnitsObject>,
    ) {
        // Clone the before/after images for backward compatibility
        let before_clone = before_image.clone();
        let after_clone = after_image.clone();
        
        let unified_effect = UnifiedObjectEffect {
            transaction_hash,
            object_id,
            before_image,
            after_image,
        };
        
        self.object_effects.push(ObjectEffect::Unified(unified_effect));
        
        // Add to legacy effects for backward compatibility if possible
        if let Some(after) = &after_clone {
            if after.is_token() {
                // Try to convert to a legacy TokenizedObject if it's a token type
                if let (Some(token_type), Some(token_manager)) = (after.token_type(), after.token_manager()) {
                    let token_obj = TokenizedObject::new(
                        *after.id(),
                        *after.owner(),
                        token_type,
                        *token_manager,
                        after.data().to_vec(),
                    );
                    
                    // Create before image if it exists
                    let before_token = before_clone.as_ref().and_then(|before| {
                        if before.is_token() {
                            if let (Some(token_type), Some(token_manager)) = (before.token_type(), before.token_manager()) {
                                Some(TokenizedObject::new(
                                    *before.id(),
                                    *before.owner(),
                                    token_type,
                                    *token_manager,
                                    before.data().to_vec(),
                                ))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    });
                    
                    let legacy_effect = TransactionEffect {
                        transaction_hash,
                        object_id,
                        before_image: before_token,
                        after_image: Some(token_obj),
                    };
                    
                    self.effects.push(legacy_effect);
                }
            }
        } else if before_clone.is_some() {
            // Object was deleted - add legacy effect if it was a token
            if let Some(before) = &before_clone {
                if before.is_token() {
                    if let (Some(token_type), Some(token_manager)) = (before.token_type(), before.token_manager()) {
                        let token_obj = TokenizedObject::new(
                            *before.id(),
                            *before.owner(),
                            token_type,
                            *token_manager,
                            before.data().to_vec(),
                        );
                        
                        let legacy_effect = TransactionEffect {
                            transaction_hash,
                            object_id,
                            before_image: Some(token_obj),
                            after_image: None,
                        };
                        
                        self.effects.push(legacy_effect);
                    }
                }
            }
        }
    }

    /// Add a generic object effect to the receipt
    pub fn add_object_effect(&mut self, effect: ObjectEffect) {
        // If it's a TokenizedObject effect, also add to legacy effects for backward compatibility
        if let ObjectEffect::Token(token_effect) = &effect {
            self.effects.push(token_effect.clone());
        }
        self.object_effects.push(effect);
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
    
    /// Get the total number of effects (both token and code objects)
    pub fn effect_count(&self) -> usize {
        self.object_effects.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::objects::{UnitsObject, TokenType};
    use crate::id::UnitsObjectId;
    
    #[test]
    fn test_unified_object_effect() {
        // Create an ID for testing
        let id = UnitsObjectId::new([1; 32]);
        let owner = UnitsObjectId::new([2; 32]);
        let token_manager = UnitsObjectId::new([3; 32]);
        let data = vec![0, 1, 2, 3, 4];
        let transaction_hash = [4; 32];
        
        // Create a token object
        let token_obj = UnitsObject::new_token(
            id,
            owner,
            TokenType::Native,
            token_manager,
            data.clone(),
        );
        
        // Create a token creation effect
        let creation_effect = UnifiedObjectEffect::new_creation(
            transaction_hash,
            token_obj.clone(),
        );
        
        // Check the effect properties
        assert!(creation_effect.is_creation());
        assert!(!creation_effect.is_deletion());
        assert!(!creation_effect.is_modification());
        assert_eq!(creation_effect.object_id, id);
        assert_eq!(creation_effect.transaction_hash, transaction_hash);
        assert_eq!(creation_effect.before_image, None);
        assert!(creation_effect.after_image.is_some());
        
        // Check conversion to legacy
        let legacy_effect = creation_effect.to_legacy_token_effect();
        assert!(legacy_effect.is_some());
        let legacy = legacy_effect.unwrap();
        assert_eq!(legacy.transaction_hash, transaction_hash);
        assert_eq!(legacy.object_id, id);
        assert_eq!(legacy.before_image, None);
        assert!(legacy.after_image.is_some());
        
        // Create a modified object
        let mut modified_obj = token_obj.clone();
        modified_obj.data = vec![5, 6, 7, 8, 9];
        
        // Create a modification effect
        let modification_effect = UnifiedObjectEffect::new_modification(
            transaction_hash,
            token_obj.clone(),
            modified_obj.clone(),
        );
        
        // Check the effect properties
        assert!(!modification_effect.is_creation());
        assert!(!modification_effect.is_deletion());
        assert!(modification_effect.is_modification());
        
        // Create a deletion effect
        let deletion_effect = UnifiedObjectEffect::new_deletion(
            transaction_hash,
            token_obj.clone(),
        );
        
        // Check the effect properties
        assert!(!deletion_effect.is_creation());
        assert!(deletion_effect.is_deletion());
        assert!(!deletion_effect.is_modification());
    }
    
    #[test]
    fn test_object_effect_conversion() {
        // Create an ID for testing
        let id = UnitsObjectId::new([1; 32]);
        let owner = UnitsObjectId::new([2; 32]);
        let token_manager = UnitsObjectId::new([3; 32]);
        let data = vec![0, 1, 2, 3, 4];
        let transaction_hash = [4; 32];
        
        // Create objects
        let token_obj = UnitsObject::new_token(
            id,
            owner,
            TokenType::Native,
            token_manager,
            data.clone(),
        );
        
        let code_obj = UnitsObject::new_code(
            id,
            owner,
            data.clone(),
            crate::transaction::RuntimeType::Wasm,
            "main".to_string(),
        );
        
        // Create different effect types
        let token_effect = UnifiedObjectEffect::new_creation(
            transaction_hash,
            token_obj.clone(),
        );
        
        let code_effect = UnifiedObjectEffect::new_creation(
            transaction_hash,
            code_obj.clone(),
        );
        
        // Create ObjectEffect enum variants
        let obj_effect_token = ObjectEffect::Unified(token_effect);
        let obj_effect_code = ObjectEffect::Unified(code_effect);
        
        // Convert to unified and check properties
        let unified_token = obj_effect_token.to_unified();
        let unified_code = obj_effect_code.to_unified();
        
        assert!(unified_token.after_image.as_ref().unwrap().is_token());
        assert!(unified_code.after_image.as_ref().unwrap().is_code());
        
        // Check effect state helpers
        assert!(obj_effect_token.is_creation());
        assert!(!obj_effect_token.is_deletion());
        assert!(!obj_effect_token.is_modification());
    }
    
    #[test]
    fn test_transaction_receipt_unified_effects() {
        // Create an ID for testing
        let id = UnitsObjectId::new([1; 32]);
        let owner = UnitsObjectId::new([2; 32]);
        let token_manager = UnitsObjectId::new([3; 32]);
        let data = vec![0, 1, 2, 3, 4];
        let transaction_hash = [4; 32];
        
        // Create a token object
        let token_obj = UnitsObject::new_token(
            id,
            owner,
            TokenType::Native,
            token_manager,
            data.clone(),
        );
        
        // Create a transaction receipt
        let mut receipt = TransactionReceipt::new(
            transaction_hash,
            1234, // slot
            true, // success
            56789, // timestamp
        );
        
        // Add a unified effect
        receipt.add_unified_effect(
            transaction_hash,
            id,
            None,
            Some(token_obj.clone()),
        );
        
        // Check that the receipt contains both the unified effect and legacy effect
        assert_eq!(receipt.effect_count(), 1);
        assert_eq!(receipt.effects.len(), 1);
        
        // Create a code object
        let code_obj = UnitsObject::new_code(
            id,
            owner,
            data.clone(),
            crate::transaction::RuntimeType::Wasm,
            "main".to_string(),
        );
        
        // Add a code effect
        receipt.add_unified_effect(
            transaction_hash,
            id,
            Some(token_obj.clone()),
            Some(code_obj.clone()),
        );
        
        // Check that we have two effects in the effects list (token creation and token-to-code conversion)
        assert_eq!(receipt.effect_count(), 2);
        assert_eq!(receipt.effects.len(), 1); // Only 1 legacy effect since converting token to code doesn't create a legacy effect
    }
}
