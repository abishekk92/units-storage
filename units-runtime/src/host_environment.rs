//! Host environment implementations for different runtime backends
//!
//! This module provides concrete implementations of the HostEnvironment trait
//! for various runtime backends, enabling guest programs to interact with host
//! resources in a controlled manner.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use log::{debug, error, info, warn};
use serde_json;

use units_core::id::UnitsObjectId;
use units_core::objects::TokenizedObject;
use units_core::transaction::TransactionHash;

/// A trait that abstracts the host machine functionalities for runtime backends
///
/// This trait provides a standard interface for guest programs to interact with
/// host resources in a controlled manner. It allows for different implementations
/// of host environments (e.g., sandboxed, testing, production) while maintaining
/// a consistent interface for runtime backends.
pub trait HostEnvironment: Send + Sync {
    /// Log a message from the guest program at the specified level
    /// - level 0: debug
    /// - level 1: info
    /// - level 2: warn
    /// - level 3: error
    fn log(&self, level: u8, message: &str);

    /// Store modified objects from guest program execution
    /// Returns the number of objects successfully stored
    fn store_modified_objects(
        &mut self,
        objects: HashMap<UnitsObjectId, TokenizedObject>,
    ) -> Result<usize>;

    /// Get the serialized objects data accessible to the guest program
    fn get_serialized_objects(&self) -> &[u8];

    /// Get the transaction hash for the current execution
    fn get_transaction_hash(&self) -> &[u8; 32];

    /// Get the serialized parameters for the guest program
    fn get_serialized_parameters(&self) -> &[u8];

    /// Get the program ID if the execution is in a program context
    fn get_program_id(&self) -> Option<&UnitsObjectId>;

    /// Get the instruction parameters for the guest program
    fn get_instruction_params(&self) -> &[u8];

    /// Get the raw objects that this environment has access to
    fn get_objects(&self) -> &HashMap<UnitsObjectId, TokenizedObject>;

    /// Get mutable access to the objects this environment has access to
    fn get_objects_mut(&mut self) -> &mut HashMap<UnitsObjectId, TokenizedObject>;

    /// Get the object modifications that have been stored during execution
    fn get_modified_objects(&self) -> &HashMap<UnitsObjectId, TokenizedObject>;
}

/// Standard implementation of the host environment for all runtime backends
pub struct StandardHostEnvironment {
    /// Objects accessible to the guest program
    objects: HashMap<UnitsObjectId, TokenizedObject>,

    /// Objects modified by the guest program
    modified_objects: HashMap<UnitsObjectId, TokenizedObject>,

    /// Transaction hash for the current execution
    transaction_hash: [u8; 32],

    /// Serialized objects (JSON format)
    serialized_objects: Vec<u8>,

    /// Serialized parameters (JSON format)
    serialized_parameters: Vec<u8>,

    /// Program ID if execution is in a program context
    program_id: Option<UnitsObjectId>,

    /// Raw instruction parameters
    instruction_params: Vec<u8>,
}

impl StandardHostEnvironment {
    /// Create a new host environment with the given objects and parameters
    pub fn new(
        objects: HashMap<UnitsObjectId, TokenizedObject>,
        parameters: &HashMap<String, String>,
        transaction_hash: [u8; 32],
        program_id: Option<UnitsObjectId>,
        instruction_params: &[u8],
    ) -> Result<Self> {
        // Serialize objects to JSON format
        let serialized_objects = serde_json::to_vec(&objects)
            .map_err(|e| anyhow!("Failed to serialize objects to JSON: {}", e))?;

        // Serialize parameters to JSON format
        let serialized_parameters = serde_json::to_vec(parameters)
            .map_err(|e| anyhow!("Failed to serialize parameters to JSON: {}", e))?;

        Ok(Self {
            objects,
            modified_objects: HashMap::new(),
            transaction_hash,
            serialized_objects,
            serialized_parameters,
            program_id,
            instruction_params: instruction_params.to_vec(),
        })
    }

    // This method is removed as we no longer support direct instruction execution

    /// Create a host environment for program execution
    pub fn for_program(
        program_id: UnitsObjectId,
        args: &[u8],
        objects: HashMap<UnitsObjectId, TokenizedObject>,
        parameters: &HashMap<String, String>,
        transaction_hash: &TransactionHash,
    ) -> Result<Self> {
        Self::new(
            objects,
            parameters,
            *transaction_hash,
            Some(program_id),
            args,
        )
    }
}

impl HostEnvironment for StandardHostEnvironment {
    fn log(&self, level: u8, message: &str) {
        match level {
            0 => debug!("[GUEST] {}", message),
            1 => info!("[GUEST] {}", message),
            2 => warn!("[GUEST] {}", message),
            3 => error!("[GUEST] {}", message),
            _ => debug!("[GUEST] {}", message),
        }
    }

    fn store_modified_objects(
        &mut self,
        objects: HashMap<UnitsObjectId, TokenizedObject>,
    ) -> Result<usize> {
        let mut count = 0;

        for (_id, object) in objects {
            // Validate the object
            if object.id == UnitsObjectId::default() {
                return Err(anyhow!("Object ID cannot be null"));
            }

            // Check that the object exists in the context
            if !self.objects.contains_key(&object.id) {
                return Err(anyhow!("Cannot modify non-existent object: {}", object.id));
            }

            // Store the modified object
            self.modified_objects.insert(object.id, object);
            count += 1;
        }

        // Return the number of objects stored
        Ok(count)
    }

    fn get_serialized_objects(&self) -> &[u8] {
        &self.serialized_objects
    }

    fn get_transaction_hash(&self) -> &[u8; 32] {
        &self.transaction_hash
    }

    fn get_serialized_parameters(&self) -> &[u8] {
        &self.serialized_parameters
    }

    fn get_program_id(&self) -> Option<&UnitsObjectId> {
        self.program_id.as_ref()
    }

    fn get_instruction_params(&self) -> &[u8] {
        &self.instruction_params
    }

    fn get_objects(&self) -> &HashMap<UnitsObjectId, TokenizedObject> {
        &self.objects
    }

    fn get_objects_mut(&mut self) -> &mut HashMap<UnitsObjectId, TokenizedObject> {
        &mut self.objects
    }

    fn get_modified_objects(&self) -> &HashMap<UnitsObjectId, TokenizedObject> {
        &self.modified_objects
    }
}

/// Factory function to create a standard host environment
pub fn create_standard_host_environment(
    objects: HashMap<UnitsObjectId, TokenizedObject>,
    parameters: &HashMap<String, String>,
    transaction_hash: [u8; 32],
    program_id: Option<UnitsObjectId>,
    instruction_params: &[u8],
) -> Result<Arc<dyn HostEnvironment>> {
    let host_env = StandardHostEnvironment::new(
        objects,
        parameters,
        transaction_hash,
        program_id,
        instruction_params,
    )?;

    Ok(Arc::new(host_env))
}
