//! Host environment for guest program execution

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use log::{debug, error, info, warn};
use serde_json;

use units_core::id::UnitsObjectId;
use units_core::objects::UnitsObject;
use units_core::transaction::TransactionHash;

/// Host environment interface for guest programs
pub trait HostEnvironment: Send + Sync {
    /// Log a message from guest program (0=debug, 1=info, 2=warn, 3=error)
    fn log(&self, level: u8, message: &str);
    
    /// Store objects modified by the guest program
    fn store_modified_objects(
        &mut self,
        objects: HashMap<UnitsObjectId, UnitsObject>,
    ) -> Result<usize>;
    
    // Accessors for guest program
    fn get_serialized_objects(&self) -> &[u8];
    fn get_transaction_hash(&self) -> &[u8; 32];
    fn get_serialized_parameters(&self) -> &[u8];
    fn get_program_id(&self) -> Option<&UnitsObjectId>;
    fn get_instruction_params(&self) -> &[u8];
    fn get_objects(&self) -> &HashMap<UnitsObjectId, UnitsObject>;
    fn get_objects_mut(&mut self) -> &mut HashMap<UnitsObjectId, UnitsObject>;
    fn get_modified_objects(&self) -> &HashMap<UnitsObjectId, UnitsObject>;
}

/// Standard implementation of host environment
pub struct StandardHostEnvironment {
    objects: HashMap<UnitsObjectId, UnitsObject>,
    modified_objects: HashMap<UnitsObjectId, UnitsObject>,
    transaction_hash: [u8; 32],
    serialized_objects: Vec<u8>,
    serialized_parameters: Vec<u8>,
    program_id: Option<UnitsObjectId>,
    instruction_params: Vec<u8>,
}

impl StandardHostEnvironment {
    /// Create a new host environment
    pub fn new(
        objects: HashMap<UnitsObjectId, UnitsObject>,
        parameters: &HashMap<String, String>,
        transaction_hash: [u8; 32],
        program_id: Option<UnitsObjectId>,
        instruction_params: &[u8],
    ) -> Result<Self> {
        // Serialize data for guest program access
        let serialized_objects = serde_json::to_vec(&objects)
            .map_err(|e| anyhow!("Failed to serialize objects: {}", e))?;

        let serialized_parameters = serde_json::to_vec(parameters)
            .map_err(|e| anyhow!("Failed to serialize parameters: {}", e))?;

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

    /// Create environment for program execution
    pub fn for_program(
        program_id: UnitsObjectId,
        args: &[u8],
        objects: HashMap<UnitsObjectId, UnitsObject>,
        parameters: &HashMap<String, String>,
        transaction_hash: &TransactionHash,
    ) -> Result<Self> {
        Self::new(objects, parameters, *transaction_hash, Some(program_id), args)
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
        objects: HashMap<UnitsObjectId, UnitsObject>,
    ) -> Result<usize> {
        let mut count = 0;

        for (_id, object) in objects {
            // Validate object
            if *object.id() == UnitsObjectId::default() {
                return Err(anyhow!("Object ID cannot be null"));
            }

            // Verify object exists in context
            if !self.objects.contains_key(object.id()) {
                return Err(anyhow!("Cannot modify non-existent object: {}", object.id()));
            }

            // Store modified object
            self.modified_objects.insert(*object.id(), object);
            count += 1;
        }

        Ok(count)
    }

    // Implement accessors
    fn get_serialized_objects(&self) -> &[u8] { &self.serialized_objects }
    fn get_transaction_hash(&self) -> &[u8; 32] { &self.transaction_hash }
    fn get_serialized_parameters(&self) -> &[u8] { &self.serialized_parameters }
    fn get_program_id(&self) -> Option<&UnitsObjectId> { self.program_id.as_ref() }
    fn get_instruction_params(&self) -> &[u8] { &self.instruction_params }
    fn get_objects(&self) -> &HashMap<UnitsObjectId, UnitsObject> { &self.objects }
    fn get_objects_mut(&mut self) -> &mut HashMap<UnitsObjectId, UnitsObject> { &mut self.objects }
    fn get_modified_objects(&self) -> &HashMap<UnitsObjectId, UnitsObject> { &self.modified_objects }
}

/// Create a standard host environment
pub fn create_host_environment(
    objects: HashMap<UnitsObjectId, UnitsObject>,
    parameters: &HashMap<String, String>,
    transaction_hash: [u8; 32],
    program_id: Option<UnitsObjectId>,
    instruction_params: &[u8],
) -> Result<Arc<dyn HostEnvironment>> {
    let env = StandardHostEnvironment::new(
        objects, 
        parameters, 
        transaction_hash, 
        program_id, 
        instruction_params
    )?;
    
    Ok(Arc::new(env))
}

/// Create a standard host environment (alias for create_host_environment)
pub fn create_standard_host_environment(
    objects: HashMap<UnitsObjectId, UnitsObject>,
    parameters: &HashMap<String, String>,
    transaction_hash: [u8; 32],
    program_id: Option<UnitsObjectId>,
    instruction_params: &[u8],
) -> Result<Arc<dyn HostEnvironment>> {
    create_host_environment(
        objects,
        parameters,
        transaction_hash,
        program_id,
        instruction_params
    )
}
