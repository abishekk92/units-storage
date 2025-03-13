use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use units_core::id::UnitsObjectId;
use units_core::objects::TokenizedObject;
use units_core::transaction::{Instruction, InstructionType, TransactionHash};

/// Result type for instruction execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InstructionResult {
    /// Success result with a map of updated objects
    Success(HashMap<UnitsObjectId, TokenizedObject>),
    /// Error with message
    Error(String),
}

/// The execution context provided to an instruction
#[derive(Debug, Clone)]
pub struct InstructionContext<'a> {
    /// The transaction hash for the current execution
    pub transaction_hash: &'a TransactionHash,
    
    /// The objects that this instruction has access to (read or write)
    pub objects: HashMap<UnitsObjectId, TokenizedObject>,
    
    /// Additional parameters available to the instruction
    pub parameters: HashMap<String, String>,
}

/// Trait defining the interface for a runtime backend
pub trait RuntimeBackend: Send + Sync {
    /// Get a string identifier for this runtime backend
    fn name(&self) -> &str;
    
    /// Check if this runtime can execute the provided instruction
    fn can_execute(&self, instruction: &Instruction) -> bool;
    
    /// Execute an instruction with the provided context
    /// 
    /// This method takes an instruction and its execution context and returns either
    /// a map of modified objects or an error.
    fn execute<'a>(
        &self,
        instruction: &Instruction,
        context: InstructionContext<'a>,
    ) -> InstructionResult;
}

/// Factory trait for creating runtime backends
pub trait RuntimeBackendFactory: Send + Sync {
    /// Create a new instance of the runtime backend
    fn create(&self) -> Box<dyn RuntimeBackend>;
    
    /// Get the name of the runtime backend this factory creates
    fn backend_name(&self) -> &str;
    
    /// Get the instruction type this factory's backends can execute
    fn instruction_type(&self) -> InstructionType;
}

/// Error returned when execution fails
#[derive(Debug, thiserror::Error)]
pub enum ExecutionError {
    #[error("No runtime backend available for instruction type: {0:?}")]
    NoBackendAvailable(InstructionType),
    
    #[error("Execution failed: {0}")]
    ExecutionFailed(String),
    
    #[error("Object not found in execution context: {0}")]
    ObjectNotFound(UnitsObjectId),
}

/// Runtime registry that manages available runtime backends
pub struct RuntimeRegistry {
    /// Map of instruction types to their runtime backend factories
    backends: RwLock<HashMap<InstructionType, Arc<dyn RuntimeBackendFactory>>>,
}

impl RuntimeRegistry {
    /// Create a new runtime registry
    pub fn new() -> Self {
        Self {
            backends: RwLock::new(HashMap::new()),
        }
    }
    
    /// Register a runtime backend factory
    pub fn register_backend(&self, factory: Arc<dyn RuntimeBackendFactory>) {
        let mut backends = self.backends.write().unwrap();
        backends.insert(factory.instruction_type(), factory);
    }
    
    /// Get a runtime backend for a specific instruction type
    pub fn get_backend(&self, instruction_type: InstructionType) -> Option<Box<dyn RuntimeBackend>> {
        let backends = self.backends.read().unwrap();
        backends.get(&instruction_type).map(|factory| factory.create())
    }
    
    /// Get a runtime backend for a specific instruction
    pub fn get_backend_for_instruction(&self, instruction: &Instruction) -> Option<Box<dyn RuntimeBackend>> {
        self.get_backend(instruction.instruction_type)
    }
    
    /// Check if a runtime backend is available for a specific instruction type
    pub fn has_backend(&self, instruction_type: InstructionType) -> bool {
        let backends = self.backends.read().unwrap();
        backends.contains_key(&instruction_type)
    }
    
    /// List all available backend types
    pub fn available_backends(&self) -> Vec<InstructionType> {
        let backends = self.backends.read().unwrap();
        backends.keys().copied().collect()
    }
    
    /// Execute an instruction using the appropriate backend
    pub fn execute_instruction<'a>(
        &self,
        instruction: &Instruction,
        context: InstructionContext<'a>,
    ) -> Result<HashMap<UnitsObjectId, TokenizedObject>, ExecutionError> {
        // Get the appropriate backend for this instruction
        let backend = self.get_backend_for_instruction(instruction)
            .ok_or_else(|| ExecutionError::NoBackendAvailable(instruction.instruction_type))?;
        
        // Execute the instruction
        match backend.execute(instruction, context) {
            InstructionResult::Success(objects) => Ok(objects),
            InstructionResult::Error(message) => Err(ExecutionError::ExecutionFailed(message)),
        }
    }
}

/// Example implementation of a WebAssembly runtime backend
/// In a real system, this would use a WebAssembly runtime like wasmtime or wasmer
pub struct WasmRuntimeBackend {
    name: String,
}

impl WasmRuntimeBackend {
    pub fn new() -> Self {
        Self {
            name: "WebAssembly Runtime".to_string(),
        }
    }
}

impl RuntimeBackend for WasmRuntimeBackend {
    fn name(&self) -> &str {
        &self.name
    }
    
    fn can_execute(&self, instruction: &Instruction) -> bool {
        instruction.instruction_type == InstructionType::Wasm
    }
    
    fn execute<'a>(
        &self,
        _instruction: &Instruction,
        _context: InstructionContext<'a>,
    ) -> InstructionResult {
        // In a real implementation, we would:
        // 1. Set up a WebAssembly runtime environment (wasmtime, wasmer, etc.)
        // 2. Load the WebAssembly module from instruction.data
        // 3. Serialize the context.objects to pass to the WebAssembly module
        // 4. Execute the module, providing access to the serialized objects
        // 5. Deserialize any objects modified by the module
        
        // For this example, we'll just return a mock result
        InstructionResult::Error("WebAssembly execution not implemented yet".to_string())
    }
}

/// Factory for creating WebAssembly runtime backends
pub struct WasmRuntimeBackendFactory;

impl RuntimeBackendFactory for WasmRuntimeBackendFactory {
    fn create(&self) -> Box<dyn RuntimeBackend> {
        Box::new(WasmRuntimeBackend::new())
    }
    
    fn backend_name(&self) -> &str {
        "WebAssembly Runtime"
    }
    
    fn instruction_type(&self) -> InstructionType {
        InstructionType::Wasm
    }
}

/// Example implementation of an eBPF runtime backend
/// In a real system, this would use an eBPF runtime like libbpf or aya
pub struct EbpfRuntimeBackend {
    name: String,
}

impl EbpfRuntimeBackend {
    pub fn new() -> Self {
        Self {
            name: "eBPF Runtime".to_string(),
        }
    }
}

impl RuntimeBackend for EbpfRuntimeBackend {
    fn name(&self) -> &str {
        &self.name
    }
    
    fn can_execute(&self, instruction: &Instruction) -> bool {
        instruction.instruction_type == InstructionType::Ebpf
    }
    
    fn execute<'a>(
        &self,
        _instruction: &Instruction,
        _context: InstructionContext<'a>,
    ) -> InstructionResult {
        // In a real implementation, we would:
        // 1. Set up an eBPF runtime environment
        // 2. Load the eBPF bytecode from instruction.data
        // 3. Set up memory maps for the objects in context
        // 4. Execute the eBPF program with the provided context
        // 5. Collect any modified objects from the memory maps
        
        // For this example, we'll just return a mock result
        InstructionResult::Error("eBPF execution not implemented yet".to_string())
    }
}

/// Factory for creating eBPF runtime backends
pub struct EbpfRuntimeBackendFactory;

impl RuntimeBackendFactory for EbpfRuntimeBackendFactory {
    fn create(&self) -> Box<dyn RuntimeBackend> {
        Box::new(EbpfRuntimeBackend::new())
    }
    
    fn backend_name(&self) -> &str {
        "eBPF Runtime"
    }
    
    fn instruction_type(&self) -> InstructionType {
        InstructionType::Ebpf
    }
}