use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use units_core::id::UnitsObjectId;
use units_core::objects::UnitsObject;
use units_core::transaction::{Instruction, RuntimeType, TransactionHash};

//==============================================================================
// INSTRUCTION EXECUTION
//==============================================================================

/// Result of a program execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InstructionResult {
    /// Success with updated objects
    Success(HashMap<UnitsObjectId, UnitsObject>),
    /// Error with message
    Error(String),
}

/// Execution context for a program or instruction
#[derive(Debug, Clone)]
pub struct InstructionContext<'a> {
    /// Transaction hash for the execution
    pub transaction_hash: &'a TransactionHash,
    /// Objects accessible to the instruction
    pub objects: HashMap<UnitsObjectId, UnitsObject>,
    /// Additional parameters
    pub parameters: HashMap<String, String>,
    /// Program object ID (for program calls)
    pub program_id: Option<UnitsObjectId>,
    /// Entrypoint function to call
    pub entrypoint: Option<String>,
}

/// Error returned when execution fails
#[derive(Debug, thiserror::Error)]
pub enum ExecutionError {
    #[error("No runtime backend available for type: {0:?}")]
    NoBackendAvailableForRuntime(RuntimeType),
    
    #[error("Execution failed: {0}")]
    ExecutionFailed(String),
    
    #[error("Object not found: {0}")]
    ObjectNotFound(UnitsObjectId),
    
    #[error("Program not found: {0}")]
    ProgramNotFound(UnitsObjectId),
    
    #[error("Invalid program: {0}")]
    InvalidProgram(UnitsObjectId),
    
    #[error("Invalid entrypoint: {0}")]
    InvalidEntrypoint(String),
}

//==============================================================================
// RUNTIME BACKEND
//==============================================================================

/// Interface for a runtime backend that can execute programs
pub trait RuntimeBackend: Send + Sync {
    /// Get the backend's name
    fn name(&self) -> &str;

    /// Get the runtime type this backend handles
    fn runtime_type(&self) -> RuntimeType;

    /// Execute a program with the given context
    fn execute_program<'a>(
        &self,
        program: &UnitsObject,
        entrypoint: &str,
        args: &[u8],
        context: InstructionContext<'a>,
    ) -> InstructionResult;
}

//==============================================================================
// RUNTIME BACKEND MANAGER
//==============================================================================

/// Manages different runtime backend implementations
pub struct RuntimeBackendManager {
    /// Available runtime backends by type
    backends_by_runtime: HashMap<RuntimeType, Arc<dyn RuntimeBackend>>,
    /// Default runtime type to use when not specified
    default_runtime: RuntimeType,
}

impl RuntimeBackendManager {
    /// Create a new runtime backend manager
    pub fn new() -> Self {
        Self {
            backends_by_runtime: HashMap::new(),
            default_runtime: RuntimeType::Wasm,
        }
    }

    /// Register a runtime backend
    pub fn register_backend(&mut self, backend: Arc<dyn RuntimeBackend>) {
        self.backends_by_runtime.insert(backend.runtime_type(), backend);
    }

    /// Set the default runtime type
    pub fn set_default_runtime(&mut self, runtime_type: RuntimeType) {
        self.default_runtime = runtime_type;
    }

    /// Get the default runtime type
    pub fn default_runtime_type(&self) -> RuntimeType {
        self.default_runtime
    }

    /// Get a runtime backend for a specific type
    pub fn get_backend_for_runtime(
        &self,
        runtime_type: RuntimeType,
    ) -> Option<Arc<dyn RuntimeBackend>> {
        self.backends_by_runtime.get(&runtime_type).cloned()
    }

    /// Execute a program call instruction
    pub fn execute_program_call<'a>(
        &self,
        program_id: &UnitsObjectId,
        instruction: &Instruction,
        mut context: InstructionContext<'a>,
    ) -> Result<HashMap<UnitsObjectId, UnitsObject>, ExecutionError> {
        // Find the program object
        let program = context
            .objects
            .get(program_id)
            .ok_or_else(|| ExecutionError::ProgramNotFound(*program_id))?
            .clone();

        // Check if code object and get runtime type
        let runtime_type = program
            .runtime_type()
            .ok_or_else(|| ExecutionError::InvalidProgram(*program_id))?;

        // Get entrypoint
        let entrypoint = instruction.entrypoint();

        // Get appropriate backend
        let backend = self
            .get_backend_for_runtime(runtime_type)
            .ok_or_else(|| ExecutionError::NoBackendAvailableForRuntime(runtime_type))?;

        // Set context details
        context.program_id = Some(*program_id);
        context.entrypoint = Some(entrypoint.to_string());

        // Execute the program
        match backend.execute_program(&program, entrypoint, &instruction.params, context) {
            InstructionResult::Success(objects) => Ok(objects),
            InstructionResult::Error(message) => Err(ExecutionError::ExecutionFailed(message)),
        }
    }

    /// Create a backend manager with default backends
    pub fn with_default_backends() -> Self {
        let mut manager = Self::new();
        
        // Register WebAssembly backend (default)
        manager.register_backend(Arc::new(WasmRuntimeBackend::new()));
        
        // Register eBPF backend
        manager.register_backend(Arc::new(EbpfRuntimeBackend::new()));
        
        // Set default runtime type
        manager.set_default_runtime(RuntimeType::Wasm);
        
        manager
    }
}

//==============================================================================
// BACKEND IMPLEMENTATIONS
//==============================================================================

/// WebAssembly runtime backend
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

    fn runtime_type(&self) -> RuntimeType {
        RuntimeType::Wasm
    }

    fn execute_program<'a>(
        &self,
        program: &UnitsObject,
        entrypoint: &str,
        args: &[u8],
        _context: InstructionContext<'a>,
    ) -> InstructionResult {
        // Validate program is code object
        if !program.is_code() {
            return InstructionResult::Error("Invalid program object - not code".to_string());
        }

        // For debugging
        log::debug!(
            "Execute WebAssembly program ({}): {} bytes, entrypoint: {}, args: {} bytes",
            program.id(),
            program.data().len(),
            entrypoint,
            args.len()
        );

        // Mock implementation
        InstructionResult::Error("WebAssembly execution not implemented".to_string())
    }
}

/// eBPF runtime backend
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

    fn runtime_type(&self) -> RuntimeType {
        RuntimeType::Ebpf
    }

    fn execute_program<'a>(
        &self,
        program: &UnitsObject,
        entrypoint: &str,
        args: &[u8],
        _context: InstructionContext<'a>,
    ) -> InstructionResult {
        // Validate program is code object
        if !program.is_code() {
            return InstructionResult::Error("Invalid program object - not code".to_string());
        }

        // For debugging
        log::debug!(
            "Execute eBPF program ({}): {} bytes, entrypoint: {}, args: {} bytes",
            program.id(),
            program.data().len(),
            entrypoint,
            args.len()
        );

        // Mock implementation
        InstructionResult::Error("eBPF execution not implemented".to_string())
    }
}
