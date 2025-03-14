use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use units_core::id::UnitsObjectId;
use units_core::objects::TokenizedObject;
use units_core::transaction::{Instruction, RuntimeType, TransactionHash};

/// Result type for instruction execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InstructionResult {
    /// Success result with a map of updated objects
    Success(HashMap<UnitsObjectId, TokenizedObject>),
    /// Error with message
    Error(String),
}

/// The execution context provided to a program via its entrypoint
#[derive(Debug, Clone)]
pub struct InstructionContext<'a> {
    /// The transaction hash for the current execution
    pub transaction_hash: &'a TransactionHash,

    /// The objects that this instruction has access to (read or write)
    pub objects: HashMap<UnitsObjectId, TokenizedObject>,

    /// Additional parameters available to the instruction
    pub parameters: HashMap<String, String>,

    /// The program object ID being executed (required for program calls)
    pub program_id: Option<UnitsObjectId>,

    /// The entrypoint function to call (required for program calls)
    pub entrypoint: Option<String>,
}

/// Trait defining the interface for a runtime backend
pub trait RuntimeBackend: Send + Sync {
    /// Get a string identifier for this runtime backend
    fn name(&self) -> &str;

    /// Get the runtime type this backend handles
    fn runtime_type(&self) -> RuntimeType;

    /// Execute a program stored in a TokenizedObject
    ///
    /// This method handles executing a code object with the given entrypoint.
    fn execute_program<'a>(
        &self,
        program: &TokenizedObject,
        entrypoint: &str,
        args: &[u8],
        context: InstructionContext<'a>,
    ) -> InstructionResult;
}

/// Error returned when execution fails
#[derive(Debug, thiserror::Error)]
pub enum ExecutionError {
    #[error("No runtime backend available for instruction type: {0:?}")]
    NoBackendAvailableForRuntime(RuntimeType),

    #[error("Execution failed: {0}")]
    ExecutionFailed(String),

    #[error("Object not found in execution context: {0}")]
    ObjectNotFound(UnitsObjectId),

    #[error("Program object not found: {0}")]
    ProgramNotFound(UnitsObjectId),

    #[error("Invalid program object: {0}")]
    InvalidProgram(UnitsObjectId),

    #[error("Invalid entrypoint: {0}")]
    InvalidEntrypoint(String),
}

/// Runtime backend manager that provides access to different backend implementations
pub struct RuntimeBackendManager {
    /// Available runtime backends by runtime type (primary mapping)
    backends_by_runtime: HashMap<RuntimeType, Arc<dyn RuntimeBackend>>,
}

impl RuntimeBackendManager {
    /// Create a new runtime backend manager
    pub fn new() -> Self {
        Self {
            backends_by_runtime: HashMap::new(),
        }
    }

    /// Register a runtime backend
    pub fn register_backend(&mut self, backend: Arc<dyn RuntimeBackend>) {
        // Get the runtime type from the backend
        let runtime_type = backend.runtime_type();

        // Store in the runtime type map
        self.backends_by_runtime
            .insert(runtime_type, backend.clone());
    }

    /// Get a runtime backend for a specific runtime type
    pub fn get_backend_for_runtime(
        &self,
        runtime_type: RuntimeType,
    ) -> Option<Arc<dyn RuntimeBackend>> {
        self.backends_by_runtime.get(&runtime_type).cloned()
    }

    /// Execute a program call instruction
    ///
    /// This looks up the program object by ID, extracts its metadata,
    /// selects the appropriate runtime backend, and executes the program.
    pub fn execute_program_call<'a>(
        &self,
        program_id: &UnitsObjectId,
        instruction: &Instruction,
        mut context: InstructionContext<'a>,
    ) -> Result<HashMap<UnitsObjectId, TokenizedObject>, ExecutionError> {
        // Find the program object in the context
        let program = context
            .objects
            .get(program_id)
            .ok_or_else(|| ExecutionError::ProgramNotFound(*program_id))?
            .clone();

        // Extract the code metadata from the program object
        let metadata = program
            .get_code_metadata()
            .ok_or_else(|| ExecutionError::InvalidProgram(*program_id))?;

        // Get the runtime type from the metadata
        let runtime_type = metadata.runtime_type;

        let entrypoint = instruction.entrypoint(); // This will return STANDARD_ENTRYPOINT

        // Get the appropriate backend for this program's runtime type
        let backend = self
            .get_backend_for_runtime(runtime_type)
            .ok_or_else(|| ExecutionError::NoBackendAvailableForRuntime(runtime_type))?;

        // Set the program ID and entrypoint in the context
        context.program_id = Some(*program_id);
        context.entrypoint = Some(entrypoint.to_string());

        // Execute the program using the instruction parameters
        match backend.execute_program(&program, entrypoint, &instruction.params, context) {
            InstructionResult::Success(objects) => Ok(objects),
            InstructionResult::Error(message) => Err(ExecutionError::ExecutionFailed(message)),
        }
    }

    /// Create a basic backend manager with default backends
    pub fn with_default_backends() -> Self {
        let mut manager = Self::new();

        // Add default WebAssembly backend implementation (mock)
        manager.register_backend(Arc::new(WasmRuntimeBackend::new()));

        // Add eBPF backend
        manager.register_backend(Arc::new(EbpfRuntimeBackend::new()));

        // When the wasmtime-backend feature is enabled, register the Wasmtime backend
        #[cfg(feature = "wasmtime-backend")]
        {
            use crate::wasmtime_backend::create_wasmtime_backend;
            // Replace the mock Wasm backend with the real Wasmtime implementation
            manager.register_backend(create_wasmtime_backend());
        }

        manager
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

    fn runtime_type(&self) -> RuntimeType {
        RuntimeType::Wasm
    }

    fn execute_program<'a>(
        &self,
        program: &TokenizedObject,
        entrypoint: &str,
        args: &[u8],
        _context: InstructionContext<'a>,
    ) -> InstructionResult {
        // Get the program code
        let code = match program.get_code() {
            Some(code) => code,
            None => return InstructionResult::Error("Invalid program object".to_string()),
        };

        // In a real implementation, we would:
        // 1. Instantiate a WebAssembly module from the code
        // 2. Locate the entrypoint function
        // 3. Serialize the context and args to pass to the function
        // 4. Execute the function
        // 5. Deserialize any modified objects

        // For debugging
        log::debug!(
            "Would execute WebAssembly program ({}): {} bytes of code, entrypoint: {}, args: {} bytes",
            program.id(),
            code.len(),
            entrypoint,
            args.len()
        );

        // For this example, we'll just return a mock result
        InstructionResult::Error("WebAssembly program execution not implemented yet".to_string())
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

    fn runtime_type(&self) -> RuntimeType {
        RuntimeType::Ebpf
    }

    fn execute_program<'a>(
        &self,
        program: &TokenizedObject,
        entrypoint: &str,
        args: &[u8],
        _context: InstructionContext<'a>,
    ) -> InstructionResult {
        // Get the program code
        let code = match program.get_code() {
            Some(code) => code,
            None => return InstructionResult::Error("Invalid program object".to_string()),
        };

        // In a real implementation, we would:
        // 1. Load the eBPF bytecode from the code
        // 2. Set up memory maps for the context and args
        // 3. Execute the eBPF program with the provided context
        // 4. Collect modified objects from memory maps

        // For debugging
        log::debug!(
            "Would execute eBPF program ({}): {} bytes of code, entrypoint: {}, args: {} bytes",
            program.id(),
            code.len(),
            entrypoint,
            args.len()
        );

        // For this example, we'll just return a mock result
        InstructionResult::Error("eBPF program execution not implemented yet".to_string())
    }
}
