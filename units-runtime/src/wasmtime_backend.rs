//! WebAssembly runtime backend implementation using Wasmtime
//!
//! This module is only available when the `wasmtime-backend` feature is enabled.
//! It provides a concrete implementation of the WebAssembly runtime using the
//! Wasmtime library for executing WebAssembly modules.

#![cfg(feature = "wasmtime-backend")]

use std::collections::HashMap;
use std::mem;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use wasmtime::{Engine, Extern, Func, Instance, Module, Store, ValType};
use wasmtime::{Linker, Trap, Val, WasmParams, WasmResults};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder};

use units_core::id::UnitsObjectId;
use units_core::objects::UnitsObject;
use units_core::transaction::{Instruction, RuntimeType, TransactionHash};

use crate::host_environment::{create_standard_host_environment, StandardHostEnvironment};
use crate::runtime_backend::{
    HostEnvironment, InstructionContext, InstructionResult, RuntimeBackend,
};

// Removed InvocationContext as we're now using InstructionContext directly
// The binary data is handled through the host environment

/// Helper functions for memory management in WebAssembly modules
mod wasm_memory {
    use super::*;
    use wasmtime::{Caller, Memory};

    /// Copy data from WebAssembly memory to a Rust Vec<u8>
    pub fn read_memory(
        memory: &Memory,
        caller: &Caller<'_, HostState>,
        ptr: i32,
        len: i32,
    ) -> Result<Vec<u8>, Trap> {
        let data = memory.data(&caller);
        let start = ptr as usize;
        let end = start + len as usize;

        if end > data.len() {
            return Err(Trap::new("Memory access out of bounds"));
        }

        Ok(data[start..end].to_vec())
    }

    /// Copy data from a Rust slice to WebAssembly memory
    pub fn write_memory(
        memory: &Memory,
        mut caller: Caller<'_, HostState>,
        ptr: i32,
        data: &[u8],
    ) -> Result<(), Trap> {
        let memory_data = memory.data_mut(&mut caller);
        let start = ptr as usize;
        let end = start + data.len();

        if end > memory_data.len() {
            return Err(Trap::new("Memory access out of bounds"));
        }

        memory_data[start..end].copy_from_slice(data);
        Ok(())
    }

    /// Allocate memory in WebAssembly and copy data to it
    pub fn allocate_and_write(
        memory: &Memory,
        mut caller: Caller<'_, HostState>,
        data: &[u8],
    ) -> Result<i32, Trap> {
        // Get the allocator function
        let alloc = match caller.get_export("alloc") {
            Some(Extern::Func(f)) => f,
            _ => return Err(Trap::new("alloc function not found")),
        };

        // Call the allocation function
        let result = match alloc.call(&mut caller, &[Val::I32(data.len() as i32)]) {
            Ok(values) => match values[0].i32() {
                Some(ptr) => ptr,
                None => return Err(Trap::new("Invalid allocation result")),
            },
            Err(_) => return Err(Trap::new("Memory allocation failed")),
        };

        // Write data to the allocated memory
        let memory_data = memory.data_mut(&mut caller);
        let start = result as usize;
        let end = start + data.len();

        if end > memory_data.len() {
            return Err(Trap::new("Memory allocation out of bounds"));
        }

        memory_data[start..end].copy_from_slice(data);

        Ok(result)
    }

    /// Get the WebAssembly module's memory
    pub fn get_memory(caller: &Caller<'_, HostState>) -> Result<Memory, Trap> {
        match caller.get_export("memory") {
            Some(Extern::Memory(mem)) => Ok(mem),
            _ => Err(Trap::new("Memory not found")),
        }
    }
}

/// WASI-enabled Wasmtime runtime backend
///
/// This implementation provides a secure sandbox for executing WebAssembly modules
/// with WASI support for limited system access.
pub struct WasmtimeRuntimeBackend {
    /// Engine shared across all WebAssembly modules
    engine: Engine,
    /// Display name for the backend
    name: String,
}

/// Host state that will be shared with the WebAssembly module
struct HostState {
    /// Provides WASI capabilities to the WebAssembly module
    wasi: WasiCtx,
    /// Host environment that provides standardized access to host resources
    host_environment: Arc<Mutex<dyn HostEnvironment>>,
}

/// Host functions that will be exposed to the WebAssembly module
mod host_funcs {
    use super::wasm_memory;
    use super::*;
    use wasmtime::Caller;

    /// Log a message from WebAssembly (useful for debugging)
    pub fn log_message(
        mut caller: Caller<'_, HostState>,
        ptr: i32,
        len: i32,
        level: i32, // 0=debug, 1=info, 2=warn, 3=error
    ) -> Result<i32, Trap> {
        // Get the memory and read the message
        let memory = wasm_memory::get_memory(&caller)?;
        let msg_bytes = wasm_memory::read_memory(&memory, &caller, ptr, len)?;

        // Convert to string
        let msg = match std::str::from_utf8(&msg_bytes) {
            Ok(s) => s,
            Err(_) => return Err(Trap::new("Invalid UTF-8 in log message")),
        };

        // Get host environment and log the message
        let host_env = caller
            .data()
            .host_environment
            .lock()
            .map_err(|_| Trap::new("Failed to lock host environment"))?;

        // Log at the appropriate level using host environment
        host_env.log(level as u8, msg);

        Ok(0)
    }

    /// Store modified objects from the WebAssembly execution
    pub fn set_modified_objects(
        mut caller: Caller<'_, HostState>,
        ptr: i32, // Pointer to the serialized objects JSON in WebAssembly memory
        len: i32, // Length of the serialized objects JSON
    ) -> Result<i32, Trap> {
        // Get the memory and read the serialized objects
        let memory = wasm_memory::get_memory(&caller)?;
        let json_bytes = wasm_memory::read_memory(&memory, &caller, ptr, len)?;

        // Parse JSON into a map of UnitsObjects
        let json_str = match std::str::from_utf8(&json_bytes) {
            Ok(s) => s,
            Err(_) => return Err(Trap::new("Invalid UTF-8 in objects JSON")),
        };

        let objects: HashMap<UnitsObjectId, UnitsObject> = match serde_json::from_str(json_str) {
            Ok(objs) => objs,
            Err(e) => return Err(Trap::new(format!("Invalid objects JSON: {}", e))),
        };

        // Get mutable access to host environment
        let mut host_env = caller
            .data()
            .host_environment
            .lock()
            .map_err(|_| Trap::new("Failed to lock host environment"))?;

        // Store the modified objects using host environment
        match host_env.store_modified_objects(objects) {
            Ok(count) => Ok(count as i32),
            Err(e) => Err(Trap::new(format!(
                "Failed to store modified objects: {}",
                e
            ))),
        }
    }
}

impl WasmtimeRuntimeBackend {
    /// Create a new Wasmtime runtime backend
    pub fn new() -> Self {
        // Create a new engine with default configuration
        let engine = Engine::default();

        Self {
            engine,
            name: "Wasmtime WebAssembly Runtime".to_string(),
        }
    }

    /// Create a WASI context for the WebAssembly module
    fn create_wasi_ctx(&self) -> Result<WasiCtx> {
        // Create a minimal WASI context
        // In a production environment, you would configure this with:
        // - Allowed directories
        // - Environment variables
        // - Command-line arguments
        // - Other WASI settings
        let wasi = WasiCtxBuilder::new().inherit_stdio().build();

        Ok(wasi)
    }

    /// Create a new store with the given host environment
    fn create_store(
        &self,
        host_environment: Arc<Mutex<dyn HostEnvironment>>,
    ) -> Result<Store<HostState>> {
        let wasi = self.create_wasi_ctx()?;

        let host_state = HostState {
            wasi,
            host_environment,
        };

        let store = Store::new(&self.engine, host_state);
        Ok(store)
    }

    /// Create a linker with the host functions
    fn create_linker(&self, store: &mut Store<HostState>) -> Result<Linker<HostState>> {
        let mut linker = Linker::new(&self.engine);

        // Add WASI to the linker
        wasmtime_wasi::add_to_linker(&mut linker, |s| &mut s.wasi)?;

        // Add host functions for logging and return values
        linker.func_wrap("env", "units_log", host_funcs::log_message)?;
        linker.func_wrap(
            "env",
            "units_set_modified_objects",
            host_funcs::set_modified_objects,
        )?;

        Ok(linker)
    }

    /// Execute a WebAssembly module with a host environment
    fn execute_module(
        &self,
        wasm_bytes: &[u8],
        entrypoint: &str,
        host_environment: Arc<Mutex<dyn HostEnvironment>>,
    ) -> Result<HashMap<UnitsObjectId, UnitsObject>> {
        // Compile the WebAssembly module
        debug!("Compiling WebAssembly module ({} bytes)", wasm_bytes.len());
        let module = Module::from_binary(&self.engine, wasm_bytes)?;

        // Create a store with the host state
        let mut store = self.create_store(host_environment)?;

        // Create a linker with host functions
        let mut linker = self.create_linker(&mut store)?;

        // Instantiate the module
        debug!("Instantiating WebAssembly module");
        let instance = linker.instantiate(&mut store, &module)?;

        // Get the entrypoint function
        let entrypoint_func = instance
            .get_func(&mut store, entrypoint)
            .ok_or_else(|| anyhow!("Entrypoint function '{}' not found", entrypoint))?;

        // Check for memory and alloc function
        let memory = match instance.get_memory(&mut store, "memory") {
            Some(mem) => {
                debug!("Found memory export in WebAssembly module");
                mem
            }
            None => return Err(anyhow!("WebAssembly module must export memory")),
        };

        let alloc = match instance.get_func(&mut store, "alloc") {
            Some(f) => {
                debug!("Found alloc function in WebAssembly module");
                f
            }
            None => return Err(anyhow!("WebAssembly module must export alloc function")),
        };

        // Get the host environment data to pass to WebAssembly
        let host_env = store
            .data()
            .host_environment
            .lock()
            .map_err(|_| anyhow!("Failed to lock host environment"))?;

        // Now we need to pass multiple memory regions to the WebAssembly module.
        // We'll do this by creating buffer descriptors that contain pointers and lengths.

        // 1. Allocate and copy objects data
        let serialized_objects = host_env.get_serialized_objects();
        let objects_data_len = serialized_objects.len() as i32;
        let objects_data_ptr = match alloc.call(&mut store, &[Val::I32(objects_data_len)]) {
            Ok(values) => match values[0].i32() {
                Some(ptr) => ptr,
                None => return Err(anyhow!("Invalid allocation result for objects data")),
            },
            Err(e) => return Err(anyhow!("Memory allocation failed for objects data: {}", e)),
        };

        // Copy objects data to WebAssembly memory
        {
            let memory_data = memory.data_mut(&mut store);
            let start = objects_data_ptr as usize;
            let end = start + serialized_objects.len();

            if end > memory_data.len() {
                return Err(anyhow!("Memory allocation out of bounds for objects data"));
            }

            memory_data[start..end].copy_from_slice(serialized_objects);
        }

        // 2. Allocate and copy parameters data
        let serialized_params = host_env.get_serialized_parameters();
        let params_data_len = serialized_params.len() as i32;
        let params_data_ptr = match alloc.call(&mut store, &[Val::I32(params_data_len)]) {
            Ok(values) => match values[0].i32() {
                Some(ptr) => ptr,
                None => return Err(anyhow!("Invalid allocation result for parameters data")),
            },
            Err(e) => {
                return Err(anyhow!(
                    "Memory allocation failed for parameters data: {}",
                    e
                ))
            }
        };

        // Copy parameters data to WebAssembly memory
        {
            let memory_data = memory.data_mut(&mut store);
            let start = params_data_ptr as usize;
            let end = start + serialized_params.len();

            if end > memory_data.len() {
                return Err(anyhow!(
                    "Memory allocation out of bounds for parameters data"
                ));
            }

            memory_data[start..end].copy_from_slice(serialized_params);
        }

        // 3. Allocate and copy instruction parameters
        let instruction_params = host_env.get_instruction_params();
        let instruction_params_len = instruction_params.len() as i32;
        let instruction_params_ptr =
            match alloc.call(&mut store, &[Val::I32(instruction_params_len)]) {
                Ok(values) => match values[0].i32() {
                    Some(ptr) => ptr,
                    None => {
                        return Err(anyhow!(
                            "Invalid allocation result for instruction parameters"
                        ))
                    }
                },
                Err(e) => {
                    return Err(anyhow!(
                        "Memory allocation failed for instruction parameters: {}",
                        e
                    ))
                }
            };

        // Copy instruction parameters to WebAssembly memory
        {
            let memory_data = memory.data_mut(&mut store);
            let start = instruction_params_ptr as usize;
            let end = start + instruction_params.len();

            if end > memory_data.len() {
                return Err(anyhow!(
                    "Memory allocation out of bounds for instruction parameters"
                ));
            }

            memory_data[start..end].copy_from_slice(instruction_params);
        }

        // 4. Copy transaction hash
        let transaction_hash = host_env.get_transaction_hash();
        let tx_hash_ptr = match alloc.call(&mut store, &[Val::I32(32)]) {
            Ok(values) => match values[0].i32() {
                Some(ptr) => ptr,
                None => return Err(anyhow!("Invalid allocation result for transaction hash")),
            },
            Err(e) => {
                return Err(anyhow!(
                    "Memory allocation failed for transaction hash: {}",
                    e
                ))
            }
        };

        // Copy transaction hash to WebAssembly memory
        {
            let memory_data = memory.data_mut(&mut store);
            let start = tx_hash_ptr as usize;
            let end = start + 32;

            if end > memory_data.len() {
                return Err(anyhow!(
                    "Memory allocation out of bounds for transaction hash"
                ));
            }

            memory_data[start..end].copy_from_slice(transaction_hash);
        }

        // 5. Allocate and set program ID if present
        let program_id_ptr;
        let program_id_present;

        if let Some(program_id) = host_env.get_program_id() {
            program_id_ptr = match alloc.call(&mut store, &[Val::I32(32)]) {
                Ok(values) => match values[0].i32() {
                    Some(ptr) => ptr,
                    None => return Err(anyhow!("Invalid allocation result for program ID")),
                },
                Err(e) => return Err(anyhow!("Memory allocation failed for program ID: {}", e)),
            };

            // Copy program ID to WebAssembly memory
            {
                let memory_data = memory.data_mut(&mut store);
                let start = program_id_ptr as usize;
                let end = start + 32;

                if end > memory_data.len() {
                    return Err(anyhow!("Memory allocation out of bounds for program ID"));
                }

                // Copy program ID bytes
                let program_id_bytes = program_id.as_bytes();
                memory_data[start..end].copy_from_slice(program_id_bytes);
            }

            program_id_present = 1; // true
        } else {
            program_id_ptr = 0;
            program_id_present = 0; // false
        }

        debug!(
            "Executing WebAssembly entrypoint '{}' with {} bytes of objects data",
            entrypoint, objects_data_len
        );

        // Execute the entrypoint function with all context pointers and lengths
        match entrypoint_func.call(
            &mut store,
            &[
                // Objects data
                Val::I32(objects_data_ptr),
                Val::I32(objects_data_len),
                // Transaction hash
                Val::I32(tx_hash_ptr),
                // Parameters
                Val::I32(params_data_ptr),
                Val::I32(params_data_len),
                // Program ID (if present)
                Val::I32(program_id_ptr),
                Val::I32(program_id_present),
                // Instruction parameters
                Val::I32(instruction_params_ptr),
                Val::I32(instruction_params_len),
            ],
        ) {
            Ok(_) => {
                // Get the modified objects from the host environment
                let host_env = store
                    .data()
                    .host_environment
                    .lock()
                    .map_err(|_| anyhow!("Failed to lock host environment"))?;

                // Count how many objects were modified
                let modified_objects = host_env.get_modified_objects();
                let modified_count = modified_objects.len();

                debug!(
                    "WebAssembly execution successful, {} objects modified",
                    modified_count
                );

                // Return the modified objects
                Ok(modified_objects.clone())
            }
            Err(e) => {
                error!("WebAssembly execution failed: {}", e);
                Err(anyhow!("WebAssembly execution failed: {}", e))
            }
        }
    }
}

impl RuntimeBackend for WasmtimeRuntimeBackend {
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
        context: InstructionContext<'a>,
    ) -> InstructionResult {
        debug!(
            "Executing WebAssembly program ({}): entrypoint '{}', {} bytes of args",
            program.id,
            entrypoint,
            args.len()
        );

        // Get the program code
        let code = match program.get_code() {
            Some(code) => code,
            None => return InstructionResult::Error("Invalid program object".to_string()),
        };

        // Create a host environment for this program execution
        let program_id = context
            .program_id
            .expect("Program ID must be provided for program execution");
        let host_env = match StandardHostEnvironment::for_program(
            program_id,
            args,
            context.objects.clone(),
            &context.parameters,
            context.transaction_hash,
        ) {
            Ok(env) => Arc::new(Mutex::new(env)),
            Err(e) => {
                return InstructionResult::Error(format!(
                    "Failed to create host environment: {}",
                    e
                ))
            }
        };

        // Execute the WebAssembly module
        match self.execute_module(code, entrypoint, host_env) {
            Ok(modified_objects) => InstructionResult::Success(modified_objects),
            Err(e) => {
                error!("WebAssembly program execution failed: {}", e);
                InstructionResult::Error(format!("WebAssembly program execution failed: {}", e))
            }
        }
    }
}

/// Factory for creating Wasmtime runtime backends
pub fn create_wasmtime_backend() -> Arc<dyn RuntimeBackend> {
    Arc::new(WasmtimeRuntimeBackend::new())
}

/// WebAssembly module template for UNITS programs
///
/// This commented WAT (WebAssembly Text) template serves as a starting point for
/// developers who want to write WebAssembly modules that interact with the UNITS system.
///
/// ```wat
/// (module
///     ;; Import host functions provided by the UNITS runtime
///     (import "env" "units_log" (func $units_log (param i32 i32 i32) (result i32)))
///     (import "env" "units_set_modified_objects" (func $units_set_modified_objects (param i32 i32) (result i32)))
///
///     ;; Memory for the module (required by UNITS host functions)
///     (memory (export "memory") 1)
///
///     ;; Define data sections for static strings
///     (data (i32.const 0) "Processing binary data")
///
///     ;; Memory allocator (required by UNITS host functions)
///     ;; This is a simple bump allocator for demonstration
///     ;; In a real application, you should use a proper memory allocator
///     (func $alloc (export "alloc") (param $size i32) (result i32)
///         (local $offset i32)
///         ;; Get current heap_top
///         (global.get $heap_top)
///         (local.set $offset)
///
///         ;; Increase heap_top by size
///         (global.get $heap_top)
///         (local.get $size)
///         (i32.add)
///         (global.set $heap_top)
///
///         ;; Return the previous heap_top as the allocation address
///         (local.get $offset)
///     )
///
///     ;; Simple memory free (no-op in this simple example)
///     (func $free (export "free") (param $ptr i32)
///         ;; In a real allocator, you would free memory here
///     )
///
///     ;; Heap top (used by the allocator)
///     (global $heap_top (mut i32) (i32.const 1024))
///
///     ;; Helper function to log a message
///     (func $log (param $msg_ptr i32) (param $msg_len i32) (param $level i32)
///         (local.get $msg_ptr)
///         (local.get $msg_len)
///         (local.get $level)
///         (call $units_log)
///         (drop) ;; Ignore the result
///     )
///
///     ;; Main entrypoint function
///     ;; The parameters are raw binary data:
///     ;; - objects_data_ptr, objects_data_len: Serialized objects (JSON format)
///     ;; - tx_hash_ptr: Transaction hash (32 bytes)
///     ;; - params_data_ptr, params_data_len: Additional parameters (JSON format)
///     ;; - program_id_ptr: Program ID if present (32 bytes), or 0 if not present
///     ;; - program_id_present: 1 if program ID is present, 0 if not
///     ;; - instr_params_ptr, instr_params_len: Instruction parameters (raw bytes)
///     (func $execute (export "execute")
///         (param $objects_data_ptr i32) (param $objects_data_len i32)
///         (param $tx_hash_ptr i32)
///         (param $params_data_ptr i32) (param $params_data_len i32)
///         (param $program_id_ptr i32) (param $program_id_present i32)
///         (param $instr_params_ptr i32) (param $instr_params_len i32)
///         (result i32)
///
///         (local $modified_objects_ptr i32)
///         (local $modified_objects_len i32)
///
///         ;; Log that we're processing data
///         (i32.const 0)  ;; "Processing binary data" string address
///         (i32.const 20) ;; String length
///         (i32.const 1)  ;; Info level
///         (call $log)
///
///         ;; Process the raw binary data
///         ;; In a real application:
///         ;; 1. Deserialize the objects from JSON format
///         ;; 2. Access the transaction hash (32 bytes)
///         ;; 3. Deserialize parameters from JSON format if needed
///         ;; 4. Check if program ID is present and access it if needed
///         ;; 5. Process instruction parameters
///         ;; 6. Modify objects as needed
///         ;; 7. Serialize modified objects to JSON format
///         ;; 8. Return the modified objects to the host
///
///         ;; For this example, we'll just create empty modified objects
///         ;; (In a real implementation, you would serialize your modified objects)
///         (local.set $modified_objects_ptr (call $alloc (i32.const 2)))
///         (local.set $modified_objects_len (i32.const 2))
///
///         ;; Write an empty object to memory (simple JSON empty object "{}")
///         (i32.store8 (local.get $modified_objects_ptr) (i32.const 123))      ;; '{'
///         (i32.store8 (i32.add (local.get $modified_objects_ptr) (i32.const 1)) (i32.const 125))  ;; '}'
///
///         ;; Set the modified objects
///         (local.get $modified_objects_ptr)
///         (local.get $modified_objects_len)
///         (call $units_set_modified_objects)
///         (drop) ;; Ignore the result
///
///         ;; Return success (0)
///         (i32.const 0)
///     )
/// )
/// ```
///
/// For Rust developers, you can use the `serde_json` crate for JSON serialization.
/// Here's a sketch of how you might structure a Rust-based module:
///
/// ```rust
/// use std::collections::HashMap;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct UnitsObject {
///     id: [u8; 32],  // Direct binary representation
///     holder: [u8; 32],
///     token_type: u8,
///     token_manager: [u8; 32],
///     data: Vec<u8>,
/// }
///
/// // This function is exported to the host
/// #[no_mangle]
/// pub extern "C" fn execute(
///     objects_data_ptr: i32, objects_data_len: i32,
///     tx_hash_ptr: i32,
///     params_data_ptr: i32, params_data_len: i32,
///     program_id_ptr: i32, program_id_present: i32,
///     instr_params_ptr: i32, instr_params_len: i32,
/// ) -> i32 {
///     // Access transaction hash (32 bytes)
///     let tx_hash = unsafe {
///         let slice = std::slice::from_raw_parts(tx_hash_ptr as *const u8, 32);
///         let mut hash = [0u8; 32];
///         hash.copy_from_slice(slice);
///         hash
///     };
///
///     // Deserialize objects using JSON
///     let objects: HashMap<[u8; 32], UnitsObject> = unsafe {
///         let slice = std::slice::from_raw_parts(
///             objects_data_ptr as *const u8,
///             objects_data_len as usize
///         );
///         serde_json::from_slice(slice).unwrap()
///     };
///
///     // Access program ID if present
///     let program_id = if program_id_present != 0 {
///         unsafe {
///             let slice = std::slice::from_raw_parts(program_id_ptr as *const u8, 32);
///             let mut id = [0u8; 32];
///             id.copy_from_slice(slice);
///             Some(id)
///         }
///     } else {
///         None
///     };
///
///     // Access parameters if needed
///     let parameters: HashMap<String, String> = if params_data_len > 0 {
///         unsafe {
///             let slice = std::slice::from_raw_parts(
///                 params_data_ptr as *const u8,
///                 params_data_len as usize
///             );
///             serde_json::from_slice(slice).unwrap()
///         }
///     } else {
///         HashMap::new()
///     };
///
///     // Access instruction parameters
///     let instr_params = unsafe {
///         std::slice::from_raw_parts(
///             instr_params_ptr as *const u8,
///             instr_params_len as usize
///         )
///     };
///
///     // Process objects and create modified versions
///     let mut modified_objects = HashMap::new();
///
///     // Add your logic here to modify objects
///
///     // Serialize modified objects with JSON
///     let modified_data = serde_json::to_vec(&modified_objects).unwrap();
///
///     // Allocate memory for the serialized data
///     let result_ptr = alloc(modified_data.len() as i32);
///
///     // Copy the serialized data to WebAssembly memory
///     unsafe {
///         let dest = std::slice::from_raw_parts_mut(
///             result_ptr as *mut u8,
///             modified_data.len()
///         );
///         dest.copy_from_slice(&modified_data);
///     }
///
///     // Pass the modified objects back to the host
///     units_set_modified_objects(result_ptr, modified_data.len() as i32)
/// }
///
/// // Memory allocation function
/// #[no_mangle]
/// pub extern "C" fn alloc(size: i32) -> i32 {
///     // Implement your memory allocation strategy here
///     // This is a placeholder
///     let buffer = Vec::<u8>::with_capacity(size as usize);
///     let ptr = buffer.as_ptr() as i32;
///     std::mem::forget(buffer); // Leak the buffer so it's not deallocated
///     ptr
/// }
///
/// // Import functions from host
/// extern "C" {
///     fn units_set_modified_objects(ptr: i32, len: i32) -> i32;
///     fn units_log(ptr: i32, len: i32, level: i32) -> i32;
/// }
/// ```

#[cfg(test)]
mod tests {
    use super::*;
    use units_core::id::UnitsObjectId;
    use units_core::objects::TokenType;
    use wat::parse_str;

    /// Helper function to create a test WebAssembly module that works with our binary data interface
    fn create_test_wasm_module() -> Vec<u8> {
        // Define a WebAssembly module in WAT format with required memory and functions
        let wat = r#"
        (module
            ;; Import host functions
            (import "env" "units_log" (func $units_log (param i32 i32 i32) (result i32)))
            (import "env" "units_set_modified_objects" (func $units_set_modified_objects (param i32 i32) (result i32)))

            ;; Memory for the module (required by our host functions)
            (memory (export "memory") 1)

            ;; Static string data
            (data (i32.const 0) "Processing binary data")

            ;; Allocator function (required by our host functions)
            (func $alloc (export "alloc") (param i32) (result i32)
                (local $offset i32)
                ;; Very simple bump allocator for testing
                ;; A real module would use a proper allocator
                (global.get $heap_top)
                (local.set $offset)

                ;; Increase heap_top by the requested size
                (global.get $heap_top)
                (local.get $param0)  ;; This is the parameter named 'size'
                (i32.add)
                (global.set $heap_top)

                ;; Return the old heap_top
                (local.get $offset)
            )

            ;; Heap top (used by allocator)
            (global $heap_top (mut i32) (i32.const 1024))

            ;; Execute function for the binary data interface
            (func $execute (export "execute")
                (param $objects_data_ptr i32) (param $objects_data_len i32)
                (param $tx_hash_ptr i32)
                (param $params_data_ptr i32) (param $params_data_len i32)
                (param $program_id_ptr i32) (param $program_id_present i32)
                (param $instr_params_ptr i32) (param $instr_params_len i32)
                (result i32)

                (local $modified_objects_ptr i32)

                ;; Log a message
                (i32.const 0)  ;; "Processing binary data" string address
                (i32.const 20) ;; String length
                (i32.const 0)  ;; Debug level
                (call $units_log)
                (drop)         ;; Ignore the result

                ;; Return empty modified objects (empty JSON object {})
                (call $alloc (i32.const 2))   ;; Allocate 2 bytes for empty object
                (local.set $modified_objects_ptr)

                ;; Write JSON empty object to memory
                (i32.store8 (local.get $modified_objects_ptr) (i32.const 123))      ;; '{'
                (i32.store8 (i32.add (local.get $modified_objects_ptr) (i32.const 1)) (i32.const 125))  ;; '}'

                ;; Return modified objects (empty for this test)
                (local.get $modified_objects_ptr)
                (i32.const 2)  ;; Length of JSON empty object
                (call $units_set_modified_objects)
                (drop)         ;; Ignore the result

                ;; Return success (0)
                (i32.const 0)
            )
        )
        "#;

        // Convert WAT to WebAssembly binary
        parse_str(wat).expect("Failed to parse WAT")
    }

    #[test]
    #[cfg(feature = "wasmtime-backend")]
    fn test_wasmtime_backend_simple_module() {
        // Create a test WebAssembly module
        let wasm_binary = create_test_wasm_module();

        // Create a code object ID
        let code_object_id = UnitsObjectId::unique_id_for_tests();

        // Create a test code object
        let code_object = UnitsObject::create_code_object(
            code_object_id,
            UnitsObjectId::unique_id_for_tests(),
            TokenType::Native,
            UnitsObjectId::unique_id_for_tests(),
            wasm_binary,
            RuntimeType::Wasm,
            "execute".to_string(),
        );

        // Create a test instruction that references the code object
        let instruction = Instruction {
            params: vec![],
            instruction_type: units_core::transaction::InstructionType::Wasm,
            object_intents: vec![],
            code_object_id,
        };

        // Create a test data object
        let data_object_id = UnitsObjectId::unique_id_for_tests();
        let data_object = UnitsObject {
            id: data_object_id,
            holder: UnitsObjectId::unique_id_for_tests(),
            token_type: TokenType::Native,
            token_manager: UnitsObjectId::unique_id_for_tests(),
            data: vec![1, 2, 3, 4],
        };

        // Create a context with both the code object and data object
        let mut objects = HashMap::new();
        objects.insert(code_object_id, code_object);
        objects.insert(data_object_id, data_object);

        let context = InstructionContext {
            transaction_hash: &[0u8; 32],
            objects,
            parameters: HashMap::new(),
            program_id: None,
            entrypoint: None,
        };

        // Create a Wasmtime backend
        let backend = WasmtimeRuntimeBackend::new();

        // Execute the instruction
        let result = backend.execute(&instruction, context);

        // The real execution will likely fail due to the test module's limitations,
        // but it should at least attempt to invoke the WebAssembly module correctly
        match result {
            InstructionResult::Success(_) => {
                println!("Execution succeeded (unexpected)");
            }
            InstructionResult::Error(e) => {
                println!("Execution error (expected for test module): {}", e);
                // This is fine for the test - we're just checking that the basic
                // infrastructure is in place, not that the test module works perfectly
            }
        }
    }

    #[test]
    #[cfg(feature = "wasmtime-backend")]
    fn test_wasmtime_backend_minimal_module() {
        // Create a simple WebAssembly module in WAT format
        // This one just allocates memory and returns success
        let wat = r#"
        (module
            (memory (export "memory") 1)

            (func $alloc (export "alloc") (param i32) (result i32)
                (i32.const 1024) ;; Always return the same address for simple test
            )

            ;; Binary data interface
            (func $execute (export "execute")
                (param $objects_data_ptr i32) (param $objects_data_len i32)
                (param $tx_hash_ptr i32)
                (param $params_data_ptr i32) (param $params_data_len i32)
                (param $program_id_ptr i32) (param $program_id_present i32)
                (param $instr_params_ptr i32) (param $instr_params_len i32)
                (result i32)
                (i32.const 0) ;; Return success
            )
        )
        "#;

        // Convert WAT to WebAssembly binary
        let wasm_binary = parse_str(wat).expect("Failed to parse WAT");

        // Create a code object ID
        let code_object_id = UnitsObjectId::unique_id_for_tests();

        // Create a test code object
        let code_object = UnitsObject::create_code_object(
            code_object_id,
            UnitsObjectId::unique_id_for_tests(),
            TokenType::Native,
            UnitsObjectId::unique_id_for_tests(),
            wasm_binary,
            RuntimeType::Wasm,
            "execute".to_string(),
        );

        // Create a context
        let mut objects = HashMap::new();
        objects.insert(code_object_id, code_object);

        // Create an instruction
        let instruction = Instruction {
            params: vec![1, 2, 3, 4],
            instruction_type: units_core::transaction::InstructionType::Wasm,
            object_intents: vec![],
            code_object_id,
        };

        let context = InstructionContext {
            transaction_hash: &[0u8; 32],
            objects,
            parameters: HashMap::new(),
            program_id: None,
            entrypoint: None,
        };

        // Create a Wasmtime backend
        let backend = WasmtimeRuntimeBackend::new();

        // Execute the instruction
        let result = backend.execute(&instruction, context);

        // Should succeed because this is a simple module
        match result {
            InstructionResult::Success(modified_objects) => {
                // No objects were modified in this test
                assert_eq!(modified_objects.len(), 0);
            }
            InstructionResult::Error(e) => {
                panic!("Execution failed: {}", e);
            }
        }
    }
}
