use crate::id::UnitsObjectId;
use crate::transaction::RuntimeType;
use serde::{Deserialize, Serialize};

/// TokenizedObject represents an object in the UNITS system that has been tokenized.
///
/// A TokenizedObject is a container for arbitrary data that is uniquely identified and
/// can only be mutated by its current holder. When state changes, the storage system
/// emits a proof that commits to both the previous and new state of the object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenizedObject {
    /// Unique identifier for this tokenized object
    pub id: UnitsObjectId,

    /// The UnitsObjectId of the current holder who controls this object
    pub holder: UnitsObjectId,

    /// Specifies how this token is held (Native, Custodial, or Proxy)
    pub token_type: TokenType,

    /// The UnitsObjectId that has authority to manage token operations
    pub token_manager: UnitsObjectId,

    /// Arbitrary binary data associated with this object
    pub data: Vec<u8>,
}

/// Defines the type of token and its ownership model
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TokenType {
    /// The token is held directly by the holder with full control
    Native,

    /// The token is held by a custodian on behalf of another entity
    Custodial,

    /// The token represents a proxy to another token or asset
    Proxy,
}

/// Metadata for a code object stored inside a TokenizedObject
/// This is a placeholder that will be implemented properly in the future
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodeObjectMetadata {
    /// Type of runtime required to execute this code
    pub runtime_type: RuntimeType,
    
    /// The entrypoint function name to invoke
    pub entrypoint: String,
}

impl TokenizedObject {
    /// Create a new TokenizedObject
    pub fn new(
        id: UnitsObjectId,
        holder: UnitsObjectId,
        token_type: TokenType,
        token_manager: UnitsObjectId,
        data: Vec<u8>,
    ) -> Self {
        Self {
            id,
            holder,
            token_type,
            token_manager,
            data,
        }
    }
    
    /// Check if this object is a code object based on a simple heuristic
    /// 
    /// In a real implementation, we'd have a proper metadata format.
    /// For now, we'll consider any object with data larger than 64 bytes as potentially a code object.
    pub fn is_code_object(&self) -> bool {
        // Simplistic check - in a real implementation, we'd check for actual code metadata
        self.data.len() > 64
    }
    
    /// In a real implementation, this would parse embedded metadata from the object
    /// For now, it's a placeholder that would be implemented properly in the future
    pub fn get_code_metadata(&self) -> Option<CodeObjectMetadata> {
        if !self.is_code_object() {
            return None;
        }
        
        // This is a placeholder implementation
        // We'd actually parse real metadata in a proper implementation
        Some(CodeObjectMetadata {
            // Determine the runtime type based on the first byte as a simple heuristic
            runtime_type: match self.data.first() {
                Some(1) => RuntimeType::Wasm,
                Some(2) => RuntimeType::Ebpf,
                // Default to Wasm for unrecognized values
                _ => RuntimeType::Wasm,
            },
            entrypoint: "main".to_string(),
        })
    }
    
    /// Get the code bytes if this is a code object
    pub fn get_code(&self) -> Option<&[u8]> {
        if self.is_code_object() {
            // In a real implementation, we'd skip the metadata bytes
            // For now, just return all the data
            Some(&self.data)
        } else {
            None
        }
    }
    
    /// Create a code object from code, runtime type, and entrypoint
    pub fn create_code_object(
        id: UnitsObjectId,
        holder: UnitsObjectId,
        token_type: TokenType,
        token_manager: UnitsObjectId,
        code: Vec<u8>,
        runtime_type: RuntimeType,
        entrypoint: String,
    ) -> Self {
        // In a real implementation, we'd properly serialize metadata
        // For now, we'll use a simple format:
        // - First byte indicates the runtime type (0=Native, 1=Wasm, 2=Ebpf, 3=Script)
        // - Next bytes are a simple marker
        
        // Create data with runtime type marker and code
        let runtime_byte = match runtime_type {
            RuntimeType::Wasm => 1u8,
            RuntimeType::Ebpf => 2u8,
        };
        
        // Create combined data with simple metadata and code
        let mut data = Vec::with_capacity(64 + code.len());
        data.push(runtime_byte);
        
        // Add a simple marker for "CODE" plus the entrypoint name (up to 59 bytes)
        let marker = b"CODE:";
        data.extend_from_slice(marker);
        
        // Add entrypoint name (truncated if too long)
        let max_entrypoint_len = 58 - marker.len();
        let entrypoint_bytes = entrypoint.as_bytes();
        let entrypoint_len = std::cmp::min(entrypoint_bytes.len(), max_entrypoint_len);
        data.extend_from_slice(&entrypoint_bytes[..entrypoint_len]);
        
        // Pad metadata to fixed size
        data.resize(64, 0);
        
        // Add the actual code
        data.extend_from_slice(&code);
        
        // Create the object
        Self::new(id, holder, token_type, token_manager, data)
    }
}
