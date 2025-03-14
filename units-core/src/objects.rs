use crate::id::UnitsObjectId;
use crate::transaction::RuntimeType;
use serde::{Deserialize, Serialize};

/// Base struct for all UNITS objects
///
/// UnitsObject is the fundamental object in the UNITS system.
/// It provides core fields common to all object types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnitsObject {
    /// Unique identifier for this object
    pub id: UnitsObjectId,

    /// The UnitsObjectId of the entity who controls/owns this object
    pub owner: UnitsObjectId,

    /// Arbitrary binary data associated with this object
    pub data: Vec<u8>,
}

impl UnitsObject {
    /// Create a new UnitsObject
    pub fn new(
        id: UnitsObjectId,
        owner: UnitsObjectId,
        data: Vec<u8>,
    ) -> Self {
        Self {
            id,
            owner,
            data,
        }
    }
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

/// TokenizedObject represents an object in the UNITS system that has been tokenized.
///
/// A TokenizedObject extends UnitsObject with token-specific properties. It can be
/// tokenized and transferred between holders. When state changes, the storage system
/// emits a proof that commits to both the previous and new state of the object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenizedObject {
    /// Base object with core fields
    pub base: UnitsObject,

    /// Specifies how this token is held (Native, Custodial, or Proxy)
    pub token_type: TokenType,

    /// The UnitsObjectId that has authority to manage token operations
    pub token_manager: UnitsObjectId,
}

/// Metadata for a code object
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodeObjectMetadata {
    /// Type of runtime required to execute this code
    pub runtime_type: RuntimeType,
    
    /// The entrypoint function name to invoke
    pub entrypoint: String,
}

/// CodeObject represents executable code in the UNITS system
///
/// A CodeObject contains executable code (like WebAssembly or eBPF) that can be
/// executed by the runtime system. It includes metadata about the runtime type
/// and entrypoint information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodeObject {
    /// Base object with core fields (data field contains the executable code)
    pub base: UnitsObject,
    
    /// Code-specific metadata
    pub metadata: CodeObjectMetadata,
}

// Implementation for TokenizedObject
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
            base: UnitsObject::new(id, holder, data),
            token_type,
            token_manager,
        }
    }
    
    /// Get the object ID
    pub fn id(&self) -> &UnitsObjectId {
        &self.base.id
    }
    
    /// Get the holder (semantically the same as owner in the base object)
    pub fn holder(&self) -> &UnitsObjectId {
        &self.base.owner
    }
    
    /// Get the data
    pub fn data(&self) -> &[u8] {
        &self.base.data
    }
    
    /// Check if this object is a code object based on a simple heuristic
    /// 
    /// This is maintained for backward compatibility.
    /// In a real implementation, we'd check the object type directly.
    pub fn is_code_object(&self) -> bool {
        // Simplistic check - in a real implementation, we'd check the object type
        self.base.data.len() > 64
    }
    
    /// Get code metadata if this is a code object
    /// 
    /// This is maintained for backward compatibility.
    /// In a real implementation, we'd convert to a CodeObject or check the type.
    pub fn get_code_metadata(&self) -> Option<CodeObjectMetadata> {
        if !self.is_code_object() {
            return None;
        }
        
        // This is a placeholder implementation
        Some(CodeObjectMetadata {
            // Determine the runtime type based on the first byte as a simple heuristic
            runtime_type: match self.base.data.first() {
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
            Some(&self.base.data)
        } else {
            None
        }
    }
    
    /// Create a code object from code, runtime type, and entrypoint
    /// 
    /// This is maintained for backward compatibility.
    /// New code should create a CodeObject directly.
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
        // - First byte indicates the runtime type (1=Wasm, 2=Ebpf)
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

// Implementation for CodeObject
impl CodeObject {
    /// Create a new CodeObject
    pub fn new(
        id: UnitsObjectId,
        owner: UnitsObjectId,
        code: Vec<u8>,
        runtime_type: RuntimeType,
        entrypoint: String,
    ) -> Self {
        Self {
            base: UnitsObject::new(id, owner, code),
            metadata: CodeObjectMetadata {
                runtime_type,
                entrypoint,
            },
        }
    }
    
    /// Get the object ID
    pub fn id(&self) -> &UnitsObjectId {
        &self.base.id
    }
    
    /// Get the owner
    pub fn owner(&self) -> &UnitsObjectId {
        &self.base.owner
    }
    
    /// Get the code bytes
    pub fn code(&self) -> &[u8] {
        &self.base.data
    }
    
    /// Get the runtime type
    pub fn runtime_type(&self) -> RuntimeType {
        self.metadata.runtime_type
    }
    
    /// Get the entrypoint function name
    pub fn entrypoint(&self) -> &str {
        &self.metadata.entrypoint
    }
    
    /// Convert from a TokenizedObject that contains code
    /// 
    /// This is a convenience method for migrating from TokenizedObject to CodeObject.
    /// It attempts to extract code and metadata from a TokenizedObject.
    pub fn try_from_tokenized_object(obj: &TokenizedObject) -> Option<Self> {
        // Get code metadata
        let metadata = obj.get_code_metadata()?;
        
        // Create a new CodeObject
        Some(Self {
            base: UnitsObject::new(*obj.id(), *obj.holder(), obj.data().to_vec()),
            metadata,
        })
    }
}

// Legacy support for existing code that expects TokenizedObject fields directly
impl From<&TokenizedObject> for UnitsObject {
    fn from(obj: &TokenizedObject) -> Self {
        obj.base.clone()
    }
}

impl From<&CodeObject> for UnitsObject {
    fn from(obj: &CodeObject) -> Self {
        obj.base.clone()
    }
}
