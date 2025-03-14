use crate::id::UnitsObjectId;
use crate::transaction::RuntimeType;
use serde::{Deserialize, Serialize};

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

/// Enum to represent different object types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectType {
    /// Standard tokenized object
    Token,
    /// Executable code object
    Code,
}

/// Metadata for different object types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ObjectMetadata {
    /// Token-specific metadata
    Token {
        /// Specifies how this token is held (Native, Custodial, or Proxy)
        token_type: TokenType,
        /// The UnitsObjectId that has authority to manage token operations
        token_manager: UnitsObjectId,
    },
    /// Code-specific metadata
    Code {
        /// Type of runtime required to execute this code
        runtime_type: RuntimeType,
        /// The entrypoint function name to invoke
        entrypoint: String,
    },
}

/// Unified object structure for all UNITS objects
///
/// UnitsObject is the fundamental object in the UNITS system.
/// It provides fields for all object types with type-specific metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnitsObject {
    /// Unique identifier for this object
    pub id: UnitsObjectId,

    /// The UnitsObjectId of the entity who controls/owns this object
    pub owner: UnitsObjectId,

    /// Object type
    pub object_type: ObjectType,

    /// Type-specific metadata
    pub metadata: ObjectMetadata,

    /// Arbitrary binary data associated with this object
    pub data: Vec<u8>,
}

impl UnitsObject {
    /// Create a new token object
    pub fn new_token(
        id: UnitsObjectId,
        owner: UnitsObjectId,
        token_type: TokenType,
        token_manager: UnitsObjectId,
        data: Vec<u8>,
    ) -> Self {
        Self {
            id,
            owner,
            object_type: ObjectType::Token,
            metadata: ObjectMetadata::Token {
                token_type,
                token_manager,
            },
            data,
        }
    }

    /// Create a new code object
    pub fn new_code(
        id: UnitsObjectId,
        owner: UnitsObjectId,
        code: Vec<u8>,
        runtime_type: RuntimeType,
        entrypoint: String,
    ) -> Self {
        Self {
            id,
            owner,
            object_type: ObjectType::Code,
            metadata: ObjectMetadata::Code {
                runtime_type,
                entrypoint,
            },
            data: code,
        }
    }

    /// Get the object ID
    pub fn id(&self) -> &UnitsObjectId {
        &self.id
    }
    
    /// Get the owner/holder
    pub fn owner(&self) -> &UnitsObjectId {
        &self.owner
    }
    
    /// Get the data/code
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Check if this is a token object
    pub fn is_token(&self) -> bool {
        matches!(self.object_type, ObjectType::Token)
    }

    /// Check if this is a code object
    pub fn is_code(&self) -> bool {
        matches!(self.object_type, ObjectType::Code)
    }

    /// Get token type if this is a token object
    pub fn token_type(&self) -> Option<TokenType> {
        match &self.metadata {
            ObjectMetadata::Token { token_type, .. } => Some(*token_type),
            _ => None,
        }
    }

    /// Get token manager if this is a token object
    pub fn token_manager(&self) -> Option<&UnitsObjectId> {
        match &self.metadata {
            ObjectMetadata::Token { token_manager, .. } => Some(token_manager),
            _ => None,
        }
    }

    /// Get runtime type if this is a code object
    pub fn runtime_type(&self) -> Option<RuntimeType> {
        match &self.metadata {
            ObjectMetadata::Code { runtime_type, .. } => Some(*runtime_type),
            _ => None,
        }
    }

    /// Get entrypoint if this is a code object
    pub fn entrypoint(&self) -> Option<&str> {
        match &self.metadata {
            ObjectMetadata::Code { entrypoint, .. } => Some(entrypoint),
            _ => None,
        }
    }
}

// ---- Legacy Types for Backward Compatibility ----

/// Base struct for legacy UNITS objects
///
/// This is kept for backward compatibility.
/// New code should use the unified UnitsObject struct.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LegacyUnitsObject {
    /// Unique identifier for this object
    pub id: UnitsObjectId,

    /// The UnitsObjectId of the entity who controls/owns this object
    pub owner: UnitsObjectId,

    /// Arbitrary binary data associated with this object
    pub data: Vec<u8>,
}

impl LegacyUnitsObject {
    /// Create a new LegacyUnitsObject
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

/// Metadata for a code object
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CodeObjectMetadata {
    /// Type of runtime required to execute this code
    pub runtime_type: RuntimeType,
    
    /// The entrypoint function name to invoke
    pub entrypoint: String,
}

/// TokenizedObject represents an object in the UNITS system that has been tokenized.
///
/// This is kept for backward compatibility.
/// New code should use the unified UnitsObject struct.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TokenizedObject {
    /// Base object with core fields
    pub base: LegacyUnitsObject,

    /// Specifies how this token is held (Native, Custodial, or Proxy)
    pub token_type: TokenType,

    /// The UnitsObjectId that has authority to manage token operations
    pub token_manager: UnitsObjectId,
}

/// CodeObject represents executable code in the UNITS system
///
/// This is kept for backward compatibility.
/// New code should use the unified UnitsObject struct.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CodeObject {
    /// Base object with core fields (data field contains the executable code)
    pub base: LegacyUnitsObject,
    
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
            base: LegacyUnitsObject::new(id, holder, data),
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

    /// Convert to the new UnitsObject model
    pub fn to_units_object(&self) -> UnitsObject {
        if self.is_code_object() {
            // If this is actually a code object, convert it properly
            if let Some(metadata) = self.get_code_metadata() {
                // For code objects, the data is the actual code (after the metadata)
                // In a real implementation, we'd properly extract the code portion
                return UnitsObject::new_code(
                    self.base.id,
                    self.base.owner,
                    self.base.data.clone(), // In real code we'd extract the actual code portion
                    metadata.runtime_type,
                    metadata.entrypoint,
                );
            }
        }
        
        // Otherwise, treat it as a regular token object
        UnitsObject::new_token(
            self.base.id,
            self.base.owner,
            self.token_type,
            self.token_manager,
            self.base.data.clone(),
        )
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
            base: LegacyUnitsObject::new(id, owner, code),
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
            base: LegacyUnitsObject::new(*obj.id(), *obj.holder(), obj.data().to_vec()),
            metadata,
        })
    }

    /// Convert to the new UnitsObject model
    pub fn to_units_object(&self) -> UnitsObject {
        UnitsObject::new_code(
            self.base.id,
            self.base.owner,
            self.base.data.clone(),
            self.metadata.runtime_type,
            self.metadata.entrypoint.clone(),
        )
    }
}

// Conversion implementations
impl From<&TokenizedObject> for UnitsObject {
    fn from(obj: &TokenizedObject) -> Self {
        obj.to_units_object()
    }
}

impl From<TokenizedObject> for UnitsObject {
    fn from(obj: TokenizedObject) -> Self {
        obj.to_units_object()
    }
}

impl From<&CodeObject> for UnitsObject {
    fn from(obj: &CodeObject) -> Self {
        obj.to_units_object()
    }
}

impl From<CodeObject> for UnitsObject {
    fn from(obj: CodeObject) -> Self {
        obj.to_units_object()
    }
}

// Legacy support
impl From<&TokenizedObject> for LegacyUnitsObject {
    fn from(obj: &TokenizedObject) -> Self {
        obj.base.clone()
    }
}

impl From<&CodeObject> for LegacyUnitsObject {
    fn from(obj: &CodeObject) -> Self {
        obj.base.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::RuntimeType;
    
    #[test]
    fn test_unified_object_token_type() {
        // Create an ID for testing
        let id = UnitsObjectId::new([1; 32]);
        let owner = UnitsObjectId::new([2; 32]);
        let token_manager = UnitsObjectId::new([3; 32]);
        let data = vec![0, 1, 2, 3, 4];
        
        // Create a token object
        let token_obj = UnitsObject::new_token(
            id,
            owner,
            TokenType::Native,
            token_manager,
            data.clone(),
        );
        
        // Check type and accessors
        assert!(token_obj.is_token());
        assert!(!token_obj.is_code());
        assert_eq!(token_obj.token_type(), Some(TokenType::Native));
        assert_eq!(token_obj.token_manager(), Some(&token_manager));
        assert_eq!(token_obj.runtime_type(), None);
        assert_eq!(token_obj.entrypoint(), None);
        assert_eq!(token_obj.data(), &data);
        assert_eq!(token_obj.id(), &id);
        assert_eq!(token_obj.owner(), &owner);
    }
    
    #[test]
    fn test_unified_object_code_type() {
        // Create an ID for testing
        let id = UnitsObjectId::new([1; 32]);
        let owner = UnitsObjectId::new([2; 32]);
        let code = vec![0, 1, 2, 3, 4];
        
        // Create a code object
        let code_obj = UnitsObject::new_code(
            id,
            owner,
            code.clone(),
            RuntimeType::Wasm,
            "main".to_string(),
        );
        
        // Check type and accessors
        assert!(!code_obj.is_token());
        assert!(code_obj.is_code());
        assert_eq!(code_obj.token_type(), None);
        assert_eq!(code_obj.token_manager(), None);
        assert_eq!(code_obj.runtime_type(), Some(RuntimeType::Wasm));
        assert_eq!(code_obj.entrypoint(), Some("main"));
        assert_eq!(code_obj.data(), &code);
        assert_eq!(code_obj.id(), &id);
        assert_eq!(code_obj.owner(), &owner);
    }
    
    #[test]
    fn test_conversion_from_legacy_token_object() {
        // Create an ID for testing
        let id = UnitsObjectId::new([1; 32]);
        let owner = UnitsObjectId::new([2; 32]);
        let token_manager = UnitsObjectId::new([3; 32]);
        let data = vec![0, 1, 2, 3, 4];
        
        // Create a legacy token object
        let token_obj = TokenizedObject::new(
            id,
            owner,
            TokenType::Native,
            token_manager,
            data.clone(),
        );
        
        // Convert to unified object
        let unified = token_obj.to_units_object();
        
        // Check type and accessors
        assert!(unified.is_token());
        assert!(!unified.is_code());
        assert_eq!(unified.token_type(), Some(TokenType::Native));
        assert_eq!(unified.token_manager(), Some(&token_manager));
        assert_eq!(unified.data(), &data);
        assert_eq!(unified.id(), &id);
        assert_eq!(unified.owner(), &owner);
    }
    
    #[test]
    fn test_conversion_from_legacy_code_object() {
        // Create an ID for testing
        let id = UnitsObjectId::new([1; 32]);
        let owner = UnitsObjectId::new([2; 32]);
        let code = vec![0, 1, 2, 3, 4];
        
        // Create a legacy code object
        let code_obj = CodeObject::new(
            id,
            owner,
            code.clone(),
            RuntimeType::Wasm,
            "main".to_string(),
        );
        
        // Convert to unified object
        let unified = code_obj.to_units_object();
        
        // Check type and accessors
        assert!(!unified.is_token());
        assert!(unified.is_code());
        assert_eq!(unified.token_type(), None);
        assert_eq!(unified.token_manager(), None);
        assert_eq!(unified.runtime_type(), Some(RuntimeType::Wasm));
        assert_eq!(unified.entrypoint(), Some("main"));
        assert_eq!(unified.data(), &code);
        assert_eq!(unified.id(), &id);
        assert_eq!(unified.owner(), &owner);
    }
}
