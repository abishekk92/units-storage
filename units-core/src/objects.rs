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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::RuntimeType;

    #[test]
    fn test_token_object() {
        // Create an ID for testing
        let id = UnitsObjectId::new([1; 32]);
        let owner = UnitsObjectId::new([2; 32]);
        let token_manager = UnitsObjectId::new([3; 32]);
        let data = vec![0, 1, 2, 3, 4];

        // Create a token object
        let token_obj =
            UnitsObject::new_token(id, owner, TokenType::Native, token_manager, data.clone());

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
    fn test_code_object() {
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
}
