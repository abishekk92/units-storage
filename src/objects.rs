use crate::id::UnitsObjectId;
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
