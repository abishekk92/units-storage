use crate::id::UnitsObjectId;
use serde::{Deserialize, Serialize};

// UnitsObject is a container for objects in UNITS.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenizedObject {
    pub data: Vec<u8>,
    pub id: UnitsObjectId,
    pub holder: UnitsObjectId,
    // Should we have this level of information here? It's fine for now, we can change this later
    // if we need to.
    pub token_type: TokenType,
    pub token_manager: UnitsObjectId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TokenType {
    Native,
    Custodial,
    Proxy,
}
