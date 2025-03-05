use crate::id::UnitsObjectId;

// UnitsObject is a container for objects in UNITS.
#[derive(Debug)]
pub struct TokenizedObject {
    pub data: Vec<u8>,
    pub id: UnitsObjectId,
    // Should we have this level of information here? It's fine for now, we can change this later
    // if we need to.
    pub token_type: TokenType,
    pub holder: UnitsObjectId,
    pub controller_program: UnitsObjectId,
    // driver: [u8; 32], // TODO: Figure out a better way to represent the driver
}

#[derive(Debug)]
pub enum TokenType {
    Native,
    Custodial,
    Proxy,
}