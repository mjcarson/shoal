//! The errors that can be returned from the Shoal client.

/// The errors that can be returned from the Shoal client
#[derive(Debug)]
pub enum Errors {
    /// Attempt to cast a shoal response to the wrong type
    WrongType(String),
}
