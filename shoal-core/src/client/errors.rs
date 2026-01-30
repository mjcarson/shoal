//! The errors that can be returned from the Shoal client.

use uuid::Uuid;

use crate::shared::responses::ResponseActionNames;

/// The errors that can be returned from the Shoal client
#[derive(Debug)]
pub enum Errors {
    /// Attempt to cast a shoal response to the wrong type
    WrongType(String),
    // The shoal client got an unexpected response kind
    UnexpectedResponseKind {
        /// The response kind that was expected
        expected: ResponseActionNames,
        /// The response kind that was actually encountered
        actual: ResponseActionNames,
    },
    /// A query did not suceed
    QueryDidNotSucceed {
        id: Uuid,
        index: usize,
        kind: ResponseActionNames,
        end: bool,
    },
    /// Multiple errors in bulk
    BulkErrors(Box<Vec<Errors>>),
    /// An IO error occured
    IO(std::io::Error),
    /// An rkyv error
    Rkyv(rkyv::rancor::Error),
    /// An error sending data to a kanal channel
    KanalSend(kanal::SendError),
    /// An error receiving data from a kanal channel
    KanalReceive(kanal::ReceiveError),
    /// A stream has already ended
    StreamAlreadyTerminated,
    /// An error parsing a SHQL query string
    ShqlParse(ShqlParseError),
}

/// An error that occurred while parsing a SHQL query string
#[derive(Debug, Clone)]
pub struct ShqlParseError {
    /// The error message describing what went wrong
    pub message: String,
    /// The position in the input where the error occurred
    pub position: usize,
    /// The input string that was being parsed
    pub input: String,
}

impl ShqlParseError {
    /// Create a new SHQL parse error
    ///
    /// # Arguments
    ///
    /// * `message` - The error message
    /// * `position` - The position in the input where the error occurred
    /// * `input` - The input string being parsed
    pub fn new(message: impl Into<String>, position: usize, input: impl Into<String>) -> Self {
        ShqlParseError {
            message: message.into(),
            position,
            input: input.into(),
        }
    }
}

impl std::fmt::Display for ShqlParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SHQL parse error at position {}: {}",
            self.position, self.message
        )
    }
}

impl std::error::Error for ShqlParseError {}

impl From<ShqlParseError> for Errors {
    fn from(error: ShqlParseError) -> Self {
        Errors::ShqlParse(error)
    }
}

impl From<std::io::Error> for Errors {
    /// Convert this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The error to convert
    fn from(error: std::io::Error) -> Self {
        Errors::IO(error)
    }
}

impl From<rkyv::rancor::Error> for Errors {
    /// Convert this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The error to convert
    fn from(error: rkyv::rancor::Error) -> Self {
        Errors::Rkyv(error)
    }
}

impl From<kanal::SendError> for Errors {
    /// Convert this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The error to convert
    fn from(error: kanal::SendError) -> Self {
        Errors::KanalSend(error)
    }
}

impl From<kanal::ReceiveError> for Errors {
    /// Convert this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The error to convert
    fn from(error: kanal::ReceiveError) -> Self {
        Errors::KanalReceive(error)
    }
}

impl From<Vec<Errors>> for Errors {
    /// Convert this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The error to convert
    fn from(error: Vec<Errors>) -> Self {
        Errors::BulkErrors(Box::new(error))
    }
}
