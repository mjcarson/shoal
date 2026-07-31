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
    /// Failed to get a connection from the pool
    ConnectionPool(String),
    /// Failed to resolve a DNS address
    DnsResolution(String),
    /// A wire protocol error
    ProtocolError(String),
    /// An error parsing a SHQL query string
    ShqlParse(ShqlParseError),
    /// A shoalctl error
    Shoalctl(String),
}

impl std::fmt::Display for Errors {
    // Allow this error to be displayed
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Just use the debug output for now
        write!(f, "{self:?}")
    }
}

impl std::error::Error for Errors {}

impl Errors {
    /// Create a new shoalctl error
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to set
    pub fn shoalctl<M: Into<String>>(msg: M) -> Self {
        Errors::Shoalctl(msg.into())
    }
}

/// An error that occurred while parsing a SHQL query string
#[derive(Debug, Clone)]
pub struct ShqlParseError {
    /// The error message describing what went wrong
    pub message: String,
    /// The start position in the input where the error occurred
    pub start: usize,
    /// The end position in the input where the error occurred
    pub end: usize,
    /// The input string that was being parsed
    pub input: String,
}

impl ShqlParseError {
    /// Create a new SHQL parse error with a position range
    ///
    /// # Arguments
    ///
    /// * `message` - The error message
    /// * `start` - The start position in the input where the error occurred
    /// * `end` - The end position in the input where the error occurred
    /// * `input` - The input string being parsed
    pub fn new(
        message: impl Into<String>,
        start: usize,
        end: usize,
        input: impl Into<String>,
    ) -> Self {
        ShqlParseError {
            message: message.into(),
            start,
            end,
            input: input.into(),
        }
    }

    /// Create a new SHQL parse error with a single position (end defaults to end of input)
    pub fn at_position(
        message: impl Into<String>,
        position: usize,
        input: impl Into<String>,
    ) -> Self {
        let input_str = input.into();
        let end = input_str.len();
        ShqlParseError {
            message: message.into(),
            start: position,
            end,
            input: input_str,
        }
    }
}

impl std::fmt::Display for ShqlParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.start == self.end {
            write!(
                f,
                "SHQL parse error at position {}: {}",
                self.start, self.message
            )
        } else {
            // slice out the offending span, falling back to the whole input if the span is out
            // of range or does not land on a character boundary
            let span = self
                .input
                .get(self.start..self.end)
                .unwrap_or(self.input.as_str());
            write!(
                f,
                "SHQL parse error at positions {}-{}: {}\n  {}",
                self.start, self.end, self.message, span
            )
        }
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
