//! The errors that can be returned from the Shoal client.

/// The errors that can be returned from the Shoal client
#[derive(Debug)]
pub enum Errors {
    /// Attempt to cast a shoal response to the wrong type
    WrongType(String),
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
