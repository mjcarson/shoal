//! The errors that can be returned from the Shoal client.

use rkyv::ser::serializers::{
    AllocScratchError, CompositeSerializerError, SharedSerializeMapError,
};

/// The errors that can be returned from the Shoal client
#[derive(Debug)]
pub enum Errors {
    /// Attempt to cast a shoal response to the wrong type
    WrongType(String),
    /// An IO error occured
    IO(std::io::Error),
    /// An rkyv serialization error
    RkyvSerialize(
        CompositeSerializerError<
            std::convert::Infallible,
            AllocScratchError,
            SharedSerializeMapError,
        >,
    ),
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
    /// * `error` - The errot to convert
    fn from(error: std::io::Error) -> Self {
        Errors::IO(error)
    }
}

impl
    From<
        CompositeSerializerError<
            std::convert::Infallible,
            AllocScratchError,
            SharedSerializeMapError,
        >,
    > for Errors
{
    /// Convert this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The errot to convert
    fn from(
        error: CompositeSerializerError<
            std::convert::Infallible,
            AllocScratchError,
            SharedSerializeMapError,
        >,
    ) -> Self {
        Errors::RkyvSerialize(error)
    }
}

impl From<kanal::SendError> for Errors {
    /// Convert this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The errot to convert
    fn from(error: kanal::SendError) -> Self {
        Errors::KanalSend(error)
    }
}

impl From<kanal::ReceiveError> for Errors {
    /// Convert this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The errot to convert
    fn from(error: kanal::ReceiveError) -> Self {
        Errors::KanalReceive(error)
    }
}
