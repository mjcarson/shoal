//! The errors that can be returned from the Shoal client.

use uuid::Uuid;

use crate::shared::auth::AuthError;
use crate::shared::protocol::error::ErrorCode;
use crate::shared::protocol::ProtocolError;
use crate::shared::responses::ResponseActionNames;
use crate::shared::tls::TlsError;

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
    /// The server answered with a failure rather than with a result
    ///
    /// This is different from [`Errors::QueryDidNotSucceed`], which says a query worked and found
    /// nothing. This says it did not work.
    Server {
        /// The query bundle this failure belongs to, if it named one
        query_id: Option<Uuid>,
        /// Which query in that bundle failed, when the failure arrived with a response
        ///
        /// A frame level failure has no index. It is attached to a query id, and a query id names
        /// a whole bundle, so there is no position in the stream to put it at.
        index: Option<usize>,
        /// What class of failure this is
        code: ErrorCode,
        /// What the server said about it
        msg: String,
    },
    /// Multiple errors in bulk
    BulkErrors(Box<Vec<Errors>>),
    /// An IO error occured
    IO(std::io::Error),
    /// An rkyv error
    Rkyv(rkyv::rancor::Error),
    /// One of the clients internal channels went away
    Channel(ChannelError),
    /// A stream has already ended
    StreamAlreadyTerminated,
    /// Failed to get a connection from the pool
    ConnectionPool(String),
    /// Failed to resolve a DNS address
    DnsResolution(String),
    /// A client was described in a way that cannot be built
    ///
    /// This is a caller's mistake rather than a peer's, and it is caught before a socket is
    /// opened - a builder with no endpoint, or a pool whose minimum idle count is above its
    /// maximum size, has nothing to try.
    Config(String),
    /// A frame that could not be written or read
    Protocol(ProtocolError),
    /// A connection could not be opened, or was refused by the server
    Handshake(ConnectError),
    /// An error parsing a SHQL query string
    ShqlParse(ShqlParseError),
    /// A shoalctl error
    Shoalctl(String),
}

/// Which end of one of the clients internal channels went away
///
/// The channel types belong to whichever client implementation is in use, so their own error
/// types must not appear here - a second client built on different channels would carry two
/// variants naming a crate it never links. This says the same thing without naming one, and it
/// stays a `Copy` value rather than becoming a message, because a torn down channel is
/// something a caller branches on rather than something it prints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelError {
    /// The channel was closed at both ends
    Closed,
    /// Every receiver was dropped, so a send has nowhere to go
    ReceiveClosed,
    /// Every sender was dropped, so a receive has nothing left to wait for
    SendClosed,
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

/// The reasons a connection to a Shoal server cannot be opened
///
/// This is what `bb8` hands back out of `Pool::builder().build()`, so it is also what a caller of
/// `Shoal::new` gets. It is a typed enum rather than a string because the interesting case carries
/// two numbers a person needs to see: a schema mismatch names both fingerprints, and a caller can
/// match on it rather than reading it.
#[derive(Debug)]
pub enum ConnectError {
    /// The connection itself failed
    Io(std::io::Error),
    /// The server accepted the connection but never finished the handshake
    ///
    /// This deadline exists because `bb8`'s connection timeout bounds its retry loop and not the
    /// connect itself, so without it a server that accepts and then stalls would park
    /// `Shoal::new` forever.
    HandshakeTimeout,
    /// The server refused this connection, or answered with something we could not read
    Protocol(ProtocolError),
    /// This client could not do what the server asked of it before it sent a query
    ///
    /// This is the connect-time half of authentication: the server wants proof and this client
    /// either holds nothing, or holds nothing that does the mechanism the server selected. It is
    /// separate from [`ConnectError::AuthFailed`] because it is a *deployment* mistake — nothing
    /// crossed the wire that a different password would have fixed.
    AuthRequired(AuthError),
    /// The server refused what this client proved
    ///
    /// The message is the server's, and it is deliberately the same sentence whether the password
    /// was wrong or the user does not exist. Reading more into it than that is reading something
    /// the server refuses to say.
    AuthFailed {
        /// What the server said about it
        msg: String,
    },
    /// This connection could not be encrypted
    ///
    /// Everything from a certificate authority that could not be read to a server whose
    /// certificate does not carry the name that was asked for. Like
    /// [`ConnectError::AuthRequired`] it is a connect-time failure, and like it most causes are
    /// deployment mistakes rather than anything a retry would fix.
    Tls(TlsError),
}

impl From<TlsError> for ConnectError {
    /// Convert this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The error to convert
    fn from(error: TlsError) -> Self {
        ConnectError::Tls(error)
    }
}

impl std::fmt::Display for ConnectError {
    /// Write a legible description of this connection error
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write too
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectError::Io(error) => write!(f, "failed to connect: {error}"),
            ConnectError::HandshakeTimeout => {
                write!(f, "the server never finished the handshake")
            }
            ConnectError::Protocol(error) => write!(f, "{error}"),
            ConnectError::AuthRequired(error) => write!(f, "{error}"),
            ConnectError::AuthFailed { msg } => {
                write!(f, "the server refused this client's credentials: {msg}")
            }
            ConnectError::Tls(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ConnectError {}

impl From<AuthError> for ConnectError {
    /// Convert this error to our error type
    ///
    /// Everything an [`AuthError`] can be on the client side is something this client got wrong
    /// before or during the exchange. A server *refusing* it arrives as
    /// [`ConnectError::AuthFailed`] instead, carrying the server's own words.
    ///
    /// # Arguments
    ///
    /// * `error` - The error to convert
    fn from(error: AuthError) -> Self {
        ConnectError::AuthRequired(error)
    }
}

impl From<std::io::Error> for ConnectError {
    /// Convert this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The error to convert
    fn from(error: std::io::Error) -> Self {
        ConnectError::Io(error)
    }
}

impl From<ProtocolError> for ConnectError {
    /// Convert this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The error to convert
    fn from(error: ProtocolError) -> Self {
        ConnectError::Protocol(error)
    }
}

impl From<ConnectError> for Errors {
    /// Convert this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The error to convert
    fn from(error: ConnectError) -> Self {
        Errors::Handshake(error)
    }
}

impl From<ProtocolError> for Errors {
    /// Convert this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The error to convert
    fn from(error: ProtocolError) -> Self {
        Errors::Protocol(error)
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
