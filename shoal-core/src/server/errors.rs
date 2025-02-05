//! Any errors tht can be encountered when running Shoal

use glommio::GlommioError;
use glommio::{BuilderErrorKind, ExecutorErrorKind, ReactorErrorKind};
use std::os::fd::RawFd;
use std::path::PathBuf;
use std::time::Duration;

/// Any errors tht can be encountered when running Shoal
#[derive(Debug)]
pub enum ServerError {
    /// An error specific to Shoal code
    Shoal(ShoalError),
    /// An IO error
    IO(std::io::Error),
    /// A glommio enhanced IO errors
    GlommioIO {
        source: std::io::Error,
        op: &'static str,
        path: Option<PathBuf>,
        fd: Option<RawFd>,
    },
    /// A glommio executor error
    GlommioExectorError(ExecutorErrorKind),
    /// A glommio builder error
    GlommioBuilderError(BuilderErrorKind),
    /// A glommio reactor error
    GlommioReactorError(ReactorErrorKind),
    /// Glommio time out error
    GlommioTimedOut(Duration),
    /// An error from glommio with a generic
    GlommioGeneric(String),
    /// An config parsing error
    Config(config::ConfigError),
    /// An rkyv error
    Rkyv(rkyv::rancor::Error),
    /// An error casting a vec of bytes to a slice
    IntoSlice(std::array::TryFromSliceError),
    /// A conversion error
    Conversion(std::convert::Infallible),
    /// An error sending a message over a kanal channel
    KanalSend(kanal::SendError),
    /// An error receiving a message over a kanal channel
    KanalRecv(kanal::ReceiveError),
}

// convert all of our external error types to our error type
impl<T> From<glommio::GlommioError<T>> for ServerError {
    fn from(ext: glommio::GlommioError<T>) -> Self {
        match ext {
            GlommioError::IoError(error) => Self::IO(error),
            GlommioError::EnhancedIoError {
                source,
                op,
                path,
                fd,
            } => Self::GlommioIO {
                source,
                op,
                path,
                fd,
            },
            GlommioError::ExecutorError(error) => Self::GlommioExectorError(error),
            GlommioError::BuilderError(error) => Self::GlommioBuilderError(error),
            GlommioError::ReactorError(error) => Self::GlommioReactorError(error),
            GlommioError::TimedOut(duration) => Self::GlommioTimedOut(duration),
            GlommioError::Closed(_) => Self::GlommioGeneric(ext.to_string()),
            GlommioError::CanNotBeClosed(_, _) => Self::GlommioGeneric(ext.to_string()),
            GlommioError::WouldBlock(_) => Self::GlommioGeneric(ext.to_string()),
        }
    }
}

impl From<config::ConfigError> for ServerError {
    /// Convert this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The errot to convert
    fn from(error: config::ConfigError) -> Self {
        ServerError::Config(error)
    }
}

impl From<std::io::Error> for ServerError {
    /// Convert this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The errot to convert
    fn from(error: std::io::Error) -> Self {
        ServerError::IO(error)
    }
}

impl From<rkyv::rancor::Error> for ServerError {
    /// Convert this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The error to convert
    fn from(error: rkyv::rancor::Error) -> Self {
        ServerError::Rkyv(error)
    }
}

impl From<std::array::TryFromSliceError> for ServerError {
    /// Convert this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The errot to convert
    fn from(error: std::array::TryFromSliceError) -> Self {
        ServerError::IntoSlice(error)
    }
}

impl From<std::convert::Infallible> for ServerError {
    /// Convert this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The errot to convert
    fn from(error: std::convert::Infallible) -> Self {
        ServerError::Conversion(error)
    }
}

impl From<kanal::SendError> for ServerError {
    /// Conver this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The error to convert
    fn from(error: kanal::SendError) -> Self {
        ServerError::KanalSend(error)
    }
}

impl From<kanal::ReceiveError> for ServerError {
    /// Conver this error to our error type
    ///
    /// # Arguments
    ///
    /// * `error` - The error to convert
    fn from(error: kanal::ReceiveError) -> Self {
        ServerError::KanalRecv(error)
    }
}

/// The errors specific to Shoal server code
#[derive(Debug)]
pub enum ShoalError {
    /// An invalid non binary message type was recieved
    NonBinaryMessage,
}
