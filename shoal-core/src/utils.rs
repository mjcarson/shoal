//! Utilities used by shoal-core

use byte_unit::Byte;

use crate::server::ServerError;

pub fn deserialize_byte_size<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    // deserialize our size as bytes
    let byte_size: Byte = serde::de::Deserialize::deserialize(deserializer)?;
    // convert our size to a usize
    byte_size
        .as_u64()
        .try_into()
        .map_err(serde::de::Error::custom)
}

pub fn deserialize_byte_size_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    // deserialize our size as bytes
    let byte_size: Byte = serde::de::Deserialize::deserialize(deserializer)?;
    // convert our size to a usize
    byte_size
        .as_u64()
        .try_into()
        .map_err(serde::de::Error::custom)
}

/// A trait for types that can be converted to a byte size (usize)
///
/// This allows builder methods to accept strings like "4Gi", "100MB",
/// or raw numeric values like `1024u64` or `1024usize`. Numeric values
/// are assumed to be in mebibytes.
pub trait IntoStorageSize {
    /// Convert this value into a byte size
    ///
    /// # Errors
    /// Returns an error if the value cannot be parsed as a byte size
    fn into_bytes(self) -> Result<usize, ServerError>;
}

impl IntoStorageSize for usize {
    /// Convert this value into a byte size
    fn into_bytes(self) -> Result<usize, ServerError> {
        // convert our mebibytes into bytes
        Ok(self << 20)
    }
}

impl IntoStorageSize for u64 {
    /// Convert this value into a byte size
    fn into_bytes(self) -> Result<usize, ServerError> {
        // convert our raw value into a usize
        let converted = usize::try_from(self)?;
        // convert our mebibytes into bytes
        Ok(converted << 20)
    }
}

impl IntoStorageSize for Byte {
    /// Convert this value into a byte size
    fn into_bytes(self) -> Result<usize, ServerError> {
        Ok(self.as_u64() as usize)
    }
}

impl IntoStorageSize for &str {
    /// Convert this value into a byte size
    fn into_bytes(self) -> Result<usize, ServerError> {
        let byte = Byte::parse_str(self, true)?;
        Ok(byte.as_u64() as usize)
    }
}

impl IntoStorageSize for String {
    /// Convert this value into a byte size
    fn into_bytes(self) -> Result<usize, ServerError> {
        self.as_str().into_bytes()
    }
}
