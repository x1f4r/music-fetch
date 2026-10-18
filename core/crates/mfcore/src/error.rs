//! Errors crossing the FFI boundary.

use crate::model::ErrorCode;

/// Error returned by `Engine` methods.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[cfg_attr(feature = "ffi", derive(uniffi::Error))]
pub enum MfError {
    #[error("{message}")]
    InvalidInput { message: String },
    #[error("{message}")]
    UnsupportedLink { message: String },
    #[error("network error: {message}")]
    Network { message: String },
    #[error("rate limited; retry in {retry_in_ms} ms")]
    RateLimited { retry_in_ms: u64 },
    #[error("{message}")]
    MediaUnavailable { message: String },
    #[error("could not decode audio: {message}")]
    Decode { message: String },
    #[error("{message}")]
    Platform { message: String },
    #[error("storage error: {message}")]
    Storage { message: String },
    #[error("the melody index is not installed")]
    IndexUnavailable,
    #[error("canceled")]
    Canceled,
    #[error("internal error: {message}")]
    Internal { message: String },
}

impl MfError {
    pub fn code(&self) -> ErrorCode {
        match self {
            MfError::InvalidInput { .. } => ErrorCode::InvalidInput,
            MfError::UnsupportedLink { .. } => ErrorCode::UnsupportedLink,
            MfError::Network { .. } => ErrorCode::Network,
            MfError::RateLimited { .. } => ErrorCode::RateLimited,
            MfError::MediaUnavailable { .. } => ErrorCode::MediaUnavailable,
            MfError::Decode { .. } => ErrorCode::Decode,
            MfError::Platform { .. } => ErrorCode::Platform,
            MfError::Storage { .. } => ErrorCode::Storage,
            MfError::IndexUnavailable => ErrorCode::IndexUnavailable,
            MfError::Canceled | MfError::Internal { .. } => ErrorCode::Internal,
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        MfError::Internal { message: message.into() }
    }

    pub fn network(message: impl Into<String>) -> Self {
        MfError::Network { message: message.into() }
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        MfError::InvalidInput { message: message.into() }
    }
}

#[cfg(feature = "store")]
impl From<rusqlite::Error> for MfError {
    fn from(e: rusqlite::Error) -> Self {
        MfError::Storage { message: e.to_string() }
    }
}

impl From<std::io::Error> for MfError {
    fn from(e: std::io::Error) -> Self {
        MfError::Internal { message: e.to_string() }
    }
}

#[cfg(feature = "net")]
impl From<reqwest::Error> for MfError {
    fn from(e: reqwest::Error) -> Self {
        MfError::Network { message: e.to_string() }
    }
}

impl From<serde_json::Error> for MfError {
    fn from(e: serde_json::Error) -> Self {
        MfError::Internal { message: format!("json: {e}") }
    }
}

/// Error returned by app-implemented platform services.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[cfg_attr(feature = "ffi", derive(uniffi::Error))]
pub enum PlatformError {
    #[error("{message}")]
    Failed { message: String },
    /// The media cannot be fetched (private, removed, geo-blocked, unsupported site).
    #[error("{message}")]
    Unavailable { message: String },
    #[error("canceled")]
    Canceled,
    #[error("not supported on this platform")]
    NotSupported,
}

#[cfg(feature = "ffi")]
impl From<uniffi::UnexpectedUniFFICallbackError> for PlatformError {
    fn from(e: uniffi::UnexpectedUniFFICallbackError) -> Self {
        PlatformError::Failed { message: e.reason }
    }
}

impl From<PlatformError> for MfError {
    fn from(e: PlatformError) -> Self {
        match e {
            PlatformError::Failed { message } => MfError::Platform { message },
            PlatformError::Unavailable { message } => MfError::MediaUnavailable { message },
            PlatformError::Canceled => MfError::Canceled,
            PlatformError::NotSupported => MfError::Platform { message: "not supported on this platform".into() },
        }
    }
}

pub type Result<T, E = MfError> = std::result::Result<T, E>;
