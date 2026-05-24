use crate::log::log_def::CODE_STR;
use crate::log_e;
use crate::store::db::sql_def::DbError;
use log::error;
use std::backtrace::Backtrace;
use std::fmt;

#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq)]
/// Stable numeric error codes returned by vibe-ready.
pub enum VibeEngineErrorCode {
    /// Unknown or unmapped error.
    Unknown = -1,

    /// Operation completed successfully.
    Success = 200,

    /// Engine has already been dropped.
    EngineDropped = 100001,

    /// Database has not been opened yet.
    DatabaseNotOpened = 100002,

    /// Database open operation failed.
    DatabaseOpenFailed = 100003,
    /// Log database open operation failed.
    LogDatabaseOpenFailed = 100223,

    /// Work database open operation failed.
    WorkDatabaseOpenFailed = 132003,

    /// Database I/O operation failed.
    DatabaseIOError = 100004,

    /// Requested database target was not found.
    DatabaseTargetNotFound = 100005,

    /// Database worker thread failed.
    DatabaseThreadError = 100006,

    /// Task could not be posted to the runtime.
    PostError = 100007,
    /// Task was cancelled before it could complete (B9 scheduler).
    Cancelled = 100008,

    /// Required parameter is empty.
    ParameterEmpty = 100012,
    /// OAuth flow failed.
    OAuthError = 100013,
    /// Engine configuration is invalid.
    ConfigError = 100014,

    /// Standard I/O error.
    IOError = 100017,
    /// HTTP or protocol bad-request error.
    BadRequest = 100018,
    /// Request operation failed.
    RequestError = 100019,
    /// Remote service returned an internal server error.
    InternalServerError = 100020,
    /// Network operation failed.
    NetworkError = 100021,
    /// Requested operation is unsupported.
    UnsupportedError = 100022,

    /// Operation timed out.
    TimeoutError = 100023,
    /// Connection attempt failed.
    ConnectError = 100024,
    /// TLS connection attempt failed.
    TlsConnectError = 100025,
    /// Options parsing failed.
    OptionsParseError = 100026,

    /// Deserialization failed.
    SerdeDeserializeError = 100027,
    /// Serialization failed.
    SerdeSerializeError = 100028,

    /// Log information argument is invalid.
    InvalidArgumentLogInfo = 100029,

    /// MPSC channel send failed.
    MPSCSendError = 100037,

    /// QR generation failed.
    GenerateQRError = 10005,
    /// Runtime operation failed.
    RuntimeError = 10006,
    /// Internal beaver subsystem failed.
    BeaverError = 10007,
    /// Socket receive operation timed out.
    SocketRecvTimeout = 10008,
    /// Socket has been closed.
    SocketClosed = 10009,
    /// Protocol parsing failed.
    ProtocParseError = 10010,
    /// Socket has not been opened.
    SocketNotOpened = 10011,
    /// Task was interrupted.
    TaskInterruptionError = 10012,

    /// Connection is closed.
    ConnectionClosed = 30001,

    /// Connection already exists.
    ConnectionExists = 34001,

    /// Connection is currently closing.
    ConnectionClosing = 30027,

    /// Internal operation failed.
    InternalError = 32002,
    /// User is not logged in.
    NotLoggedInError = 32003,
    /// Page token is invalid.
    PageTokenError = 32004,
    /// Clipboard initialization failed.
    ClipboardInitializeError = 32005,

    /// Feature or backend support has not been implemented yet.
    NotSupportedYet = 999999,
}

impl VibeEngineErrorCode {
    /// Returns the stable numeric code associated with this variant.
    ///
    /// # Returns
    ///
    /// The `i32` code used in serialized errors and logs.
    ///
    /// # Examples
    ///
    /// ```
    /// use vibe_ready::VibeErrorCode;
    ///
    /// assert_eq!(VibeErrorCode::Success.code(), 200);
    /// ```
    pub fn code(&self) -> i32 {
        *self as i32
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
/// High-level category for a [`VibeEngineError`].
pub enum VibeErrorKind {
    /// Configuration or argument error.
    Config,
    /// Runtime or executor error.
    Runtime,
    /// Task scheduling or cancellation error.
    Task,
    /// Database or storage error.
    Database,
    /// Network or connection error.
    Network,
    /// Logging backend error.
    Log,
    /// Unsupported operation error.
    Unsupported,
    /// Internal SDK error.
    Internal,
    /// Unknown or uncategorized error.
    Unknown,
}

impl VibeErrorKind {
    fn from_code(code: VibeEngineErrorCode) -> Self {
        match code {
            VibeEngineErrorCode::ConfigError
            | VibeEngineErrorCode::ParameterEmpty
            | VibeEngineErrorCode::InvalidArgumentLogInfo => Self::Config,
            VibeEngineErrorCode::RuntimeError
            | VibeEngineErrorCode::EngineDropped
            | VibeEngineErrorCode::MPSCSendError => Self::Runtime,
            VibeEngineErrorCode::PostError
            | VibeEngineErrorCode::TaskInterruptionError
            | VibeEngineErrorCode::Cancelled => Self::Task,
            VibeEngineErrorCode::DatabaseNotOpened
            | VibeEngineErrorCode::DatabaseOpenFailed
            | VibeEngineErrorCode::LogDatabaseOpenFailed
            | VibeEngineErrorCode::WorkDatabaseOpenFailed
            | VibeEngineErrorCode::DatabaseIOError
            | VibeEngineErrorCode::DatabaseTargetNotFound
            | VibeEngineErrorCode::DatabaseThreadError => Self::Database,
            VibeEngineErrorCode::NetworkError
            | VibeEngineErrorCode::BadRequest
            | VibeEngineErrorCode::RequestError
            | VibeEngineErrorCode::InternalServerError
            | VibeEngineErrorCode::TimeoutError
            | VibeEngineErrorCode::ConnectError
            | VibeEngineErrorCode::TlsConnectError
            | VibeEngineErrorCode::SocketRecvTimeout
            | VibeEngineErrorCode::SocketClosed
            | VibeEngineErrorCode::SocketNotOpened
            | VibeEngineErrorCode::ConnectionClosed
            | VibeEngineErrorCode::ConnectionExists
            | VibeEngineErrorCode::ConnectionClosing => Self::Network,
            VibeEngineErrorCode::UnsupportedError | VibeEngineErrorCode::NotSupportedYet => {
                Self::Unsupported
            }
            VibeEngineErrorCode::InternalError
            | VibeEngineErrorCode::IOError
            | VibeEngineErrorCode::SerdeDeserializeError
            | VibeEngineErrorCode::SerdeSerializeError
            | VibeEngineErrorCode::OptionsParseError
            | VibeEngineErrorCode::GenerateQRError
            | VibeEngineErrorCode::BeaverError
            | VibeEngineErrorCode::ProtocParseError
            | VibeEngineErrorCode::OAuthError
            | VibeEngineErrorCode::NotLoggedInError
            | VibeEngineErrorCode::PageTokenError
            | VibeEngineErrorCode::ClipboardInitializeError => Self::Internal,
            VibeEngineErrorCode::Success | VibeEngineErrorCode::Unknown => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone)]
/// Error returned by vibe-ready operations.
pub struct VibeEngineError {
    code: i32,
    kind: VibeErrorKind,
    message: String,
    source: Option<String>,
    context: Vec<String>,
}

impl fmt::Display for VibeEngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "vibe-ready error [{:?}/{}]: {}",
            self.kind, self.code, self.message
        )?;
        if let Some(source) = &self.source {
            write!(f, "; source: {source}")?;
        }
        if !self.context.is_empty() {
            write!(f, "; context: {}", self.context.join("; "))?;
        }
        Ok(())
    }
}

impl std::error::Error for VibeEngineError {}

impl From<DbError> for VibeEngineError {
    fn from(err: DbError) -> Self {
        error!("DbError: {:?}, {:?}", err, Backtrace::capture());
        let db_error = err.clone();
        let mut error = match err {
            DbError::OpenFailed => {
                VibeEngineError::from_code(VibeEngineErrorCode::DatabaseOpenFailed)
            }
            DbError::DatabaseIOError => {
                VibeEngineError::from_code(VibeEngineErrorCode::DatabaseIOError)
            }
            DbError::NotOpen => VibeEngineError::from_code(VibeEngineErrorCode::DatabaseOpenFailed),
            DbError::TargetNotFound => {
                VibeEngineError::from_code(VibeEngineErrorCode::DatabaseTargetNotFound)
            }
            DbError::DatabaseThreadError => {
                VibeEngineError::from_code(VibeEngineErrorCode::DatabaseThreadError)
            }
            DbError::DatabaseUnlockError => {
                VibeEngineError::from_code(VibeEngineErrorCode::DatabaseOpenFailed)
            }
            DbError::NotSupportedYet => {
                VibeEngineError::from_code(VibeEngineErrorCode::DatabaseIOError)
            }
            DbError::JoinError => VibeEngineError::from_code(VibeEngineErrorCode::DatabaseIOError),
            _ => VibeEngineError::from_code(VibeEngineErrorCode::Unknown),
        };
        error.source = Some(format!("{db_error:?}"));
        error
    }
}

impl VibeEngineError {
    /// Returns the numeric error code.
    ///
    /// # Returns
    ///
    /// The stable `i32` code associated with this error.
    pub fn code(&self) -> i32 {
        self.code
    }

    /// Returns the high-level error category.
    ///
    /// # Returns
    ///
    /// A [`VibeErrorKind`] derived from the numeric code.
    pub fn kind(&self) -> VibeErrorKind {
        self.kind
    }

    /// Returns the human-readable error message.
    ///
    /// # Returns
    ///
    /// A borrowed message string.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the optional source message.
    ///
    /// # Returns
    ///
    /// `Some(&str)` when a lower-level source was attached.
    pub fn source_message(&self) -> Option<&str> {
        self.source.as_deref()
    }

    /// Returns contextual breadcrumbs attached to the error.
    ///
    /// # Returns
    ///
    /// A borrowed slice of context strings.
    pub fn context(&self) -> &[String] {
        &self.context
    }

    /// Attaches a source message to this error.
    ///
    /// # Returns
    ///
    /// The updated error value.
    ///
    /// # Examples
    ///
    /// ```
    /// use vibe_ready::{VibeEngineError, VibeErrorCode};
    ///
    /// let error = VibeEngineError::from_error_code(VibeErrorCode::RuntimeError)
    ///     .with_source("worker stopped");
    /// assert_eq!(error.source_message(), Some("worker stopped"));
    /// ```
    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }

    /// Adds contextual information to this error.
    ///
    /// # Returns
    ///
    /// The updated error value.
    pub fn with_context(mut self, context: impl Into<String>) -> Self {
        self.context.push(context.into());
        self
    }

    /// Creates the canonical success value.
    ///
    /// # Returns
    ///
    /// A [`VibeEngineError`] whose code is [`VibeEngineErrorCode::Success`].
    pub fn from_success() -> Self {
        Self::from_error_code(VibeEngineErrorCode::Success)
    }

    /// Creates an error from an unsigned 16-bit raw code.
    ///
    /// # Returns
    ///
    /// A raw-code error with [`VibeErrorKind::Unknown`].
    pub fn from_u16(code: u16) -> Self {
        VibeEngineError::from_raw_code(code as i32)
    }

    /// Creates an error from an unsigned 32-bit raw code.
    ///
    /// # Returns
    ///
    /// A raw-code error with [`VibeErrorKind::Unknown`].
    pub fn from_u32(code: u32) -> Self {
        VibeEngineError::from_raw_code(code as i32)
    }

    /// Checks whether this error represents success.
    ///
    /// # Returns
    ///
    /// `true` when the code is [`VibeEngineErrorCode::Success`].
    pub fn is_success(&self) -> bool {
        self.code == VibeEngineErrorCode::Success.code()
    }

    /// Clones this error while preserving the same code and context.
    ///
    /// # Returns
    ///
    /// A clone of this error.
    pub fn same_code(&self) -> Self {
        self.clone()
    }

    /// Creates an error from a stable code variant.
    ///
    /// # Returns
    ///
    /// A [`VibeEngineError`] with default kind and message for the code.
    pub fn from_error_code(value: VibeEngineErrorCode) -> Self {
        VibeEngineError {
            code: value.code(),
            kind: VibeErrorKind::from_code(value),
            message: default_message(value).to_string(),
            source: None,
            context: Vec::new(),
        }
    }

    /// Creates an error from a stable code variant.
    ///
    /// # Returns
    ///
    /// A [`VibeEngineError`] with default kind and message for the code.
    pub fn from_code(value: VibeEngineErrorCode) -> Self {
        Self::from_error_code(value)
    }

    /// Creates an error from a code variant and custom message.
    ///
    /// # Returns
    ///
    /// A [`VibeEngineError`] using `msg` as the display message.
    pub fn from_error_code_msg(value: VibeEngineErrorCode, msg: String) -> Self {
        VibeEngineError {
            message: msg,
            ..Self::from_error_code(value)
        }
    }

    /// Creates the standard task-interruption error.
    ///
    /// # Returns
    ///
    /// A [`VibeEngineError`] with [`VibeEngineErrorCode::TaskInterruptionError`].
    pub fn from_task_interruption_error() -> Self {
        Self::from_error_code(VibeEngineErrorCode::TaskInterruptionError)
    }

    /// Creates the standard internal error.
    ///
    /// # Returns
    ///
    /// A [`VibeEngineError`] with [`VibeEngineErrorCode::InternalError`].
    pub fn from_internal_error() -> Self {
        Self::from_error_code(VibeEngineErrorCode::InternalError)
    }

    /// Creates the standard MPSC send error.
    ///
    /// # Returns
    ///
    /// A [`VibeEngineError`] with [`VibeEngineErrorCode::MPSCSendError`].
    pub fn from_mpsc_send_error() -> Self {
        Self::from_error_code(VibeEngineErrorCode::MPSCSendError)
    }

    /// Creates the standard empty-parameter error.
    ///
    /// # Returns
    ///
    /// A [`VibeEngineError`] with [`VibeEngineErrorCode::ParameterEmpty`].
    pub fn from_parameter_empty() -> Self {
        Self::from_error_code(VibeEngineErrorCode::ParameterEmpty)
    }
    /// Logs and creates the standard empty-parameter error.
    ///
    /// # Returns
    ///
    /// A [`VibeEngineError`] with [`VibeEngineErrorCode::ParameterEmpty`].
    pub fn from_parameter_empty_log(tag: &str) -> Self {
        let code = VibeEngineErrorCode::ParameterEmpty.code();
        log_e!(tag, CODE_STR, code);
        Self::from_error_code(VibeEngineErrorCode::ParameterEmpty)
    }

    fn from_raw_code(code: i32) -> Self {
        VibeEngineError {
            code,
            kind: VibeErrorKind::Unknown,
            message: format!("unknown error code {code}"),
            source: None,
            context: Vec::new(),
        }
    }
}

fn default_message(code: VibeEngineErrorCode) -> &'static str {
    match code {
        VibeEngineErrorCode::Unknown => "unknown error",
        VibeEngineErrorCode::Success => "success",
        VibeEngineErrorCode::EngineDropped => "engine has been dropped",
        VibeEngineErrorCode::DatabaseNotOpened => "database is not opened",
        VibeEngineErrorCode::DatabaseOpenFailed => "database open failed",
        VibeEngineErrorCode::LogDatabaseOpenFailed => "log database open failed",
        VibeEngineErrorCode::WorkDatabaseOpenFailed => "work database open failed",
        VibeEngineErrorCode::DatabaseIOError => "database I/O error",
        VibeEngineErrorCode::DatabaseTargetNotFound => "database target not found",
        VibeEngineErrorCode::DatabaseThreadError => "database worker thread error",
        VibeEngineErrorCode::PostError => "task post failed",
        VibeEngineErrorCode::Cancelled => "task was cancelled",
        VibeEngineErrorCode::ParameterEmpty => "required parameter is empty",
        VibeEngineErrorCode::OAuthError => "OAuth error",
        VibeEngineErrorCode::ConfigError => "configuration error",
        VibeEngineErrorCode::IOError => "I/O error",
        VibeEngineErrorCode::BadRequest => "bad request",
        VibeEngineErrorCode::RequestError => "request error",
        VibeEngineErrorCode::InternalServerError => "internal server error",
        VibeEngineErrorCode::NetworkError => "network error",
        VibeEngineErrorCode::UnsupportedError => "unsupported operation",
        VibeEngineErrorCode::TimeoutError => "operation timed out",
        VibeEngineErrorCode::ConnectError => "connection failed",
        VibeEngineErrorCode::TlsConnectError => "TLS connection failed",
        VibeEngineErrorCode::OptionsParseError => "options parse failed",
        VibeEngineErrorCode::SerdeDeserializeError => "deserialization failed",
        VibeEngineErrorCode::SerdeSerializeError => "serialization failed",
        VibeEngineErrorCode::InvalidArgumentLogInfo => "invalid log info argument",
        VibeEngineErrorCode::MPSCSendError => "channel send failed",
        VibeEngineErrorCode::GenerateQRError => "QR generation failed",
        VibeEngineErrorCode::RuntimeError => "runtime error",
        VibeEngineErrorCode::BeaverError => "internal beaver error",
        VibeEngineErrorCode::SocketRecvTimeout => "socket receive timed out",
        VibeEngineErrorCode::SocketClosed => "socket closed",
        VibeEngineErrorCode::ProtocParseError => "protocol parse failed",
        VibeEngineErrorCode::SocketNotOpened => "socket is not opened",
        VibeEngineErrorCode::TaskInterruptionError => "task interrupted",
        VibeEngineErrorCode::ConnectionClosed => "connection closed",
        VibeEngineErrorCode::ConnectionExists => "connection already exists",
        VibeEngineErrorCode::ConnectionClosing => "connection is closing",
        VibeEngineErrorCode::InternalError => "internal error",
        VibeEngineErrorCode::NotLoggedInError => "not logged in",
        VibeEngineErrorCode::PageTokenError => "page token error",
        VibeEngineErrorCode::ClipboardInitializeError => "clipboard initialization failed",
        VibeEngineErrorCode::NotSupportedYet => "not supported yet",
    }
}

impl serde::Serialize for VibeEngineError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("VibeEngineError", 5)?;
        s.serialize_field("code", &self.code())?;
        s.serialize_field("kind", &self.kind)?;
        s.serialize_field("message", &self.message)?;
        s.serialize_field("source", &self.source)?;
        s.serialize_field("context", &self.context)?;
        s.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_std_error<T: std::error::Error>() {}

    #[test]
    fn error_model_exposes_kind_message_source_and_context() {
        assert_std_error::<VibeEngineError>();

        let error = VibeEngineError::from_error_code_msg(
            VibeEngineErrorCode::TimeoutError,
            "destroy timed out".to_string(),
        )
        .with_source("shutdown queue did not finish")
        .with_context("engine.destroy_with_timeout");

        assert_eq!(error.code(), VibeEngineErrorCode::TimeoutError.code());
        assert_eq!(error.kind(), VibeErrorKind::Network);
        assert_eq!(error.message(), "destroy timed out");
        assert_eq!(
            error.source_message(),
            Some("shutdown queue did not finish")
        );
        assert_eq!(
            error.context(),
            &["engine.destroy_with_timeout".to_string()]
        );
        assert!(error.to_string().contains("destroy timed out"));
    }
}

#[cfg(test)]
mod strict_tests {
    use super::*;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test/unit/api/engine_error_tests.rs"
    ));
}
