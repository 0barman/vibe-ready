use crate::log::log_def::CODE_STR;
use crate::log_e;
use crate::store::db::sql_def::DbError;
use log::error;
use num_derive::FromPrimitive;
use std::backtrace::Backtrace;
use std::fmt;

#[repr(i32)]
#[derive(Clone, Copy, Debug, FromPrimitive, PartialEq)]
/// Stable numeric error codes returned by vibe-ready.
pub enum VibeEngineErrorCode {
    Unknown = -1,

    Success = 200,

    /// Rust
    EngineDropped = 100001,

    /// Android, iOS, Web
    DatabaseNotOpened = 100002,

    /// Android, iOS, Web
    DatabaseOpenFailed = 100003,
    LogDatabaseOpenFailed = 100223,

    WorkDatabaseOpenFailed = 132003,

    /// Android, iOS, Web
    DatabaseIOError = 100004,

    /// Android, iOS, Web
    DatabaseTargetNotFound = 100005,

    /// Rust
    DatabaseThreadError = 100006,

    PostError = 100007,
    /// Task was cancelled before it could complete (B9 scheduler).
    Cancelled = 100008,

    ParameterEmpty = 100012,
    OAuthError = 100013,
    ConfigError = 100014,

    // std::io::Error
    IOError = 100017,
    BadRequest = 100018,
    RequestError = 100019,
    InternalServerError = 100020,
    // #[error("network error {0}")]
    NetworkError = 100021,
    // #[error("unsupported error")]
    UnsupportedError = 100022,

    // #[error("timeout error")]
    TimeoutError = 100023,
    // #[error("connect error {0}")]
    ConnectError = 100024,
    // #[error("connect error {0}")]
    TlsConnectError = 100025,
    // #[error("options parse failed {0}")]
    OptionsParseError = 100026,

    SerdeDeserializeError = 100027,
    SerdeSerializeError = 100028,

    InvalidArgumentLogInfo = 100029,

    MPSCSendError = 100037,

    GenerateQRError = 10005,
    RuntimeError = 10006,
    BeaverError = 10007,
    SocketRecvTimeout = 10008,
    SocketClosed = 10009,
    ProtocParseError = 10010,
    SocketNotOpened = 10011,
    TaskInterruptionError = 10012,

    /// Android, iOS, Web
    ConnectionClosed = 30001,

    /// Android, iOS, Web
    ConnectionExists = 34001,

    /// Rust
    ConnectionClosing = 30027,

    /// Android,iOS,Web
    InternalError = 32002,
    NotLoggedInError = 32003,
    PageTokenError = 32004,
    ClipboardInitializeError = 32005,

    NotSupportedYet = 999999,
}

impl VibeEngineErrorCode {
    pub fn code(&self) -> i32 {
        *self as i32
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
/// High-level category for a [`VibeEngineError`].
pub enum VibeErrorKind {
    Config,
    Runtime,
    Task,
    Database,
    Network,
    Log,
    Unsupported,
    Internal,
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
    pub fn code(&self) -> i32 {
        self.code
    }

    pub fn kind(&self) -> VibeErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn source_message(&self) -> Option<&str> {
        self.source.as_deref()
    }

    pub fn context(&self) -> &[String] {
        &self.context
    }

    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }

    pub fn with_context(mut self, context: impl Into<String>) -> Self {
        self.context.push(context.into());
        self
    }

    pub fn from_success() -> Self {
        Self::from_error_code(VibeEngineErrorCode::Success)
    }

    pub fn from_u16(code: u16) -> Self {
        VibeEngineError::from_raw_code(code as i32)
    }

    pub fn from_u32(code: u32) -> Self {
        VibeEngineError::from_raw_code(code as i32)
    }

    pub fn is_success(&self) -> bool {
        self.code == VibeEngineErrorCode::Success.code()
    }

    pub fn same_code(&self) -> Self {
        self.clone()
    }

    pub fn from_error_code(value: VibeEngineErrorCode) -> Self {
        VibeEngineError {
            code: value.code(),
            kind: VibeErrorKind::from_code(value),
            message: default_message(value).to_string(),
            source: None,
            context: Vec::new(),
        }
    }

    pub fn from_code(value: VibeEngineErrorCode) -> Self {
        Self::from_error_code(value)
    }

    pub fn from_error_code_msg(value: VibeEngineErrorCode, msg: String) -> Self {
        VibeEngineError {
            message: msg,
            ..Self::from_error_code(value)
        }
    }

    pub fn from_task_interruption_error() -> Self {
        Self::from_error_code(VibeEngineErrorCode::TaskInterruptionError)
    }

    pub fn from_internal_error() -> Self {
        Self::from_error_code(VibeEngineErrorCode::InternalError)
    }

    pub fn from_mpsc_send_error() -> Self {
        Self::from_error_code(VibeEngineErrorCode::MPSCSendError)
    }

    pub fn from_parameter_empty() -> Self {
        Self::from_error_code(VibeEngineErrorCode::ParameterEmpty)
    }
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
