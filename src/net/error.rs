//! Mapping from `reqwest` and serialization errors to [`VibeEngineError`].

use crate::api::engine_error::{VibeEngineError, VibeEngineErrorCode};

/// Converts a `reqwest::Error` into a [`VibeEngineError`] with a stable code.
pub(crate) fn map_reqwest_error(context: &str, err: reqwest::Error) -> VibeEngineError {
    let code = if err.is_timeout() {
        VibeEngineErrorCode::TimeoutError
    } else if err.is_connect() {
        if is_tls_error(&err) {
            VibeEngineErrorCode::TlsConnectError
        } else {
            VibeEngineErrorCode::ConnectError
        }
    } else if err.is_decode() {
        VibeEngineErrorCode::SerdeDeserializeError
    } else if err.is_builder() {
        VibeEngineErrorCode::ConfigError
    } else if err.is_request() {
        VibeEngineErrorCode::RequestError
    } else {
        VibeEngineErrorCode::NetworkError
    };

    VibeEngineError::from_error_code(code)
        .with_source(err.to_string())
        .with_context(context.to_string())
}

/// Builds an error for a non-success HTTP status code.
pub(crate) fn status_error(context: &str, status: u16) -> VibeEngineError {
    let code = if (400..500).contains(&status) {
        VibeEngineErrorCode::BadRequest
    } else {
        VibeEngineErrorCode::InternalServerError
    };
    VibeEngineError::from_error_code(code)
        .with_source(format!("http status {status}"))
        .with_context(context.to_string())
}

/// Builds a serialization error for request bodies.
pub(crate) fn serialize_error(context: &str, err: serde_json::Error) -> VibeEngineError {
    VibeEngineError::from_error_code(VibeEngineErrorCode::SerdeSerializeError)
        .with_source(err.to_string())
        .with_context(context.to_string())
}

pub(crate) fn is_tls_error(err: &reqwest::Error) -> bool {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(current) = source {
        let text = current.to_string().to_ascii_lowercase();
        if text.contains("tls") || text.contains("certificate") || text.contains("handshake") {
            return true;
        }
        source = current.source();
    }
    false
}

#[cfg(test)]
mod strict_tests {
    use super::*;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test/unit/net/error_tests.rs"
    ));
}
