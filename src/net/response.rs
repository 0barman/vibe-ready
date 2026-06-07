//! HTTP response returned by [`VibeHttpClient`](crate::VibeHttpClient).

use serde::de::DeserializeOwned;

use crate::api::engine_error::VibeEngineError;
use crate::net::error::{map_reqwest_error, status_error};

/// HTTP response with status, headers, and a lazily-read body.
#[derive(Debug)]
pub struct VibeHttpResponse {
    inner: reqwest::Response,
    context: String,
}

impl VibeHttpResponse {
    pub(crate) fn new(inner: reqwest::Response, context: String) -> Self {
        Self { inner, context }
    }

    /// Returns the HTTP status code.
    pub fn status(&self) -> u16 {
        self.inner.status().as_u16()
    }

    /// Returns whether the status code is in the 2xx range.
    pub fn is_success(&self) -> bool {
        self.inner.status().is_success()
    }

    /// Returns the first value of a response header, if present and valid UTF-8.
    pub fn header(&self, name: &str) -> Option<String> {
        self.inner
            .headers()
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.to_string())
    }

    /// Returns all response headers as name/value pairs.
    pub fn headers(&self) -> Vec<(String, String)> {
        self.inner
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|v| (name.as_str().to_string(), v.to_string()))
            })
            .collect()
    }

    /// Returns `self` when the status is 2xx, otherwise a status error.
    pub fn error_for_status(self) -> Result<Self, VibeEngineError> {
        if self.is_success() {
            Ok(self)
        } else {
            Err(status_error(&self.context, self.status()))
        }
    }

    /// Reads the response body and deserializes it as JSON.
    pub async fn json<T: DeserializeOwned>(self) -> Result<T, VibeEngineError> {
        let context = self.context.clone();
        self.inner
            .json::<T>()
            .await
            .map_err(|err| map_reqwest_error(&context, err))
    }

    /// Reads the response body as a UTF-8 string.
    pub async fn text(self) -> Result<String, VibeEngineError> {
        let context = self.context.clone();
        self.inner
            .text()
            .await
            .map_err(|err| map_reqwest_error(&context, err))
    }

    /// Reads the response body as raw bytes.
    pub async fn bytes(self) -> Result<Vec<u8>, VibeEngineError> {
        let context = self.context.clone();
        self.inner
            .bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|err| map_reqwest_error(&context, err))
    }
}

#[cfg(test)]
mod strict_tests {
    use super::*;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test/unit/net/response_tests.rs"
    ));
}
