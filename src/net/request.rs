//! HTTP request builder used by [`VibeHttpClient`](crate::VibeHttpClient).

use std::time::Duration;

use serde::Serialize;

use crate::api::engine_error::{VibeEngineError, VibeEngineErrorCode};
use crate::net::error::{map_reqwest_error, serialize_error};
use crate::net::response::VibeHttpResponse;
use crate::net::retry::VibeRetryPolicy;

/// A configurable HTTP request awaiting [`send`](VibeHttpRequest::send).
pub struct VibeHttpRequest {
    method: reqwest::Method,
    url: String,
    builder: reqwest::RequestBuilder,
    retry: VibeRetryPolicy,
}

impl VibeHttpRequest {
    pub(crate) fn new(
        client: reqwest::Client,
        method: reqwest::Method,
        url: String,
        retry: VibeRetryPolicy,
    ) -> Self {
        let builder = client.request(method.clone(), &url);
        Self {
            method,
            url,
            builder,
            retry,
        }
    }

    fn context_label(&self) -> String {
        context_label(&self.method, &self.url)
    }

    /// Adds a request header.
    pub fn header(mut self, key: impl AsRef<str>, value: impl AsRef<str>) -> Self {
        self.builder = self.builder.header(key.as_ref(), value.as_ref());
        self
    }

    /// Adds a query parameter.
    pub fn query(mut self, key: impl AsRef<str>, value: impl AsRef<str>) -> Self {
        self.builder = self.builder.query(&[(key.as_ref(), value.as_ref())]);
        self
    }

    /// Adds a bearer authorization header.
    pub fn bearer_auth(mut self, token: impl Into<String>) -> Self {
        self.builder = self.builder.bearer_auth(token.into());
        self
    }

    /// Adds a basic authorization header.
    pub fn basic_auth(mut self, username: impl Into<String>, password: Option<String>) -> Self {
        self.builder = self.builder.basic_auth(username.into(), password);
        self
    }

    /// Sets a JSON request body and the `content-type` header.
    pub fn json<T: Serialize>(mut self, value: &T) -> Result<Self, VibeEngineError> {
        let context = self.context_label();
        let body = serde_json::to_vec(value).map_err(|err| serialize_error(&context, err))?;
        self.builder = self
            .builder
            .header("content-type", "application/json")
            .body(body);
        Ok(self)
    }

    /// Sets a raw byte request body.
    pub fn body_bytes(mut self, body: Vec<u8>) -> Self {
        self.builder = self.builder.body(body);
        self
    }

    /// Sets a text request body.
    pub fn body_text(mut self, body: impl Into<String>) -> Self {
        self.builder = self.builder.body(body.into());
        self
    }

    /// Overrides the request timeout for this request only.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.builder = self.builder.timeout(timeout);
        self
    }

    /// Overrides the retry policy for this request only.
    pub fn retry(mut self, retry: VibeRetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Sends the request, applying the configured retry policy.
    pub async fn send(self) -> Result<VibeHttpResponse, VibeEngineError> {
        let VibeHttpRequest {
            method,
            url,
            builder,
            retry,
        } = self;
        let context = context_label(&method, &url);
        let max = retry.max_retries_value();
        let mut attempt: u32 = 0;

        loop {
            let attempt_builder = builder.try_clone().ok_or_else(|| {
                VibeEngineError::from_error_code(VibeEngineErrorCode::RequestError)
                    .with_source("request body is not retryable (stream)")
                    .with_context(context.clone())
            })?;

            match attempt_builder.send().await {
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    if attempt < max && retry.should_retry_status(status) {
                        let wait = backoff_with_retry_after(&retry, attempt, &resp);
                        sleep_backoff(wait).await;
                        attempt += 1;
                        continue;
                    }
                    return Ok(VibeHttpResponse::new(resp, context));
                }
                Err(err) => {
                    let retryable = (err.is_timeout() || err.is_connect())
                        && !crate::net::error::is_tls_error(&err);
                    if attempt < max && retryable {
                        let base = retry.backoff_for(attempt);
                        let wait = retry.apply_jitter(base, seed(attempt));
                        sleep_backoff(wait).await;
                        attempt += 1;
                        continue;
                    }
                    return Err(map_reqwest_error(&context, err));
                }
            }
        }
    }
}

fn seed(attempt: u32) -> u64 {
    (crate::platform::now() as u64) ^ (attempt as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

fn context_label(method: &reqwest::Method, url: &str) -> String {
    format!("http {} {}", method.as_str(), url)
}

// Computes the wait before the next retry. Honors a numeric (delta-seconds)
// `Retry-After` header when present, clamped to the policy's `max_backoff` so a
// hostile or misconfigured header cannot stall the request indefinitely. The
// HTTP-date form of `Retry-After` is not parsed and falls back to exponential
// backoff.
fn backoff_with_retry_after(
    retry: &VibeRetryPolicy,
    attempt: u32,
    resp: &reqwest::Response,
) -> Duration {
    if let Some(value) = resp.headers().get("retry-after") {
        if let Ok(text) = value.to_str() {
            if let Ok(secs) = text.trim().parse::<u64>() {
                return Duration::from_secs(secs).min(retry.max_backoff_value());
            }
        }
    }
    let base = retry.backoff_for(attempt);
    retry.apply_jitter(base, seed(attempt))
}

async fn sleep_backoff(duration: Duration) {
    if !duration.is_zero() {
        tokio::time::sleep(duration).await;
    }
}

#[cfg(test)]
mod strict_tests {
    use super::*;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test/unit/net/request_tests.rs"
    ));
}
