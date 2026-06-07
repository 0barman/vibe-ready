//! HTTP client foundation for vibe-ready projects.

use std::time::Duration;

use serde::Serialize;

use crate::api::engine_error::{VibeEngineError, VibeEngineErrorCode};
use crate::net::method::VibeHttpMethod;
use crate::net::request::VibeHttpRequest;
use crate::net::response::VibeHttpResponse;
use crate::net::retry::VibeRetryPolicy;

/// HTTP client with connection pooling, timeouts, retries, and JSON helpers.
#[derive(Clone, Debug)]
pub struct VibeHttpClient {
    inner: reqwest::Client,
    base_url: Option<String>,
    retry: VibeRetryPolicy,
}

impl VibeHttpClient {
    /// Creates a client with default configuration.
    pub fn new() -> Result<Self, VibeEngineError> {
        Self::builder().build()
    }

    /// Returns a builder for advanced configuration.
    pub fn builder() -> VibeHttpClientBuilder {
        VibeHttpClientBuilder::new()
    }

    fn resolve_url(&self, url: &str) -> String {
        match &self.base_url {
            Some(base) if !(url.starts_with("http://") || url.starts_with("https://")) => {
                format!(
                    "{}/{}",
                    base.trim_end_matches('/'),
                    url.trim_start_matches('/')
                )
            }
            _ => url.to_string(),
        }
    }

    /// Starts building a request with the given method and URL.
    pub fn request(&self, method: VibeHttpMethod, url: impl AsRef<str>) -> VibeHttpRequest {
        VibeHttpRequest::new(
            self.inner.clone(),
            method.into(),
            self.resolve_url(url.as_ref()),
            self.retry.clone(),
        )
    }

    /// Sends a GET request.
    pub async fn get(&self, url: impl AsRef<str>) -> Result<VibeHttpResponse, VibeEngineError> {
        self.request(VibeHttpMethod::Get, url).send().await
    }

    /// Sends a DELETE request.
    pub async fn delete(&self, url: impl AsRef<str>) -> Result<VibeHttpResponse, VibeEngineError> {
        self.request(VibeHttpMethod::Delete, url).send().await
    }

    /// Sends a HEAD request.
    pub async fn head(&self, url: impl AsRef<str>) -> Result<VibeHttpResponse, VibeEngineError> {
        self.request(VibeHttpMethod::Head, url).send().await
    }

    /// Sends a POST request with a JSON body.
    pub async fn post_json<T: Serialize>(
        &self,
        url: impl AsRef<str>,
        body: &T,
    ) -> Result<VibeHttpResponse, VibeEngineError> {
        self.request(VibeHttpMethod::Post, url)
            .json(body)?
            .send()
            .await
    }

    /// Sends a PUT request with a JSON body.
    pub async fn put_json<T: Serialize>(
        &self,
        url: impl AsRef<str>,
        body: &T,
    ) -> Result<VibeHttpResponse, VibeEngineError> {
        self.request(VibeHttpMethod::Put, url)
            .json(body)?
            .send()
            .await
    }

    /// Sends a PATCH request with a JSON body.
    pub async fn patch_json<T: Serialize>(
        &self,
        url: impl AsRef<str>,
        body: &T,
    ) -> Result<VibeHttpResponse, VibeEngineError> {
        self.request(VibeHttpMethod::Patch, url)
            .json(body)?
            .send()
            .await
    }
}

/// Builder for [`VibeHttpClient`].
pub struct VibeHttpClientBuilder {
    timeout: Option<Duration>,
    connect_timeout: Option<Duration>,
    pool_max_idle_per_host: Option<usize>,
    pool_idle_timeout: Option<Duration>,
    user_agent: Option<String>,
    default_headers: Vec<(String, String)>,
    base_url: Option<String>,
    retry: VibeRetryPolicy,
}

impl VibeHttpClientBuilder {
    fn new() -> Self {
        Self {
            timeout: Some(Duration::from_secs(30)),
            connect_timeout: Some(Duration::from_secs(10)),
            pool_max_idle_per_host: None,
            pool_idle_timeout: None,
            user_agent: Some(format!("vibe-ready/{}", env!("CARGO_PKG_VERSION"))),
            default_headers: Vec::new(),
            base_url: None,
            retry: VibeRetryPolicy::default(),
        }
    }

    /// Sets the overall request timeout.
    pub fn timeout(mut self, value: Duration) -> Self {
        self.timeout = Some(value);
        self
    }

    /// Sets the connection timeout.
    pub fn connect_timeout(mut self, value: Duration) -> Self {
        self.connect_timeout = Some(value);
        self
    }

    /// Sets the maximum number of idle connections kept per host.
    pub fn pool_max_idle_per_host(mut self, value: usize) -> Self {
        self.pool_max_idle_per_host = Some(value);
        self
    }

    /// Sets how long idle connections stay in the pool.
    pub fn pool_idle_timeout(mut self, value: Duration) -> Self {
        self.pool_idle_timeout = Some(value);
        self
    }

    /// Sets the `User-Agent` header for all requests.
    pub fn user_agent(mut self, value: impl Into<String>) -> Self {
        self.user_agent = Some(value.into());
        self
    }

    /// Adds a default header sent with every request.
    pub fn default_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.default_headers.push((key.into(), value.into()));
        self
    }

    /// Adds a default bearer authorization header.
    pub fn bearer_auth(mut self, token: impl Into<String>) -> Self {
        self.default_headers.push((
            "authorization".to_string(),
            format!("Bearer {}", token.into()),
        ));
        self
    }

    /// Sets a base URL used to resolve relative request URLs.
    pub fn base_url(mut self, value: impl Into<String>) -> Self {
        self.base_url = Some(value.into());
        self
    }

    /// Sets the default retry policy applied to requests.
    pub fn retry(mut self, value: VibeRetryPolicy) -> Self {
        self.retry = value;
        self
    }

    /// Builds the configured [`VibeHttpClient`].
    pub fn build(self) -> Result<VibeHttpClient, VibeEngineError> {
        let mut builder = reqwest::Client::builder();
        if let Some(t) = self.timeout {
            builder = builder.timeout(t);
        }
        if let Some(t) = self.connect_timeout {
            builder = builder.connect_timeout(t);
        }
        if let Some(n) = self.pool_max_idle_per_host {
            builder = builder.pool_max_idle_per_host(n);
        }
        if let Some(t) = self.pool_idle_timeout {
            builder = builder.pool_idle_timeout(t);
        }
        if let Some(ua) = &self.user_agent {
            builder = builder.user_agent(ua);
        }

        if !self.default_headers.is_empty() {
            let mut headers = reqwest::header::HeaderMap::new();
            for (key, value) in &self.default_headers {
                let name = reqwest::header::HeaderName::from_bytes(key.as_bytes())
                    .map_err(|err| config_error(err.to_string()))?;
                let val = reqwest::header::HeaderValue::from_str(value)
                    .map_err(|err| config_error(err.to_string()))?;
                headers.insert(name, val);
            }
            builder = builder.default_headers(headers);
        }

        let inner = builder
            .build()
            .map_err(|err| config_error(err.to_string()))?;

        Ok(VibeHttpClient {
            inner,
            base_url: self.base_url,
            retry: self.retry,
        })
    }
}

fn config_error(message: String) -> VibeEngineError {
    VibeEngineError::from_error_code(VibeEngineErrorCode::ConfigError)
        .with_source(message)
        .with_context("VibeHttpClientBuilder::build")
}

#[cfg(test)]
mod strict_tests {
    use super::*;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test/unit/net/client_tests.rs"
    ));
}
