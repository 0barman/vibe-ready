//! HTTP method used by [`VibeHttpClient`](crate::VibeHttpClient).

/// HTTP request method.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum VibeHttpMethod {
    /// HTTP GET.
    Get,
    /// HTTP POST.
    Post,
    /// HTTP PUT.
    Put,
    /// HTTP PATCH.
    Patch,
    /// HTTP DELETE.
    Delete,
    /// HTTP HEAD.
    Head,
    /// HTTP OPTIONS.
    Options,
}

impl VibeHttpMethod {
    /// Returns the uppercase HTTP method name.
    pub fn as_str(&self) -> &'static str {
        match self {
            VibeHttpMethod::Get => "GET",
            VibeHttpMethod::Post => "POST",
            VibeHttpMethod::Put => "PUT",
            VibeHttpMethod::Patch => "PATCH",
            VibeHttpMethod::Delete => "DELETE",
            VibeHttpMethod::Head => "HEAD",
            VibeHttpMethod::Options => "OPTIONS",
        }
    }
}

impl std::fmt::Display for VibeHttpMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<VibeHttpMethod> for reqwest::Method {
    fn from(method: VibeHttpMethod) -> Self {
        match method {
            VibeHttpMethod::Get => reqwest::Method::GET,
            VibeHttpMethod::Post => reqwest::Method::POST,
            VibeHttpMethod::Put => reqwest::Method::PUT,
            VibeHttpMethod::Patch => reqwest::Method::PATCH,
            VibeHttpMethod::Delete => reqwest::Method::DELETE,
            VibeHttpMethod::Head => reqwest::Method::HEAD,
            VibeHttpMethod::Options => reqwest::Method::OPTIONS,
        }
    }
}

#[cfg(test)]
mod strict_tests {
    use super::*;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test/unit/net/method_tests.rs"
    ));
}
