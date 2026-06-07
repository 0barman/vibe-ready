//! Networking primitives for vibe-ready projects.
//!
//! Enabled by the `net-http` feature. Provides [`VibeHttpClient`] and its
//! request/response/retry types. WebSocket support is reserved for the future
//! `net-ws` feature.

#[cfg(feature = "net-http")]
mod client;
#[cfg(feature = "net-http")]
mod error;
#[cfg(feature = "net-http")]
mod method;
#[cfg(feature = "net-http")]
mod request;
#[cfg(feature = "net-http")]
mod response;
#[cfg(feature = "net-http")]
mod retry;

#[cfg(feature = "net-http")]
pub use client::{VibeHttpClient, VibeHttpClientBuilder};
#[cfg(feature = "net-http")]
pub use method::VibeHttpMethod;
#[cfg(feature = "net-http")]
pub use request::VibeHttpRequest;
#[cfg(feature = "net-http")]
pub use response::VibeHttpResponse;
#[cfg(feature = "net-http")]
pub use retry::VibeRetryPolicy;
