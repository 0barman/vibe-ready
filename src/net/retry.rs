//! Retry and backoff policy for HTTP requests.

use std::time::Duration;

/// Retry and backoff policy applied to HTTP requests.
#[derive(Clone, Debug)]
pub struct VibeRetryPolicy {
    max_retries: u32,
    initial_backoff: Duration,
    max_backoff: Duration,
    backoff_multiplier: f64,
    retry_on_status: bool,
    jitter: bool,
}

impl VibeRetryPolicy {
    /// Creates a policy that never retries.
    pub fn none() -> Self {
        Self {
            max_retries: 0,
            initial_backoff: Duration::from_millis(0),
            max_backoff: Duration::from_millis(0),
            backoff_multiplier: 1.0,
            retry_on_status: false,
            jitter: false,
        }
    }

    /// Sets the maximum number of retries after the first attempt.
    pub fn max_retries(mut self, value: u32) -> Self {
        self.max_retries = value;
        self
    }

    /// Sets the initial backoff before the first retry.
    pub fn initial_backoff(mut self, value: Duration) -> Self {
        self.initial_backoff = value;
        self
    }

    /// Sets the maximum backoff between retries.
    pub fn max_backoff(mut self, value: Duration) -> Self {
        self.max_backoff = value;
        self
    }

    /// Sets the exponential backoff multiplier.
    pub fn backoff_multiplier(mut self, value: f64) -> Self {
        self.backoff_multiplier = value;
        self
    }

    /// Enables or disables retrying on retryable HTTP status codes.
    pub fn retry_on_status(mut self, value: bool) -> Self {
        self.retry_on_status = value;
        self
    }

    /// Enables or disables full jitter on backoff.
    pub fn jitter(mut self, value: bool) -> Self {
        self.jitter = value;
        self
    }

    /// Returns the configured maximum number of retries.
    pub fn max_retries_value(&self) -> u32 {
        self.max_retries
    }

    /// Returns whether jitter is enabled.
    pub fn jitter_enabled(&self) -> bool {
        self.jitter
    }

    /// Returns the configured maximum backoff between retries.
    pub fn max_backoff_value(&self) -> Duration {
        self.max_backoff
    }

    /// Returns whether a status code should trigger a retry.
    pub fn should_retry_status(&self, status: u16) -> bool {
        self.retry_on_status && matches!(status, 408 | 429 | 500 | 502 | 503 | 504)
    }

    /// Computes the backoff for a given zero-based attempt index.
    pub fn backoff_for(&self, attempt: u32) -> Duration {
        if self.initial_backoff.is_zero() {
            return Duration::from_millis(0);
        }
        let factor = self.backoff_multiplier.max(1.0).powi(attempt as i32);
        let computed = self.initial_backoff.as_millis() as f64 * factor;
        let capped = computed.min(self.max_backoff.as_millis() as f64);
        Duration::from_millis(capped as u64)
    }

    /// Applies full jitter to a base backoff using a caller-supplied seed.
    ///
    /// Returns a duration in `[0, base]`. When jitter is disabled, returns `base`.
    pub fn apply_jitter(&self, base: Duration, seed: u64) -> Duration {
        if !self.jitter || base.is_zero() {
            return base;
        }
        let mut x = seed | 1;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let frac = (x % 1000) as f64 / 1000.0;
        Duration::from_millis((base.as_millis() as f64 * frac) as u64)
    }
}

impl Default for VibeRetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 2,
            initial_backoff: Duration::from_millis(200),
            max_backoff: Duration::from_secs(5),
            backoff_multiplier: 2.0,
            retry_on_status: true,
            jitter: true,
        }
    }
}

#[cfg(test)]
mod strict_tests {
    use super::*;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test/unit/net/retry_tests.rs"
    ));
}
