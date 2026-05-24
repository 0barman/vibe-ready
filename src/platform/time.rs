#[cfg(not(target_arch = "wasm32"))]
/// Sleeps asynchronously for `timeout` on the current Tokio runtime.
///
/// # Returns
///
/// This async function returns `()` after the duration elapses.
///
/// # Examples
///
/// ```no_run
/// # async fn demo() {
/// vibe_ready::platform::sleep(std::time::Duration::from_millis(10)).await;
/// # }
/// ```
pub async fn sleep(timeout: std::time::Duration) {
    tokio::time::sleep(timeout).await;
}

#[cfg(not(target_arch = "wasm32"))]
/// Returns the current Unix timestamp in milliseconds.
///
/// # Returns
///
/// Milliseconds since the Unix epoch, or `0` if the system clock is before the epoch.
///
/// # Examples
///
/// ```
/// let now = vibe_ready::platform::now();
/// assert!(now >= 0);
/// ```
pub fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod strict_tests {
    use super::*;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test/unit/platform/time_tests.rs"
    ));
}
