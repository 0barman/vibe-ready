#[cfg(not(target_arch = "wasm32"))]
pub async fn sleep(timeout: std::time::Duration) {
    tokio::time::sleep(timeout).await;
}

#[cfg(not(target_arch = "wasm32"))]
pub fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
