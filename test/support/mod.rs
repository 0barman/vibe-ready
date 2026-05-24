#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use vibe_ready::{
    VibeEngine, VibeEngineConfig, VibeErrorCode, VibeLogBackend, VibePlatformType, VibeResult,
    VibeStoreBackend,
};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

pub fn unique_name(prefix: &str) -> String {
    let id = NEXT_ID.fetch_add(1, Ordering::SeqCst);
    format!("{}-{}-{}", prefix, std::process::id(), id)
}

pub fn unique_store_root(prefix: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(unique_name(prefix))
}

pub fn noop_config(prefix: &str) -> VibeEngineConfig {
    VibeEngineConfig::builder()
        .platform(VibePlatformType::MacOS)
        .app_name(unique_name(prefix))
        .namespace("tests")
        .store_root_path(unique_store_root(prefix))
        .log_backend(VibeLogBackend::Noop)
        .store_backend(VibeStoreBackend::Noop)
        .runtime_worker_threads(1)
        .callback_threads(1)
        .queue_capacity(16, 8)
        .priority_queue_capacity(32)
        .build()
}

pub fn diesel_config(prefix: &str) -> VibeEngineConfig {
    VibeEngineConfig::builder()
        .platform(VibePlatformType::MacOS)
        .app_name(unique_name(prefix))
        .namespace("tests")
        .store_root_path(unique_store_root(prefix))
        .runtime_worker_threads(1)
        .callback_threads(1)
        .queue_capacity(32, 16)
        .priority_queue_capacity(64)
        .build()
}

pub fn engine_with_noop(prefix: &str) -> VibeResult<VibeEngine> {
    VibeEngine::create(noop_config(prefix))
}

pub fn engine_with_diesel(prefix: &str) -> VibeResult<VibeEngine> {
    VibeEngine::create(diesel_config(prefix))
}

pub fn assert_error_code(error: &vibe_ready::VibeEngineError, code: VibeErrorCode) {
    assert_eq!(error.code(), code.code(), "unexpected error: {error}");
}

pub fn recv_timeout<T>(rx: &mpsc::Receiver<T>) -> T {
    rx.recv_timeout(Duration::from_secs(2))
        .expect("expected message before timeout")
}

pub fn sleep_short() {
    std::thread::sleep(Duration::from_millis(120));
}
