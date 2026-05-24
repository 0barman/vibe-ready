use crate::log::log_level::LogLevel;

fn strict_backend_path(prefix: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("strict-log-backend-{prefix}-{}", crate::platform::now()))
}

#[test]
fn noop_log_backend_drops_logs_and_closes() {
    let backend = VibeLogBackendHandle::try_open(
        VibeLogBackend::Noop,
        strict_backend_path("noop"),
        false,
        "user".to_string(),
        8,
    )
    .expect("noop backend");
    backend.send_2_writer(VibeLogInfo::new(LogLevel::Info, "tag".to_string(), "content".to_string(), 1));
    backend.close().expect("noop close");
}

#[cfg(feature = "log-diesel")]
#[test]
fn diesel_log_backend_opens_sends_large_log_and_closes() {
    let path = strict_backend_path("diesel");
    let backend = VibeLogBackendHandle::try_open(
        VibeLogBackend::DieselSqlite,
        path.clone(),
        false,
        "user".to_string(),
        4,
    )
    .expect("diesel backend");
    backend.send_2_writer(VibeLogInfo::new(LogLevel::Info, "tag".to_string(), "x".repeat(1024 * 1024 + 1), 1));
    std::thread::sleep(std::time::Duration::from_millis(120));
    backend.close().expect("diesel close");
    assert!(path.join(crate::store::db::db_common::LOG_DB_NAME).exists());
}
