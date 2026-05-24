use crate::log::log_level::LogLevel;

#[cfg(feature = "log-diesel")]
#[test]
fn log_diesel_create_send_close_and_reopen() {
    let path = std::env::temp_dir().join(format!("strict-log-diesel-{}", crate::platform::now()));
    let db_log = VibeDbLog::try_open(path.clone(), false, "user".to_string(), 2).expect("open log db");
    db_log.send_2_writer(VibeLogInfo::new(LogLevel::Error, "first".to_string(), "content".to_string(), 1));
    db_log.send_2_writer(VibeLogInfo::new(LogLevel::Warn, "second".to_string(), "content".to_string(), 2));
    db_log.send_2_writer(VibeLogInfo::new(LogLevel::Info, "third".to_string(), "content".to_string(), 3));
    std::thread::sleep(std::time::Duration::from_millis(150));
    db_log.close().expect("close log db");

    let reopened = VibeDbLog::try_open(path, false, "user".to_string(), 2).expect("reopen log db");
    reopened.close().expect("close reopened");
}
