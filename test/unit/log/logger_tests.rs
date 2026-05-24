use std::sync::mpsc;
use std::time::Duration;

fn strict_log_path(prefix: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("strict-logger-{prefix}-{}", crate::platform::now()))
}

#[test]
fn noop_logger_filters_listener_and_closes_idempotently() {
    let logger = VibeLogger::try_new(
        VibeLogBackend::Noop,
        strict_log_path("noop"),
        false,
        "user".to_string(),
        LogLevel::Error,
        false,
        false,
        8,
    )
    .expect("create noop logger");
    let (tx, rx) = mpsc::channel();
    logger.set_log_listener(Some(Box::new(move |info| {
        tx.send((info.level(), info.tag())).expect("send listener event");
    })));
    std::thread::sleep(Duration::from_millis(50));

    logger.insert_log(true, LogLevel::Info as i32, "info".to_string(), "hidden".to_string(), 1);
    assert!(rx.recv_timeout(Duration::from_millis(120)).is_err());
    logger.insert_log(true, LogLevel::Error as i32, "err".to_string(), "shown".to_string(), 2);
    assert_eq!(rx.recv_timeout(Duration::from_secs(1)).expect("error event"), (LogLevel::Error, "err".to_string()));

    logger.set_filter(LogLevel::Debug);
    std::thread::sleep(Duration::from_millis(50));
    logger.insert_log(true, LogLevel::Info as i32, "info2".to_string(), "shown".to_string(), 3);
    assert_eq!(rx.recv_timeout(Duration::from_secs(1)).expect("info event"), (LogLevel::Info, "info2".to_string()));

    logger.set_log_listener(None);
    logger.close().expect("close logger");
    logger.close().expect("double close logger");
    logger.insert_log(true, LogLevel::Error as i32, "after-close".to_string(), "ignored".to_string(), 4);
}

#[test]
fn register_listener_installs_and_close_clears_global_listener() {
    let logger = VibeLogger::try_new(
        VibeLogBackend::Noop,
        strict_log_path("global"),
        false,
        "user".to_string(),
        LogLevel::Debug,
        false,
        false,
        8,
    )
    .expect("create logger");
    logger.register_listener();
    logger.close().expect("close clears global listener");
    VibeLogger::clear_global_log_listener();
}
