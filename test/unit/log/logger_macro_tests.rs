use std::sync::mpsc;
use std::time::Duration;

#[test]
fn create_log_content_handles_empty_missing_and_extra_values() {
    assert_eq!(create_log_content(None, None), "{}");
    assert_eq!(create_log_content(Some("a|b"), Some(vec![serde_json::json!(1)])), "{\"a\":1}");
    assert_eq!(
        create_log_content(Some("a"), Some(vec![serde_json::json!(1), serde_json::json!(2)])),
        "{\"a\":1}"
    );
}

#[test]
fn global_log_listener_receives_tag_location_level_and_content() {
    let (tx, rx) = mpsc::channel();
    VibeLogger::register_log_listener(Some(move |location, level, tag, content| {
        tx.send((location, level, tag, content)).expect("send log event");
    }));

    on_log(
        "file.rs:1:1".to_string(),
        LogLevel::Info,
        Some("ext".to_string()),
        "tag",
        Some("code|desc"),
        Some(vec![serde_json::json!(200), serde_json::json!("ok")]),
        "I",
    );
    let (location, level, tag, content) = rx.recv_timeout(Duration::from_secs(1)).expect("log event");
    assert_eq!(location, "file.rs:1:1");
    assert_eq!(level, LogLevel::Info);
    assert_eq!(tag, "V-ext-tag-I");
    assert_eq!(content, "{\"code\":200,\"desc\":\"ok\"}");

    VibeLogger::clear_global_log_listener();
}
