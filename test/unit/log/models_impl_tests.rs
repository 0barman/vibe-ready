#[test]
fn log_info_getters_and_csv_escape_quotes() {
    let info = VibeLogInfo::new(LogLevel::Warn, "tag".to_string(), "a \"quote\"".to_string(), 42);
    assert_eq!(info.level(), LogLevel::Warn);
    assert_eq!(info.tag(), "tag");
    assert_eq!(info.content(), "a \"quote\"");
    assert_eq!(info.create_time(), 42);
    assert_eq!(info.to_csv(), "42,2,tag,\"a \"\"quote\"\"\"\n");
}

#[test]
fn log_info_slice_keeps_small_records_and_chunks_large_unicode_content() {
    let small = VibeLogInfo::new(LogLevel::Info, "small".to_string(), "x".repeat(16), 1);
    assert_eq!(small.slice().len(), 1);

    let large = VibeLogInfo::new(LogLevel::Info, "large".to_string(), "值".repeat(600_000), 2);
    let chunks = large.slice();
    assert!(chunks.len() > 1);
    assert!(chunks.iter().all(|chunk| chunk.level == LogLevel::Info));
    assert!(chunks.iter().all(|chunk| chunk.tag == "large"));
    assert_eq!(chunks.iter().map(|chunk| chunk.content.chars().count()).sum::<usize>(), 600_000);
}
