use crate::log::log_level::LogLevel;

#[test]
fn table_log_copies_log_info_and_escapes_csv() {
    let info = VibeLogInfo::new(LogLevel::Error, "tag".to_string(), "content \"quoted\"".to_string(), 99);
    let row = VibeTableLog::new(7, &info);
    assert_eq!(row.id, 7);
    assert_eq!(row.level, LogLevel::Error as i16);
    assert_eq!(row.tag, "tag");
    assert_eq!(row.content, "content \"quoted\"");
    assert_eq!(row.create_time, 99);
    assert_eq!(row.to_csv(), "99,1,tag,\"content \"\"quoted\"\"\"\n");
}
