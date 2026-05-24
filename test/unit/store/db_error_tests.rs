use crate::api::engine_error::{VibeEngineError, VibeEngineErrorCode};

#[test]
fn db_error_info_getters_and_display_cover_optional_sql() {
    let info = VibeDbErrorInfo::new(
        "file.rs:1:2".to_string(),
        "failed".to_string(),
        DbError::DatabaseIOError,
        Some("select 1".to_string()),
    );
    assert_eq!(info.location(), "file.rs:1:2");
    assert_eq!(info.desc(), "failed");
    assert_eq!(info.code(), DbError::DatabaseIOError);
    assert_eq!(info.sql(), "select 1");
    assert!(info.to_string().contains("select 1"));

    let no_sql = VibeDbErrorInfo::new("loc".to_string(), "desc".to_string(), DbError::OpenFailed, None);
    assert_eq!(no_sql.sql(), "");
}

#[test]
fn db_error_info_constructors_map_to_expected_codes() {
    assert_eq!(VibeDbErrorInfo::from_thread("worker".to_string()).code(), DbError::DatabaseThreadError);
    assert_eq!(VibeDbErrorInfo::from_io("io".to_string()).code(), DbError::OpenFailed);
    assert_eq!(VibeDbErrorInfo::from_not_found().code(), DbError::TargetNotFound);
    assert_eq!(VibeDbErrorInfo::from_not_supported("nope".to_string()).code(), DbError::NotSupportedYet);
}

#[test]
fn db_error_converts_to_engine_error_categories() {
    let cases = [
        (DbError::OpenFailed, VibeEngineErrorCode::DatabaseOpenFailed),
        (DbError::DatabaseIOError, VibeEngineErrorCode::DatabaseIOError),
        (DbError::TargetNotFound, VibeEngineErrorCode::DatabaseTargetNotFound),
        (DbError::DatabaseThreadError, VibeEngineErrorCode::DatabaseThreadError),
        (DbError::JoinError, VibeEngineErrorCode::DatabaseIOError),
        (DbError::NotSupportedYet, VibeEngineErrorCode::DatabaseIOError),
    ];
    for (db_error, code) in cases {
        let engine_error = VibeEngineError::from(db_error);
        assert_eq!(engine_error.code(), code.code());
        assert!(engine_error.source_message().is_some());
    }
}
