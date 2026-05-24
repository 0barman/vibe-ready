#[test]
fn raw_codes_and_context_preserve_error_details() {
    let error = VibeEngineError::from_error_code(VibeEngineErrorCode::DatabaseIOError)
        .with_source("disk full")
        .with_context("db write")
        .with_context("kv set");

    assert_eq!(error.code(), VibeEngineErrorCode::DatabaseIOError.code());
    assert_eq!(error.kind(), VibeErrorKind::Database);
    assert_eq!(error.source_message(), Some("disk full"));
    assert_eq!(error.context(), &["db write".to_string(), "kv set".to_string()]);
    assert_eq!(error.same_code().code(), error.code());
    assert!(error.to_string().contains("disk full"));

    let raw_u16 = VibeEngineError::from_u16(7);
    assert_eq!(raw_u16.code(), 7);
    assert_eq!(raw_u16.kind(), VibeErrorKind::Unknown);

    let raw_u32 = VibeEngineError::from_u32(u16::MAX as u32 + 1);
    assert_eq!(raw_u32.code(), 65_536);
    assert_eq!(raw_u32.kind(), VibeErrorKind::Unknown);
}

#[test]
fn all_public_error_constructors_have_expected_codes() {
    assert!(VibeEngineError::from_success().is_success());
    assert_eq!(
        VibeEngineError::from_task_interruption_error().code(),
        VibeEngineErrorCode::TaskInterruptionError.code()
    );
    assert_eq!(
        VibeEngineError::from_internal_error().code(),
        VibeEngineErrorCode::InternalError.code()
    );
    assert_eq!(
        VibeEngineError::from_mpsc_send_error().code(),
        VibeEngineErrorCode::MPSCSendError.code()
    );
    assert_eq!(
        VibeEngineError::from_parameter_empty().code(),
        VibeEngineErrorCode::ParameterEmpty.code()
    );
}

#[test]
fn serde_output_contains_code_kind_message_source_and_context() {
    let error = VibeEngineError::from_error_code_msg(
        VibeEngineErrorCode::ConfigError,
        "bad config".to_string(),
    )
    .with_source("empty field")
    .with_context("builder");
    let json = serde_json::to_value(&error).expect("serialize error");

    assert_eq!(json["code"], VibeEngineErrorCode::ConfigError.code());
    assert_eq!(json["message"], "bad config");
    assert_eq!(json["source"], "empty field");
    assert_eq!(json["context"][0], "builder");
}
