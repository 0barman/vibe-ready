use std::path::PathBuf;

fn assert_config_error(config: VibeEngineConfig, expected_fragment: &str) {
    let error = config.validate().expect_err("config should be invalid");
    assert_eq!(error.code(), VibeEngineErrorCode::ConfigError.code());
    assert_eq!(error.kind(), crate::VibeErrorKind::Config);
    assert!(
        error.message().contains(expected_fragment),
        "message `{}` did not contain `{expected_fragment}`",
        error.message()
    );
}

#[test]
fn builder_sets_every_public_field_and_app_store_path() -> Result<(), VibeEngineError> {
    let root = PathBuf::from("/tmp/vibe-ready-strict-config");
    let config = VibeEngineConfig::builder()
        .platform(VibePlatformType::Linux)
        .store_root_path(&root)
        .app_name("strict-app")
        .namespace("strict_ns")
        .log_backend(VibeLogBackend::Noop)
        .log_level(LogLevel::Debug)
        .log_write_to_store(false)
        .log_output_stdout(false)
        .log_retention_days(1)
        .log_max_rows(1)
        .store_backend(VibeStoreBackend::Noop)
        .backup_strategy(VibeBackupStrategy::Manual)
        .runtime_worker_threads(1)
        .callback_threads(1)
        .queue_capacity(1, 1)
        .priority_queue_capacity(1)
        .encrypt(true)
        .build();

    config.validate()?;
    assert_eq!(config.platform(), VibePlatformType::Linux);
    assert_eq!(config.store_path(), &root);
    assert!(config.is_encrypt());
    assert_eq!(config.app_name(), "strict-app");
    assert_eq!(config.namespace(), "strict_ns");
    assert_eq!(config.app_store_path(), root.join("strict_ns").join("strict-app"));
    assert_eq!(config.log_config().backend, VibeLogBackend::Noop);
    assert_eq!(config.log_config().level, LogLevel::Debug);
    assert!(!config.log_config().write_to_store);
    assert!(!config.log_config().output_stdout);
    assert_eq!(config.log_config().retention_days, 1);
    assert_eq!(config.log_config().max_rows, 1);
    assert_eq!(config.store_config().backend, VibeStoreBackend::Noop);
    assert!(config.store_config().encrypt);
    assert_eq!(config.store_config().backup_strategy, VibeBackupStrategy::Manual);
    assert_eq!(config.runtime_config().worker_threads, 1);
    assert_eq!(config.runtime_config().callback_threads, 1);
    assert_eq!(config.runtime_config().async_queue_capacity, 1);
    assert_eq!(config.runtime_config().sync_queue_capacity, 1);
    assert_eq!(config.runtime_config().priority_queue_capacity, 1);
    assert!(config.to_string().contains("strict-app"));
    Ok(())
}

#[test]
fn validate_rejects_empty_and_path_like_identifiers() {
    assert_config_error(VibeEngineConfig::builder().app_name("").build(), "app_name");
    assert_config_error(VibeEngineConfig::builder().app_name("   ").build(), "app_name");
    assert_config_error(VibeEngineConfig::builder().app_name(".").build(), "path separators");
    assert_config_error(VibeEngineConfig::builder().namespace("").build(), "namespace");
    assert_config_error(VibeEngineConfig::builder().namespace("\t").build(), "namespace");
    assert_config_error(VibeEngineConfig::builder().namespace("..").build(), "path separators");
    assert_config_error(VibeEngineConfig::builder().namespace("../bad").build(), "path separators");
    assert_config_error(VibeEngineConfig::builder().app_name("bad/name").build(), "path separators");
    assert_config_error(VibeEngineConfig::builder().app_name("bad\\name").build(), "path separators");
    assert_config_error(VibeEngineConfig::builder().namespace("bad..name").build(), "path separators");
}

#[test]
fn validate_rejects_empty_path_and_zero_numeric_limits() {
    assert_config_error(
        VibeEngineConfig::builder().store_root_path(PathBuf::new()).build(),
        "store_root_path",
    );
    assert_config_error(VibeEngineConfig::builder().log_retention_days(0).build(), "retention_days");
    assert_config_error(VibeEngineConfig::builder().log_max_rows(0).build(), "max_rows");
    assert_config_error(VibeEngineConfig::builder().runtime_worker_threads(0).build(), "worker_threads");
    assert_config_error(VibeEngineConfig::builder().callback_threads(0).build(), "callback_threads");
    assert_config_error(VibeEngineConfig::builder().queue_capacity(0, 1).build(), "async_queue_capacity");
    assert_config_error(VibeEngineConfig::builder().queue_capacity(1, 0).build(), "sync_queue_capacity");
    assert_config_error(VibeEngineConfig::builder().priority_queue_capacity(0).build(), "priority_queue_capacity");
}

#[test]
fn private_backend_defaults_reflect_feature_flags() {
    assert_eq!(default_log_backend() == VibeLogBackend::DieselSqlite, cfg!(feature = "log-diesel"));
    assert_eq!(
        default_store_backend() == VibeStoreBackend::DieselSqlite,
        cfg!(feature = "store-diesel-sqlite")
    );
}
