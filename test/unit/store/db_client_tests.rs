fn strict_db_path(prefix: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("strict-db-client-{prefix}-{}", crate::platform::now()))
}

#[tokio::test]
async fn db_client_reports_not_open_and_rejects_empty_keys() {
    let client = VibeDbClient::with_backend(VibeStoreBackend::Noop);
    assert_eq!(
        client.current_user_id().await.expect_err("not open").code(),
        VibeEngineErrorCode::DatabaseNotOpened.code()
    );
    client
        .try_open(strict_db_path("noop"), "user".to_string(), false)
        .await
        .expect("open noop");
    assert_eq!(client.current_user_id().await.expect("user id"), "user");
    assert_eq!(
        client.set("".to_string(), VibeKvValue::String("bad".to_string())).await.expect_err("empty key").code(),
        VibeEngineErrorCode::ParameterEmpty.code()
    );
    client.close().await.expect("close");
    client.close().await.expect("double close");
}

#[tokio::test]
async fn noop_db_client_discards_reads_and_handles_empty_batches() -> Result<(), VibeEngineError> {
    let client = VibeDbClient::with_backend(VibeStoreBackend::Noop);
    client
        .try_open(strict_db_path("noop-batches"), "user".to_string(), false)
        .await?;
    client.set_str("name".to_string(), "vibe".to_string()).await?;
    client.set_bool("enabled".to_string(), true).await?;
    client.set_i32("count".to_string(), 7).await?;

    assert_eq!(client.get_str("name".to_string()).await?, None);
    assert_eq!(client.get_bool("enabled".to_string()).await?, None);
    assert_eq!(client.get_i32("count".to_string()).await?, None);
    assert_eq!(client.get_many_in_bucket("default".to_string(), Vec::new()).await?, Vec::new());
    client.set_many_in_bucket("default".to_string(), Vec::new()).await?;
    client.remove_many_in_bucket("default".to_string(), Vec::new()).await?;
    assert_eq!(client.purge_expired().await?, 0);
    client.close().await?;
    Ok(())
}

#[cfg(feature = "store-diesel-sqlite")]
#[tokio::test]
async fn diesel_db_client_persists_values_and_expiry_semantics() -> Result<(), VibeEngineError> {
    let path = strict_db_path("diesel");
    let client = VibeDbClient::with_backend(VibeStoreBackend::DieselSqlite);
    client.try_open(path.clone(), "user".to_string(), false).await?;
    client.set_in_bucket("b".to_string(), "k".to_string(), VibeKvValue::I32(5), EXPIRES_AT_NEVER).await?;
    client.set_in_bucket("b".to_string(), "expired".to_string(), VibeKvValue::String("gone".to_string()), crate::platform::now()).await?;

    assert_eq!(client.get_in_bucket("b".to_string(), "k".to_string()).await?.and_then(|v| v.as_i32()), Some(5));
    assert_eq!(client.get_in_bucket("b".to_string(), "expired".to_string()).await?, None);
    assert!(client.contains_in_bucket("b".to_string(), "k".to_string()).await?);
    assert!(!client.contains_in_bucket("b".to_string(), "expired".to_string()).await?);
    assert!(client.purge_expired().await? >= 1);
    client.close().await?;

    let reopened = VibeDbClient::with_backend(VibeStoreBackend::DieselSqlite);
    reopened.try_open(path, "user".to_string(), false).await?;
    assert_eq!(reopened.get_in_bucket("b".to_string(), "k".to_string()).await?.and_then(|v| v.as_i32()), Some(5));
    reopened.close().await?;
    Ok(())
}

#[tokio::test]
async fn validate_key_rejects_empty_and_whitespace() {
    assert_eq!(validate_key("").expect_err("empty").code(), VibeEngineErrorCode::ParameterEmpty.code());
    assert_eq!(validate_key(" \t").expect_err("blank").code(), VibeEngineErrorCode::ParameterEmpty.code());
    validate_key("x").expect("non-empty key");
}
