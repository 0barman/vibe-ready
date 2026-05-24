use crate::store::db::tables::key_val::{VibeKvValue, VibeTableKeyVal, DEFAULT_BUCKET};

#[tokio::test]
async fn db_worker_noop_backend_discards_operations() {
    let mut worker = DbWorker::with_backend(VibeStoreBackend::Noop);
    worker
        .try_open(std::env::temp_dir().join("strict-sql-def-noop"), "user".to_string(), false)
        .await
        .expect("noop open");
    let row = VibeTableKeyVal::new("user", "key", VibeKvValue::String("value".to_string()));
    worker.insert_or_replace_key_val(row).await.expect("noop set");
    assert_eq!(
        worker
            .get_key_val("user".to_string(), DEFAULT_BUCKET.to_string(), "key".to_string())
            .await
            .expect("noop get"),
        None
    );
    assert!(!worker
        .contains_key_val("user".to_string(), DEFAULT_BUCKET.to_string(), "key".to_string())
        .await
        .expect("noop contains"));
    assert_eq!(worker.purge_expired(crate::platform::now()).await.expect("noop purge"), 0);
    worker.close().await.expect("noop close");
}
