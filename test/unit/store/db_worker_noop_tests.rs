use crate::store::db::tables::key_val::{VibeKvValue, VibeTableKeyVal, DEFAULT_BUCKET};

#[tokio::test]
async fn noop_worker_open_close_and_all_operations_are_non_persistent() {
    let mut worker = VibeDbWorkerNoop::new();
    worker
        .try_open(std::env::temp_dir().join("strict-noop-worker"), "user".to_string(), false)
        .await
        .expect("open noop worker");
    let row = VibeTableKeyVal::new("user", "key", VibeKvValue::I32(1));
    worker.insert_or_replace_key_val(row.clone()).await.expect("insert noop");
    assert_eq!(
        worker
            .get_key_val("user".to_string(), DEFAULT_BUCKET.to_string(), "key".to_string())
            .await
            .expect("get noop"),
        None
    );
    assert_eq!(
        worker
            .get_key_val_vec("user".to_string(), DEFAULT_BUCKET.to_string(), vec!["key".to_string()])
            .await
            .expect("get many noop"),
        Vec::<VibeTableKeyVal>::new()
    );
    assert!(!worker
        .remove_key_val("user".to_string(), DEFAULT_BUCKET.to_string(), "key".to_string())
        .await
        .expect("remove noop"));
    assert!(worker
        .list_key_vals("user".to_string(), DEFAULT_BUCKET.to_string())
        .await
        .expect("list noop")
        .is_empty());
    worker.close().await.expect("close noop");
}
