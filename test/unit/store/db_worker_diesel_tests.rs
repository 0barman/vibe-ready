use crate::store::db::tables::key_val::{VibeKvValue, VibeTableKeyVal, DEFAULT_BUCKET};

#[cfg(feature = "store-diesel-sqlite")]
#[tokio::test]
async fn diesel_worker_reports_not_open_then_persists_and_removes_values() {
    let mut worker = VibeDbWorkerDiesel::new();
    assert_eq!(
        worker
            .get_key_val("user".to_string(), DEFAULT_BUCKET.to_string(), "key".to_string())
            .await
            .expect_err("not open"),
        DbError::NotOpen
    );

    let path = std::env::temp_dir().join(format!("strict-worker-diesel-{}", crate::platform::now()));
    worker
        .try_open(path, "user".to_string(), false)
        .await
        .expect("open worker");
    let row = VibeTableKeyVal::new("user", "key", VibeKvValue::I64(i64::MIN));
    worker
        .insert_or_replace_key_val(row)
        .await
        .expect("insert worker row");
    assert!(worker
        .contains_key_val("user".to_string(), DEFAULT_BUCKET.to_string(), "key".to_string())
        .await
        .expect("contains worker row"));
    assert_eq!(
        worker
            .get_key_val("user".to_string(), DEFAULT_BUCKET.to_string(), "key".to_string())
            .await
            .expect("get worker row")
            .and_then(|row| row.value())
            .and_then(|value| value.as_i64()),
        Some(i64::MIN)
    );
    assert_eq!(
        worker
            .get_key_val_vec("user".to_string(), DEFAULT_BUCKET.to_string(), Vec::new())
            .await
            .expect("empty many"),
        Vec::<VibeTableKeyVal>::new()
    );
    assert_eq!(
        worker
            .list_key_vals("user".to_string(), DEFAULT_BUCKET.to_string())
            .await
            .expect("list worker keys"),
        vec!["key".to_string()]
    );
    assert!(worker
        .remove_key_val("user".to_string(), DEFAULT_BUCKET.to_string(), "key".to_string())
        .await
        .expect("remove worker row"));
    assert_eq!(worker.purge_expired(crate::platform::now()).await.expect("purge worker"), 0);
    worker.close().await.expect("close worker");
    worker.close().await.expect("double close worker");
}
