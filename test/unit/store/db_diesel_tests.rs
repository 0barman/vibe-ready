use crate::store::db::enums::db_error::DbError;

#[cfg(feature = "store-diesel-sqlite")]
#[test]
fn diesel_sqlite_direct_api_covers_crud_transactions_listeners_and_unsupported_ops() {
    let path = std::env::temp_dir().join(format!("strict-db-diesel-{}", crate::platform::now()));
    let db = VibeDbSqlite::try_open(path, "user".to_string(), false).expect("open diesel db");
    assert_eq!(db.user_id, "user");

    VibeDbSqlite::register_sql_perf_listener(Some(|_, _, _, _, _, _, _, _| {}));
    VibeDbSqlite::register_sql_exception_listener(Some(|_, _| {}));
    VibeDbSqlite::register_db_exception_listener(Some(|_, _| {}));
    VibeDbSqlite::un_register_db_listener();

    let row = VibeTableKeyVal::new_in_bucket("user", "bucket", "key", VibeKvValue::String("value".to_string()), EXPIRES_AT_NEVER);
    db.insert_or_replace_key_val(row).expect("insert row");
    assert!(db.contains_key_val_in_bucket("user", "bucket", "key").expect("contains"));
    assert_eq!(
        db.get_key_val_in_bucket("user", "bucket", "key")
            .expect("get row")
            .and_then(|row| row.value())
            .and_then(|value| value.as_str().map(str::to_string)),
        Some("value".to_string())
    );
    assert_eq!(
        db.get_key_val_vec_in_bucket("user", "bucket", Vec::new())
            .expect("empty get many"),
        Vec::<VibeTableKeyVal>::new()
    );
    assert_eq!(db.list_key_vals_in_bucket("user", "bucket").expect("list"), vec!["key".to_string()]);

    let tx_row = VibeTableKeyVal::new_in_bucket("user", "bucket", "tx", VibeKvValue::I32(5), EXPIRES_AT_NEVER);
    db.transaction(vec![DbKvOp::Set(tx_row)]).expect("set transaction");
    assert!(db.contains_key_val_in_bucket("user", "bucket", "tx").expect("contains tx"));
    db.transaction(vec![DbKvOp::Remove {
        user_id: "user".to_string(),
        bucket: "bucket".to_string(),
        key: "tx".to_string(),
    }])
    .expect("remove transaction");
    assert!(!db.contains_key_val_in_bucket("user", "bucket", "tx").expect("removed tx"));

    let expired = VibeTableKeyVal::new_in_bucket("user", "bucket", "expired", VibeKvValue::Bool(true), crate::platform::now());
    db.insert_or_replace_key_val(expired).expect("insert expired");
    assert!(db.purge_expired(crate::platform::now()).expect("purge") >= 1);
    assert_eq!(db.manual_backup().expect_err("backup unsupported").code(), DbError::NotSupportedYet);
    assert_eq!(db.manual_retrieve().expect_err("retrieve unsupported").code(), DbError::NotSupportedYet);
    db.close();
}
