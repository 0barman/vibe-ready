#[test]
fn kv_value_accessors_accept_only_matching_variants_and_edge_values() {
    let values = vec![
        VibeKvValue::from(""),
        VibeKvValue::from(String::from("unicode-值")),
        VibeKvValue::from(true),
        VibeKvValue::from(i32::MIN),
        VibeKvValue::from(i64::MAX),
        VibeKvValue::from(-0.0_f64),
        VibeKvValue::from(Vec::<u8>::new()),
        VibeKvValue::from(&[1_u8, 2, 3][..]),
        VibeKvValue::from(serde_json::json!(null)),
    ];

    assert_eq!(values[0].as_str(), Some(""));
    assert_eq!(values[2].as_bool(), Some(true));
    assert_eq!(values[3].as_i32(), Some(i32::MIN));
    assert_eq!(values[4].as_i64(), Some(i64::MAX));
    assert_eq!(values[5].as_f64(), Some(-0.0));
    assert_eq!(values[6].as_bytes(), Some(&[][..]));
    assert_eq!(values[8].as_json(), Some(&serde_json::Value::Null));

    assert_eq!(values[0].as_i32(), None);
    assert_eq!(values[2].as_str(), None);
    assert_eq!(values[3].as_bool(), None);
    assert_eq!(values[8].as_bytes(), None);
}

#[test]
fn table_key_val_round_trips_all_value_types_and_expiry_edges() {
    let cases = vec![
        ("str", VibeKvValue::String("".to_string())),
        ("bool", VibeKvValue::Bool(false)),
        ("i32", VibeKvValue::I32(i32::MAX)),
        ("i64", VibeKvValue::I64(i64::MIN)),
        ("f64", VibeKvValue::F64(12.5)),
        ("bytes", VibeKvValue::Bytes(vec![0, 255])),
        ("json", VibeKvValue::Json(serde_json::json!({"nested": [1, true, null]}))),
    ];

    for (key, value) in cases {
        let row = VibeTableKeyVal::new_in_bucket("user", "bucket", key, value.clone(), 123);
        assert_eq!(row.user_id(), "user");
        assert_eq!(row.bucket(), "bucket");
        assert_eq!(row.key(), key);
        assert_eq!(row.expires_at_ms(), 123);
        assert_eq!(row.value(), Some(value));
        assert!(!row.is_expired(122));
        assert!(row.is_expired(123));
    }

    let never = VibeTableKeyVal::new("user", "never", VibeKvValue::Bool(true));
    assert_eq!(never.expires_at_ms(), EXPIRES_AT_NEVER);
    assert!(!never.is_expired(i64::MAX));
}

#[test]
fn table_key_val_invalid_type_or_invalid_json_returns_none() {
    let mut row = VibeTableKeyVal::new("user", "bad", VibeKvValue::String("value".to_string()));
    row.value_type = 99;
    assert_eq!(row.value(), None);

    let mut json_row = VibeTableKeyVal::new("user", "json", VibeKvValue::Json(serde_json::json!({"ok": true})));
    json_row.value_json = "{".to_string();
    assert_eq!(json_row.value(), None);
}

#[test]
fn typed_row_getters_reject_mismatched_types() {
    let str_row = VibeTableKeyVal::new_with_str("user", "str", "value");
    assert_eq!(str_row.get_value_str(), Some("value"));
    assert_eq!(str_row.get_value_bool(), None);
    assert_eq!(str_row.get_value_i32(), None);

    let bool_row = VibeTableKeyVal::new_with_bool("user", "bool", true);
    assert_eq!(bool_row.get_value_bool(), Some(true));
    assert_eq!(bool_row.get_value_str(), None);

    let i32_row = VibeTableKeyVal::new_with_i32("user", "i32", -7);
    assert_eq!(i32_row.get_value_i32(), Some(-7));
    assert_eq!(i32_row.get_value_str(), None);
}
