#[cfg(feature = "store-diesel-sqlite")]
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(feature = "store-diesel-sqlite")]
use std::sync::Arc;
use std::time::Duration;

use crate::api::engine::VibeEngine;
use crate::api::engine_config::{VibeEngineConfig, VibeLogBackend, VibeStoreBackend};
use crate::api::engine_error::VibeEngineErrorCode;
use crate::api::platform_type::VibePlatformType;

fn strict_store_config(name: &str) -> VibeEngineConfig {
    VibeEngineConfig::builder()
        .platform(VibePlatformType::MacOS)
        .app_name(format!("strict-kv-{name}-{}", crate::platform::now()))
        .namespace("tests")
        .store_root_path(std::env::temp_dir().join(format!("strict-kv-{name}-{}", crate::platform::now())))
        .runtime_worker_threads(1)
        .callback_threads(1)
        .queue_capacity(32, 16)
        .priority_queue_capacity(64)
        .build()
}

    fn strict_noop_store_config(name: &str) -> VibeEngineConfig {
        let config = strict_store_config(name);
        VibeEngineConfig::builder()
        .platform(config.platform())
        .app_name(config.app_name())
        .namespace(config.namespace())
        .store_root_path(config.store_path())
        .log_backend(VibeLogBackend::Noop)
        .store_backend(VibeStoreBackend::Noop)
        .runtime_worker_threads(1)
        .callback_threads(1)
        .queue_capacity(16, 8)
        .build()
    }

#[test]
fn private_pattern_matcher_handles_exact_wildcard_prefix_and_literals() {
    assert!(pattern_matches("*", "anything"));
    assert!(pattern_matches("conf.*", "conf.theme"));
    assert!(!pattern_matches("conf.*", "config.theme"));
    assert!(pattern_matches("literal[", "literal["));
    assert!(!pattern_matches("literal[", "literalx"));
    assert!(pattern_matches("", ""));
}

#[test]
fn ttl_conversion_handles_none_zero_and_large_duration() {
    assert_eq!(ttl_to_expires_at(None), EXPIRES_AT_NEVER);
    let zero = ttl_to_expires_at(Some(Duration::ZERO));
    assert!(zero <= crate::platform::now());
    let large = ttl_to_expires_at(Some(Duration::from_millis(u64::MAX)));
    assert_eq!(large, i64::MAX);
}

#[cfg(feature = "store-diesel-sqlite")]
#[test]
fn kv_store_accepts_empty_unicode_and_large_values() -> Result<(), VibeEngineError> {
    let engine = VibeEngine::create(strict_store_config("edge-values"))?;
    let store = engine.store();
    let bucket = store.bucket("桶");
    assert_eq!(bucket.name(), "桶");

    bucket.set("空", "")?;
    bucket.set("large", "x".repeat(64 * 1024))?;
    store.set("json-null", VibeKvValue::Json(serde_json::json!(null)))?;
    store.set("bytes-empty", Vec::<u8>::new())?;
    store.set("bytes-large", vec![7_u8; 256 * 1024])?;

    assert_eq!(bucket.get("空")?.and_then(|v| v.as_str().map(str::to_string)), Some("".to_string()));
    assert_eq!(bucket.get("large")?.and_then(|v| v.as_str().map(str::len)), Some(64 * 1024));
    assert_eq!(store.get("json-null")?.and_then(|v| v.as_json().cloned()), Some(serde_json::Value::Null));
    assert_eq!(store.get("bytes-empty")?.and_then(|v| v.as_bytes().map(<[_]>::to_vec)), Some(Vec::<u8>::new()));
    assert_eq!(store.get("bytes-large")?.and_then(|v| v.as_bytes().map(<[_]>::len)), Some(256 * 1024));

    engine.destroy_with_timeout(Duration::from_secs(5))?;
    Ok(())
}

#[cfg(feature = "store-diesel-sqlite")]
#[test]
fn kv_store_batches_empty_duplicate_and_missing_keys_are_safe() -> Result<(), VibeEngineError> {
    let engine = VibeEngine::create(strict_store_config("batch-edges"))?;
    let store = engine.store();
    let bucket = store.bucket("batch");

    bucket.set_many(Vec::<(&str, VibeKvValue)>::new())?;
    assert!(bucket.get_many(Vec::<&str>::new())?.is_empty());
    bucket.remove_many(Vec::<&str>::new())?;

    bucket.set_many(vec![("dup", VibeKvValue::I32(1)), ("dup", VibeKvValue::I32(2))])?;
    assert_eq!(bucket.get("dup")?.and_then(|v| v.as_i32()), Some(2));
    assert!(!bucket.remove("missing")?);
    bucket.remove_many(vec!["dup", "missing"])?;
    assert!(!bucket.contains("dup")?);

    engine.destroy_with_timeout(Duration::from_secs(5))?;
    Ok(())
}

#[cfg(feature = "store-diesel-sqlite")]
#[test]
fn transaction_commit_rollback_and_listener_dispatch_are_strict() -> Result<(), VibeEngineError> {
    let engine = VibeEngine::create(strict_store_config("tx-listener"))?;
    let store = engine.store();
    let bucket = store.bucket("tx");
    let count = Arc::new(AtomicUsize::new(0));
    let count_clone = Arc::clone(&count);
    let id = bucket.on_change("item*", move |change| {
        assert_eq!(change.bucket, "tx");
        count_clone.fetch_add(1, Ordering::SeqCst);
    });

    let result: i32 = bucket.transaction(|tx| {
        assert_eq!(tx.bucket(), "tx");
        tx.set("item-1", "one")?;
        tx.set_with_ttl("item-2", "two", Duration::from_secs(60))?;
        Ok(2)
    })?;
    assert_eq!(result, 2);
    std::thread::sleep(Duration::from_millis(120));
    assert_eq!(count.load(Ordering::SeqCst), 2);

    let rollback: Result<(), VibeEngineError> = bucket.transaction(|tx| {
        tx.set("item-1", "changed")?;
        Err(VibeEngineError::from_parameter_empty())
    });
    assert!(rollback.is_err());
    assert_eq!(bucket.get("item-1")?.and_then(|v| v.as_str().map(str::to_string)), Some("one".to_string()));

    assert!(bucket.off_change(id));
    assert!(!bucket.off_change(id));
    engine.destroy_with_timeout(Duration::from_secs(5))?;
    Ok(())
}

#[test]
fn noop_backend_validates_empty_keys_but_discards_valid_values() -> Result<(), VibeEngineError> {
    let engine = VibeEngine::create(strict_noop_store_config("noop"))?;
    let store = engine.store();

    assert_eq!(store.set("", "bad").expect_err("empty key").code(), VibeEngineErrorCode::ParameterEmpty.code());
    store.set("valid", "value")?;
    assert_eq!(store.get("valid")?, None);
    assert!(!store.contains("valid")?);
    assert!(store.list_keys()?.is_empty());
    engine.destroy_with_timeout(Duration::from_secs(5))?;
    Ok(())
}

#[test]
fn listener_callback_can_reenter_store_without_deadlock() -> Result<(), VibeEngineError> {
    let engine = VibeEngine::create(strict_noop_store_config("listener-reentrant"))?;
    let store = engine.store();
    let (tx, rx) = std::sync::mpsc::channel();

    let nested_store = store.clone();
    store.on_change("root", move |_| {
        nested_store
            .set("nested", "value")
            .expect("nested set should not deadlock");
    });
    store.on_change("nested", move |_| {
        tx.send(()).expect("send nested listener event");
    });

    store.set("root", "value")?;
    rx.recv_timeout(Duration::from_secs(2))
        .expect("reentrant listener should finish before timeout");
    engine.destroy_with_timeout(Duration::from_secs(5))?;
    Ok(())
}
