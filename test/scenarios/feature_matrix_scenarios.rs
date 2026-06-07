#[path = "../support/mod.rs"]
mod support;

use std::time::Duration;

use support::*;
use vibe_ready::*;

#[test]
fn capabilities_match_compile_time_feature_matrix() {
    let capabilities = VibeCapabilities::current();
    assert_eq!(capabilities, VibeCapabilities::CURRENT);
    assert_eq!(capabilities.log_store, cfg!(feature = "log-diesel"));
    assert_eq!(
        capabilities.work_store,
        cfg!(feature = "store-diesel-sqlite")
    );
    assert_eq!(capabilities.wasm, cfg!(target_arch = "wasm32"));
    assert_eq!(
        capabilities.network,
        cfg!(feature = "net-http") || cfg!(feature = "net-ws")
    );
    assert!(!capabilities.encryption);
    assert!(!capabilities.tracing);
    assert!(!capabilities.metrics);
}

#[test]
fn noop_backend_matrix_has_no_persistent_side_effects() -> VibeResult<()> {
    let engine = engine_with_noop("scenario-feature-noop")?;
    assert_eq!(engine.capabilities(), VibeCapabilities::current());
    let store = engine.store();
    store.set_str("key", "value")?;
    assert_eq!(store.get_str("key")?, None);
    engine.insert_log(
        true,
        VibeLogLevel::Info,
        "feature".to_string(),
        "noop".to_string(),
    );
    engine.destroy_with_timeout(Duration::from_secs(2))?;
    Ok(())
}

#[cfg(all(feature = "log-diesel", feature = "store-diesel-sqlite"))]
#[test]
fn default_diesel_feature_matrix_persists_store_values() -> VibeResult<()> {
    let config = diesel_config("scenario-feature-diesel");
    let engine = VibeEngine::create(config.clone())?;
    assert!(engine.capabilities().log_store);
    assert!(engine.capabilities().work_store);
    engine.store().set_str("key", "value")?;
    engine.destroy_with_timeout(Duration::from_secs(5))?;

    let reopened = VibeEngine::create(config)?;
    assert_eq!(reopened.store().get_str("key")?, Some("value".to_string()));
    reopened.destroy_with_timeout(Duration::from_secs(5))?;
    Ok(())
}
