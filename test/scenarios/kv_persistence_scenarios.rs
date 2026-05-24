#[path = "../support/mod.rs"]
mod support;

#[cfg(feature = "store-diesel-sqlite")]
use std::sync::mpsc;
use std::time::Duration;

use support::*;
use vibe_ready::*;

#[cfg(feature = "store-diesel-sqlite")]
#[test]
fn kv_values_ttl_and_listener_persistence_boundaries_are_explicit() -> VibeResult<()> {
    let config = diesel_config("scenario-persist");
    let engine = VibeEngine::create(config.clone())?;
    let store = engine.store();
    let (tx, rx) = mpsc::channel();
    store.on_change("persist*", move |change| {
        tx.send((change.key.clone(), change.kind.clone()))
            .expect("send change");
    });

    store.set_str("persist-key", "kept")?;
    assert_eq!(
        recv_timeout(&rx),
        ("persist-key".to_string(), VibeKvChangeKind::Set)
    );
    store.set_with_ttl("temp", "gone", Duration::from_millis(50))?;
    engine.destroy_with_timeout(Duration::from_secs(5))?;

    std::thread::sleep(Duration::from_millis(90));
    let reopened = VibeEngine::create(config)?;
    assert_eq!(
        reopened.store().get_str("persist-key")?,
        Some("kept".to_string())
    );
    assert_eq!(reopened.store().get_str("temp")?, None);

    reopened.store().set_str("persist-key", "changed")?;
    assert!(
        rx.recv_timeout(Duration::from_millis(150)).is_err(),
        "listener registry must not persist across engine reopen"
    );
    reopened.destroy_with_timeout(Duration::from_secs(5))?;
    Ok(())
}

#[cfg(not(feature = "store-diesel-sqlite"))]
#[test]
fn kv_persistence_scenario_documents_noop_build_behavior() -> VibeResult<()> {
    let engine = engine_with_noop("scenario-noop-persist")?;
    let store = engine.store();
    store.set_str("key", "value")?;
    assert_eq!(store.get_str("key")?, None);
    engine.destroy_with_timeout(Duration::from_secs(2))?;
    Ok(())
}
