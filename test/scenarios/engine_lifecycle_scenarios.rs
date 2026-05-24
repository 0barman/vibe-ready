#[path = "../support/mod.rs"]
mod support;

use std::sync::mpsc;
use std::time::Duration;

use support::*;
use vibe_ready::*;

#[test]
fn engine_full_lifecycle_drives_log_store_scheduler_and_destroy() -> VibeResult<()> {
    let engine = engine_with_diesel("scenario-lifecycle")?;
    assert_eq!(engine.state(), VibeEngineState::Running);

    let (log_tx, log_rx) = mpsc::channel();
    engine.set_log_listener(Some(Box::new(move |info| {
        log_tx.send((info.tag(), info.content())).expect("send log");
    })));
    std::thread::sleep(Duration::from_millis(50));
    engine.insert_log(
        true,
        VibeLogLevel::Info,
        "scenario".to_string(),
        "ready".to_string(),
    );
    assert_eq!(
        recv_timeout(&log_rx),
        ("scenario".to_string(), "ready".to_string())
    );

    let store = engine.store();
    store.set_str("status", "ready")?;
    #[cfg(feature = "store-diesel-sqlite")]
    assert_eq!(store.get_str("status")?, Some("ready".to_string()));
    #[cfg(not(feature = "store-diesel-sqlite"))]
    assert_eq!(store.get_str("status")?, None);

    let (task_tx, task_rx) = mpsc::channel();
    let handle = engine.schedule_after(
        "scenario-delay",
        Duration::from_millis(10),
        move |_| async move {
            task_tx.send(1).expect("send task result");
        },
    )?;
    assert_eq!(recv_timeout(&task_rx), 1);
    engine.invoke(async move { handle.join().await })??;

    engine.destroy_with_timeout(Duration::from_secs(5))?;
    assert_eq!(engine.state(), VibeEngineState::Closed);
    Ok(())
}

#[cfg(feature = "store-diesel-sqlite")]
#[test]
fn engines_with_different_namespaces_do_not_share_store_values() -> VibeResult<()> {
    let root = unique_store_root("scenario-isolation");
    let config_a = VibeEngineConfig::builder()
        .platform(VibePlatformType::MacOS)
        .app_name("same-app")
        .namespace("tenant-a")
        .store_root_path(&root)
        .runtime_worker_threads(1)
        .callback_threads(1)
        .queue_capacity(16, 8)
        .build();
    let config_b = VibeEngineConfig::builder()
        .platform(VibePlatformType::MacOS)
        .app_name("same-app")
        .namespace("tenant-b")
        .store_root_path(&root)
        .runtime_worker_threads(1)
        .callback_threads(1)
        .queue_capacity(16, 8)
        .build();

    let engine_a = VibeEngine::create(config_a)?;
    let engine_b = VibeEngine::create(config_b)?;
    engine_a.store().set_str("shared", "a")?;
    engine_b.store().set_str("shared", "b")?;
    assert_eq!(engine_a.store().get_str("shared")?, Some("a".to_string()));
    assert_eq!(engine_b.store().get_str("shared")?, Some("b".to_string()));
    engine_a.destroy_with_timeout(Duration::from_secs(5))?;
    engine_b.destroy_with_timeout(Duration::from_secs(5))?;
    Ok(())
}
