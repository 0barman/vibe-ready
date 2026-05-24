#[path = "../support/mod.rs"]
mod support;

#[cfg(feature = "store-diesel-sqlite")]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
#[cfg(feature = "store-diesel-sqlite")]
use std::sync::Arc;
use std::time::Duration;

use support::*;
use vibe_ready::*;

#[cfg(feature = "store-diesel-sqlite")]
#[test]
fn concurrent_store_writes_and_listener_dispatch_finish_before_timeout() -> VibeResult<()> {
    let engine = engine_with_diesel("scenario-concurrent-store")?;
    let store = engine.store();
    let listener_hits = Arc::new(AtomicUsize::new(0));
    let listener_hits_clone = Arc::clone(&listener_hits);
    store.on_change("k*", move |_| {
        listener_hits_clone.fetch_add(1, Ordering::SeqCst);
    });

    let (done_tx, done_rx) = mpsc::channel();
    for thread_index in 0..8 {
        let store = store.clone();
        let done_tx = done_tx.clone();
        std::thread::spawn(move || {
            for value in 0..20 {
                store
                    .set_i32(format!("k-{thread_index}-{value}"), value)
                    .expect("concurrent set");
            }
            done_tx.send(()).expect("send done");
        });
    }
    drop(done_tx);
    for _ in 0..8 {
        done_rx
            .recv_timeout(Duration::from_secs(4))
            .expect("thread did not deadlock");
    }
    sleep_short();
    assert!(listener_hits.load(Ordering::SeqCst) >= 160);
    assert_eq!(store.get_i32("k-0-19")?, Some(19));
    engine.destroy_with_timeout(Duration::from_secs(5))?;
    Ok(())
}

#[test]
fn invoking_executor_from_engine_runtime_does_not_deadlock() -> VibeResult<()> {
    let engine = engine_with_noop("scenario-runtime-invoke")?;
    let executor = engine.executor();
    let executor_inside = executor.clone();
    let (tx, rx) = mpsc::channel();
    executor.post(async move {
        let result = executor_inside.invoke(async { 42 });
        tx.send(result).expect("send invoke result");
    })?;
    let result = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("invoke should not deadlock");
    assert_eq!(result?, 42);
    engine.destroy_with_timeout(Duration::from_secs(2))?;
    Ok(())
}

#[test]
#[ignore]
fn stress_many_scheduled_tasks_and_store_ops() -> VibeResult<()> {
    let engine = engine_with_noop("scenario-stress")?;
    for batch in 0..25 {
        let mut handles = Vec::new();
        for i in 0..20 {
            handles.push(engine.post_with_priority(
                format!("stress-{batch}-{i}"),
                VibeTaskPriority::Normal,
                async {},
            )?);
        }
        for handle in handles {
            engine.invoke(async move { handle.join().await })??;
        }
    }
    engine.destroy_with_timeout(Duration::from_secs(5))?;
    Ok(())
}
