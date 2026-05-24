use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::api::engine_error::VibeEngineErrorCode;

#[tokio::test]
async fn status_manager_notifies_only_for_actual_valid_changes() -> Result<(), VibeEngineError> {
    let manager = VibeStatusManager::new();
    assert_eq!(manager.get_connection_status().await, VibeConnectionStatus::Idle);

    let count = Arc::new(AtomicUsize::new(0));
    let count_clone = Arc::clone(&count);
    manager
        .set_connection_status_listener(Some(Box::new(move |status| {
            assert_eq!(status, VibeConnectionStatus::Connecting);
            count_clone.fetch_add(1, Ordering::SeqCst);
        })))
        .await;

    manager.set_connection_status(VibeConnectionStatus::Idle).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(count.load(Ordering::SeqCst), 0);

    manager
        .set_connection_status(VibeConnectionStatus::Connecting)
        .await?;
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(count.load(Ordering::SeqCst), 1);

    manager.set_connection_status_listener(None).await;
    manager
        .set_connection_status(VibeConnectionStatus::Disconnected)
        .await?;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(count.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn invalid_status_transition_returns_error_without_notifying() {
    let manager = VibeStatusManager::new();
    let count = Arc::new(AtomicUsize::new(0));
    let count_clone = Arc::clone(&count);
    manager
        .set_connection_status_listener(Some(Box::new(move |_| {
            count_clone.fetch_add(1, Ordering::SeqCst);
        })))
        .await;

    let error = manager
        .set_connection_status(VibeConnectionStatus::Connected)
        .await
        .expect_err("idle to connected is invalid");
    assert_eq!(error.code(), VibeEngineErrorCode::InternalError.code());
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert_eq!(manager.get_connection_status().await, VibeConnectionStatus::Idle);
}
