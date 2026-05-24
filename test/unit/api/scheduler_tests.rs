use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn cancellation_token_is_idempotent_and_wakes_late_waiters() {
    let token = VibeCancellationToken::new();
    assert!(!token.is_cancelled());
    token.cancel();
    token.cancel();
    assert!(token.is_cancelled());
    tokio::time::timeout(Duration::from_millis(50), token.cancelled())
        .await
        .expect("cancelled token should resolve immediately");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scheduler_cleans_up_completed_cancelled_and_panicked_tasks() {
    let scheduler = VibeTaskScheduler::new(tokio::runtime::Handle::current(), 4);

    let completed = scheduler
        .post_with_priority("complete", VibeTaskPriority::High, async {})
        .expect("post complete");
    completed.join().await.expect("completed task should join");
    assert!(completed.is_finished().expect("finished state"));

    let cancelled = scheduler
        .schedule_after("cancelled", Duration::from_millis(100), |_| async {})
        .expect("schedule cancelled");
    cancelled.cancel();
    let error = cancelled.join().await.expect_err("cancelled task should error");
    assert_eq!(error.code(), VibeEngineErrorCode::Cancelled.code());

    let failed = scheduler
        .post_with_priority("panic", VibeTaskPriority::Normal, async { panic!("boom") })
        .expect("post panic");
    let error = failed.join().await.expect_err("panic task should fail");
    assert_eq!(error.code(), VibeEngineErrorCode::InternalError.code());

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(scheduler.panel().count().expect("registry count"), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schedule_every_stops_after_cancel_without_deadlock() {
    let scheduler = VibeTaskScheduler::new(tokio::runtime::Handle::current(), 8);
    let runs = Arc::new(AtomicUsize::new(0));
    let runs_clone = Arc::clone(&runs);
    let handle = scheduler
        .schedule_every("periodic", Duration::from_millis(10), move |_| {
            let runs = Arc::clone(&runs_clone);
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
            }
        })
        .expect("schedule periodic");

    tokio::time::sleep(Duration::from_millis(45)).await;
    assert!(runs.load(Ordering::SeqCst) >= 2);
    handle.cancel();
    let error = tokio::time::timeout(Duration::from_secs(1), handle.join())
        .await
        .expect("join should not deadlock")
        .expect_err("cancelled periodic should return error");
    assert_eq!(error.code(), VibeEngineErrorCode::Cancelled.code());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_priority_lane_returns_post_error_when_full() {
    let scheduler = VibeTaskScheduler::new(tokio::runtime::Handle::current(), 1);
    let first = scheduler
        .post_with_priority("blocking", VibeTaskPriority::Low, async {
            tokio::time::sleep(Duration::from_millis(120)).await;
        })
        .expect("first task fits lane");
    let second = scheduler.post_with_priority("overflow", VibeTaskPriority::Low, async {});

    if let Err(error) = second {
        assert_eq!(error.code(), VibeEngineErrorCode::PostError.code());
    }
    let _ = first.join().await;
    scheduler.shutdown();
}
