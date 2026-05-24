#[path = "../support/mod.rs"]
mod support;

use std::time::Duration;

use support::*;
use vibe_ready::*;

#[test]
fn scheduled_panic_timeout_and_closed_engine_errors_are_reported() -> VibeResult<()> {
    let engine = engine_with_noop("scenario-panic-timeout")?;

    let failed = engine.post_with_priority("panic-once", VibeTaskPriority::High, async {
        panic!("scenario panic");
    })?;
    let join_result = engine.invoke(async move { failed.join().await })?;
    let join_error = join_result.expect_err("panic join should error");
    assert_error_code(&join_error, VibeErrorCode::InternalError);

    let timeout_error = engine
        .destroy_with_timeout(Duration::ZERO)
        .expect_err("zero timeout should fail");
    assert_error_code(&timeout_error, VibeErrorCode::TimeoutError);

    engine.destroy_with_timeout(Duration::from_secs(2))?;
    assert_eq!(engine.state(), VibeEngineState::Closed);

    let closed_error = engine
        .invoke(async { 1 })
        .expect_err("closed invoke should fail");
    assert_error_code(&closed_error, VibeErrorCode::PostError);
    let schedule_error =
        match engine.schedule_after("closed", Duration::from_millis(1), |_| async {}) {
            Ok(_) => panic!("closed scheduler should fail"),
            Err(error) => error,
        };
    assert_error_code(&schedule_error, VibeErrorCode::PostError);
    Ok(())
}

#[test]
fn cancellation_join_is_bounded_and_returns_cancelled() -> VibeResult<()> {
    let engine = engine_with_noop("scenario-cancel")?;
    let handle =
        engine.schedule_after("cancel-before-start", Duration::from_secs(5), |_| async {})?;
    handle.cancel();
    let result = engine
        .invoke(async move { tokio::time::timeout(Duration::from_secs(1), handle.join()).await })?;
    let error = result
        .expect("join should not time out")
        .expect_err("cancelled join");
    assert_error_code(&error, VibeErrorCode::Cancelled);
    engine.destroy_with_timeout(Duration::from_secs(2))?;
    Ok(())
}
