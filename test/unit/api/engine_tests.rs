use std::sync::mpsc;
use std::time::Duration;

use crate::api::engine_config::{VibeLogBackend, VibeStoreBackend};
use crate::api::platform_type::VibePlatformType;

fn strict_engine_config(name: &str) -> VibeEngineConfig {
    VibeEngineConfig::builder()
        .platform(VibePlatformType::MacOS)
        .app_name(format!("strict-{name}-{}", crate::platform::now()))
        .namespace("tests")
        .store_root_path(std::env::temp_dir().join(format!("strict-engine-{name}-{}", crate::platform::now())))
        .log_backend(VibeLogBackend::Noop)
        .store_backend(VibeStoreBackend::Noop)
        .runtime_worker_threads(1)
        .callback_threads(1)
        .queue_capacity(2, 2)
        .priority_queue_capacity(8)
        .build()
}

#[test]
fn closed_engine_rejects_work_and_destroy_zero_timeout_errors() -> Result<(), VibeEngineError> {
    let engine = VibeEngine::create(strict_engine_config("closed"))?;
    let timeout_error = engine
        .destroy_with_timeout(Duration::ZERO)
        .expect_err("zero timeout should be rejected");
    assert_eq!(timeout_error.code(), VibeEngineErrorCode::TimeoutError.code());
    engine.destroy_with_timeout(Duration::from_secs(2))?;

    assert_eq!(engine.state(), VibeEngineState::Closed);
    assert_eq!(
        engine.invoke(async { 1 }).expect_err("closed invoke").code(),
        VibeEngineErrorCode::PostError.code()
    );
    let priority_error = match engine.post_with_priority("closed", VibeTaskPriority::Normal, async {}) {
        Ok(_) => panic!("closed priority post should fail"),
        Err(error) => error,
    };
    assert_eq!(priority_error.code(), VibeEngineErrorCode::PostError.code());
    let delayed_error = match engine.schedule_after("closed", Duration::from_millis(1), |_| async {}) {
        Ok(_) => panic!("closed delayed schedule should fail"),
        Err(error) => error,
    };
    assert_eq!(delayed_error.code(), VibeEngineErrorCode::PostError.code());
    let periodic_error = match engine.schedule_every("closed", Duration::from_millis(1), |_| async {}) {
        Ok(_) => panic!("closed periodic schedule should fail"),
        Err(error) => error,
    };
    assert_eq!(periodic_error.code(), VibeEngineErrorCode::PostError.code());
    Ok(())
}

#[test]
fn destroy_callback_reports_result_on_callback_pool() -> Result<(), VibeEngineError> {
    let engine = VibeEngine::create(strict_engine_config("destroy-callback"))?;
    let (tx, rx) = mpsc::channel();
    engine.destroy(move |result| {
        tx.send(result.map(|_| ())).expect("send destroy result");
    });
    rx.recv_timeout(Duration::from_secs(2))
        .expect("destroy callback")?;
    assert_eq!(engine.state(), VibeEngineState::Closed);
    Ok(())
}

#[test]
fn log_listener_receives_inserted_log_and_can_be_cleared() -> Result<(), VibeEngineError> {
    let engine = VibeEngine::create(strict_engine_config("log-listener"))?;
    let (tx, rx) = mpsc::channel();
    engine.set_log_listener(Some(Box::new(move |info| {
        let _ = tx.send((info.tag(), info.content()));
    })));
    std::thread::sleep(Duration::from_millis(50));
    engine.insert_log(true, LogLevel::Info, "tag".into(), "content".into());
    let received = rx.recv_timeout(Duration::from_secs(2)).expect("log listener");
    assert_eq!(received, ("tag".to_string(), "content".to_string()));
    engine.set_log_listener(None);
    engine.destroy_with_timeout(Duration::from_secs(2))?;
    Ok(())
}
