use crate::api::engine_config::{VibeEngineConfig, VibeLogBackend, VibeStoreBackend};
use crate::api::platform_type::VibePlatformType;

#[test]
fn engine_context_new_opens_noop_clients_and_close_is_idempotent() -> Result<(), VibeEngineError> {
    let config = VibeEngineConfig::builder()
        .platform(VibePlatformType::MacOS)
        .app_name(format!("strict-context-{}", crate::platform::now()))
        .namespace("tests")
        .store_root_path(std::env::temp_dir().join(format!(
            "strict-context-{}",
            crate::platform::now()
        )))
        .log_backend(VibeLogBackend::Noop)
        .store_backend(VibeStoreBackend::Noop)
        .runtime_worker_threads(1)
        .callback_threads(1)
        .queue_capacity(4, 4)
        .build();

    let context = VibeEngineContext::new(config)?;
    assert_eq!(
        futures::executor::block_on(context.db_client().current_user_id())?,
        format!("{}:{}", "tests", context.engine_config().app_name())
    );
    assert!(std::sync::Arc::strong_count(context.log_db_client()) >= 1);
    futures::executor::block_on(context.close())?;
    futures::executor::block_on(context.close())?;
    Ok(())
}
