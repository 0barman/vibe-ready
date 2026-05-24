#[test]
fn channel_buffer_constants_are_positive_and_match_runtime_defaults() {
    let config = crate::api::engine_config::VibeEngineConfig::builder().build();
    assert!(config.runtime_config().async_queue_capacity > 0);
    assert!(config.runtime_config().sync_queue_capacity > 0);
    assert!(std::hint::black_box(DB_CHANNEL_BUFFER_SIZE) > 0);
    assert_eq!(
        config.runtime_config().async_queue_capacity,
        ENGINE_CHANNEL_BUFFER_SIZE
    );
    assert_eq!(
        config.runtime_config().sync_queue_capacity,
        ENGINE_SYNC_CHANNEL_BUFFER_SIZE
    );
}
