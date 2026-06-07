#[test]
fn capabilities_default_current_and_json_are_stable() {
    let current = VibeCapabilities::current();
    assert_eq!(current, VibeCapabilities::CURRENT);
    assert_eq!(VibeCapabilities::default(), current);
    assert_eq!(current.log_store, cfg!(feature = "log-diesel"));
    assert_eq!(current.work_store, cfg!(feature = "store-diesel-sqlite"));
    assert_eq!(current.wasm, cfg!(target_arch = "wasm32"));
    assert_eq!(
        current.network,
        cfg!(feature = "net-http") || cfg!(feature = "net-ws")
    );
    assert!(!current.encryption);
    assert!(!current.tracing);
    assert!(!current.metrics);

    let json = serde_json::to_string(&current).expect("serialize capabilities");
    let decoded: VibeCapabilities = serde_json::from_str(&json).expect("deserialize capabilities");
    assert_eq!(decoded, current);
}
