#[test]
fn new_builds_default_client() {
    let client = VibeHttpClient::new().expect("default client");
    // base_url unset means absolute URLs pass through unchanged.
    assert_eq!(client.resolve_url("http://x/y"), "http://x/y");
}

#[test]
fn base_url_resolves_relative_paths() {
    let client = VibeHttpClient::builder()
        .base_url("https://api.example.com/")
        .build()
        .expect("client");
    assert_eq!(
        client.resolve_url("/v1/items"),
        "https://api.example.com/v1/items"
    );
    assert_eq!(
        client.resolve_url("v1/items"),
        "https://api.example.com/v1/items"
    );
    // Absolute URLs ignore the base.
    assert_eq!(client.resolve_url("http://other/x"), "http://other/x");
}

#[test]
fn invalid_default_header_is_a_config_error() {
    let result = VibeHttpClient::builder()
        .default_header("inva lid", "x")
        .build();
    let err = result.unwrap_err();
    assert_eq!(
        err.code(),
        crate::api::engine_error::VibeEngineErrorCode::ConfigError.code()
    );
}
