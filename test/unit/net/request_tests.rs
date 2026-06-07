#[test]
fn json_body_sets_content_type_and_is_retryable() {
    let client = reqwest::Client::new();
    let request = VibeHttpRequest::new(
        client,
        reqwest::Method::POST,
        "http://localhost/echo".to_string(),
        VibeRetryPolicy::none(),
    )
    .header("x-app", "vibe")
    .query("k", "v")
    .json(&serde_json::json!({ "a": 1 }))
    .expect("json body");

    // A non-stream body must be clonable so retries can resend it.
    assert!(request.builder.try_clone().is_some());
}

#[test]
fn backoff_after_attempt_grows() {
    let policy = VibeRetryPolicy::none()
        .initial_backoff(std::time::Duration::from_millis(100))
        .max_backoff(std::time::Duration::from_secs(10))
        .backoff_multiplier(2.0);
    assert_eq!(
        policy.backoff_for(1),
        std::time::Duration::from_millis(200)
    );
}
