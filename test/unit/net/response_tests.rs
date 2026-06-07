#[test]
fn response_wraps_status_and_headers() {
    let http_response = http::Response::builder()
        .status(201)
        .header("x-test", "abc")
        .body("hello")
        .expect("build response");
    let response = VibeHttpResponse::new(reqwest::Response::from(http_response), "ctx".to_string());

    assert_eq!(response.status(), 201);
    assert!(response.is_success());
    assert_eq!(response.header("x-test"), Some("abc".to_string()));
    assert!(response
        .headers()
        .iter()
        .any(|(k, v)| k == "x-test" && v == "abc"));
}

#[test]
fn error_for_status_rejects_non_2xx() {
    let http_response = http::Response::builder()
        .status(500)
        .body("boom")
        .expect("build response");
    let response = VibeHttpResponse::new(reqwest::Response::from(http_response), "ctx".to_string());
    let err = response.error_for_status().unwrap_err();
    assert_eq!(
        err.code(),
        crate::api::engine_error::VibeEngineErrorCode::InternalServerError.code()
    );
}
