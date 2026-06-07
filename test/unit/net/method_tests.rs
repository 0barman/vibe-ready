#[test]
fn method_as_str_matches_http_names() {
    assert_eq!(VibeHttpMethod::Get.as_str(), "GET");
    assert_eq!(VibeHttpMethod::Post.as_str(), "POST");
    assert_eq!(VibeHttpMethod::Put.as_str(), "PUT");
    assert_eq!(VibeHttpMethod::Patch.as_str(), "PATCH");
    assert_eq!(VibeHttpMethod::Delete.as_str(), "DELETE");
    assert_eq!(VibeHttpMethod::Head.as_str(), "HEAD");
    assert_eq!(VibeHttpMethod::Options.as_str(), "OPTIONS");
}

#[test]
fn method_converts_to_reqwest_method() {
    let method: reqwest::Method = VibeHttpMethod::Post.into();
    assert_eq!(method, reqwest::Method::POST);
}

#[test]
fn method_display_uses_as_str() {
    assert_eq!(format!("{}", VibeHttpMethod::Get), "GET");
}
