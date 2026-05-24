#[test]
fn log_def_constants_and_long_text_accept_empty_and_large_content() {
    assert_eq!(CODE_STR, "code");
    assert_eq!(RET_STR, "ret");
    assert_eq!(DESC, "desc");
    long_text("strict", "empty", "");
    long_text("strict", "large", &"x".repeat(128));
}
