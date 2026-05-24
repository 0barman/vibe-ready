#[test]
fn now_is_non_negative_and_monotonic_for_adjacent_calls() {
    let first = now();
    let second = now();
    assert!(first >= 0);
    assert!(second >= first);
}
