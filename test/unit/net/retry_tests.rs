use std::time::Duration;

#[test]
fn default_policy_retries_twice_with_status() {
    let policy = VibeRetryPolicy::default();
    assert_eq!(policy.max_retries_value(), 2);
    assert!(policy.should_retry_status(503));
    assert!(policy.should_retry_status(429));
    assert!(!policy.should_retry_status(404));
}

#[test]
fn none_policy_does_not_retry() {
    let policy = VibeRetryPolicy::none();
    assert_eq!(policy.max_retries_value(), 0);
    assert!(!policy.should_retry_status(503));
}

#[test]
fn backoff_is_exponential_and_capped() {
    let policy = VibeRetryPolicy::none()
        .initial_backoff(Duration::from_millis(100))
        .max_backoff(Duration::from_millis(350))
        .backoff_multiplier(2.0);
    assert_eq!(policy.backoff_for(0), Duration::from_millis(100));
    assert_eq!(policy.backoff_for(1), Duration::from_millis(200));
    // 100 * 2^2 = 400, capped to 350
    assert_eq!(policy.backoff_for(2), Duration::from_millis(350));
}

#[test]
fn jitter_stays_within_base() {
    let policy = VibeRetryPolicy::default();
    let base = Duration::from_millis(200);
    for seed in 1..50u64 {
        let jittered = policy.apply_jitter(base, seed);
        assert!(jittered <= base, "jitter {jittered:?} exceeded base");
    }
}

#[test]
fn jitter_disabled_returns_base() {
    let policy = VibeRetryPolicy::none().jitter(false);
    let base = Duration::from_millis(200);
    assert_eq!(policy.apply_jitter(base, 12345), base);
}

#[test]
fn max_backoff_value_returns_configured_ceiling() {
    let policy = VibeRetryPolicy::none().max_backoff(Duration::from_secs(7));
    assert_eq!(policy.max_backoff_value(), Duration::from_secs(7));
}
