#[test]
fn exec_time_end_accepts_past_and_future_start_times() {
    let now = crate::platform::now();
    exec_time_end("strict-past", now.saturating_sub(10));
    exec_time_end("strict-future", now.saturating_add(10));
}
