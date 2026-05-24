use crate::log_s;

/// Logs the elapsed time since a caller-provided start timestamp in milliseconds.
pub(crate) fn exec_time_end(method_name: &str, start_time_ms: i64) {
    let elapsed_ms = crate::platform::now() - start_time_ms;
    log_s!(method_name, "elapsed_ms", elapsed_ms);
}

#[cfg(test)]
mod strict_tests {
    use super::*;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test/unit/utils/date_util_tests.rs"
    ));
}
