use crate::log::log_level::LogLevel;

/// Standard structured-log field name for error or status codes.
pub static CODE_STR: &str = "code";
/// Standard structured-log field name for return values.
pub static RET_STR: &str = "ret";
/// Standard structured-log field name for human-readable descriptions.
pub static DESC: &str = "desc";

/// Callback invoked when a log record is emitted.
pub type LogListener = Box<dyn Fn(VibeLogInfo) + Send + Sync + 'static>;

#[derive(Debug, Clone)]
/// Structured SDK log record.
pub struct VibeLogInfo {
    /// Log severity.
    pub level: LogLevel,
    /// Log tag identifying the subsystem or operation.
    pub tag: String,
    /// Log payload, often JSON formatted by the exported log macros.
    pub content: String,
    /// Creation timestamp in Unix milliseconds.
    pub create_time: i64,
}

/// Logs a shortened preview for very long text content.
///
/// # Returns
///
/// This method returns `()` after emitting the shortened log line.
///
/// # Examples
///
/// ```ignore
/// vibe_ready::log::long_text("upload", "payload", "large body text");
/// ```
#[allow(dead_code)]
pub fn long_text(method_name: &str, tag: &str, content: &str) {
    let content_short = {
        let mut iter = content.chars();
        let head: String = iter.by_ref().take(20).collect();
        if iter.next().is_some() {
            let total_mb = content.len() as f64 / (1024.0 * 1024.0);
            format!("{head}...[{total_mb:.2} MB]")
        } else {
            head
        }
    };

    crate::log_s!(method_name, tag, content_short.as_str());
}

#[cfg(test)]
mod strict_tests {
    use super::*;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test/unit/log/log_def_tests.rs"
    ));
}
