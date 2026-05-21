use crate::log::log_level::LogLevel;

pub static CODE_STR: &str = "code";
pub static RET_STR: &str = "ret";
pub static DESC: &str = "desc";

/// Callback invoked when a log record is emitted.
pub type LogListener = Box<dyn Fn(VibeLogInfo) + Send + Sync + 'static>;

#[repr(i32)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
/// Source category for an SDK log record.
pub enum LogType {
    None = 0,
    Database = 1,
    Engine = 2,
    WSS = 3,
}

#[derive(Debug, Clone)]
/// Structured SDK log record.
pub struct VibeLogInfo {
    pub level: LogLevel,
    pub tag: String,
    pub content: String,
    pub create_time: i64,
}

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
