use serde::{Deserialize, Serialize};

#[repr(i32)]
#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Serialize, Eq, PartialOrd, Default)]
/// Severity level for SDK log records.
pub enum LogLevel {
    /// Disable log output.
    None = 0,
    /// Error-level log entry.
    Error = 1,
    /// Warning-level log entry.
    Warn = 2,
    /// Informational log entry.
    #[default]
    Info = 3,
    /// Debug-level log entry.
    Debug = 4,
}

impl From<LogLevel> for i32 {
    fn from(val: LogLevel) -> Self {
        val as i32
    }
}

impl From<i32> for LogLevel {
    fn from(value: i32) -> Self {
        if value == 0 {
            LogLevel::None
        } else if value == 1 {
            LogLevel::Error
        } else if value == 2 {
            LogLevel::Warn
        } else if value == 3 {
            LogLevel::Info
        } else if value == 4 {
            LogLevel::Debug
        } else {
            LogLevel::None
        }
    }
}

#[cfg(test)]
mod strict_tests {
    use super::*;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test/unit/log/log_level_tests.rs"
    ));
}
