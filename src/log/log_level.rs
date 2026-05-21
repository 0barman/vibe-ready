use serde::{Deserialize, Serialize};

#[repr(i32)]
#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Serialize, Eq, PartialOrd, Default)]
/// Severity level for SDK log records.
pub enum LogLevel {
    None = 0,
    Error = 1,
    Warn = 2,
    #[default]
    Info = 3,
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
