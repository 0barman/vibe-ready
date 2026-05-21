use crate::log::log_def::LogType;
use crate::log::log_level::LogLevel;
use crate::log::logger::VibeLogger;
use chrono::Local;
use indexmap::IndexMap;
use lazy_static::lazy_static;
use serde_json::Value;
use tokio::sync::RwLock;

pub static LOG_PREFIX: &str = "L";

pub trait LogListenerTrait: Fn(String, LogLevel, LogType, String, String) + Send + Sync {}
pub type DBLogListener = Box<dyn LogListenerTrait>;
impl<T> LogListenerTrait for T where T: Fn(String, LogLevel, LogType, String, String) + Send + Sync {}

pub fn create_log_content(first_field: Option<&str>, values: Option<Vec<Value>>) -> String {
    if first_field.is_none() && values.is_none() {
        return "{}".to_string();
    }

    let first_field = first_field.unwrap_or_default();
    let values = values.unwrap_or_default();

    let keys: Vec<&str> = first_field.split('|').collect();

    let mut map = IndexMap::new();

    for (i, key) in keys.iter().enumerate() {
        if i < values.len() {
            map.insert(*key, values[i].clone());
        }
    }

    match serde_json::to_string(&map) {
        Ok(json_str) => json_str,
        Err(_) => format!("{:?}", map),
    }
}

pub fn on_log(
    location: String,
    level: LogLevel,
    log_type: LogType,
    tag: &str,
    first_field: Option<&str>,
    values: Option<Vec<Value>>,
    suffix: &str,
) {
    let content = create_log_content(first_field, values);
    let tag_data = match log_type {
        LogType::None => {
            format!("{}-{}-{}", LOG_PREFIX, tag, suffix)
        }
        LogType::Database => {
            format!("{}_{}-{}-{}", LOG_PREFIX, "DB", tag, suffix)
        }
        LogType::Engine => {
            format!("{}-{}-{}", LOG_PREFIX, tag, suffix)
        }
        LogType::WSS => {
            format!("{}_{}-{}-{}", LOG_PREFIX, "WSS", tag, suffix)
        }
    };

    match GLOBAL_DB_LOG_LISTENER.try_read() {
        Ok(listener) => {
            if let Some(on_log) = &*listener {
                on_log(
                    location.clone(),
                    level,
                    log_type,
                    tag_data.clone(),
                    content.clone(),
                );
            }
        }
        Err(error) => {
            println!(
                "Error occurred while locking GLOBAL_LOG_LISTENER: {:?}",
                error
            );
        }
    }

    let time = Local::now().format("%H:%M:%S%.3f");
    println!(
        "{} {} {:?} {:?} {}",
        time,
        tag_data.clone(),
        level,
        log_type,
        content
    );
}

#[doc(hidden)]
#[macro_export]
macro_rules! location {
    () => {{
        let location = std::panic::Location::caller();
        let location_str = format!(
            "{}:{}:{}",
            location.file(),
            location.line(),
            location.column()
        );
        location_str
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! internal_log_db {
    ($log_type:expr, $level:expr, $suffix:expr, $tag:expr, $first_field:expr, $( $value:expr ),*) => {
        let values = vec![$( serde_json::json!($value) ),*];
        let location = std::panic::Location::caller();
        let location = format!(
            "{}:{}:{}",
            location.file(),
            location.line(),
            location.column()
        );
        $crate::__vibe_internal_log_on_log(location.clone(), $level, $log_type, $tag, Some($first_field), Some(values), $suffix)
    };

    ($log_type:expr, $level:expr, $suffix:expr, $tag:expr, $( $value:expr ),*) => {
        let values = vec![$( serde_json::json!($value) ),*];
        let location = std::panic::Location::caller();
        let location = format!(
            "{}:{}:{}",
            location.file(),
            location.line(),
            location.column()
        );
        $crate::__vibe_internal_log_on_log(location.clone(), $level, $log_type, $tag, None, None, $suffix)
    };

    ($log_type:expr, $level:expr, $suffix:expr, $tag:expr) => {
        let location = std::panic::Location::caller();
        let location = format!(
            "{}:{}:{}",
            location.file(),
            location.line(),
            location.column()
        );
        $crate::__vibe_internal_log_on_log(location.clone(), $level, $log_type, $tag, None, None, $suffix)
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! internal_log_wss {
    ($log_type:expr, $level:expr, $suffix:expr, $tag:expr, $first_field:expr, $( $value:expr ),*) => {
        let values = vec![$( serde_json::json!($value) ),*];
        let location = std::panic::Location::caller();
        let location = format!(
            "{}:{}:{}",
            location.file(),
            location.line(),
            location.column()
        );
        $crate::__vibe_internal_log_on_log(location.clone(), $level, $log_type, $tag, Some($first_field), Some(values), $suffix)
    };

    ($log_type:expr, $level:expr, $suffix:expr, $tag:expr, $( $value:expr ),*) => {
        let values = vec![$( serde_json::json!($value) ),*];
        let location = std::panic::Location::caller();
        let location = format!(
            "{}:{}:{}",
            location.file(),
            location.line(),
            location.column()
        );
        $crate::__vibe_internal_log_on_log(location.clone(), $level, $log_type, $tag, None, None, $suffix)
    };

    ($log_type:expr, $level:expr, $suffix:expr, $tag:expr) => {
        let location = std::panic::Location::caller();
        let location = format!(
            "{}:{}:{}",
            location.file(),
            location.line(),
            location.column()
        );
        $crate::__vibe_internal_log_on_log(location.clone(), $level, $log_type, $tag, None, None, $suffix)
    };
}

#[macro_export]
macro_rules! log_db_t {
    ($tag:expr, $first_field:expr, $( $value:expr ),*) => {
        $crate::internal_log_db!($crate::VibeLogType::Database, $crate::VibeLogLevel::Info, "T", $tag, $first_field, $( $value ),*)
    };

    ($tag:expr, $( $value:expr ),*) => {
        $crate::internal_log_db!($crate::VibeLogType::Database, $crate::VibeLogLevel::Info, "T", $tag, $( $value ),*)
    };

    ($tag:expr) => {
        $crate::internal_log_db!($crate::VibeLogType::Database, $crate::VibeLogLevel::Info, "T", $tag)
    };
}

#[macro_export]
macro_rules! log_db_r {
    ($tag:expr, $first_field:expr, $( $value:expr ),*) => {
        $crate::internal_log_db!($crate::VibeLogType::Database, $crate::VibeLogLevel::Debug, "R", $tag, $first_field, $( $value ),*)
    };

    ($tag:expr, $( $value:expr ),*) => {
        $crate::internal_log_db!($crate::VibeLogType::Database, $crate::VibeLogLevel::Debug, "R", $tag, $( $value ),*)
    };

    ($tag:expr) => {
        $crate::internal_log_db!($crate::VibeLogType::Database, $crate::VibeLogLevel::Debug, "R", $tag)
    };
}

#[macro_export]
macro_rules! log_db_s {
    ($tag:expr, $first_field:expr, $( $value:expr ),*) => {
        $crate::internal_log_db!($crate::VibeLogType::Database, $crate::VibeLogLevel::Debug, "S", $tag, $first_field, $( $value ),*)
    };

    ($tag:expr, $( $value:expr ),*) => {
        $crate::internal_log_db!($crate::VibeLogType::Database, $crate::VibeLogLevel::Debug, "S", $tag, $( $value ),*)
    };

    ($tag:expr) => {
        $crate::internal_log_db!($crate::VibeLogType::Database, $crate::VibeLogLevel::Debug, "S", $tag)
    };
}

#[macro_export]
macro_rules! log_db_e {
    ($tag:expr, $first_field:expr, $( $value:expr ),*) => {
        $crate::internal_log_db!($crate::VibeLogType::Database, $crate::VibeLogLevel::Error, "E", $tag, $first_field, $( $value ),*)
    };

    ($tag:expr, $( $value:expr ),*) => {
        $crate::internal_log_db!($crate::VibeLogType::Database, $crate::VibeLogLevel::Error, "E", $tag, $( $value ),*)
    };

    ($tag:expr) => {
        $crate::internal_log_db!($crate::VibeLogType::Database, $crate::VibeLogLevel::Error, "E", $tag)
    };
}

#[macro_export]
macro_rules! log_wss_t {
    ($tag:expr, $first_field:expr, $( $value:expr ),*) => {
        $crate::internal_log_wss!($crate::VibeLogType::WSS, $crate::VibeLogLevel::Info, "T", $tag, $first_field, $( $value ),*)
    };

    ($tag:expr, $( $value:expr ),*) => {
        $crate::internal_log_wss!($crate::VibeLogType::WSS, $crate::VibeLogLevel::Info, "T", $tag, $( $value ),*)
    };

    ($tag:expr) => {
        $crate::internal_log_wss!($crate::VibeLogType::WSS, $crate::VibeLogLevel::Info, "T", $tag)
    };
}

#[macro_export]
macro_rules! log_wss_r {
    ($tag:expr, $first_field:expr, $( $value:expr ),*) => {
        $crate::internal_log_wss!($crate::VibeLogType::WSS, $crate::VibeLogLevel::Debug, "R", $tag, $first_field, $( $value ),*)
    };

    ($tag:expr, $( $value:expr ),*) => {
        $crate::internal_log_wss!($crate::VibeLogType::WSS, $crate::VibeLogLevel::Debug, "R", $tag, $( $value ),*)
    };

    ($tag:expr) => {
        $crate::internal_log_wss!($crate::VibeLogType::WSS, $crate::VibeLogLevel::Debug, "R", $tag)
    };
}

#[macro_export]
macro_rules! log_wss_s {
    ($tag:expr, $first_field:expr, $( $value:expr ),*) => {
        $crate::internal_log_wss!($crate::VibeLogType::WSS, $crate::VibeLogLevel::Debug, "S", $tag, $first_field, $( $value ),*)
    };

    ($tag:expr, $( $value:expr ),*) => {
        $crate::internal_log_wss!($crate::VibeLogType::WSS, $crate::VibeLogLevel::Debug, "S", $tag, $( $value ),*)
    };

    ($tag:expr) => {
        $crate::internal_log_wss!($crate::VibeLogType::WSS, $crate::VibeLogLevel::Debug, "S", $tag)
    };
}

#[macro_export]
macro_rules! log_wss_e {
    ($tag:expr, $first_field:expr, $( $value:expr ),*) => {
        $crate::internal_log_wss!($crate::VibeLogType::WSS, $crate::VibeLogLevel::Error, "E", $tag, $first_field, $( $value ),*)
    };

    ($tag:expr, $( $value:expr ),*) => {
        $crate::internal_log_wss!($crate::VibeLogType::WSS, $crate::VibeLogLevel::Error, "E", $tag, $( $value ),*)
    };

    ($tag:expr) => {
        $crate::internal_log_wss!($crate::VibeLogType::WSS, $crate::VibeLogLevel::Error, "E", $tag)
    };
}

#[macro_export]
macro_rules! log_i {
    ($tag:expr, $first_field:expr, $( $value:expr ),*) => {
        $crate::internal_log_db!($crate::VibeLogType::Engine, $crate::VibeLogLevel::Info, "I", $tag, $first_field, $( $value ),*)
    };

    ($tag:expr, $( $value:expr ),*) => {
        $crate::internal_log_db!($crate::VibeLogType::Engine, $crate::VibeLogLevel::Info, "I", $tag, $( $value ),*)
    };

    ($tag:expr) => {
        $crate::internal_log_db!($crate::VibeLogType::Engine, $crate::VibeLogLevel::Info, "I", $tag)
    };
}

#[macro_export]
macro_rules! log_t {
    ($tag:expr, $first_field:expr, $( $value:expr ),*) => {
        $crate::internal_log_db!($crate::VibeLogType::Engine, $crate::VibeLogLevel::Info, "T", $tag, $first_field, $( $value ),*)
    };

    ($tag:expr, $( $value:expr ),*) => {
        $crate::internal_log_db!($crate::VibeLogType::Engine, $crate::VibeLogLevel::Info, "T", $tag, $( $value ),*)
    };

    ($tag:expr) => {
        $crate::internal_log_db!($crate::VibeLogType::Engine, $crate::VibeLogLevel::Info, "T", $tag)
    };
}

#[macro_export]
macro_rules! log_r {
    ($tag:expr, $first_field:expr, $( $value:expr ),*) => {
        $crate::internal_log_db!($crate::VibeLogType::Engine, $crate::VibeLogLevel::Debug, "R", $tag, $first_field, $( $value ),*)
    };

    ($tag:expr, $( $value:expr ),*) => {
        $crate::internal_log_db!($crate::VibeLogType::Engine, $crate::VibeLogLevel::Debug, "R", $tag, $( $value ),*)
    };

    ($tag:expr) => {
        $crate::internal_log_db!($crate::VibeLogType::Engine, $crate::VibeLogLevel::Debug, "R", $tag)
    };
}

#[macro_export]
macro_rules! log_s {
    ($tag:expr, $first_field:expr, $( $value:expr ),*) => {
        $crate::internal_log_db!($crate::VibeLogType::Engine, $crate::VibeLogLevel::Debug, "S", $tag, $first_field, $( $value ),*)
    };

    ($tag:expr, $( $value:expr ),*) => {
        $crate::internal_log_db!($crate::VibeLogType::Engine, $crate::VibeLogLevel::Debug, "S", $tag, $( $value ),*)
    };

    ($tag:expr) => {
        $crate::internal_log_db!($crate::VibeLogType::Engine, $crate::VibeLogLevel::Debug, "S", $tag)
    };
}

#[macro_export]
macro_rules! log_e {
    ($tag:expr, $first_field:expr, $( $value:expr ),*) => {
        $crate::internal_log_db!($crate::VibeLogType::Engine, $crate::VibeLogLevel::Error, "E", $tag, $first_field, $( $value ),*)
    };

    ($tag:expr, $( $value:expr ),*) => {
        $crate::internal_log_db!($crate::VibeLogType::Engine, $crate::VibeLogLevel::Error, "E", $tag, $( $value ),*)
    };

    ($tag:expr) => {
        $crate::internal_log_db!($crate::VibeLogType::Engine, $crate::VibeLogLevel::Error, "E", $tag)
    };
}

#[macro_export]
macro_rules! array_to_json_string {
    ($vec:expr) => {{
        let cloned_vec = $vec.clone();

        match serde_json::to_string(&cloned_vec) {
            Ok(json) => json,
            Err(e) => format!("JSON serialization failed: {}", e),
        }
    }};
}

#[macro_export]
macro_rules! obj_array_to_json_string {
    ($vec:expr) => {{
        format!(
            "[{}]",
            $vec.iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }};
}

#[macro_export]
macro_rules! basic_type_map_to_json_string {
    ($map:expr) => {{
        use serde::Serialize;
        use std::collections::HashMap;

        let cloned_map: HashMap<String, _> = $map.clone();

        (|| -> String {
            match serde_json::to_string(&cloned_map) {
                Ok(json) => json,
                Err(e) => format!("JSON serialization failed: {}", e),
            }
        })()
    }};
}

#[macro_export]
macro_rules! impl_display_json {
    ($($struct:ty),*) => {
        $(
            impl std::fmt::Display for $struct {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    match serde_json::to_string(self) {
                        Ok(json) => write!(f, "{}", json),
                        Err(e) => write!(f, "Serialization error: {}", e),
                    }
                }
            }
        )*
    };
}

#[macro_export]
macro_rules! err {
    ($err:expr) => {{
        log::error!(
            "Error: {:?}, {:?}",
            $err,
            std::backtrace::Backtrace::capture()
        );
        $err
    }};
}

lazy_static! {
    static ref GLOBAL_DB_LOG_LISTENER: RwLock<Option<DBLogListener>> = RwLock::new(None);
}

impl VibeLogger {
    pub fn register_log_listener<T: LogListenerTrait + 'static>(listener: Option<T>) {
        if let Ok(mut guard) = GLOBAL_DB_LOG_LISTENER.try_write() {
            match listener {
                None => *guard = None,
                Some(listener) => *guard = Some(Box::new(listener)),
            }
        }
    }

    pub fn clear_global_log_listener() {
        if let Ok(mut guard) = GLOBAL_DB_LOG_LISTENER.try_write() {
            *guard = None;
        }
    }
}
