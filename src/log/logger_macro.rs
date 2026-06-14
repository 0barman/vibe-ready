use crate::log::log_level::LogLevel;
use crate::log::logger::VibeLogger;
use chrono::Local;
use indexmap::IndexMap;
use lazy_static::lazy_static;
use serde_json::Value;
use tokio::sync::RwLock;

/// Prefix used by vibe-ready structured log tags.
pub static LOG_PREFIX: &str = "V";

/// Trait implemented by global log listener callbacks.
pub trait LogListenerTrait: Fn(String, LogLevel, String, String) + Send + Sync {}
/// Boxed global log listener used by the log macro pipeline.
pub type DBLogListener = Box<dyn LogListenerTrait>;
impl<T> LogListenerTrait for T where T: Fn(String, LogLevel, String, String) + Send + Sync {}

/// Builds JSON log content from field names and values.
///
/// # Returns
///
/// A JSON object string when serialization succeeds, or a debug/error string
/// when serialization fails.
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

/// Receives a structured log event from exported log macros.
///
/// # Returns
///
/// This function returns `()` after dispatching to the global listener and stdout.
pub fn on_log(
    location: String,
    level: LogLevel,
    ext: Option<String>,
    tag: &str,
    first_field: Option<&str>,
    values: Option<Vec<Value>>,
    suffix: &str,
) {
    let content = create_log_content(first_field, values);
    let tag_data = match ext {
        Some(ext) => format!("{}-{}-{}-{}", LOG_PREFIX, ext, tag, suffix),
        None => format!("{}-{}-{}", LOG_PREFIX, tag, suffix),
    };

    match GLOBAL_DB_LOG_LISTENER.try_read() {
        Ok(listener) => {
            if let Some(on_log) = &*listener {
                on_log(location.clone(), level, tag_data.clone(), content.clone());
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
    println!("{} {} {:?} {}", time, tag_data.clone(), level, content);
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
    ($level:expr, $suffix:expr, $tag:expr, $first_field:expr, $( $value:expr ),*) => {
        let values = vec![$( $crate::__serde_json::json!($value) ),*];
        let location = std::panic::Location::caller();
        let location = format!(
            "{}:{}:{}",
            location.file(),
            location.line(),
            location.column()
        );
        $crate::__vibe_internal_log_on_log(location.clone(), $level, None, $tag, Some($first_field), Some(values), $suffix)
    };

    ($level:expr, $suffix:expr, $tag:expr, $( $value:expr ),*) => {
        let values = vec![$( $crate::__serde_json::json!($value) ),*];
        let location = std::panic::Location::caller();
        let location = format!(
            "{}:{}:{}",
            location.file(),
            location.line(),
            location.column()
        );
        $crate::__vibe_internal_log_on_log(location.clone(), $level, None, $tag, None, None, $suffix)
    };

    ($level:expr, $suffix:expr, $tag:expr) => {
        let location = std::panic::Location::caller();
        let location = format!(
            "{}:{}:{}",
            location.file(),
            location.line(),
            location.column()
        );
        $crate::__vibe_internal_log_on_log(location.clone(), $level, None, $tag, None, None, $suffix)
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! internal_log_db_ext {
    ($ext:expr, $level:expr, $suffix:expr, $tag:expr, $first_field:expr, $( $value:expr ),*) => {
        let values = vec![$( $crate::__serde_json::json!($value) ),*];
        let location = std::panic::Location::caller();
        let location = format!(
            "{}:{}:{}",
            location.file(),
            location.line(),
            location.column()
        );
        $crate::__vibe_internal_log_on_log(location.clone(), $level, $ext, $tag, Some($first_field), Some(values), $suffix)
    };

    ($ext:expr, $level:expr, $suffix:expr, $tag:expr, $( $value:expr ),*) => {
        let values = vec![$( $crate::__serde_json::json!($value) ),*];
        let location = std::panic::Location::caller();
        let location = format!(
            "{}:{}:{}",
            location.file(),
            location.line(),
            location.column()
        );
        $crate::__vibe_internal_log_on_log(location.clone(), $level, $ext, $tag, None, None, $suffix)
    };

    ($ext:expr, $level:expr, $suffix:expr, $tag:expr) => {
        let location = std::panic::Location::caller();
        let location = format!(
            "{}:{}:{}",
            location.file(),
            location.line(),
            location.column()
        );
        $crate::__vibe_internal_log_on_log(location.clone(), $level, $ext, $tag, None, None, $suffix)
    };
}

/// Emits an info-level structured log entry.
///
/// # Examples
///
/// ```
/// vibe_ready::log_i!("startup", "status", "ready");
/// ```
#[macro_export]
macro_rules! log_i {
    ($tag:expr, $first_field:expr, $( $value:expr ),*) => {
        $crate::internal_log_db!($crate::VibeLogLevel::Info, "I", $tag, $first_field, $( $value ),*)
    };

    ($tag:expr, $( $value:expr ),*) => {
        $crate::internal_log_db!($crate::VibeLogLevel::Info, "I", $tag, $( $value ),*)
    };

    ($tag:expr) => {
        $crate::internal_log_db!($crate::VibeLogLevel::Info, "I", $tag)
    };
}

/// Emits an info-level structured log entry with an extension segment in the tag.
///
/// # Examples
///
/// ```
/// vibe_ready::log_i_!(Some("host".to_string()), "startup", "status", "ready");
/// ```
#[macro_export]
macro_rules! log_i_ {
    ($ext:expr, $tag:expr, $first_field:expr, $( $value:expr ),*) => {
        $crate::internal_log_db_ext!($ext, $crate::VibeLogLevel::Info, "I", $tag, $first_field, $( $value ),*)
    };

    ($ext:expr, $tag:expr, $( $value:expr ),*) => {
        $crate::internal_log_db_ext!($ext, $crate::VibeLogLevel::Info, "I", $tag, $( $value ),*)
    };

    ($ext:expr, $tag:expr) => {
        $crate::internal_log_db_ext!($ext, $crate::VibeLogLevel::Info, "I", $tag)
    };
}

/// Emits an info-level trace-style structured log entry.
#[macro_export]
macro_rules! log_t {
    ($tag:expr, $first_field:expr, $( $value:expr ),*) => {
        $crate::internal_log_db!($crate::VibeLogLevel::Info, "T", $tag, $first_field, $( $value ),*)
    };

    ($tag:expr, $( $value:expr ),*) => {
        $crate::internal_log_db!($crate::VibeLogLevel::Info, "T", $tag, $( $value ),*)
    };

    ($tag:expr) => {
        $crate::internal_log_db!($crate::VibeLogLevel::Info, "T", $tag)
    };
}

/// Emits an info-level trace-style structured log entry with an extension segment.
#[macro_export]
macro_rules! log_t_ {
    ($ext:expr, $tag:expr, $first_field:expr, $( $value:expr ),*) => {
        $crate::internal_log_db_ext!($ext, $crate::VibeLogLevel::Info, "T", $tag, $first_field, $( $value ),*)
    };

    ($ext:expr, $tag:expr, $( $value:expr ),*) => {
        $crate::internal_log_db_ext!($ext, $crate::VibeLogLevel::Info, "T", $tag, $( $value ),*)
    };

    ($ext:expr, $tag:expr) => {
        $crate::internal_log_db_ext!($ext, $crate::VibeLogLevel::Info, "T", $tag)
    };
}

/// Emits a debug-level read-style structured log entry.
#[macro_export]
macro_rules! log_r {
    ($tag:expr, $first_field:expr, $( $value:expr ),*) => {
        $crate::internal_log_db!($crate::VibeLogLevel::Debug, "R", $tag, $first_field, $( $value ),*)
    };

    ($tag:expr, $( $value:expr ),*) => {
        $crate::internal_log_db!($crate::VibeLogLevel::Debug, "R", $tag, $( $value ),*)
    };

    ($tag:expr) => {
        $crate::internal_log_db!($crate::VibeLogLevel::Debug, "R", $tag)
    };
}

/// Emits a debug-level read-style structured log entry with an extension segment.
#[macro_export]
macro_rules! log_r_ {
    ($ext:expr, $tag:expr, $first_field:expr, $( $value:expr ),*) => {
        $crate::internal_log_db_ext!($ext, $crate::VibeLogLevel::Debug, "R", $tag, $first_field, $( $value ),*)
    };

    ($ext:expr, $tag:expr, $( $value:expr ),*) => {
        $crate::internal_log_db_ext!($ext, $crate::VibeLogLevel::Debug, "R", $tag, $( $value ),*)
    };

    ($ext:expr, $tag:expr) => {
        $crate::internal_log_db_ext!($ext, $crate::VibeLogLevel::Debug, "R", $tag)
    };
}

/// Emits a debug-level state-style structured log entry.
#[macro_export]
macro_rules! log_s {
    ($tag:expr, $first_field:expr, $( $value:expr ),*) => {
        $crate::internal_log_db!($crate::VibeLogLevel::Debug, "S", $tag, $first_field, $( $value ),*)
    };

    ($tag:expr, $( $value:expr ),*) => {
        $crate::internal_log_db!($crate::VibeLogLevel::Debug, "S", $tag, $( $value ),*)
    };

    ($tag:expr) => {
        $crate::internal_log_db!($crate::VibeLogLevel::Debug, "S", $tag)
    };
}

/// Emits a debug-level state-style structured log entry with an extension segment.
#[macro_export]
macro_rules! log_s_ {
    ($ext:expr, $tag:expr, $first_field:expr, $( $value:expr ),*) => {
        $crate::internal_log_db_ext!($ext, $crate::VibeLogLevel::Debug, "S", $tag, $first_field, $( $value ),*)
    };

    ($ext:expr, $tag:expr, $( $value:expr ),*) => {
        $crate::internal_log_db_ext!($ext, $crate::VibeLogLevel::Debug, "S", $tag, $( $value ),*)
    };

    ($ext:expr, $tag:expr) => {
        $crate::internal_log_db_ext!($ext, $crate::VibeLogLevel::Debug, "S", $tag)
    };
}

/// Emits an error-level structured log entry.
///
/// # Examples
///
/// ```
/// vibe_ready::log_e!("startup", "reason", "failed");
/// ```
#[macro_export]
macro_rules! log_e {
    ($tag:expr, $first_field:expr, $( $value:expr ),*) => {
        $crate::internal_log_db!($crate::VibeLogLevel::Error, "E", $tag, $first_field, $( $value ),*)
    };

    ($tag:expr, $( $value:expr ),*) => {
        $crate::internal_log_db!($crate::VibeLogLevel::Error, "E", $tag, $( $value ),*)
    };

    ($tag:expr) => {
        $crate::internal_log_db!($crate::VibeLogLevel::Error, "E", $tag)
    };
}

/// Emits an error-level structured log entry with an extension segment.
#[macro_export]
macro_rules! log_e_ {
    ($ext:expr, $tag:expr, $first_field:expr, $( $value:expr ),*) => {
        $crate::internal_log_db_ext!($ext, $crate::VibeLogLevel::Error, "E", $tag, $first_field, $( $value ),*)
    };

    ($ext:expr, $tag:expr, $( $value:expr ),*) => {
        $crate::internal_log_db_ext!($ext, $crate::VibeLogLevel::Error, "E", $tag, $( $value ),*)
    };

    ($ext:expr, $tag:expr) => {
        $crate::internal_log_db_ext!($ext, $crate::VibeLogLevel::Error, "E", $tag)
    };
}

/// Converts an array-like value to a JSON string.
///
/// # Examples
///
/// ```
/// let values = vec![1, 2, 3];
/// let json = vibe_ready::array_to_json_string!(values);
/// assert_eq!(json, "[1,2,3]");
/// ```
#[macro_export]
macro_rules! array_to_json_string {
    ($vec:expr) => {{
        let cloned_vec = $vec.clone();

        match $crate::__serde_json::to_string(&cloned_vec) {
            Ok(json) => json,
            Err(e) => format!("JSON serialization failed: {}", e),
        }
    }};
}

/// Converts displayable objects to a JSON-array-like string using `Display`.
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

/// Converts a string-keyed map of basic serializable values to JSON.
#[macro_export]
macro_rules! basic_type_map_to_json_string {
    ($map:expr) => {{
        use std::collections::HashMap;

        let cloned_map: HashMap<String, _> = $map.clone();

        (|| -> String {
            match $crate::__serde_json::to_string(&cloned_map) {
                Ok(json) => json,
                Err(e) => format!("JSON serialization failed: {}", e),
            }
        })()
    }};
}

/// Implements `Display` for serializable types by rendering JSON.
#[macro_export]
macro_rules! impl_display_json {
    ($($struct:ty),*) => {
        $(
            impl std::fmt::Display for $struct {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    match $crate::__serde_json::to_string(self) {
                        Ok(json) => write!(f, "{}", json),
                        Err(e) => write!(f, "Serialization error: {}", e),
                    }
                }
            }
        )*
    };
}

/// Logs an error with a backtrace and returns the same error expression.
///
/// # Examples
///
/// ```
/// use vibe_ready::{err, VibeEngineError, VibeErrorCode};
///
/// let error = err!(VibeEngineError::from_error_code(VibeErrorCode::RuntimeError));
/// assert_eq!(error.code(), VibeErrorCode::RuntimeError.code());
/// ```
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
    /// Registers or clears the global listener used by exported log macros.
    ///
    /// # Returns
    ///
    /// This method returns `()` after updating the listener when the lock is available.
    pub fn register_log_listener<T: LogListenerTrait + 'static>(listener: Option<T>) {
        if let Ok(mut guard) = GLOBAL_DB_LOG_LISTENER.try_write() {
            match listener {
                None => *guard = None,
                Some(listener) => *guard = Some(Box::new(listener)),
            }
        }
    }

    /// Clears the global listener used by exported log macros.
    ///
    /// # Returns
    ///
    /// This method returns `()` after clearing the listener when the lock is available.
    pub fn clear_global_log_listener() {
        if let Ok(mut guard) = GLOBAL_DB_LOG_LISTENER.try_write() {
            *guard = None;
        }
    }
}

#[cfg(test)]
mod strict_tests {
    use super::*;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test/unit/log/logger_macro_tests.rs"
    ));
}
