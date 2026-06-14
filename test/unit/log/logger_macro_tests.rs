use std::sync::mpsc;
use std::time::Duration;

// The log listener is a single process-global slot, so tests that register one
// must not run concurrently or they clobber each other's listener. Serialize
// every listener-using test through this lock. Poisoning is ignored: a panicking
// test still leaves the slot in a usable state for the next acquirer.
static LISTENER_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn lock_listener() -> std::sync::MutexGuard<'static, ()> {
    LISTENER_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[test]
fn create_log_content_handles_empty_missing_and_extra_values() {
    assert_eq!(create_log_content(None, None), "{}");
    assert_eq!(create_log_content(Some("a|b"), Some(vec![serde_json::json!(1)])), "{\"a\":1}");
    assert_eq!(
        create_log_content(Some("a"), Some(vec![serde_json::json!(1), serde_json::json!(2)])),
        "{\"a\":1}"
    );
}

#[test]
fn global_log_listener_receives_tag_location_level_and_content() {
    let _guard = lock_listener();
    let (tx, rx) = mpsc::channel();
    VibeLogger::register_log_listener(Some(move |location, level, tag, content| {
        tx.send((location, level, tag, content)).expect("send log event");
    }));

    on_log(
        "file.rs:1:1".to_string(),
        LogLevel::Info,
        Some("ext".to_string()),
        "tag",
        Some("code|desc"),
        Some(vec![serde_json::json!(200), serde_json::json!("ok")]),
        "I",
    );
    let (location, level, tag, content) = rx.recv_timeout(Duration::from_secs(1)).expect("log event");
    assert_eq!(location, "file.rs:1:1");
    assert_eq!(level, LogLevel::Info);
    assert_eq!(tag, "V-ext-tag-I");
    assert_eq!(content, "{\"code\":200,\"desc\":\"ok\"}");

    VibeLogger::clear_global_log_listener();
}

/// The macro-hygiene fix re-exports `serde_json` under a hidden alias so the
/// exported macros can reference it through `$crate` instead of the caller's
/// crate scope. Lock that re-export in: it is the contract the macros rely on.
#[test]
fn reexported_serde_json_is_reachable_via_crate_path() {
    let value = crate::__serde_json::json!({ "a": 1, "b": [2, 3] });
    assert_eq!(
        crate::__serde_json::to_string(&value).expect("serialize re-exported value"),
        "{\"a\":1,\"b\":[2,3]}"
    );
}

/// End-to-end coverage of every exported `log_*!` macro through the global
/// listener. This is the behavior the fix must preserve byte-for-byte: the
/// macros now expand to `$crate::__serde_json::json!`, but the emitted level,
/// tag, and JSON content must be identical to before.
#[test]
fn log_macros_emit_expected_levels_tags_and_content() {
    let _guard = lock_listener();
    let (tx, rx) = mpsc::channel();
    VibeLogger::register_log_listener(Some(move |_location, level, tag, content| {
        tx.send((level, tag, content)).expect("send log event");
    }));
    let next = || {
        rx.recv_timeout(Duration::from_secs(1))
            .expect("log event within timeout")
    };

    // Info / single field.
    crate::log_i!("startup", "status", "ready");
    assert_eq!(
        next(),
        (LogLevel::Info, "V-startup-I".to_string(), "{\"status\":\"ready\"}".to_string())
    );

    // Info trace / multiple fields, field order preserved, numeric value.
    crate::log_t!("startup", "status|count", "ready", 3);
    assert_eq!(
        next(),
        (
            LogLevel::Info,
            "V-startup-T".to_string(),
            "{\"status\":\"ready\",\"count\":3}".to_string()
        )
    );

    // Debug read.
    crate::log_r!("startup", "k", "v");
    assert_eq!(
        next(),
        (LogLevel::Debug, "V-startup-R".to_string(), "{\"k\":\"v\"}".to_string())
    );

    // Debug state / tag-only arm yields an empty object.
    crate::log_s!("startup");
    assert_eq!(
        next(),
        (LogLevel::Debug, "V-startup-S".to_string(), "{}".to_string())
    );

    // Error / mixed value types.
    crate::log_e!("startup", "code|reason", 500, "boom");
    assert_eq!(
        next(),
        (
            LogLevel::Error,
            "V-startup-E".to_string(),
            "{\"code\":500,\"reason\":\"boom\"}".to_string()
        )
    );

    VibeLogger::clear_global_log_listener();
}

/// The `_`-suffixed macros add an extension segment between the prefix and tag.
#[test]
fn log_ext_macros_include_extension_segment_in_tag() {
    let _guard = lock_listener();
    let (tx, rx) = mpsc::channel();
    VibeLogger::register_log_listener(Some(move |_location, level, tag, content| {
        tx.send((level, tag, content)).expect("send log event");
    }));
    let next = || {
        rx.recv_timeout(Duration::from_secs(1))
            .expect("log event within timeout")
    };

    crate::log_i_!(Some("host".to_string()), "startup", "status", "ready");
    assert_eq!(
        next(),
        (
            LogLevel::Info,
            "V-host-startup-I".to_string(),
            "{\"status\":\"ready\"}".to_string()
        )
    );

    crate::log_e_!(Some("host".to_string()), "startup", "code", 1);
    assert_eq!(
        next(),
        (LogLevel::Error, "V-host-startup-E".to_string(), "{\"code\":1}".to_string())
    );

    // Tag-only ext arm.
    crate::log_s_!(Some("host".to_string()), "startup");
    assert_eq!(
        next(),
        (LogLevel::Debug, "V-host-startup-S".to_string(), "{}".to_string())
    );

    VibeLogger::clear_global_log_listener();
}

/// `array_to_json_string!` serializes a slice/vec via the re-exported serde_json.
#[test]
fn array_to_json_string_serializes_via_reexport() {
    let values = vec![1, 2, 3];
    assert_eq!(crate::array_to_json_string!(values), "[1,2,3]");
}

/// `obj_array_to_json_string!` renders elements through `Display` (no serde_json).
#[test]
fn obj_array_to_json_string_joins_display_values() {
    let values = vec![1, 2, 3];
    assert_eq!(crate::obj_array_to_json_string!(values), "[1, 2, 3]");
}

/// `basic_type_map_to_json_string!` serializes a string-keyed map via the
/// re-exported serde_json, without the caller needing serde / serde_json.
#[test]
fn basic_type_map_to_json_string_serializes_single_entry() {
    use std::collections::HashMap;
    let mut map: HashMap<String, i32> = HashMap::new();
    map.insert("a".to_string(), 1);
    assert_eq!(crate::basic_type_map_to_json_string!(map), "{\"a\":1}");
}

/// `impl_display_json!` wires up a JSON `Display` impl via the re-exported
/// serde_json. Defining the struct/impl inside the test keeps the macro's
/// caller-scope path resolution under test.
#[test]
fn impl_display_json_renders_struct_as_json() {
    #[derive(serde::Serialize)]
    struct Point {
        x: i32,
        y: i32,
    }
    crate::impl_display_json!(Point);

    assert_eq!(Point { x: 1, y: 2 }.to_string(), "{\"x\":1,\"y\":2}");
}
