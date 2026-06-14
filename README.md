# vibe-ready

Composable runtime, logging, scheduling, and storage foundations for vibe-coding Rust projects.

`vibe-ready` is a small application foundation crate for developers who want to build quickly with AI coding tools. It gives an AI agent a stable set of primitives for project setup: a runtime engine, task execution, scheduled jobs, structured logs, persistent key-value storage, cancellation, status tracking, and a consistent error model.

The goal is simple: describe the product you want, tell your AI to use `vibe-ready`, and let it assemble the common project infrastructure from this crate instead of reinventing it.

## What You Get

- Engine lifecycle: create, run, inspect, and destroy a `VibeEngine`.
- Runtime execution: run futures synchronously with `invoke`, post background work with `post`, or use the lower-level `VibeEngineExecutor`.
- Callback pool: dispatch user callbacks away from async runtime threads with `VibeCallbackExecutor`.
- Scheduler: priority tasks, delayed tasks, periodic tasks, cancellation tokens, task handles, and task inspection.
- Key-value store: typed values, buckets, TTL, batch operations, transactions, change listeners, and expired-row cleanup.
- Configuration model: app identity, platform, runtime sizing, logging behavior, storage behavior, and backup strategy.
- Logging: structured log records, log listeners, stdout output, optional persistent log storage, standard log fields, and exported log macros.
- Storage backends: Diesel SQLite persistence by default, or no-op backends for tests and lightweight builds.
- Error model: stable error codes, error categories, messages, sources, and context breadcrumbs.
- Platform helpers: current time, async sleep, and task spawning helpers.
- AI-friendly public API: documented re-exports and a `prelude` for fast project bootstrapping.

## Installation

Add the crate to your Rust project:

```toml
[dependencies]
vibe-ready = "0.2.2"
```
Default features enable SQLite-backed log and work storage:

```toml
[dependencies]
vibe-ready = { version = "0.2.2", features = ["log-diesel", "store-diesel-sqlite"] }
```

For tests or projects that do not need persistence:

```toml
[dependencies]
vibe-ready = { version = "0.2.2", default-features = false, features = ["log-noop", "store-noop"] }
```

## Quick Start

```rust
use std::time::Duration;
use vibe_ready::{VibeEngine, VibeEngineConfig, VibePlatformType, VibeResult};

fn main() -> VibeResult<()> {
    let config = VibeEngineConfig::builder()
        .platform(VibePlatformType::MacOS)
        .app_name("demo-app")
        .namespace("examples")
        .build();

    let engine = VibeEngine::create(config)?;

    engine.store().set_str("status", "ready")?;
    let status = engine.store().get_str("status")?;
    assert_eq!(status.as_deref(), Some("ready"));

    engine.post(async {
        println!("background task started");
    });

    engine.destroy_with_timeout(Duration::from_secs(2))?;
    Ok(())
}
```

## AI Agent Usage

This README is intentionally written so an AI coding agent can use `vibe-ready` as a project foundation without reading the entire source tree first.

### One-Line Prompt

Use this when asking an AI to build a Rust app:

```text
Build this Rust project using vibe-ready as the foundation: create a VibeEngine, configure app identity and storage, use VibeKvStore for local state, use the scheduler for background jobs, use structured logging, and return VibeResult from fallible app flows.
```

### Detailed AI Prompt

```text
You are building a Rust project with the vibe-ready crate.

Use vibe_ready::prelude::*.
Create configuration with VibeEngineConfig::builder().
Set app_name, namespace, platform, store_root_path when needed, and choose no-op backends for tests.
Create the runtime with VibeEngine::create(config) or VibeEngine::create_with_runtime_handle(config, handle).
Use engine.invoke for async work that must return a value synchronously.
Use engine.post for fire-and-forget async work.
Use engine.post_with_priority, schedule_after, and schedule_every for background jobs.
Use VibeCancellationToken in scheduled tasks and check token.is_cancelled() or await token.cancelled().
Use engine.tasks().list() and count() for diagnostics.
Use engine.store() for app state. Use set/get typed values, buckets, TTL, transactions, batch operations, and on_change listeners instead of creating ad hoc storage.
Use engine.insert_log, engine.set_log_listener, and log_i!/log_e! macros for structured logs.
Return VibeResult<T> from public fallible functions and attach context with VibeEngineError::with_context when useful.
Always call engine.destroy_with_timeout before shutdown in examples and tests.
```

### What AI Should Build With Each Capability

| Need | Use |
| --- | --- |
| App runtime foundation | `VibeEngine`, `VibeEngineConfig` |
| Shared project imports | `vibe_ready::prelude::*` |
| Local app state | `VibeKvStore` |
| Namespaced state | `VibeKvBucket` |
| Temporary state | `set_with_ttl`, `purge_expired` |
| Atomic state changes | `transaction`, `VibeKvTx` |
| React to state changes | `on_change`, `VibeKvChange` |
| Fire-and-forget background work | `VibeEngine::post` |
| Blocking wait for async result | `VibeEngine::invoke` |
| Priority background jobs | `post_with_priority`, `VibeTaskPriority` |
| Delayed or periodic jobs | `schedule_after`, `schedule_every` |
| Cooperative cancellation | `VibeCancellationToken`, `VibeTaskHandle::cancel` |
| Task diagnostics | `VibeTaskPanel`, `VibeTaskInfo` |
| Structured logs | `VibeLogInfo`, `VibeLogLevel`, `log_i!`, `log_e!` |
| Stable app errors | `VibeEngineError`, `VibeError`, `VibeErrorCode`, `VibeErrorKind`, `VibeResult`, `err!` |
| Platform time/spawn helpers | `vibe_ready::platform::{now, sleep, spawn}` |
| Advanced storage access | `VibeDbClient`, `VibeTableKeyVal`, `VibeDbErrorInfo` |
| JSON/display utility macros | `array_to_json_string!`, `obj_array_to_json_string!`, `basic_type_map_to_json_string!`, `impl_display_json!` |
| Outbound HTTP / call external APIs | `VibeHttpClient` (feature `net-http`) |

## Core Concepts

### Engine

`VibeEngine` is the main entry point. It owns or connects to a Tokio runtime, initializes logging and work storage, exposes a task executor, and gives the application access to a key-value store.

```rust
use vibe_ready::{VibeEngine, VibeEngineConfig, VibePlatformType, VibeResult};

fn create_engine() -> VibeResult<VibeEngine> {
    let config = VibeEngineConfig::builder()
        .platform(VibePlatformType::MacOS)
        .app_name("my-product")
        .namespace("dev")
        .runtime_worker_threads(4)
        .callback_threads(2)
        .queue_capacity(1024, 256)
        .build();

    VibeEngine::create(config)
}
```

Use an external Tokio runtime when the host application already owns one:

```rust
use vibe_ready::{VibeEngine, VibeEngineConfig, VibeResult};

fn create_with_host_runtime(runtime: &tokio::runtime::Runtime) -> VibeResult<VibeEngine> {
    VibeEngine::create_with_runtime_handle(
        VibeEngineConfig::builder().app_name("hosted-app").build(),
        runtime.handle().clone(),
    )
}
```

### Configuration

`VibeEngineConfig::builder()` starts with defaults suitable for local development. Configure only what your app needs.

```rust
use vibe_ready::{
    VibeEngineConfig, VibeLogBackend, VibeLogLevel, VibePlatformType, VibeStoreBackend,
};

let config = VibeEngineConfig::builder()
    .platform(VibePlatformType::MacOS)
    .store_root_path("./.vibe-ready")
    .app_name("notes-app")
    .namespace("local")
    .log_backend(VibeLogBackend::DieselSqlite)
    .log_level(VibeLogLevel::Info)
    .log_write_to_store(true)
    .log_output_stdout(true)
    .log_retention_days(7)
    .log_max_rows(120_000)
    .store_backend(VibeStoreBackend::DieselSqlite)
    .encrypt(false)
    .runtime_worker_threads(4)
    .callback_threads(3)
    .queue_capacity(1024, 256)
    .priority_queue_capacity(1024)
    .build();

config.validate().expect("valid configuration");
```

Important configuration fields:

- `app_name`: the application identity used in storage paths and log user ids.
- `namespace`: a logical environment, tenant, or product namespace.
- `store_root_path`: root path for SDK-managed data.
- `log`: log backend, level, stdout behavior, persistence, and retention.
- `store`: work-store backend, encryption request, and backup strategy.
- `runtime`: Tokio worker threads, callback threads, task queue sizes, and scheduler lane capacity.

Configuration types and options:

- `VibeAppConfig`: explicit `app_name` and `namespace` values.
- `VibeLogConfig`: log backend, level, persistence, stdout output, retention days, and max rows.
- `VibeStoreConfig`: store backend, encryption request, and backup strategy.
- `VibeRuntimeConfig`: runtime worker threads, callback threads, async/sync queue capacities, and priority queue capacity.
- `VibeLogBackend`: `Noop` or `DieselSqlite`.
- `VibeStoreBackend`: `Noop` or `DieselSqlite`.
- `VibeBackupStrategy`: `Disabled` or `Manual`.
- `VibePlatformType`: platform identity. Variants: `Android`, `IOS`, `HarmonyOS`, `Windows`, `MacOS`, `Linux`, `Electron`, `Web`, `HarmonyOSPC`, `MiniWeb`, `PC`, `IPad`, `APad`, `HPad`, `Unknown`.

The builder is the recommended entry point, but a constructed `VibeEngineConfig` also exposes read-only getters that an AI agent can use for diagnostics: `store_path()`, `is_encrypt()`, `platform()`, `app_name()`, `namespace()`, `log_config()`, `store_config()`, `runtime_config()`, and `app_store_path()`.

### Runtime Execution

Use `invoke` when the caller needs a result:

```rust
use vibe_ready::{VibeEngine, VibeEngineConfig, VibeResult};

fn compute() -> VibeResult<i32> {
    let engine = VibeEngine::create(VibeEngineConfig::builder().app_name("compute").build())?;
    let answer = engine.invoke(async { 40 + 2 })?;
    engine.destroy_with_timeout(std::time::Duration::from_secs(1))?;
    Ok(answer)
}
```

Use `post` when the caller does not need a result:

```rust
engine.post(async move {
    println!("fire-and-forget work");
});
```

Use `executor()` for advanced integrations:

```rust
// In a function that returns VibeResult<()>:
let executor = engine.executor();
executor.post(async {})?;
let callbacks = executor.callback();
callbacks.execute(|| println!("callback on callback pool"));
```

`VibeCallbackExecutor` also exposes `once`, `once2`, and `once3` helpers that wrap a closure so it runs on the callback pool when invoked with one, two, or three arguments. `VibeEngine` re-exports the one-argument and two-argument shortcuts as `cb_pool_once` and `cb_pool_once2` for AI-generated bridging code that needs to hand a callback to native or FFI layers.

### Scheduler

The scheduler handles priority, delayed, and periodic work.

```rust
use std::time::Duration;
use vibe_ready::{VibeTaskPriority, VibeResult};

// In a function that receives engine: &VibeEngine and returns VibeResult<()>:
let handle = engine.post_with_priority("refresh-cache", VibeTaskPriority::High, async move {
    println!("high-priority task");
})?;

assert_eq!(handle.name(), "refresh-cache");

let delayed = engine.schedule_after("warmup", Duration::from_secs(5), |token| async move {
    if !token.is_cancelled() {
        println!("warmup after delay");
    }
})?;

let periodic = engine.schedule_every("heartbeat", Duration::from_secs(30), |token| async move {
    if token.is_cancelled() {
        return;
    }
    println!("heartbeat");
})?;

periodic.cancel();
let task_count = engine.tasks().count()?;
let snapshots = engine.tasks().list()?;

drop((handle, delayed, task_count, snapshots));
```

Scheduler types:

- `VibeTaskPriority`: `High`, `Normal`, `Low`.
- `VibeTaskKind`: `Once`, `Delayed`, `Periodic`.
- `VibeTaskState`: `Pending`, `Running`, `Completed`, `Cancelled`, `Failed`.
- `VibeTaskHandle` methods: `id()`, `name()`, `kind()`, `priority()`, `state()`, `token()`, `info()`, `cancel()`, `is_finished()`, and `join().await`.
- `VibeTaskPanel` methods: `list() -> Result<Vec<VibeTaskInfo>>`, `count() -> Result<usize>`.
- `VibeCancellationToken` methods: `cancel()`, `is_cancelled()`, and `cancelled().await`.

### Key-Value Store

`VibeKvStore` is the high-level app state API. It supports typed values, named buckets, TTL, batch operations, transactions, and listeners.

```rust
use std::time::Duration;
use vibe_ready::{VibeEngine, VibeEngineConfig, VibeKvValue, VibeResult};

fn use_store() -> VibeResult<()> {
    let engine = VibeEngine::create(VibeEngineConfig::builder().app_name("store-demo").build())?;
    let store = engine.store();

    store.set_str("name", "vibe")?;
    store.set_bool("enabled", true)?;
    store.set_i32("count", 3)?;
    store.set("score", 99_i64)?;
    store.set("ratio", 0.75_f64)?;
    store.set("payload", vec![1_u8, 2, 3])?;
    store.set("json", serde_json::json!({ "ready": true }))?;

    assert_eq!(store.get_str("name")?, Some("vibe".to_string()));
    assert_eq!(store.get("count")?, Some(VibeKvValue::I32(3)));

    store.set_with_ttl("session", "temporary", Duration::from_secs(60))?;

    engine.destroy_with_timeout(Duration::from_secs(1))?;
    Ok(())
}
```

Use buckets for logical separation:

```rust
// In a function that returns VibeResult<()>:
let settings = engine.store().bucket("settings");
settings.set("theme", "dark")?;
assert_eq!(settings.name(), "settings");
```

Use batch operations when writing or reading multiple keys:

```rust
// In a function that returns VibeResult<()>:
let store = engine.store();
store.set_many(vec![("a", 1_i32), ("b", 2_i32)])?;
let values = store.get_many(vec!["a", "b"])?;
store.remove_many(vec!["a", "b"])?;
drop(values);
```

Use inspection and maintenance helpers when an AI agent needs to introspect or clean state:

```rust
// In a function that returns VibeResult<()>:
let store = engine.store();
let exists = store.contains("name")?;
let keys = store.list_keys()?;
let removed_expired_rows = store.purge_expired()?;
drop((exists, keys, removed_expired_rows));
```

Use transactions for atomic state changes:

```rust
// In a function that returns VibeResult<()>:
let result = engine.store().transaction(|tx| {
    tx.set("current_user", "alice")?
        .set_with_ttl("session", "token", std::time::Duration::from_secs(3600))?
        .remove("stale_user")?;
    Ok("committed")
})?;

assert_eq!(result, "committed");
```

Use listeners to react to state changes:

```rust
// In a function that returns VibeResult<()>:
let store = engine.store();
let listener_id = store.on_change("user*", |change| {
    println!("{} changed in bucket {}", change.key, change.bucket);
});

store.set_str("user.name", "Alice")?;
assert!(store.off_change(listener_id));
```

Pattern matching supports:

- exact keys, for example `"user.name"`;
- all keys with `"*"`;
- one trailing wildcard, for example `"user*"`.

### Logging

Use engine logging for explicit log entries:

```rust
use vibe_ready::VibeLogLevel;

engine.insert_log(true, VibeLogLevel::Info, "startup".into(), "ready".into());
engine.set_log_listener(Some(Box::new(|info| {
    println!("log: {:?} {} {}", info.level, info.tag, info.content);
})));
```

Use exported macros for structured logs:

```rust
vibe_ready::log_i!("startup", "status", "ready");
vibe_ready::log_s!("store", "key|result", "status", "ok");
vibe_ready::log_e!("network", "reason", "timeout");
```

Available log helpers:

- `log_i!`: info log.
- `log_i_!`: info log with an extension segment.
- `log_t!`: info trace-style log.
- `log_t_!`: info trace-style log with an extension segment.
- `log_r!`: debug read-style log.
- `log_r_!`: debug read-style log with an extension segment.
- `log_s!`: debug state-style log.
- `log_s_!`: debug state-style log with an extension segment.
- `log_e!`: error log.
- `log_e_!`: error log with an extension segment.

Standard log field constants are also exported for consistent structured payloads:

- `CODE_STR`: field name for error or status codes.
- `DESC`: field name for human-readable descriptions.
- `RET_STR`: field name for return values.

### Errors

Most fallible APIs return `VibeResult<T>`, an alias for `Result<T, VibeEngineError>`.

```rust
use vibe_ready::{VibeEngineError, VibeErrorCode, VibeResult};

fn fail_with_context() -> VibeResult<()> {
    Err(VibeEngineError::from_error_code(VibeErrorCode::RuntimeError)
        .with_context("failed while starting background workers"))
}
```

Useful error APIs:

- `from_error_code(VibeErrorCode)`: construct an error from a stable error code.
- `from_success()`: construct a success sentinel value.
- `is_success()`: check whether an error represents success.
- `from_u16(u16)` / `from_u32(u32)`: construct an error from a raw numeric code.
- `code()`: stable numeric code.
- `kind()`: high-level `VibeErrorKind` category.
- `message()`: human-readable message.
- `source_message()`: optional lower-level source.
- `context()`: context breadcrumb list.
- `with_source(...)`: attach source text.
- `with_context(...)`: attach context text.

Use `err!` when you want to log an error with a backtrace and return the same error expression:

```rust
use vibe_ready::{err, VibeEngineError, VibeErrorCode};

let error = err!(VibeEngineError::from_error_code(VibeErrorCode::RuntimeError));
assert_eq!(error.code(), VibeErrorCode::RuntimeError.code());
```

### Utility Macros

`vibe-ready` also exports small utility macros used by generated projects and logging helpers:

```rust
let values = vec![1, 2, 3];
let json = vibe_ready::array_to_json_string!(values);
assert_eq!(json, "[1,2,3]");
```

- `array_to_json_string!`: serializes an array-like value to JSON.
- `obj_array_to_json_string!`: renders displayable objects as a JSON-array-like string.
- `basic_type_map_to_json_string!`: serializes a string-keyed map of basic values to JSON.
- `impl_display_json!`: implements `Display` for serializable types by rendering JSON.

### Platform Helpers

```rust
let timestamp_ms = vibe_ready::platform::now();

vibe_ready::platform::spawn(async move {
    vibe_ready::platform::sleep(std::time::Duration::from_millis(10)).await;
    println!("spawned at {}", timestamp_ms);
});
```

### Capabilities

Use `VibeCapabilities` to inspect compile-time support:

```rust
let capabilities = vibe_ready::VibeCapabilities::current();
println!("log store: {}", capabilities.log_store);
println!("work store: {}", capabilities.work_store);
```

Fields:

- `log_store`: log persistence support is compiled in.
- `work_store`: work/key-value persistence support is compiled in.
- `network`: reserved network API support flag.
- `encryption`: reserved encryption support flag.
- `wasm`: current target is WebAssembly.
- `tracing`: reserved tracing integration flag.
- `metrics`: reserved metrics integration flag.

### Status Manager

`VibeStatusManager` tracks `VibeConnectionStatus` and notifies a listener when the status changes. Most applications do not need it directly, but integrations can use it for connection-like workflows.

```rust
use vibe_ready::{VibeConnectionStatus, VibeStatusManager, VibeResult};

async fn status_flow() -> VibeResult<()> {
    let manager = VibeStatusManager::new();
    manager
        .set_connection_status_listener(Some(Box::new(|status| {
            println!("status changed: {status}");
        })))
        .await;
    manager
        .set_connection_status(VibeConnectionStatus::Connecting)
        .await?;
    manager
        .set_connection_status(VibeConnectionStatus::Connected)
        .await?;
    Ok(())
}
```

### Networking (HTTP)

Enable the `net-http` feature to get `VibeHttpClient`, an async HTTP client with
timeouts, retry/backoff, connection pooling, JSON helpers, and auth headers.

```toml
[dependencies]
vibe-ready = { version = "0.2.2", features = ["net-http"] }
```

```rust
# #[cfg(feature = "net-http")]
# async fn demo() -> vibe_ready::VibeResult<()> {
use vibe_ready::{VibeHttpClient, VibeRetryPolicy};

let client = VibeHttpClient::builder()
    .bearer_auth("token")
    .retry(VibeRetryPolicy::default())
    .build()?;

let response = client
    .get("https://api.example.com/health")
    .await?
    .error_for_status()?;
let body: serde_json::Value = response.json().await?;
# let _ = body;
# Ok(())
# }
```

You can also reach a shared client through the engine with `engine.http()`.
Both the client and per-request builders expose `bearer_auth`, `basic_auth`,
`header`, `query`, `json`, `timeout`, and `retry`.

### Advanced Database APIs

Prefer `VibeKvStore` for application state. Use `VibeDbClient` only when an integration needs lower-level row access or custom database operations.

```rust
use vibe_ready::{VibeDbClient, VibeKvValue, VibeStoreBackend};

async fn low_level_store() -> vibe_ready::VibeResult<()> {
    let client = VibeDbClient::with_backend(VibeStoreBackend::Noop);
    client
        .try_open("./.vibe-ready/work".into(), "dev:advanced".to_string(), false)
        .await?;
    client.set("key".to_string(), VibeKvValue::from("value")).await?;
    client.close().await?;
    Ok(())
}
```

Advanced row types:

- `VibeKvValue`: typed value enum for string, bool, i32, i64, f64, bytes, and JSON.
- `VibeTableKeyVal`: row representation for low-level storage APIs.
- `VibeDbErrorInfo`: database backend error details.
- `DbError`: database error category.

## Feature Flags

| Feature | Default | Purpose |
| --- | --- | --- |
| `log-diesel` | Yes | Enables Diesel SQLite log persistence. |
| `store-diesel-sqlite` | Yes | Enables Diesel SQLite work/key-value persistence. |
| `log-noop` | No | Compatibility feature for no-op log behavior. |
| `store-noop` | No | Compatibility feature for no-op store behavior. |
| `net-http` | No | Enables `VibeHttpClient` (reqwest + rustls) for outbound HTTP. |
| `net-ws` | No | Reserved for a future WebSocket client. |

No-op backends are always available at runtime through `VibeLogBackend::Noop` and `VibeStoreBackend::Noop`. They are useful for tests, examples, and generated prototypes.

## Common Recipes for AI-Generated Projects

### CLI Tool With Persistent State

Tell your AI:

```text
Use vibe-ready to create a CLI app. Store user settings in engine.store().bucket("settings"), use VibeResult for fallible commands, and call destroy_with_timeout before process exit.
```

Implementation shape:

```rust
use vibe_ready::prelude::*;

fn run() -> VibeResult<()> {
    let engine = VibeEngine::create(
        VibeEngineConfig::builder()
            .app_name("my-cli")
            .namespace("prod")
            .build(),
    )?;

    let settings = engine.store().bucket("settings");
    settings.set("first_run", false)?;

    engine.destroy_with_timeout(std::time::Duration::from_secs(2))?;
    Ok(())
}
```

### Background Worker

Tell your AI:

```text
Use vibe-ready to create a background worker. Use schedule_every for periodic sync, VibeCancellationToken for cooperative cancellation, engine.tasks() for diagnostics, and structured logs for each run.
```

Implementation shape:

```rust
use std::time::Duration;
use vibe_ready::prelude::*;

fn start_worker(engine: &VibeEngine) -> VibeResult<VibeTaskHandle> {
    engine.schedule_every("sync", Duration::from_secs(60), |token| async move {
        if token.is_cancelled() {
            return;
        }
        vibe_ready::log_i!("sync", "status", "tick");
    })
}
```

### Local-First App Prototype

Tell your AI:

```text
Use vibe-ready to build a local-first prototype. Put domain state in named VibeKvStore buckets, use transactions for multi-key changes, and use on_change listeners to update derived state.
```

Implementation shape:

```rust
use vibe_ready::prelude::*;

fn create_note(engine: &VibeEngine, id: &str, title: &str) -> VibeResult<()> {
    let notes = engine.store().bucket("notes");
    notes.transaction(|tx| {
        tx.set(format!("note:{id}:title"), title)?;
        tx.set(format!("note:{id}:updated"), vibe_ready::platform::now() as i64)?;
        Ok(())
    })
}
```

### Test-Friendly Generated Code

Tell your AI:

```text
When writing tests with vibe-ready, configure VibeLogBackend::Noop and VibeStoreBackend::Noop unless persistence behavior is under test.
```

Implementation shape:

```rust
use vibe_ready::prelude::*;

fn test_config() -> VibeEngineConfig {
    VibeEngineConfig::builder()
        .app_name("test-app")
        .namespace("tests")
        .log_backend(VibeLogBackend::Noop)
        .store_backend(VibeStoreBackend::Noop)
        .runtime_worker_threads(1)
        .callback_threads(1)
        .queue_capacity(16, 8)
        .build()
}
```

## Public API Map

Most users should import from the prelude:

```rust
use vibe_ready::prelude::*;
```

Important exported types:

- Engine and configuration: `VibeEngine`, `VibeEngineState`, `VibeEngineConfig`, `VibeEngineConfigBuilder`, `VibeEngineContext`, `VibeAppConfig`, `VibeBackupStrategy`, `VibeRuntimeConfig`, `VibeStoreConfig`, `VibeStoreBackend`.
- Runtime: `VibeEngineExecutor`, `VibeCallbackExecutor`.
- Scheduler: `VibeTaskHandle`, `VibeTaskInfo`, `VibeTaskKind`, `VibeTaskPanel`, `VibeTaskPriority`, `VibeTaskState`, `VibeCancellationToken`.
- Store: `VibeKvStore`, `VibeKvBucket`, `VibeKvTx`, `VibeKvValue`, `VibeKvChange`, `VibeKvChangeKind`, `VibeKvListenerId`.
- Database: `VibeDbClient`, `VibeTableKeyVal`, `VibeDbErrorInfo`, `DbError`.
- Logging: `VibeLogConfig`, `VibeLogBackend`, `VibeLogInfo`, `VibeLogLevel`, `VibeLogListener`, `VibeLogger`, `CODE_STR`, `DESC`, `RET_STR`.
- Errors: `VibeEngineError`, `VibeError`, `VibeErrorCode`, `VibeErrorKind`, `VibeResult`.
- Platform and status: `VibePlatformType`, `VibeConnectionStatus`, `VibeStatusManager`, `VibeCapabilities`.
- Networking (feature `net-http`): `VibeHttpClient`, `VibeHttpClientBuilder`, `VibeHttpRequest`, `VibeHttpResponse`, `VibeHttpMethod`, `VibeRetryPolicy`.

## Shutdown Pattern

Always shut down engines explicitly in examples, tests, and applications:

```rust
engine.destroy_with_timeout(std::time::Duration::from_secs(2))?;
```

Use `destroy` when you need callback-style reporting:

```rust
engine.destroy(|result| {
    if let Err(error) = result {
        eprintln!("shutdown failed: {error}");
    }
});
```

## Development

Useful commands for contributors and AI agents:

```bash
cargo fmt
RUSTDOCFLAGS="-D warnings -D missing_docs" cargo doc --no-deps
cargo test --doc
cargo test
```

## License

MIT
