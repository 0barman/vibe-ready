//! Project foundations for vibe-coding applications.
//!
//! `vibe-ready` provides a small runtime layer for applications that need a
//! ready-to-use engine, async task execution, scheduled work, structured
//! logging, and a key-value store without assembling those pieces from scratch.
//! Most applications start by building a [`VibeEngineConfig`], creating a
//! [`VibeEngine`], and then using the engine for tasks, callbacks, and storage.
//!
//! # Examples
//!
//! ```no_run
//! use vibe_ready::{VibeEngine, VibeEngineConfig, VibePlatformType, VibeResult};
//!
//! fn main() -> VibeResult<()> {
//!     let config = VibeEngineConfig::builder()
//!         .platform(VibePlatformType::MacOS)
//!         .app_name("demo-app")
//!         .namespace("examples")
//!         .build();
//!
//!     let engine = VibeEngine::create(config)?;
//!     engine.store().set_str("status", "ready")?;
//!     engine.destroy_with_timeout(std::time::Duration::from_secs(2))?;
//!     Ok(())
//! }
//! ```

pub(crate) mod api;
pub(crate) mod log;
pub(crate) mod net;
pub mod platform;
pub(crate) mod status;
pub(crate) mod store;
pub(crate) mod utils;

/// Build-time capabilities available in the current crate build.
pub use api::capabilities::VibeCapabilities;
/// Connection state reported by the SDK runtime.
pub use api::connection_status::VibeConnectionStatus;
/// Main entry point for creating and driving a vibe-ready runtime.
pub use api::engine::VibeEngine;
/// Lifecycle state of a [`VibeEngine`].
pub use api::engine::VibeEngineState;
/// Application identity used to isolate SDK data on disk.
pub use api::engine_config::VibeAppConfig;
/// Backup behavior for SDK managed storage.
pub use api::engine_config::VibeBackupStrategy;
/// Configuration used to create a [`VibeEngine`].
pub use api::engine_config::VibeEngineConfig;
/// Builder for [`VibeEngineConfig`].
pub use api::engine_config::VibeEngineConfigBuilder;
/// Log backend selected for SDK log persistence.
pub use api::engine_config::VibeLogBackend;
/// Logging behavior used by the SDK.
pub use api::engine_config::VibeLogConfig;
/// Runtime sizing and queue capacity used by [`VibeEngine`].
pub use api::engine_config::VibeRuntimeConfig;
/// Store backend selected for SDK persistence.
pub use api::engine_config::VibeStoreBackend;
/// Storage behavior used by the SDK.
pub use api::engine_config::VibeStoreConfig;
/// Low-level context shared by engine services.
pub use api::engine_context::VibeEngineContext;
/// Error type returned by vibe-ready operations.
pub use api::engine_error::VibeEngineError;
/// Stable error code used by [`VibeError`].
pub use api::engine_error::VibeEngineErrorCode as VibeErrorCode;
/// High-level error category returned by [`VibeEngineError`].
pub use api::engine_error::VibeErrorKind;
/// Callback executor returned by [`VibeEngineExecutor::callback`].
pub use api::engine_executor::VibeCallbackExecutor;
/// Task executor used internally by [`VibeEngine`] and exposed for advanced integrations.
pub use api::engine_executor::VibeEngineExecutor;
/// Platform identifier used by [`VibeEngineConfig`].
pub use api::platform_type::VibePlatformType;
/// Cooperative cancellation token consumed by scheduled tasks.
pub use api::scheduler::VibeCancellationToken;
/// Handle returned by `VibeEngine::schedule_after` / `schedule_every` / `post_with_priority`.
pub use api::scheduler::VibeTaskHandle;
/// Snapshot of a scheduler-tracked task.
pub use api::scheduler::VibeTaskInfo;
/// Origin category of a scheduler-tracked task.
pub use api::scheduler::VibeTaskKind;
/// Diagnostic panel listing live scheduler tasks.
pub use api::scheduler::VibeTaskPanel;
/// Priority lane used by the task scheduler.
pub use api::scheduler::VibeTaskPriority;
/// Lifecycle state of a scheduler-tracked task.
pub use api::scheduler::VibeTaskState;
/// Log listener callback type.
pub use log::log_def::LogListener as VibeLogListener;
/// Log entry delivered to log listeners.
pub use log::log_def::VibeLogInfo;
/// Standard log field for error codes.
pub use log::log_def::CODE_STR;
/// Standard log field for descriptions.
pub use log::log_def::DESC;
/// Standard log field for return values.
pub use log::log_def::RET_STR;
/// Log severity used by the SDK.
pub use log::log_level::LogLevel as VibeLogLevel;
/// Low-level logger used by the engine log subsystem.
pub use log::logger::VibeLogger;
/// HTTP client foundation for outbound requests (requires `net-http`).
#[cfg(feature = "net-http")]
pub use net::VibeHttpClient;
/// Builder for [`VibeHttpClient`] (requires `net-http`).
#[cfg(feature = "net-http")]
pub use net::VibeHttpClientBuilder;
/// HTTP method used by [`VibeHttpClient`] (requires `net-http`).
#[cfg(feature = "net-http")]
pub use net::VibeHttpMethod;
/// Configurable HTTP request (requires `net-http`).
#[cfg(feature = "net-http")]
pub use net::VibeHttpRequest;
/// HTTP response (requires `net-http`).
#[cfg(feature = "net-http")]
pub use net::VibeHttpResponse;
/// Retry and backoff policy for HTTP requests (requires `net-http`).
#[cfg(feature = "net-http")]
pub use net::VibeRetryPolicy;
/// Runtime status manager exposed for advanced integrations.
pub use status::status_manager::VibeStatusManager;
/// Database client exposed for advanced integrations.
pub use store::db::db_client::VibeDbClient;
/// Database error category used by storage backends.
pub use store::db::enums::db_error::DbError;
/// Database error details exposed for advanced integrations.
pub use store::db::enums::db_error::VibeDbErrorInfo;
/// High-level value accepted by the SDK key-value store.
pub use store::db::tables::key_val::VibeKvValue;
/// Key-value row returned by advanced database APIs.
pub use store::db::tables::key_val::VibeTableKeyVal;
/// Bucket-scoped view of [`VibeKvStore`].
pub use store::kv_store::VibeKvBucket;
/// Change notification dispatched to KV listeners.
pub use store::kv_store::VibeKvChange;
/// Variant of [`VibeKvChange`] indicating whether a key was set or removed.
pub use store::kv_store::VibeKvChangeKind;
/// Cancellation handle returned by `VibeKvStore::on_change`.
pub use store::kv_store::VibeKvListenerId;
/// High-level key-value store facade.
pub use store::kv_store::VibeKvStore;
/// Buffered transaction handle used inside `VibeKvStore::transaction`.
pub use store::kv_store::VibeKvTx;

#[doc(hidden)]
pub use log::logger_macro::on_log as __vibe_internal_log_on_log;

/// Result alias used by vibe-ready public APIs.
pub type VibeResult<T> = Result<T, VibeEngineError>;

/// Short alias for [`VibeEngineError`].
pub type VibeError = VibeEngineError;

/// Common imports for applications using vibe-ready.
pub mod prelude {
    pub use crate::{
        DbError, VibeAppConfig, VibeBackupStrategy, VibeCallbackExecutor, VibeCancellationToken,
        VibeCapabilities, VibeConnectionStatus, VibeEngine, VibeEngineConfig,
        VibeEngineConfigBuilder, VibeEngineContext, VibeEngineError, VibeEngineExecutor,
        VibeEngineState, VibeError, VibeErrorCode, VibeErrorKind, VibeKvBucket, VibeKvChange,
        VibeKvChangeKind, VibeKvListenerId, VibeKvStore, VibeKvTx, VibeKvValue, VibeLogBackend,
        VibeLogConfig, VibeLogInfo, VibeLogLevel, VibeLogListener, VibeLogger, VibePlatformType,
        VibeResult, VibeRuntimeConfig, VibeStoreBackend, VibeStoreConfig, VibeTableKeyVal,
        VibeTaskHandle, VibeTaskInfo, VibeTaskKind, VibeTaskPanel, VibeTaskPriority, VibeTaskState,
    };

    #[cfg(feature = "net-http")]
    pub use crate::{
        VibeHttpClient, VibeHttpClientBuilder, VibeHttpMethod, VibeHttpRequest, VibeHttpResponse,
        VibeRetryPolicy,
    };
}
