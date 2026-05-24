use crate::api::engine_error::{VibeEngineError, VibeEngineErrorCode};
use crate::api::platform_type::VibePlatformType;
use crate::log::log_level::LogLevel;
use crate::utils::global_ref::{ENGINE_CHANNEL_BUFFER_SIZE, ENGINE_SYNC_CHANNEL_BUFFER_SIZE};
use std::path::{Path, PathBuf};
use std::{fmt, fmt::Formatter};

const DEFAULT_APP_NAME: &str = "vibe-ready-app";
const DEFAULT_NAMESPACE: &str = "default";
const DEFAULT_RUNTIME_WORKER_THREADS: usize = 4;
const DEFAULT_CALLBACK_THREADS: usize = 3;
const DEFAULT_LOG_RETENTION_DAYS: u32 = 7;
const DEFAULT_LOG_MAX_ROWS: usize = 120_000;
const DEFAULT_PRIORITY_QUEUE_CAPACITY: usize = 1024;

/// Configuration used when creating a [`crate::VibeEngine`].
#[derive(Clone, Debug)]
pub struct VibeEngineConfig {
    /// Platform identifier used by integrations and logs.
    pub platform_type: VibePlatformType,
    /// Root directory where vibe-ready stores app data.
    pub store_root_path: PathBuf,
    /// Whether persistent stores should use encryption when supported.
    pub is_encrypt: bool,
    /// Application identity used for namespacing local data.
    pub app: VibeAppConfig,
    /// Logging backend and retention configuration.
    pub log: VibeLogConfig,
    /// Work-store backend and storage configuration.
    pub store: VibeStoreConfig,
    /// Runtime worker and queue configuration.
    pub runtime: VibeRuntimeConfig,
}

/// Builder for [`VibeEngineConfig`].
#[derive(Clone, Debug)]
pub struct VibeEngineConfigBuilder {
    platform_type: VibePlatformType,
    store_root_path: PathBuf,
    app: VibeAppConfig,
    log: VibeLogConfig,
    store: VibeStoreConfig,
    runtime: VibeRuntimeConfig,
}

/// Application identity used to isolate SDK data on disk.
#[derive(Clone, Debug)]
pub struct VibeAppConfig {
    /// Application name used as the final data-directory segment.
    pub app_name: String,
    /// Namespace used to separate environments, tenants, or products.
    pub namespace: String,
}

/// Logging behavior used by the SDK.
#[derive(Clone, Debug)]
pub struct VibeLogConfig {
    /// Backend used to persist log entries.
    pub backend: VibeLogBackend,
    /// Minimum level emitted by the logger.
    pub level: LogLevel,
    /// Whether logs should be written to the selected store backend.
    pub write_to_store: bool,
    /// Whether logs should also be printed to stdout.
    pub output_stdout: bool,
    /// Number of days log data should be retained by supported backends.
    pub retention_days: u32,
    /// Maximum rows retained by supported log backends.
    pub max_rows: usize,
}

/// Log backend selected for SDK log persistence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VibeLogBackend {
    /// Disable persistent logging while keeping in-process callbacks available.
    Noop,
    /// Persist logs through Diesel using SQLite.
    DieselSqlite,
}

/// Store backend selected for SDK persistence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VibeStoreBackend {
    /// Disable persistent key-value storage.
    Noop,
    /// Persist key-value data through Diesel using SQLite.
    DieselSqlite,
}

/// Backup behavior for SDK managed storage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VibeBackupStrategy {
    /// Do not create SDK-managed backups.
    Disabled,
    /// Reserve backup operations for explicit application control.
    Manual,
}

/// Storage behavior used by the SDK.
#[derive(Clone, Debug)]
pub struct VibeStoreConfig {
    /// Backend used for work/key-value persistence.
    pub backend: VibeStoreBackend,
    /// Whether storage encryption should be requested when supported.
    pub encrypt: bool,
    /// Backup behavior for SDK-managed storage.
    pub backup_strategy: VibeBackupStrategy,
}

/// Runtime sizing and queue capacity used by [`crate::VibeEngine`].
#[derive(Clone, Debug)]
pub struct VibeRuntimeConfig {
    /// Number of Tokio worker threads for the engine-owned runtime.
    pub worker_threads: usize,
    /// Number of threads used for callback dispatch.
    pub callback_threads: usize,
    /// Capacity of the fire-and-forget async task queue.
    pub async_queue_capacity: usize,
    /// Capacity of the synchronous invoke queue.
    pub sync_queue_capacity: usize,
    /// Capacity per priority lane used by the B9 task scheduler
    /// (high / normal / low). Each lane is sized identically.
    pub priority_queue_capacity: usize,
}

impl VibeEngineConfig {
    /// Starts building a [`VibeEngineConfig`] with production-ready defaults.
    ///
    /// # Returns
    ///
    /// A [`VibeEngineConfigBuilder`] that can be customized before calling
    /// [`VibeEngineConfigBuilder::build`].
    ///
    /// # Examples
    ///
    /// ```
    /// use vibe_ready::{VibeEngineConfig, VibePlatformType};
    ///
    /// let config = VibeEngineConfig::builder()
    ///     .platform(VibePlatformType::MacOS)
    ///     .app_name("demo")
    ///     .namespace("examples")
    ///     .build();
    /// assert_eq!(config.app_name(), "demo");
    /// ```
    pub fn builder() -> VibeEngineConfigBuilder {
        VibeEngineConfigBuilder {
            platform_type: Default::default(),
            store_root_path: default_store_root_path(),
            app: VibeAppConfig::default(),
            log: VibeLogConfig::default(),
            store: VibeStoreConfig::default(),
            runtime: VibeRuntimeConfig::default(),
        }
    }

    /// Returns the configured root directory for SDK data.
    ///
    /// # Returns
    ///
    /// A borrowed [`PathBuf`] pointing to the storage root.
    pub fn store_path(&self) -> &PathBuf {
        &self.store_root_path
    }

    /// Returns whether storage encryption was requested.
    ///
    /// # Returns
    ///
    /// `true` when encryption is enabled in the store configuration.
    pub fn is_encrypt(&self) -> bool {
        self.is_encrypt
    }

    /// Returns the configured platform identifier.
    ///
    /// # Returns
    ///
    /// The [`VibePlatformType`] stored in this configuration.
    pub fn platform(&self) -> VibePlatformType {
        self.platform_type
    }

    /// Returns the application name used for data isolation.
    ///
    /// # Returns
    ///
    /// A borrowed application name string.
    pub fn app_name(&self) -> &str {
        &self.app.app_name
    }

    /// Returns the namespace used for data isolation.
    ///
    /// # Returns
    ///
    /// A borrowed namespace string.
    pub fn namespace(&self) -> &str {
        &self.app.namespace
    }

    /// Returns the logging configuration.
    ///
    /// # Returns
    ///
    /// A borrowed [`VibeLogConfig`].
    pub fn log_config(&self) -> &VibeLogConfig {
        &self.log
    }

    /// Returns the storage configuration.
    ///
    /// # Returns
    ///
    /// A borrowed [`VibeStoreConfig`].
    pub fn store_config(&self) -> &VibeStoreConfig {
        &self.store
    }

    /// Returns the runtime configuration.
    ///
    /// # Returns
    ///
    /// A borrowed [`VibeRuntimeConfig`].
    pub fn runtime_config(&self) -> &VibeRuntimeConfig {
        &self.runtime
    }

    /// Builds the app-specific storage directory path.
    ///
    /// # Returns
    ///
    /// `store_root_path / namespace / app_name` as a [`PathBuf`].
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::PathBuf;
    /// use vibe_ready::VibeEngineConfig;
    ///
    /// let config = VibeEngineConfig::builder()
    ///     .store_root_path("/tmp/vibe-ready")
    ///     .namespace("dev")
    ///     .app_name("app")
    ///     .build();
    /// assert_eq!(config.app_store_path(), PathBuf::from("/tmp/vibe-ready/dev/app"));
    /// ```
    pub fn app_store_path(&self) -> PathBuf {
        self.store_root_path
            .join(&self.app.namespace)
            .join(&self.app.app_name)
    }

    /// Validates identifiers, backend availability, and runtime capacities.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the configuration can be used to create an engine, or
    /// [`VibeEngineError`] with configuration context when invalid.
    pub fn validate(&self) -> Result<(), VibeEngineError> {
        validate_identifier("app_name", &self.app.app_name)?;
        validate_identifier("namespace", &self.app.namespace)?;

        if self.store_root_path.as_os_str().is_empty() {
            return Err(config_error("store_root_path must not be empty"));
        }
        validate_log_backend(self.log.backend)?;
        validate_store_backend(self.store.backend)?;
        if self.log.retention_days == 0 {
            return Err(config_error("log.retention_days must be greater than zero"));
        }
        if self.log.max_rows == 0 {
            return Err(config_error("log.max_rows must be greater than zero"));
        }
        if self.runtime.worker_threads == 0 {
            return Err(config_error(
                "runtime.worker_threads must be greater than zero",
            ));
        }
        if self.runtime.callback_threads == 0 {
            return Err(config_error(
                "runtime.callback_threads must be greater than zero",
            ));
        }
        if self.runtime.async_queue_capacity == 0 {
            return Err(config_error(
                "runtime.async_queue_capacity must be greater than zero",
            ));
        }
        if self.runtime.sync_queue_capacity == 0 {
            return Err(config_error(
                "runtime.sync_queue_capacity must be greater than zero",
            ));
        }
        if self.runtime.priority_queue_capacity == 0 {
            return Err(config_error(
                "runtime.priority_queue_capacity must be greater than zero",
            ));
        }

        Ok(())
    }
}

impl fmt::Display for VibeEngineConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "VibeEngineConfig {{ app_name: {}, namespace: {}, platform: {}, store_root_path: {}, is_encrypt: {}, log_backend: {:?}, log_level: {:?}, store_backend: {:?}, worker_threads: {}, callback_threads: {}, async_queue_capacity: {}, sync_queue_capacity: {} }}",
            self.app.app_name,
            self.app.namespace,
            self.platform_type.to_i32(),
            self.store_root_path.display(),
            self.is_encrypt,
            self.log.backend,
            self.log.level,
            self.store.backend,
            self.runtime.worker_threads,
            self.runtime.callback_threads,
            self.runtime.async_queue_capacity,
            self.runtime.sync_queue_capacity,
        )
    }
}

impl VibeEngineConfigBuilder {
    /// Sets the host platform identifier.
    ///
    /// # Returns
    ///
    /// The updated builder.
    pub fn platform(mut self, platform: VibePlatformType) -> Self {
        self.platform_type = platform;
        self
    }

    /// Sets the root directory used for SDK data.
    ///
    /// # Returns
    ///
    /// The updated builder.
    pub fn store_root_path(mut self, path: impl AsRef<Path>) -> Self {
        self.store_root_path = path.as_ref().to_path_buf();
        self
    }

    /// Sets the application name.
    ///
    /// # Returns
    ///
    /// The updated builder.
    pub fn app_name(mut self, app_name: impl Into<String>) -> Self {
        self.app.app_name = app_name.into();
        self
    }

    /// Sets the namespace used to isolate data.
    ///
    /// # Returns
    ///
    /// The updated builder.
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.app.namespace = namespace.into();
        self
    }

    /// Replaces the full application configuration.
    ///
    /// # Returns
    ///
    /// The updated builder.
    pub fn app_config(mut self, app: VibeAppConfig) -> Self {
        self.app = app;
        self
    }

    /// Replaces the full logging configuration.
    ///
    /// # Returns
    ///
    /// The updated builder.
    pub fn log_config(mut self, log: VibeLogConfig) -> Self {
        self.log = log;
        self
    }

    /// Sets the logging backend.
    ///
    /// # Returns
    ///
    /// The updated builder.
    pub fn log_backend(mut self, backend: VibeLogBackend) -> Self {
        self.log.backend = backend;
        self
    }

    /// Sets the logging level.
    ///
    /// # Returns
    ///
    /// The updated builder.
    pub fn log_level(mut self, level: LogLevel) -> Self {
        self.log.level = level;
        self
    }

    /// Enables or disables log persistence.
    ///
    /// # Returns
    ///
    /// The updated builder.
    pub fn log_write_to_store(mut self, write_to_store: bool) -> Self {
        self.log.write_to_store = write_to_store;
        self
    }

    /// Enables or disables stdout logging.
    ///
    /// # Returns
    ///
    /// The updated builder.
    pub fn log_output_stdout(mut self, output_stdout: bool) -> Self {
        self.log.output_stdout = output_stdout;
        self
    }

    /// Sets log retention in days for supported backends.
    ///
    /// # Returns
    ///
    /// The updated builder.
    pub fn log_retention_days(mut self, retention_days: u32) -> Self {
        self.log.retention_days = retention_days;
        self
    }

    /// Sets the maximum retained log rows for supported backends.
    ///
    /// # Returns
    ///
    /// The updated builder.
    pub fn log_max_rows(mut self, max_rows: usize) -> Self {
        self.log.max_rows = max_rows;
        self
    }

    /// Replaces the full storage configuration.
    ///
    /// # Returns
    ///
    /// The updated builder.
    pub fn store_config(mut self, store: VibeStoreConfig) -> Self {
        self.store = store;
        self
    }

    /// Sets the key-value store backend.
    ///
    /// # Returns
    ///
    /// The updated builder.
    pub fn store_backend(mut self, backend: VibeStoreBackend) -> Self {
        self.store.backend = backend;
        self
    }

    /// Sets the backup strategy for SDK-managed storage.
    ///
    /// # Returns
    ///
    /// The updated builder.
    pub fn backup_strategy(mut self, backup_strategy: VibeBackupStrategy) -> Self {
        self.store.backup_strategy = backup_strategy;
        self
    }

    /// Replaces the full runtime configuration.
    ///
    /// # Returns
    ///
    /// The updated builder.
    pub fn runtime_config(mut self, runtime: VibeRuntimeConfig) -> Self {
        self.runtime = runtime;
        self
    }

    /// Sets the Tokio worker-thread count for an engine-owned runtime.
    ///
    /// # Returns
    ///
    /// The updated builder.
    pub fn runtime_worker_threads(mut self, worker_threads: usize) -> Self {
        self.runtime.worker_threads = worker_threads;
        self
    }

    /// Sets the callback thread-pool size.
    ///
    /// # Returns
    ///
    /// The updated builder.
    pub fn callback_threads(mut self, callback_threads: usize) -> Self {
        self.runtime.callback_threads = callback_threads;
        self
    }

    /// Sets async and sync queue capacities.
    ///
    /// # Returns
    ///
    /// The updated builder.
    pub fn queue_capacity(
        mut self,
        async_queue_capacity: usize,
        sync_queue_capacity: usize,
    ) -> Self {
        self.runtime.async_queue_capacity = async_queue_capacity;
        self.runtime.sync_queue_capacity = sync_queue_capacity;
        self
    }

    /// Sets the per-lane priority queue capacity.
    ///
    /// # Returns
    ///
    /// The updated builder.
    pub fn priority_queue_capacity(mut self, capacity: usize) -> Self {
        self.runtime.priority_queue_capacity = capacity;
        self
    }

    /// Enables or disables storage encryption when supported by the backend.
    ///
    /// # Returns
    ///
    /// The updated builder.
    pub fn encrypt(mut self, encrypt: bool) -> Self {
        self.store.encrypt = encrypt;
        self
    }

    /// Finalizes the builder into a [`VibeEngineConfig`].
    ///
    /// # Returns
    ///
    /// The completed configuration. Call [`VibeEngineConfig::validate`] or
    /// [`crate::VibeEngine::create`] to validate it.
    pub fn build(self) -> VibeEngineConfig {
        VibeEngineConfig {
            platform_type: self.platform_type,
            store_root_path: self.store_root_path,
            is_encrypt: self.store.encrypt,
            app: self.app,
            log: self.log,
            store: self.store,
            runtime: self.runtime,
        }
    }
}

impl Default for VibeAppConfig {
    fn default() -> Self {
        Self {
            app_name: DEFAULT_APP_NAME.to_string(),
            namespace: DEFAULT_NAMESPACE.to_string(),
        }
    }
}

impl Default for VibeLogConfig {
    fn default() -> Self {
        Self {
            backend: default_log_backend(),
            level: LogLevel::Info,
            write_to_store: true,
            output_stdout: true,
            retention_days: DEFAULT_LOG_RETENTION_DAYS,
            max_rows: DEFAULT_LOG_MAX_ROWS,
        }
    }
}

impl Default for VibeLogBackend {
    fn default() -> Self {
        default_log_backend()
    }
}

impl Default for VibeStoreBackend {
    fn default() -> Self {
        default_store_backend()
    }
}

impl Default for VibeStoreConfig {
    fn default() -> Self {
        Self {
            backend: default_store_backend(),
            encrypt: false,
            backup_strategy: VibeBackupStrategy::Disabled,
        }
    }
}

impl Default for VibeRuntimeConfig {
    fn default() -> Self {
        Self {
            worker_threads: DEFAULT_RUNTIME_WORKER_THREADS,
            callback_threads: DEFAULT_CALLBACK_THREADS,
            async_queue_capacity: ENGINE_CHANNEL_BUFFER_SIZE,
            sync_queue_capacity: ENGINE_SYNC_CHANNEL_BUFFER_SIZE,
            priority_queue_capacity: DEFAULT_PRIORITY_QUEUE_CAPACITY,
        }
    }
}

fn default_store_root_path() -> PathBuf {
    std::env::var_os("VIBE_READY_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".vibe-ready")))
        .unwrap_or_else(|| std::env::temp_dir().join("vibe-ready"))
}

fn default_log_backend() -> VibeLogBackend {
    if cfg!(feature = "log-diesel") {
        VibeLogBackend::DieselSqlite
    } else {
        VibeLogBackend::Noop
    }
}

fn default_store_backend() -> VibeStoreBackend {
    if cfg!(feature = "store-diesel-sqlite") {
        VibeStoreBackend::DieselSqlite
    } else {
        VibeStoreBackend::Noop
    }
}

fn validate_identifier(field: &str, value: &str) -> Result<(), VibeEngineError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(config_error(format!("{field} must not be empty")));
    }
    if trimmed == "." || value.contains('/') || value.contains('\\') || value.contains("..") {
        return Err(config_error(format!(
            "{field} must not contain path separators, '.', or '..'"
        )));
    }
    Ok(())
}

fn validate_log_backend(backend: VibeLogBackend) -> Result<(), VibeEngineError> {
    match backend {
        VibeLogBackend::Noop => Ok(()),
        VibeLogBackend::DieselSqlite if cfg!(feature = "log-diesel") => Ok(()),
        _ => Err(config_error(format!(
            "log.backend {:?} is not enabled by current feature set",
            backend
        ))),
    }
}

fn validate_store_backend(backend: VibeStoreBackend) -> Result<(), VibeEngineError> {
    match backend {
        VibeStoreBackend::Noop => Ok(()),
        VibeStoreBackend::DieselSqlite if cfg!(feature = "store-diesel-sqlite") => Ok(()),
        _ => Err(config_error(format!(
            "store.backend {:?} is not enabled by current feature set",
            backend
        ))),
    }
}

fn config_error(message: impl Into<String>) -> VibeEngineError {
    VibeEngineError::from_error_code_msg(VibeEngineErrorCode::ConfigError, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid_and_uses_app_scoped_path() -> Result<(), VibeEngineError> {
        let config = VibeEngineConfig::builder().build();

        config.validate()?;
        assert_eq!(config.app_name(), DEFAULT_APP_NAME);
        assert_eq!(config.namespace(), DEFAULT_NAMESPACE);
        assert!(config.app_store_path().ends_with(DEFAULT_APP_NAME));
        Ok(())
    }

    #[test]
    fn validate_rejects_invalid_namespace_and_queue_capacity() {
        let invalid_namespace = VibeEngineConfig::builder().namespace("../bad").build();
        assert_eq!(
            invalid_namespace.validate().unwrap_err().code(),
            VibeEngineErrorCode::ConfigError.code()
        );

        let invalid_queue = VibeEngineConfig::builder().queue_capacity(0, 1).build();
        assert_eq!(
            invalid_queue.validate().unwrap_err().code(),
            VibeEngineErrorCode::ConfigError.code()
        );
    }

    #[test]
    fn validate_accepts_noop_backends() -> Result<(), VibeEngineError> {
        let config = VibeEngineConfig::builder()
            .log_backend(VibeLogBackend::Noop)
            .store_backend(VibeStoreBackend::Noop)
            .build();

        config.validate()
    }

    #[cfg(not(feature = "log-diesel"))]
    #[test]
    fn validate_rejects_diesel_log_backend_when_feature_is_disabled() {
        let config = VibeEngineConfig::builder()
            .log_backend(VibeLogBackend::DieselSqlite)
            .build();

        assert_eq!(
            config.validate().unwrap_err().code(),
            VibeEngineErrorCode::ConfigError.code()
        );
    }

    #[cfg(not(feature = "store-diesel-sqlite"))]
    #[test]
    fn validate_rejects_diesel_store_backend_when_feature_is_disabled() {
        let config = VibeEngineConfig::builder()
            .store_backend(VibeStoreBackend::DieselSqlite)
            .build();

        assert_eq!(
            config.validate().unwrap_err().code(),
            VibeEngineErrorCode::ConfigError.code()
        );
    }
}

#[cfg(test)]
mod strict_tests {
    use super::*;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test/unit/api/engine_config_tests.rs"
    ));
}
