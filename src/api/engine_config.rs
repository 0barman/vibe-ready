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
    pub platform_type: VibePlatformType,
    pub store_root_path: PathBuf,
    pub is_encrypt: bool,
    pub app: VibeAppConfig,
    pub log: VibeLogConfig,
    pub store: VibeStoreConfig,
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
    pub app_name: String,
    pub namespace: String,
}

/// Logging behavior used by the SDK.
#[derive(Clone, Debug)]
pub struct VibeLogConfig {
    pub backend: VibeLogBackend,
    pub level: LogLevel,
    pub write_to_store: bool,
    pub output_stdout: bool,
    pub retention_days: u32,
    pub max_rows: usize,
}

/// Log backend selected for SDK log persistence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VibeLogBackend {
    Noop,
    DieselSqlite,
}

/// Store backend selected for SDK persistence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VibeStoreBackend {
    Noop,
    DieselSqlite,
}

/// Backup behavior for SDK managed storage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VibeBackupStrategy {
    Disabled,
    Manual,
}

/// Storage behavior used by the SDK.
#[derive(Clone, Debug)]
pub struct VibeStoreConfig {
    pub backend: VibeStoreBackend,
    pub encrypt: bool,
    pub backup_strategy: VibeBackupStrategy,
}

/// Runtime sizing and queue capacity used by [`crate::VibeEngine`].
#[derive(Clone, Debug)]
pub struct VibeRuntimeConfig {
    pub worker_threads: usize,
    pub callback_threads: usize,
    pub async_queue_capacity: usize,
    pub sync_queue_capacity: usize,
    /// Capacity per priority lane used by the B9 task scheduler
    /// (high / normal / low). Each lane is sized identically.
    pub priority_queue_capacity: usize,
}

impl VibeEngineConfig {
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

    pub fn store_path(&self) -> &PathBuf {
        &self.store_root_path
    }

    pub fn is_encrypt(&self) -> bool {
        self.is_encrypt
    }

    pub fn platform(&self) -> VibePlatformType {
        self.platform_type
    }

    pub fn app_name(&self) -> &str {
        &self.app.app_name
    }

    pub fn namespace(&self) -> &str {
        &self.app.namespace
    }

    pub fn log_config(&self) -> &VibeLogConfig {
        &self.log
    }

    pub fn store_config(&self) -> &VibeStoreConfig {
        &self.store
    }

    pub fn runtime_config(&self) -> &VibeRuntimeConfig {
        &self.runtime
    }

    pub fn app_store_path(&self) -> PathBuf {
        self.store_root_path
            .join(&self.app.namespace)
            .join(&self.app.app_name)
    }

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
    pub fn platform(mut self, platform: VibePlatformType) -> Self {
        self.platform_type = platform;
        self
    }

    pub fn store_root_path(mut self, path: impl AsRef<Path>) -> Self {
        self.store_root_path = path.as_ref().to_path_buf();
        self
    }

    pub fn app_name(mut self, app_name: impl Into<String>) -> Self {
        self.app.app_name = app_name.into();
        self
    }

    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.app.namespace = namespace.into();
        self
    }

    pub fn app_config(mut self, app: VibeAppConfig) -> Self {
        self.app = app;
        self
    }

    pub fn log_config(mut self, log: VibeLogConfig) -> Self {
        self.log = log;
        self
    }

    pub fn log_backend(mut self, backend: VibeLogBackend) -> Self {
        self.log.backend = backend;
        self
    }

    pub fn log_level(mut self, level: LogLevel) -> Self {
        self.log.level = level;
        self
    }

    pub fn log_write_to_store(mut self, write_to_store: bool) -> Self {
        self.log.write_to_store = write_to_store;
        self
    }

    pub fn log_output_stdout(mut self, output_stdout: bool) -> Self {
        self.log.output_stdout = output_stdout;
        self
    }

    pub fn log_retention_days(mut self, retention_days: u32) -> Self {
        self.log.retention_days = retention_days;
        self
    }

    pub fn log_max_rows(mut self, max_rows: usize) -> Self {
        self.log.max_rows = max_rows;
        self
    }

    pub fn store_config(mut self, store: VibeStoreConfig) -> Self {
        self.store = store;
        self
    }

    pub fn store_backend(mut self, backend: VibeStoreBackend) -> Self {
        self.store.backend = backend;
        self
    }

    pub fn backup_strategy(mut self, backup_strategy: VibeBackupStrategy) -> Self {
        self.store.backup_strategy = backup_strategy;
        self
    }

    pub fn runtime_config(mut self, runtime: VibeRuntimeConfig) -> Self {
        self.runtime = runtime;
        self
    }

    pub fn runtime_worker_threads(mut self, worker_threads: usize) -> Self {
        self.runtime.worker_threads = worker_threads;
        self
    }

    pub fn callback_threads(mut self, callback_threads: usize) -> Self {
        self.runtime.callback_threads = callback_threads;
        self
    }

    pub fn queue_capacity(
        mut self,
        async_queue_capacity: usize,
        sync_queue_capacity: usize,
    ) -> Self {
        self.runtime.async_queue_capacity = async_queue_capacity;
        self.runtime.sync_queue_capacity = sync_queue_capacity;
        self
    }

    pub fn priority_queue_capacity(mut self, capacity: usize) -> Self {
        self.runtime.priority_queue_capacity = capacity;
        self
    }

    pub fn encrypt(mut self, encrypt: bool) -> Self {
        self.store.encrypt = encrypt;
        self
    }

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
    if value.trim().is_empty() {
        return Err(config_error(format!("{field} must not be empty")));
    }
    if value.contains('/') || value.contains('\\') || value.contains("..") {
        return Err(config_error(format!(
            "{field} must not contain path separators or '..'"
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
