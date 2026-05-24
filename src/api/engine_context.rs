use crate::api::engine_config::VibeEngineConfig;
use crate::api::engine_error::{VibeEngineError, VibeEngineErrorCode};
use crate::log::logger::VibeLogger;
use crate::status::status_manager::VibeStatusManager;
use crate::store::db::db_client::VibeDbClient;
use std::sync::Arc;

/// Low-level context shared by engine services.
///
/// Most applications should use [`crate::VibeEngine`] instead. This type is
/// exposed for advanced integrations that need direct access to the database
/// or logging clients initialized by the engine.
pub struct VibeEngineContext {
    status_manager: VibeStatusManager,
    engine_config: VibeEngineConfig,
    db_client: VibeDbClient,
    log_db_client: Arc<VibeLogger>,
}

impl VibeEngineContext {
    /// Creates a context from validated engine configuration.
    ///
    /// # Returns
    ///
    /// `Ok(VibeEngineContext)` when log and work stores open successfully, or
    /// [`VibeEngineError`] when initialization fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use vibe_ready::{VibeEngineConfig, VibeEngineContext, VibeResult};
    ///
    /// # fn demo() -> VibeResult<()> {
    /// let context = VibeEngineContext::new(VibeEngineConfig::builder().build())?;
    /// futures::executor::block_on(context.close())?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(config: VibeEngineConfig) -> Result<Self, VibeEngineError> {
        let log_config = config.log_config();
        let store_config = config.store_config();
        let is_encrypt: bool = store_config.encrypt;
        // log db
        let mut log_store_path = config.app_store_path();
        log_store_path.push("log");
        //
        let db_log_ret = VibeLogger::try_new(
            log_config.backend,
            log_store_path,
            is_encrypt,
            format!("{}:{}", config.namespace(), config.app_name()),
            log_config.level,
            log_config.write_to_store,
            log_config.output_stdout,
            log_config.max_rows,
        );

        let db_log = match db_log_ret {
            Ok(l) => l,
            Err(_) => {
                return Err(VibeEngineError::from_error_code(
                    VibeEngineErrorCode::LogDatabaseOpenFailed,
                ))
            }
        };

        let db_client = VibeDbClient::with_backend(store_config.backend);
        let mut work_store_path = config.app_store_path();
        work_store_path.push("work");
        let work_user_id = format!("{}:{}", config.namespace(), config.app_name());
        futures::executor::block_on(db_client.try_open(work_store_path, work_user_id, is_encrypt))
            .map_err(|error| {
                VibeEngineError::from_error_code(VibeEngineErrorCode::WorkDatabaseOpenFailed)
                    .with_source(error.to_string())
            })?;
        Ok(VibeEngineContext {
            status_manager: VibeStatusManager::new(),
            engine_config: config,
            db_client,
            log_db_client: Arc::new(db_log),
        })
    }
}

impl VibeEngineContext {
    /// Returns the initialized log client.
    ///
    /// # Returns
    ///
    /// A shared reference to the internal logger used by [`crate::VibeEngine`].
    pub fn log_db_client(&self) -> &Arc<VibeLogger> {
        &self.log_db_client
    }

    /// Returns the initialized work-store database client.
    ///
    /// # Returns
    ///
    /// A reference to the [`crate::VibeDbClient`] used by high-level storage APIs.
    pub fn db_client(&self) -> &VibeDbClient {
        &self.db_client
    }

    /// Returns the validated engine configuration used to create this context.
    ///
    /// # Returns
    ///
    /// A reference to the immutable [`VibeEngineConfig`] stored by the context.
    pub fn engine_config(&self) -> &VibeEngineConfig {
        &self.engine_config
    }

    /// Closes listeners, work-store resources, and log-store resources.
    ///
    /// # Returns
    ///
    /// `Ok(())` when all resources close, or [`VibeEngineError`] on close failure.
    pub async fn close(&self) -> Result<(), VibeEngineError> {
        self.status_manager
            .set_connection_status_listener(None)
            .await;
        self.db_client.close().await?;
        self.log_db_client.close().map_err(|_| {
            VibeEngineError::from_error_code(VibeEngineErrorCode::LogDatabaseOpenFailed)
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod strict_tests {
    use super::*;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test/unit/api/engine_context_tests.rs"
    ));
}
