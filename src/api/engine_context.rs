use crate::api::engine_config::VibeEngineConfig;
use crate::api::engine_error::{VibeEngineError, VibeEngineErrorCode};
use crate::log::logger::VibeLogger;
use crate::status::status_manager::VibeStatusManager;
use crate::store::db::db_client::VibeDbClient;
use std::sync::Arc;

pub struct VibeEngineContext {
    status_manager: VibeStatusManager,
    engine_config: VibeEngineConfig,
    db_client: VibeDbClient,
    log_db_client: Arc<VibeLogger>,
}

impl VibeEngineContext {
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
    pub fn log_db_client(&self) -> &Arc<VibeLogger> {
        &self.log_db_client
    }

    pub fn db_client(&self) -> &VibeDbClient {
        &self.db_client
    }

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
