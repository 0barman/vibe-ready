use crate::api::engine_config::VibeStoreBackend;
#[cfg(feature = "store-diesel-sqlite")]
pub use crate::store::db::db_impl::db_diesel::DbKvOp;
#[cfg(feature = "store-diesel-sqlite")]
use crate::store::db::db_impl::db_worker_diesel::VibeDbWorkerDiesel;
pub use crate::store::db::db_impl::db_worker_noop::start_worker_loop;
use crate::store::db::db_impl::db_worker_noop::VibeDbWorkerNoop;
use crate::store::db::tables::key_val::VibeTableKeyVal;
use std::path::PathBuf;

pub type DbError = crate::store::db::enums::db_error::DbError;

/// Backend-agnostic transactional op accepted by [`DbWorker::transaction`].
#[cfg(not(feature = "store-diesel-sqlite"))]
#[derive(Debug, Clone)]
pub enum DbKvOp {
    Set(VibeTableKeyVal),
    Remove {
        user_id: String,
        bucket: String,
        key: String,
    },
}

pub enum DbWorker {
    Noop(VibeDbWorkerNoop),
    #[cfg(feature = "store-diesel-sqlite")]
    DieselSqlite(VibeDbWorkerDiesel),
    #[cfg(not(feature = "store-diesel-sqlite"))]
    Unsupported,
}

impl DbWorker {
    pub fn with_backend(backend: VibeStoreBackend) -> Self {
        match backend {
            VibeStoreBackend::Noop => Self::Noop(VibeDbWorkerNoop::new()),
            VibeStoreBackend::DieselSqlite => {
                #[cfg(feature = "store-diesel-sqlite")]
                {
                    Self::DieselSqlite(VibeDbWorkerDiesel::new())
                }
                #[cfg(not(feature = "store-diesel-sqlite"))]
                {
                    Self::Unsupported
                }
            }
        }
    }

    pub async fn try_open(
        &mut self,
        store_path: PathBuf,
        user_id: String,
        is_encrypt: bool,
    ) -> Result<(), DbError> {
        match self {
            Self::Noop(worker) => worker.try_open(store_path, user_id, is_encrypt).await,
            #[cfg(feature = "store-diesel-sqlite")]
            Self::DieselSqlite(worker) => worker.try_open(store_path, user_id, is_encrypt).await,
            #[cfg(not(feature = "store-diesel-sqlite"))]
            Self::Unsupported => Err(DbError::NotSupportedYet),
        }
    }

    pub async fn close(&mut self) -> Result<(), DbError> {
        match self {
            Self::Noop(worker) => worker.close().await,
            #[cfg(feature = "store-diesel-sqlite")]
            Self::DieselSqlite(worker) => worker.close().await,
            #[cfg(not(feature = "store-diesel-sqlite"))]
            Self::Unsupported => Ok(()),
        }
    }

    pub async fn insert_or_replace_key_val(&self, table: VibeTableKeyVal) -> Result<(), DbError> {
        match self {
            Self::Noop(worker) => worker.insert_or_replace_key_val(table).await,
            #[cfg(feature = "store-diesel-sqlite")]
            Self::DieselSqlite(worker) => worker.insert_or_replace_key_val(table).await,
            #[cfg(not(feature = "store-diesel-sqlite"))]
            Self::Unsupported => Err(DbError::NotSupportedYet),
        }
    }

    pub async fn get_key_val(
        &self,
        user_id: String,
        bucket: String,
        key: String,
    ) -> Result<Option<VibeTableKeyVal>, DbError> {
        match self {
            Self::Noop(worker) => worker.get_key_val(user_id, bucket, key).await,
            #[cfg(feature = "store-diesel-sqlite")]
            Self::DieselSqlite(worker) => worker.get_key_val(user_id, bucket, key).await,
            #[cfg(not(feature = "store-diesel-sqlite"))]
            Self::Unsupported => Err(DbError::NotSupportedYet),
        }
    }

    pub async fn get_key_val_vec(
        &self,
        user_id: String,
        bucket: String,
        keys: Vec<String>,
    ) -> Result<Vec<VibeTableKeyVal>, DbError> {
        match self {
            Self::Noop(worker) => worker.get_key_val_vec(user_id, bucket, keys).await,
            #[cfg(feature = "store-diesel-sqlite")]
            Self::DieselSqlite(worker) => worker.get_key_val_vec(user_id, bucket, keys).await,
            #[cfg(not(feature = "store-diesel-sqlite"))]
            Self::Unsupported => Err(DbError::NotSupportedYet),
        }
    }

    pub async fn remove_key_val(
        &self,
        user_id: String,
        bucket: String,
        key: String,
    ) -> Result<bool, DbError> {
        match self {
            Self::Noop(worker) => worker.remove_key_val(user_id, bucket, key).await,
            #[cfg(feature = "store-diesel-sqlite")]
            Self::DieselSqlite(worker) => worker.remove_key_val(user_id, bucket, key).await,
            #[cfg(not(feature = "store-diesel-sqlite"))]
            Self::Unsupported => Err(DbError::NotSupportedYet),
        }
    }

    pub async fn contains_key_val(
        &self,
        user_id: String,
        bucket: String,
        key: String,
    ) -> Result<bool, DbError> {
        match self {
            Self::Noop(worker) => worker.contains_key_val(user_id, bucket, key).await,
            #[cfg(feature = "store-diesel-sqlite")]
            Self::DieselSqlite(worker) => worker.contains_key_val(user_id, bucket, key).await,
            #[cfg(not(feature = "store-diesel-sqlite"))]
            Self::Unsupported => Err(DbError::NotSupportedYet),
        }
    }

    pub async fn list_key_vals(
        &self,
        user_id: String,
        bucket: String,
    ) -> Result<Vec<String>, DbError> {
        match self {
            Self::Noop(worker) => worker.list_key_vals(user_id, bucket).await,
            #[cfg(feature = "store-diesel-sqlite")]
            Self::DieselSqlite(worker) => worker.list_key_vals(user_id, bucket).await,
            #[cfg(not(feature = "store-diesel-sqlite"))]
            Self::Unsupported => Err(DbError::NotSupportedYet),
        }
    }

    pub async fn transaction(&self, ops: Vec<DbKvOp>) -> Result<(), DbError> {
        match self {
            Self::Noop(_) => {
                let _ = ops;
                Ok(())
            }
            #[cfg(feature = "store-diesel-sqlite")]
            Self::DieselSqlite(worker) => worker.transaction(ops).await,
            #[cfg(not(feature = "store-diesel-sqlite"))]
            Self::Unsupported => {
                let _ = ops;
                Err(DbError::NotSupportedYet)
            }
        }
    }

    pub async fn purge_expired(&self, now_ms: i64) -> Result<usize, DbError> {
        match self {
            Self::Noop(worker) => worker.purge_expired(now_ms).await,
            #[cfg(feature = "store-diesel-sqlite")]
            Self::DieselSqlite(worker) => worker.purge_expired(now_ms).await,
            #[cfg(not(feature = "store-diesel-sqlite"))]
            Self::Unsupported => Err(DbError::NotSupportedYet),
        }
    }
}

#[cfg(test)]
mod strict_tests {
    use super::*;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test/unit/store/sql_def_tests.rs"
    ));
}
