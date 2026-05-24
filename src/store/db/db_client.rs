use crate::api::engine_config::VibeStoreBackend;
use crate::api::engine_error::{VibeEngineError, VibeEngineErrorCode};
use crate::log::log_def::DESC;
use crate::log_e;
use crate::store::db::sql_def::{start_worker_loop, DbError, DbKvOp, DbWorker};
use crate::store::db::tables::key_val::{
    VibeKvValue, VibeTableKeyVal, DEFAULT_BUCKET, EXPIRES_AT_NEVER,
};
use crate::utils::date_util::exec_time_end;
use crate::utils::global_ref::DB_CHANNEL_BUFFER_SIZE;
use futures::channel::mpsc;
use futures::SinkExt;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::oneshot::Receiver;
use tokio::sync::{oneshot, Mutex, RwLock};

#[cfg(target_arch = "wasm32")]
type TaskType = Pin<Box<dyn Future<Output = ()> + 'static>>;
#[cfg(not(target_arch = "wasm32"))]
type TaskType = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

#[derive(Clone)]
/// Client used by the SDK to coordinate database worker operations.
pub struct VibeDbClient {
    db_worker: Arc<RwLock<DbWorker>>,
    task_tx: Arc<Mutex<mpsc::Sender<TaskType>>>,
    user_id: Arc<RwLock<Option<String>>>,
    is_closed: Arc<AtomicBool>,
}

impl VibeDbClient {
    /// Creates a database client using the default store backend.
    ///
    /// # Returns
    ///
    /// A [`VibeDbClient`] with its worker loop initialized.
    pub fn new() -> Self {
        Self::with_backend(VibeStoreBackend::default())
    }

    /// Creates a database client using a specific store backend.
    ///
    /// # Returns
    ///
    /// A [`VibeDbClient`] with its worker loop initialized for `backend`.
    ///
    /// # Examples
    ///
    /// ```
    /// use vibe_ready::{VibeDbClient, VibeStoreBackend};
    ///
    /// let client = VibeDbClient::with_backend(VibeStoreBackend::Noop);
    /// drop(client);
    /// ```
    pub fn with_backend(backend: VibeStoreBackend) -> Self {
        let (task_tx, task_rx) = mpsc::channel(DB_CHANNEL_BUFFER_SIZE);

        let _worker_handle = start_worker_loop(task_rx);
        Self {
            db_worker: Arc::new(RwLock::new(DbWorker::with_backend(backend))),
            task_tx: Arc::new(tokio::sync::Mutex::new(task_tx)),
            user_id: Arc::new(RwLock::new(None)),
            is_closed: Arc::new(AtomicBool::new(false)),
        }
    }

    async fn execute<T>(
        &self,
        task: TaskType,
        resp_rx: Receiver<Result<T, DbError>>,
    ) -> Result<T, VibeEngineError> {
        let mut sender = self.task_tx.lock().await;
        if let Err(_) = sender.send(task).await {
            return Err(VibeEngineError::from_code(
                VibeEngineErrorCode::DatabaseThreadError,
            ));
        }

        match resp_rx.await {
            Ok(ret) => ret.map_err(VibeEngineError::from),
            Err(_) => Err(VibeEngineError::from_code(
                VibeEngineErrorCode::DatabaseThreadError,
            )),
        }
    }

    #[allow(dead_code)]
    async fn execute_exec_time<T>(
        &self,
        task: TaskType,
        resp_rx: Receiver<Result<T, DbError>>,
        method_name: &str,
        time: i64,
    ) -> Result<T, VibeEngineError> {
        let mut sender = self.task_tx.lock().await;
        if let Err(_) = sender.send(task).await {
            return Err(VibeEngineError::from_code(
                VibeEngineErrorCode::DatabaseThreadError,
            ));
        }

        match resp_rx.await {
            Ok(ret) => {
                exec_time_end(method_name, time);
                ret.map_err(VibeEngineError::from)
            }
            Err(_) => {
                exec_time_end(method_name, time);
                Err(VibeEngineError::from_code(
                    VibeEngineErrorCode::DatabaseThreadError,
                ))
            }
        }
    }

    #[allow(dead_code)]
    async fn execute_trans<T, R, FN, Fut>(
        &self,
        task: TaskType,
        resp_rx: Receiver<Result<T, DbError>>,
        result_converter: FN,
    ) -> Result<R, VibeEngineError>
    where
        FN: FnOnce(Result<T, DbError>) -> Fut,
        Fut: Future<Output = Result<R, VibeEngineError>>,
    {
        let mut sender = self.task_tx.lock().await;
        if let Err(_) = sender.send(task).await {
            return Err(VibeEngineError::from_code(
                VibeEngineErrorCode::DatabaseThreadError,
            ));
        }

        match resp_rx.await {
            Ok(db_result) => result_converter(db_result).await,
            Err(_) => Err(VibeEngineError::from_code(
                VibeEngineErrorCode::DatabaseThreadError,
            )),
        }
    }
}

impl VibeDbClient {
    /// Closes the worker channel and database backend.
    ///
    /// # Returns
    ///
    /// `Ok(())` when closed or already closed, otherwise [`VibeEngineError`].
    pub async fn close(&self) -> Result<(), VibeEngineError> {
        if self.is_closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let mut sender = self.task_tx.lock().await;
        let ret = sender.close().await;
        if let Err(error) = ret {
            log_e!(
                "close",
                DESC,
                format!("close sender error: {}", error.to_string())
            );
        }
        let db_lock = self.db_worker.write();
        db_lock.await.close().await?;
        Ok(())
    }

    /// Opens the configured backend at `store_path` for `user_id`.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the backend is ready, or [`VibeEngineError`] on open failure.
    pub async fn try_open(
        &self,
        store_path: PathBuf,
        user_id: String,
        is_encrypt: bool,
    ) -> Result<(), VibeEngineError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let db_worker_clone = self.db_worker.clone();
        let opened_user_id = user_id.clone();

        let task = Box::pin(async move {
            let mut db_worker = db_worker_clone.write().await;
            let result = db_worker.try_open(store_path, user_id, is_encrypt).await;
            let _ = resp_tx.send(result);
        });

        self.execute(task, resp_rx).await?;
        *self.user_id.write().await = Some(opened_user_id);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Backward compatible API: routes to the default bucket.
// ---------------------------------------------------------------------------
impl VibeDbClient {
    /// Sets a value in the default bucket.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the value is written.
    pub async fn set(&self, key: String, value: VibeKvValue) -> Result<(), VibeEngineError> {
        self.set_in_bucket(DEFAULT_BUCKET.to_string(), key, value, EXPIRES_AT_NEVER)
            .await
    }

    /// Sets a string value in the default bucket.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the value is written.
    pub async fn set_str(&self, key: String, value: String) -> Result<(), VibeEngineError> {
        self.set(key, VibeKvValue::String(value)).await
    }

    /// Sets a boolean value in the default bucket.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the value is written.
    pub async fn set_bool(&self, key: String, value: bool) -> Result<(), VibeEngineError> {
        self.set(key, VibeKvValue::Bool(value)).await
    }

    /// Sets an `i32` value in the default bucket.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the value is written.
    pub async fn set_i32(&self, key: String, value: i32) -> Result<(), VibeEngineError> {
        self.set(key, VibeKvValue::I32(value)).await
    }

    /// Gets a value from the default bucket.
    ///
    /// # Returns
    ///
    /// `Ok(Some(value))`, `Ok(None)`, or [`VibeEngineError`].
    pub async fn get(&self, key: String) -> Result<Option<VibeKvValue>, VibeEngineError> {
        self.get_in_bucket(DEFAULT_BUCKET.to_string(), key).await
    }

    /// Gets a string value from the default bucket.
    ///
    /// # Returns
    ///
    /// `Ok(Some(String))` only when the value is a string.
    pub async fn get_str(&self, key: String) -> Result<Option<String>, VibeEngineError> {
        match self.get(key).await? {
            Some(VibeKvValue::String(value)) => Ok(Some(value)),
            _ => Ok(None),
        }
    }

    /// Gets a boolean value from the default bucket.
    ///
    /// # Returns
    ///
    /// `Ok(Some(bool))` only when the value is a boolean.
    pub async fn get_bool(&self, key: String) -> Result<Option<bool>, VibeEngineError> {
        match self.get(key).await? {
            Some(VibeKvValue::Bool(value)) => Ok(Some(value)),
            _ => Ok(None),
        }
    }

    /// Gets an `i32` value from the default bucket.
    ///
    /// # Returns
    ///
    /// `Ok(Some(i32))` only when the value is an `i32`.
    pub async fn get_i32(&self, key: String) -> Result<Option<i32>, VibeEngineError> {
        match self.get(key).await? {
            Some(VibeKvValue::I32(value)) => Ok(Some(value)),
            _ => Ok(None),
        }
    }

    /// Removes a key from the default bucket.
    ///
    /// # Returns
    ///
    /// `Ok(true)` when the key existed and was removed.
    pub async fn remove(&self, key: String) -> Result<bool, VibeEngineError> {
        self.remove_in_bucket(DEFAULT_BUCKET.to_string(), key).await
    }

    /// Checks whether a key exists in the default bucket.
    ///
    /// # Returns
    ///
    /// `Ok(true)` when the key exists and is not expired.
    pub async fn contains(&self, key: String) -> Result<bool, VibeEngineError> {
        self.contains_in_bucket(DEFAULT_BUCKET.to_string(), key)
            .await
    }

    /// Lists keys in the default bucket.
    ///
    /// # Returns
    ///
    /// A vector of key names.
    pub async fn list_keys(&self) -> Result<Vec<String>, VibeEngineError> {
        self.list_keys_in_bucket(DEFAULT_BUCKET.to_string()).await
    }

    /// Returns the user id associated with the open backend.
    ///
    /// # Returns
    ///
    /// `Ok(String)` after [`VibeDbClient::try_open`], otherwise a database-not-open error.
    pub async fn current_user_id(&self) -> Result<String, VibeEngineError> {
        self.user_id
            .read()
            .await
            .clone()
            .ok_or_else(|| VibeEngineError::from_error_code(VibeEngineErrorCode::DatabaseNotOpened))
    }
}

// ---------------------------------------------------------------------------
// Bucket-aware API.
// ---------------------------------------------------------------------------
impl VibeDbClient {
    /// Sets a value in a named bucket.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the value is written.
    pub async fn set_in_bucket(
        &self,
        bucket: String,
        key: String,
        value: VibeKvValue,
        expires_at_ms: i64,
    ) -> Result<(), VibeEngineError> {
        validate_key(&key)?;
        let user_id = self.current_user_id().await?;
        let row = VibeTableKeyVal::new_in_bucket(&user_id, &bucket, &key, value, expires_at_ms);
        self.insert_or_replace_key_val(row).await
    }

    /// Gets a value from a named bucket.
    ///
    /// # Returns
    ///
    /// `Ok(Some(value))`, `Ok(None)` for missing/expired keys, or an error.
    pub async fn get_in_bucket(
        &self,
        bucket: String,
        key: String,
    ) -> Result<Option<VibeKvValue>, VibeEngineError> {
        validate_key(&key)?;
        let user_id = self.current_user_id().await?;
        let row = self.get_key_val(user_id, bucket, key).await?;
        let now = crate::platform::now();
        Ok(row.and_then(|r| if r.is_expired(now) { None } else { r.value() }))
    }

    /// Removes a key from a named bucket.
    ///
    /// # Returns
    ///
    /// `Ok(true)` when the key existed and was removed.
    pub async fn remove_in_bucket(
        &self,
        bucket: String,
        key: String,
    ) -> Result<bool, VibeEngineError> {
        validate_key(&key)?;
        let user_id = self.current_user_id().await?;
        self.remove_key_val(user_id, bucket, key).await
    }

    /// Checks whether a key exists in a named bucket.
    ///
    /// # Returns
    ///
    /// `Ok(true)` when the key exists and is not expired.
    pub async fn contains_in_bucket(
        &self,
        bucket: String,
        key: String,
    ) -> Result<bool, VibeEngineError> {
        validate_key(&key)?;
        let user_id = self.current_user_id().await?;
        // Treat expired keys as missing to keep semantics in sync with `get`.
        let row = self.get_key_val(user_id, bucket, key).await?;
        let now = crate::platform::now();
        Ok(row.map(|r| !r.is_expired(now)).unwrap_or(false))
    }

    /// Lists keys in a named bucket.
    ///
    /// # Returns
    ///
    /// A vector of key names.
    pub async fn list_keys_in_bucket(
        &self,
        bucket: String,
    ) -> Result<Vec<String>, VibeEngineError> {
        let user_id = self.current_user_id().await?;
        self.list_key_vals(user_id, bucket).await
    }

    /// Reads multiple keys from a named bucket.
    ///
    /// # Returns
    ///
    /// A vector of `(key, value)` pairs for existing, non-expired keys.
    pub async fn get_many_in_bucket(
        &self,
        bucket: String,
        keys: Vec<String>,
    ) -> Result<Vec<(String, VibeKvValue)>, VibeEngineError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        for k in &keys {
            validate_key(k)?;
        }
        let user_id = self.current_user_id().await?;
        let rows = self.get_key_val_vec(user_id, bucket, keys).await?;
        let now = crate::platform::now();
        Ok(rows
            .into_iter()
            .filter(|r| !r.is_expired(now))
            .filter_map(|r| {
                let key = r.key.clone();
                r.value().map(|v| (key, v))
            })
            .collect())
    }

    /// Writes multiple values to a named bucket in one transaction.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the batch commit succeeds.
    pub async fn set_many_in_bucket(
        &self,
        bucket: String,
        items: Vec<(String, VibeKvValue, i64 /* expires_at_ms */)>,
    ) -> Result<(), VibeEngineError> {
        for (k, _, _) in &items {
            validate_key(k)?;
        }
        let user_id = self.current_user_id().await?;
        let ops: Vec<DbKvOp> = items
            .into_iter()
            .map(|(k, v, expires)| {
                DbKvOp::Set(VibeTableKeyVal::new_in_bucket(
                    &user_id, &bucket, &k, v, expires,
                ))
            })
            .collect();
        self.transaction_ops(ops).await
    }

    /// Removes multiple keys from a named bucket in one transaction.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the batch commit succeeds.
    pub async fn remove_many_in_bucket(
        &self,
        bucket: String,
        keys: Vec<String>,
    ) -> Result<(), VibeEngineError> {
        for k in &keys {
            validate_key(k)?;
        }
        let user_id = self.current_user_id().await?;
        let ops: Vec<DbKvOp> = keys
            .into_iter()
            .map(|k| DbKvOp::Remove {
                user_id: user_id.clone(),
                bucket: bucket.clone(),
                key: k,
            })
            .collect();
        self.transaction_ops(ops).await
    }

    /// Apply pre-built ops (already user-scoped) inside one DB transaction.
    ///
    /// # Returns
    ///
    /// `Ok(())` when every operation commits atomically.
    pub async fn transaction_ops(&self, ops: Vec<DbKvOp>) -> Result<(), VibeEngineError> {
        if ops.is_empty() {
            return Ok(());
        }
        let (resp_tx, resp_rx) = oneshot::channel();
        let db_worker_clone = self.db_worker.clone();
        let task = Box::pin(async move {
            let db_worker = db_worker_clone.read().await;
            let result = db_worker.transaction(ops).await;
            let _ = resp_tx.send(result);
        });
        self.execute(task, resp_rx).await
    }

    /// Purges expired rows from the backend.
    ///
    /// # Returns
    ///
    /// Number of rows removed by the backend.
    pub async fn purge_expired(&self) -> Result<usize, VibeEngineError> {
        let now = crate::platform::now();
        let (resp_tx, resp_rx) = oneshot::channel();
        let db_worker_clone = self.db_worker.clone();
        let task = Box::pin(async move {
            let db_worker = db_worker_clone.read().await;
            let result = db_worker.purge_expired(now).await;
            let _ = resp_tx.send(result);
        });
        self.execute(task, resp_rx).await
    }
}

// ---------------------------------------------------------------------------
// Lower level row APIs (kept for advanced integrations).
// ---------------------------------------------------------------------------
impl VibeDbClient {
    /// Inserts or replaces a low-level key-value row.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the row is written.
    pub async fn insert_or_replace_key_val(
        &self,
        table: VibeTableKeyVal,
    ) -> Result<(), VibeEngineError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let db_worker_clone = self.db_worker.clone();

        let task = Box::pin(async move {
            let db_worker = db_worker_clone.read().await;
            let result = db_worker.insert_or_replace_key_val(table).await;
            let _ = resp_tx.send(result);
        });

        self.execute(task, resp_rx).await
    }

    /// Gets a low-level key-value row.
    ///
    /// # Returns
    ///
    /// `Ok(Some(row))`, `Ok(None)`, or [`VibeEngineError`].
    pub async fn get_key_val(
        &self,
        user_id: String,
        bucket: String,
        key: String,
    ) -> Result<Option<VibeTableKeyVal>, VibeEngineError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let db_worker_clone = self.db_worker.clone();

        let task = Box::pin(async move {
            let db_worker = db_worker_clone.read().await;
            let result = db_worker.get_key_val(user_id, bucket, key).await;
            let _ = resp_tx.send(result);
        });

        self.execute(task, resp_rx).await
    }

    /// Gets multiple low-level key-value rows.
    ///
    /// # Returns
    ///
    /// A vector of matching rows.
    pub async fn get_key_val_vec(
        &self,
        user_id: String,
        bucket: String,
        keys: Vec<String>,
    ) -> Result<Vec<VibeTableKeyVal>, VibeEngineError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let db_worker_clone = self.db_worker.clone();

        let task = Box::pin(async move {
            let db_worker = db_worker_clone.read().await;
            let result = db_worker.get_key_val_vec(user_id, bucket, keys).await;
            let _ = resp_tx.send(result);
        });

        self.execute(task, resp_rx).await
    }

    /// Removes a low-level key-value row.
    ///
    /// # Returns
    ///
    /// `Ok(true)` when a row was removed.
    pub async fn remove_key_val(
        &self,
        user_id: String,
        bucket: String,
        key: String,
    ) -> Result<bool, VibeEngineError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let db_worker_clone = self.db_worker.clone();

        let task = Box::pin(async move {
            let db_worker = db_worker_clone.read().await;
            let result = db_worker.remove_key_val(user_id, bucket, key).await;
            let _ = resp_tx.send(result);
        });

        self.execute(task, resp_rx).await
    }

    /// Checks whether a low-level key-value row exists.
    ///
    /// # Returns
    ///
    /// `Ok(true)` when the row exists.
    pub async fn contains_key_val(
        &self,
        user_id: String,
        bucket: String,
        key: String,
    ) -> Result<bool, VibeEngineError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let db_worker_clone = self.db_worker.clone();

        let task = Box::pin(async move {
            let db_worker = db_worker_clone.read().await;
            let result = db_worker.contains_key_val(user_id, bucket, key).await;
            let _ = resp_tx.send(result);
        });

        self.execute(task, resp_rx).await
    }

    /// Lists low-level key names for a user and bucket.
    ///
    /// # Returns
    ///
    /// A vector of key names.
    pub async fn list_key_vals(
        &self,
        user_id: String,
        bucket: String,
    ) -> Result<Vec<String>, VibeEngineError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let db_worker_clone = self.db_worker.clone();

        let task = Box::pin(async move {
            let db_worker = db_worker_clone.read().await;
            let result = db_worker.list_key_vals(user_id, bucket).await;
            let _ = resp_tx.send(result);
        });

        self.execute(task, resp_rx).await
    }
}

fn validate_key(key: &str) -> Result<(), VibeEngineError> {
    if key.trim().is_empty() {
        return Err(VibeEngineError::from_error_code(
            VibeEngineErrorCode::ParameterEmpty,
        ));
    }
    Ok(())
}

#[cfg(test)]
mod strict_tests {
    use super::*;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test/unit/store/db_client_tests.rs"
    ));
}
