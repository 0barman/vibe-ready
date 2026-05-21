use crate::api::engine_error::VibeEngineError;
use crate::api::engine_executor::VibeEngineExecutor;
use crate::store::db::db_client::VibeDbClient;
use crate::store::db::sql_def::DbKvOp;
use crate::store::db::tables::key_val::{
    VibeKvValue, VibeTableKeyVal, DEFAULT_BUCKET, EXPIRES_AT_NEVER,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Description of a change applied to the KV store, delivered to listeners
/// registered via [`VibeKvStore::on_change`] / [`VibeKvBucket::on_change`].
#[derive(Debug, Clone)]
pub struct VibeKvChange {
    pub bucket: String,
    pub key: String,
    pub kind: VibeKvChangeKind,
    pub value: Option<VibeKvValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VibeKvChangeKind {
    Set,
    Remove,
}

/// Identifier returned by [`VibeKvStore::on_change`] used to cancel a listener.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VibeKvListenerId(u64);

type ListenerFn = Arc<dyn Fn(&VibeKvChange) + Send + Sync + 'static>;

struct ListenerEntry {
    id: VibeKvListenerId,
    bucket_filter: Option<String>,
    pattern: String,
    cb: ListenerFn,
}

#[derive(Default)]
struct ListenerRegistry {
    next_id: AtomicU64,
    entries: Mutex<Vec<ListenerEntry>>,
}

impl ListenerRegistry {
    fn add(
        &self,
        bucket_filter: Option<String>,
        pattern: String,
        cb: ListenerFn,
    ) -> VibeKvListenerId {
        let id = VibeKvListenerId(self.next_id.fetch_add(1, Ordering::SeqCst));
        let entry = ListenerEntry {
            id,
            bucket_filter,
            pattern,
            cb,
        };
        if let Ok(mut guard) = self.entries.lock() {
            guard.push(entry);
        }
        id
    }

    fn remove(&self, id: VibeKvListenerId) -> bool {
        if let Ok(mut guard) = self.entries.lock() {
            let len_before = guard.len();
            guard.retain(|e| e.id != id);
            return guard.len() != len_before;
        }
        false
    }

    fn matching(&self, change: &VibeKvChange) -> Vec<ListenerFn> {
        let Ok(guard) = self.entries.lock() else {
            return Vec::new();
        };
        guard
            .iter()
            .filter(|e| {
                e.bucket_filter
                    .as_deref()
                    .map(|b| b == change.bucket)
                    .unwrap_or(true)
                    && pattern_matches(&e.pattern, &change.key)
            })
            .map(|e| Arc::clone(&e.cb))
            .collect()
    }
}

/// Glob-ish matcher: supports exact keys, full wildcard `*`, and a single
/// trailing wildcard like `prefix*`.
fn pattern_matches(pattern: &str, key: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return key.starts_with(prefix);
    }
    pattern == key
}

/// High-level key-value store facade backed by the configured SDK store backend.
#[derive(Clone)]
pub struct VibeKvStore {
    db_client: VibeDbClient,
    executor: VibeEngineExecutor,
    listeners: Arc<ListenerRegistry>,
}

impl VibeKvStore {
    pub(crate) fn new(db_client: VibeDbClient, executor: VibeEngineExecutor) -> Self {
        Self {
            db_client,
            executor,
            listeners: Arc::new(ListenerRegistry::default()),
        }
    }

    /// Returns a handle scoped to `name` so all reads/writes target that bucket.
    pub fn bucket(&self, name: impl Into<String>) -> VibeKvBucket {
        VibeKvBucket {
            store: self.clone(),
            name: name.into(),
        }
    }

    fn dispatch(&self, change: VibeKvChange) {
        let cbs = self.listeners.matching(&change);
        if cbs.is_empty() {
            return;
        }
        let executor = self.executor.callback();
        for cb in cbs {
            let change_clone = change.clone();
            executor.execute(move || cb(&change_clone));
        }
    }

    pub fn set(
        &self,
        key: impl AsRef<str>,
        value: impl Into<VibeKvValue>,
    ) -> Result<(), VibeEngineError> {
        self.set_in_bucket(DEFAULT_BUCKET, key, value, None)
    }

    /// Store `value` with a time-to-live. The key is treated as missing once
    /// `ttl` has elapsed.
    pub fn set_with_ttl(
        &self,
        key: impl AsRef<str>,
        value: impl Into<VibeKvValue>,
        ttl: Duration,
    ) -> Result<(), VibeEngineError> {
        self.set_in_bucket(DEFAULT_BUCKET, key, value, Some(ttl))
    }

    fn set_in_bucket(
        &self,
        bucket: &str,
        key: impl AsRef<str>,
        value: impl Into<VibeKvValue>,
        ttl: Option<Duration>,
    ) -> Result<(), VibeEngineError> {
        let key = key.as_ref().to_string();
        let value = value.into();
        let bucket_owned = bucket.to_string();
        let expires_at = ttl_to_expires_at(ttl);
        let db_client = self.db_client.clone();
        let bucket_for_call = bucket_owned.clone();
        let value_for_dispatch = value.clone();
        let key_for_dispatch = key.clone();
        self.executor.invoke(async move {
            db_client
                .set_in_bucket(bucket_for_call, key, value, expires_at)
                .await
        })??;
        self.dispatch(VibeKvChange {
            bucket: bucket_owned,
            key: key_for_dispatch,
            kind: VibeKvChangeKind::Set,
            value: Some(value_for_dispatch),
        });
        Ok(())
    }

    pub fn set_str(
        &self,
        key: impl AsRef<str>,
        value: impl AsRef<str>,
    ) -> Result<(), VibeEngineError> {
        self.set(key, VibeKvValue::String(value.as_ref().to_string()))
    }

    pub fn set_bool(&self, key: impl AsRef<str>, value: bool) -> Result<(), VibeEngineError> {
        self.set(key, VibeKvValue::Bool(value))
    }

    pub fn set_i32(&self, key: impl AsRef<str>, value: i32) -> Result<(), VibeEngineError> {
        self.set(key, VibeKvValue::I32(value))
    }

    pub fn get(&self, key: impl AsRef<str>) -> Result<Option<VibeKvValue>, VibeEngineError> {
        let key = key.as_ref().to_string();
        let db_client = self.db_client.clone();
        self.executor
            .invoke(async move { db_client.get_in_bucket(DEFAULT_BUCKET.to_string(), key).await })?
    }

    pub fn get_str(&self, key: impl AsRef<str>) -> Result<Option<String>, VibeEngineError> {
        Ok(self.get(key)?.and_then(|v| match v {
            VibeKvValue::String(s) => Some(s),
            _ => None,
        }))
    }

    pub fn get_bool(&self, key: impl AsRef<str>) -> Result<Option<bool>, VibeEngineError> {
        Ok(self.get(key)?.and_then(|v| match v {
            VibeKvValue::Bool(b) => Some(b),
            _ => None,
        }))
    }

    pub fn get_i32(&self, key: impl AsRef<str>) -> Result<Option<i32>, VibeEngineError> {
        Ok(self.get(key)?.and_then(|v| match v {
            VibeKvValue::I32(i) => Some(i),
            _ => None,
        }))
    }

    pub fn remove(&self, key: impl AsRef<str>) -> Result<bool, VibeEngineError> {
        let key_owned = key.as_ref().to_string();
        let db_client = self.db_client.clone();
        let key_for_call = key_owned.clone();
        let removed = self.executor.invoke(async move {
            db_client
                .remove_in_bucket(DEFAULT_BUCKET.to_string(), key_for_call)
                .await
        })??;
        if removed {
            self.dispatch(VibeKvChange {
                bucket: DEFAULT_BUCKET.to_string(),
                key: key_owned,
                kind: VibeKvChangeKind::Remove,
                value: None,
            });
        }
        Ok(removed)
    }

    pub fn contains(&self, key: impl AsRef<str>) -> Result<bool, VibeEngineError> {
        let key = key.as_ref().to_string();
        let db_client = self.db_client.clone();
        self.executor.invoke(async move {
            db_client
                .contains_in_bucket(DEFAULT_BUCKET.to_string(), key)
                .await
        })?
    }

    pub fn list_keys(&self) -> Result<Vec<String>, VibeEngineError> {
        let db_client = self.db_client.clone();
        self.executor.invoke(async move {
            db_client
                .list_keys_in_bucket(DEFAULT_BUCKET.to_string())
                .await
        })?
    }

    /// Batch set on the default bucket. All writes happen in a single DB
    /// transaction; failures roll the whole batch back.
    pub fn set_many<K, V>(&self, items: Vec<(K, V)>) -> Result<(), VibeEngineError>
    where
        K: AsRef<str>,
        V: Into<VibeKvValue>,
    {
        self.bucket(DEFAULT_BUCKET).set_many(items)
    }

    pub fn get_many<K>(
        &self,
        keys: Vec<K>,
    ) -> Result<Vec<(String, VibeKvValue)>, VibeEngineError>
    where
        K: AsRef<str>,
    {
        self.bucket(DEFAULT_BUCKET).get_many(keys)
    }

    pub fn remove_many<K>(&self, keys: Vec<K>) -> Result<(), VibeEngineError>
    where
        K: AsRef<str>,
    {
        self.bucket(DEFAULT_BUCKET).remove_many(keys)
    }

    /// Run a closure that buffers a batch of `set`/`remove` operations and
    /// commits them in a single transaction. Returning `Err` from the closure
    /// (or any DB failure during commit) rolls every buffered op back.
    pub fn transaction<F, T>(&self, f: F) -> Result<T, VibeEngineError>
    where
        F: FnOnce(&mut VibeKvTx) -> Result<T, VibeEngineError>,
    {
        let mut tx = VibeKvTx::new(DEFAULT_BUCKET.to_string());
        let outcome = f(&mut tx)?;
        let bucket = tx.bucket.clone();
        let mut changes = Vec::new();
        let mut ops = Vec::with_capacity(tx.ops.len());
        let user_id = current_user_id_blocking(&self.db_client, &self.executor)?;
        for op in tx.ops.drain(..) {
            match op {
                BufferedOp::Set { key, value, ttl } => {
                    let expires = ttl_to_expires_at(ttl);
                    ops.push(DbKvOp::Set(VibeTableKeyVal::new_in_bucket(
                        &user_id,
                        &bucket,
                        &key,
                        value.clone(),
                        expires,
                    )));
                    changes.push(VibeKvChange {
                        bucket: bucket.clone(),
                        key,
                        kind: VibeKvChangeKind::Set,
                        value: Some(value),
                    });
                }
                BufferedOp::Remove { key } => {
                    ops.push(DbKvOp::Remove {
                        user_id: user_id.clone(),
                        bucket: bucket.clone(),
                        key: key.clone(),
                    });
                    changes.push(VibeKvChange {
                        bucket: bucket.clone(),
                        key,
                        kind: VibeKvChangeKind::Remove,
                        value: None,
                    });
                }
            }
        }
        let db_client = self.db_client.clone();
        self.executor
            .invoke(async move { db_client.transaction_ops(ops).await })??;
        for change in changes {
            self.dispatch(change);
        }
        Ok(outcome)
    }

    /// Register a listener for changes whose key matches `pattern` in the
    /// default bucket. Pattern is exact-match, `*`, or `prefix*`.
    pub fn on_change<F>(
        &self,
        pattern: impl Into<String>,
        listener: F,
    ) -> VibeKvListenerId
    where
        F: Fn(&VibeKvChange) + Send + Sync + 'static,
    {
        self.listeners.add(
            Some(DEFAULT_BUCKET.to_string()),
            pattern.into(),
            Arc::new(listener),
        )
    }

    /// Unregister a listener previously returned by `on_change`.
    pub fn off_change(&self, id: VibeKvListenerId) -> bool {
        self.listeners.remove(id)
    }

    /// Eagerly purge expired rows from the underlying store.
    pub fn purge_expired(&self) -> Result<usize, VibeEngineError> {
        let db_client = self.db_client.clone();
        self.executor
            .invoke(async move { db_client.purge_expired().await })?
    }
}

/// Bucket-scoped view of [`VibeKvStore`].
#[derive(Clone)]
pub struct VibeKvBucket {
    store: VibeKvStore,
    name: String,
}

impl VibeKvBucket {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn set(
        &self,
        key: impl AsRef<str>,
        value: impl Into<VibeKvValue>,
    ) -> Result<(), VibeEngineError> {
        self.store.set_in_bucket(&self.name, key, value, None)
    }

    pub fn set_with_ttl(
        &self,
        key: impl AsRef<str>,
        value: impl Into<VibeKvValue>,
        ttl: Duration,
    ) -> Result<(), VibeEngineError> {
        self.store.set_in_bucket(&self.name, key, value, Some(ttl))
    }

    pub fn get(&self, key: impl AsRef<str>) -> Result<Option<VibeKvValue>, VibeEngineError> {
        let bucket = self.name.clone();
        let key = key.as_ref().to_string();
        let db_client = self.store.db_client.clone();
        self.store
            .executor
            .invoke(async move { db_client.get_in_bucket(bucket, key).await })?
    }

    pub fn remove(&self, key: impl AsRef<str>) -> Result<bool, VibeEngineError> {
        let bucket = self.name.clone();
        let key_owned = key.as_ref().to_string();
        let db_client = self.store.db_client.clone();
        let key_for_call = key_owned.clone();
        let bucket_for_call = bucket.clone();
        let removed = self.store.executor.invoke(async move {
            db_client
                .remove_in_bucket(bucket_for_call, key_for_call)
                .await
        })??;
        if removed {
            self.store.dispatch(VibeKvChange {
                bucket,
                key: key_owned,
                kind: VibeKvChangeKind::Remove,
                value: None,
            });
        }
        Ok(removed)
    }

    pub fn contains(&self, key: impl AsRef<str>) -> Result<bool, VibeEngineError> {
        let bucket = self.name.clone();
        let key = key.as_ref().to_string();
        let db_client = self.store.db_client.clone();
        self.store
            .executor
            .invoke(async move { db_client.contains_in_bucket(bucket, key).await })?
    }

    pub fn list_keys(&self) -> Result<Vec<String>, VibeEngineError> {
        let bucket = self.name.clone();
        let db_client = self.store.db_client.clone();
        self.store
            .executor
            .invoke(async move { db_client.list_keys_in_bucket(bucket).await })?
    }

    pub fn set_many<K, V>(&self, items: Vec<(K, V)>) -> Result<(), VibeEngineError>
    where
        K: AsRef<str>,
        V: Into<VibeKvValue>,
    {
        let owned: Vec<(String, VibeKvValue, i64)> = items
            .into_iter()
            .map(|(k, v)| (k.as_ref().to_string(), v.into(), EXPIRES_AT_NEVER))
            .collect();
        let bucket = self.name.clone();
        let db_client = self.store.db_client.clone();
        let owned_for_dispatch = owned.clone();
        let bucket_for_call = bucket.clone();
        self.store.executor.invoke(async move {
            db_client.set_many_in_bucket(bucket_for_call, owned).await
        })??;
        for (k, v, _) in owned_for_dispatch {
            self.store.dispatch(VibeKvChange {
                bucket: bucket.clone(),
                key: k,
                kind: VibeKvChangeKind::Set,
                value: Some(v),
            });
        }
        Ok(())
    }

    pub fn get_many<K>(
        &self,
        keys: Vec<K>,
    ) -> Result<Vec<(String, VibeKvValue)>, VibeEngineError>
    where
        K: AsRef<str>,
    {
        let keys_owned: Vec<String> = keys.into_iter().map(|k| k.as_ref().to_string()).collect();
        let bucket = self.name.clone();
        let db_client = self.store.db_client.clone();
        self.store
            .executor
            .invoke(async move { db_client.get_many_in_bucket(bucket, keys_owned).await })?
    }

    pub fn remove_many<K>(&self, keys: Vec<K>) -> Result<(), VibeEngineError>
    where
        K: AsRef<str>,
    {
        let keys_owned: Vec<String> = keys.into_iter().map(|k| k.as_ref().to_string()).collect();
        let bucket = self.name.clone();
        let db_client = self.store.db_client.clone();
        let keys_for_dispatch = keys_owned.clone();
        let bucket_for_call = bucket.clone();
        self.store.executor.invoke(async move {
            db_client
                .remove_many_in_bucket(bucket_for_call, keys_owned)
                .await
        })??;
        for k in keys_for_dispatch {
            self.store.dispatch(VibeKvChange {
                bucket: bucket.clone(),
                key: k,
                kind: VibeKvChangeKind::Remove,
                value: None,
            });
        }
        Ok(())
    }

    pub fn transaction<F, T>(&self, f: F) -> Result<T, VibeEngineError>
    where
        F: FnOnce(&mut VibeKvTx) -> Result<T, VibeEngineError>,
    {
        let mut tx = VibeKvTx::new(self.name.clone());
        let outcome = f(&mut tx)?;
        let bucket = tx.bucket.clone();
        let mut changes = Vec::new();
        let mut ops = Vec::with_capacity(tx.ops.len());
        let user_id =
            current_user_id_blocking(&self.store.db_client, &self.store.executor)?;
        for op in tx.ops.drain(..) {
            match op {
                BufferedOp::Set { key, value, ttl } => {
                    let expires = ttl_to_expires_at(ttl);
                    ops.push(DbKvOp::Set(VibeTableKeyVal::new_in_bucket(
                        &user_id,
                        &bucket,
                        &key,
                        value.clone(),
                        expires,
                    )));
                    changes.push(VibeKvChange {
                        bucket: bucket.clone(),
                        key,
                        kind: VibeKvChangeKind::Set,
                        value: Some(value),
                    });
                }
                BufferedOp::Remove { key } => {
                    ops.push(DbKvOp::Remove {
                        user_id: user_id.clone(),
                        bucket: bucket.clone(),
                        key: key.clone(),
                    });
                    changes.push(VibeKvChange {
                        bucket: bucket.clone(),
                        key,
                        kind: VibeKvChangeKind::Remove,
                        value: None,
                    });
                }
            }
        }
        let db_client = self.store.db_client.clone();
        self.store
            .executor
            .invoke(async move { db_client.transaction_ops(ops).await })??;
        for change in changes {
            self.store.dispatch(change);
        }
        Ok(outcome)
    }

    pub fn on_change<F>(
        &self,
        pattern: impl Into<String>,
        listener: F,
    ) -> VibeKvListenerId
    where
        F: Fn(&VibeKvChange) + Send + Sync + 'static,
    {
        self.store.listeners.add(
            Some(self.name.clone()),
            pattern.into(),
            Arc::new(listener),
        )
    }

    pub fn off_change(&self, id: VibeKvListenerId) -> bool {
        self.store.listeners.remove(id)
    }
}

/// Buffered transaction handle accumulating writes that are committed atomically.
pub struct VibeKvTx {
    bucket: String,
    ops: Vec<BufferedOp>,
}

enum BufferedOp {
    Set {
        key: String,
        value: VibeKvValue,
        ttl: Option<Duration>,
    },
    Remove {
        key: String,
    },
}

impl VibeKvTx {
    fn new(bucket: String) -> Self {
        Self {
            bucket,
            ops: Vec::new(),
        }
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn set(
        &mut self,
        key: impl AsRef<str>,
        value: impl Into<VibeKvValue>,
    ) -> Result<&mut Self, VibeEngineError> {
        self.ops.push(BufferedOp::Set {
            key: key.as_ref().to_string(),
            value: value.into(),
            ttl: None,
        });
        Ok(self)
    }

    pub fn set_with_ttl(
        &mut self,
        key: impl AsRef<str>,
        value: impl Into<VibeKvValue>,
        ttl: Duration,
    ) -> Result<&mut Self, VibeEngineError> {
        self.ops.push(BufferedOp::Set {
            key: key.as_ref().to_string(),
            value: value.into(),
            ttl: Some(ttl),
        });
        Ok(self)
    }

    pub fn remove(&mut self, key: impl AsRef<str>) -> Result<&mut Self, VibeEngineError> {
        self.ops.push(BufferedOp::Remove {
            key: key.as_ref().to_string(),
        });
        Ok(self)
    }
}

fn ttl_to_expires_at(ttl: Option<Duration>) -> i64 {
    match ttl {
        None => EXPIRES_AT_NEVER,
        Some(d) => crate::platform::now().saturating_add(d.as_millis() as i64),
    }
}

fn current_user_id_blocking(
    db_client: &VibeDbClient,
    executor: &VibeEngineExecutor,
) -> Result<String, VibeEngineError> {
    let db_client = db_client.clone();
    executor.invoke(async move { db_client.current_user_id().await })?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::engine::VibeEngine;
    use crate::api::engine_config::{VibeEngineConfig, VibeStoreBackend};
    use crate::api::engine_error::VibeEngineErrorCode;
    use crate::api::platform_type::VibePlatformType;
    use std::time::Duration;

    fn build_config(name: &str) -> VibeEngineConfig {
        let store_path = std::env::temp_dir().join(format!(
            "vibe-ready-kv-store-{}-{}",
            name,
            crate::platform::now()
        ));
        VibeEngineConfig::builder()
            .platform(VibePlatformType::MacOS)
            .app_name(name)
            .namespace("tests")
            .runtime_worker_threads(1)
            .callback_threads(1)
            .queue_capacity(16, 8)
            .store_root_path(store_path)
            .build()
    }

    #[test]
    fn kv_store_supports_set_get_remove_contains_and_list_keys() -> Result<(), VibeEngineError> {
        let config = build_config("kv-store-test");
        let engine = VibeEngine::create(config.clone())?;
        let store = engine.store();

        store.set_str("name", "vibe")?;
        store.set_bool("enabled", true)?;
        store.set_i32("count", 3)?;

        #[cfg(feature = "store-diesel-sqlite")]
        {
            assert_eq!(store.get_str("name")?, Some("vibe".to_string()));
            assert_eq!(store.get_bool("enabled")?, Some(true));
            assert_eq!(store.get_i32("count")?, Some(3));
            assert_eq!(store.get_str("enabled")?, None);
            assert!(store.contains("name")?);
            assert_eq!(store.list_keys()?, vec!["count", "enabled", "name"]);
            assert!(store.remove("name")?);
            assert!(!store.contains("name")?);
        }

        #[cfg(not(feature = "store-diesel-sqlite"))]
        {
            assert_eq!(store.get("name")?, None);
            assert!(!store.contains("name")?);
            assert!(store.list_keys()?.is_empty());
            assert!(!store.remove("name")?);
        }

        engine.destroy_with_timeout(Duration::from_secs(5))?;

        #[cfg(feature = "store-diesel-sqlite")]
        {
            let engine = VibeEngine::create(config)?;
            let store = engine.store();
            assert_eq!(store.get_bool("enabled")?, Some(true));
            assert_eq!(store.get_i32("count")?, Some(3));
            assert_eq!(store.get_str("name")?, None);
            engine.destroy_with_timeout(Duration::from_secs(5))?;
        }

        Ok(())
    }

    #[test]
    fn kv_store_noop_backend_discards_values() -> Result<(), VibeEngineError> {
        let store_path = std::env::temp_dir().join(format!(
            "vibe-ready-kv-store-noop-{}",
            crate::platform::now()
        ));
        let config = VibeEngineConfig::builder()
            .platform(VibePlatformType::MacOS)
            .app_name("kv-store-noop-test")
            .namespace("tests")
            .runtime_worker_threads(1)
            .callback_threads(1)
            .queue_capacity(16, 8)
            .store_root_path(store_path)
            .store_backend(VibeStoreBackend::Noop)
            .build();
        let engine = VibeEngine::create(config)?;
        let store = engine.store();

        store.set_str("name", "vibe")?;

        assert_eq!(store.get_str("name")?, None);
        assert!(!store.contains("name")?);
        assert!(store.list_keys()?.is_empty());
        assert!(!store.remove("name")?);

        engine.destroy_with_timeout(Duration::from_secs(5))?;
        Ok(())
    }

    #[cfg(feature = "store-diesel-sqlite")]
    #[test]
    fn kv_store_supports_extended_value_types() -> Result<(), VibeEngineError> {
        let engine = VibeEngine::create(build_config("kv-extended-types"))?;
        let store = engine.store();

        store.set("big", VibeKvValue::I64(i64::MAX))?;
        store.set("ratio", VibeKvValue::F64(3.14))?;
        store.set("blob", VibeKvValue::Bytes(vec![1, 2, 3, 4]))?;
        store.set(
            "json",
            VibeKvValue::Json(serde_json::json!({ "a": 1, "b": [true, "x"] })),
        )?;

        assert_eq!(store.get("big")?.and_then(|v| v.as_i64()), Some(i64::MAX));
        assert_eq!(store.get("ratio")?.and_then(|v| v.as_f64()), Some(3.14));
        assert_eq!(
            store.get("blob")?.and_then(|v| v.as_bytes().map(|b| b.to_vec())),
            Some(vec![1, 2, 3, 4])
        );
        assert_eq!(
            store.get("json")?.and_then(|v| v.as_json().cloned()),
            Some(serde_json::json!({ "a": 1, "b": [true, "x"] }))
        );

        engine.destroy_with_timeout(Duration::from_secs(5))?;
        Ok(())
    }

    #[cfg(feature = "store-diesel-sqlite")]
    #[test]
    fn kv_store_supports_buckets() -> Result<(), VibeEngineError> {
        let engine = VibeEngine::create(build_config("kv-buckets"))?;
        let store = engine.store();
        let settings = store.bucket("settings");
        let cache = store.bucket("cache");

        settings.set("theme", "dark")?;
        cache.set("theme", "light")?;

        assert_eq!(
            settings.get("theme")?.and_then(|v| v.as_str().map(|s| s.to_string())),
            Some("dark".to_string())
        );
        assert_eq!(
            cache.get("theme")?.and_then(|v| v.as_str().map(|s| s.to_string())),
            Some("light".to_string())
        );
        assert!(store.get_str("theme")?.is_none());

        engine.destroy_with_timeout(Duration::from_secs(5))?;
        Ok(())
    }

    #[cfg(feature = "store-diesel-sqlite")]
    #[test]
    fn kv_store_transaction_rolls_back_on_error() -> Result<(), VibeEngineError> {
        use crate::api::engine_error::VibeEngineErrorCode;
        let engine = VibeEngine::create(build_config("kv-tx-rollback"))?;
        let store = engine.store();

        store.set_str("keep", "before")?;

        let result: Result<(), VibeEngineError> = store.transaction(|tx| {
            tx.set("keep", "after")?;
            tx.set("new", "value")?;
            Err(VibeEngineError::from_error_code(
                VibeEngineErrorCode::ParameterEmpty,
            ))
        });
        assert!(result.is_err());

        // Closure failed before commit -> nothing applied.
        assert_eq!(store.get_str("keep")?, Some("before".to_string()));
        assert_eq!(store.get_str("new")?, None);

        engine.destroy_with_timeout(Duration::from_secs(5))?;
        Ok(())
    }

    #[cfg(feature = "store-diesel-sqlite")]
    #[test]
    fn kv_store_batch_ops_are_atomic() -> Result<(), VibeEngineError> {
        let engine = VibeEngine::create(build_config("kv-batch"))?;
        let store = engine.store();

        store.set_many(vec![
            ("a", VibeKvValue::I32(1)),
            ("b", VibeKvValue::I32(2)),
            ("c", VibeKvValue::I32(3)),
        ])?;
        let mut got = store.get_many(vec!["a", "b", "c"])?;
        got.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].0, "a");
        assert_eq!(got[1].0, "b");
        assert_eq!(got[2].0, "c");

        store.remove_many(vec!["a", "b"])?;
        assert!(!store.contains("a")?);
        assert!(!store.contains("b")?);
        assert!(store.contains("c")?);

        engine.destroy_with_timeout(Duration::from_secs(5))?;
        Ok(())
    }

    #[cfg(feature = "store-diesel-sqlite")]
    #[test]
    fn kv_store_ttl_expires_after_deadline() -> Result<(), VibeEngineError> {
        let engine = VibeEngine::create(build_config("kv-ttl"))?;
        let store = engine.store();

        store.set_with_ttl("temp", "value", Duration::from_millis(200))?;
        assert!(store.contains("temp")?);
        std::thread::sleep(Duration::from_millis(350));
        assert_eq!(store.get_str("temp")?, None);
        assert!(!store.contains("temp")?);

        // Active purge clears the row.
        store.set_with_ttl("temp2", "v", Duration::from_millis(50))?;
        std::thread::sleep(Duration::from_millis(120));
        let purged = store.purge_expired()?;
        assert!(purged >= 1);

        engine.destroy_with_timeout(Duration::from_secs(5))?;
        Ok(())
    }

    #[cfg(feature = "store-diesel-sqlite")]
    #[test]
    fn kv_store_change_listener_fires() -> Result<(), VibeEngineError> {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let engine = VibeEngine::create(build_config("kv-listener"))?;
        let store = engine.store();
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = Arc::clone(&count);
        let id = store.on_change("conf.*", move |_change| {
            count_clone.fetch_add(1, Ordering::SeqCst);
        });

        store.set_str("conf.theme", "dark")?;
        store.set_str("conf.lang", "en")?;
        store.set_str("other", "x")?;
        // Give callback pool a tick.
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(count.load(Ordering::SeqCst), 2);

        assert!(store.off_change(id));
        store.set_str("conf.theme", "light")?;
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(count.load(Ordering::SeqCst), 2);

        engine.destroy_with_timeout(Duration::from_secs(5))?;
        Ok(())
    }

    #[cfg(feature = "store-diesel-sqlite")]
    #[test]
    fn kv_store_migrates_legacy_v0_database() -> Result<(), VibeEngineError> {
        use diesel::connection::SimpleConnection;
        use diesel::prelude::*;
        use diesel::sqlite::SqliteConnection;

        // Build a v0 schema (matches pre-B8 layout) and pre-populate it.
        let store_root =
            std::env::temp_dir().join(format!("vibe-ready-kv-migrate-{}", crate::platform::now()));
        // The engine derives the actual DB path as
        // `<store_root>/<namespace>/<app_name>/work/loona_desktop_storage.db`.
        let work_dir = store_root.join("tests").join("kv-migrate").join("work");
        std::fs::create_dir_all(&work_dir).map_err(|err| {
            VibeEngineError::from_error_code(VibeEngineErrorCode::IOError).with_source(err.to_string())
        })?;
        let db_path = work_dir.join("loona_desktop_storage.db");
        {
            let db_path_str = db_path.to_str().ok_or_else(|| {
                VibeEngineError::from_error_code_msg(
                    VibeEngineErrorCode::ConfigError,
                    "database path is not valid UTF-8".to_string(),
                )
            })?;
            let mut conn = SqliteConnection::establish(db_path_str).map_err(|err| {
                VibeEngineError::from_error_code(VibeEngineErrorCode::DatabaseOpenFailed)
                    .with_source(err.to_string())
            })?;
            conn.batch_execute(
                "CREATE TABLE vibe_ready_key_val (
                    user_id TEXT NOT NULL,
                    \"key\" TEXT NOT NULL,
                    value_type SMALLINT NOT NULL,
                    value_str TEXT NOT NULL,
                    value_bool BOOLEAN NOT NULL,
                    value_i32 INTEGER NOT NULL,
                    PRIMARY KEY(user_id, \"key\")
                );
                INSERT INTO vibe_ready_key_val
                    (user_id, \"key\", value_type, value_str, value_bool, value_i32)
                VALUES
                    ('tests:kv-migrate', 'legacy_str', 1, 'kept', 0, 0),
                    ('tests:kv-migrate', 'legacy_bool', 2, '', 1, 0),
                    ('tests:kv-migrate', 'legacy_i32', 3, '', 0, 42);",
            )
            .map_err(|err| {
                VibeEngineError::from_error_code(VibeEngineErrorCode::DatabaseIOError)
                    .with_source(err.to_string())
            })?;
        }

        let config = VibeEngineConfig::builder()
            .platform(VibePlatformType::MacOS)
            .app_name("kv-migrate")
            .namespace("tests")
            .runtime_worker_threads(1)
            .callback_threads(1)
            .queue_capacity(16, 8)
            .store_root_path(store_root)
            .build();
        let engine = VibeEngine::create(config)?;
        let store = engine.store();

        assert_eq!(store.get_str("legacy_str")?, Some("kept".to_string()));
        assert_eq!(store.get_bool("legacy_bool")?, Some(true));
        assert_eq!(store.get_i32("legacy_i32")?, Some(42));

        // The new schema also accepts new types.
        store.set("upgraded", VibeKvValue::I64(7))?;
        assert_eq!(store.get("upgraded")?.and_then(|v| v.as_i64()), Some(7));

        engine.destroy_with_timeout(Duration::from_secs(5))?;
        Ok(())
    }
}
