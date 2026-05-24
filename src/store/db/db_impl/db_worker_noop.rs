use crate::store::db::enums::db_error::DbError;
use crate::store::db::tables::key_val::VibeTableKeyVal;
use futures::channel::mpsc::Receiver;
use futures::StreamExt;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use tokio::runtime;

pub fn start_worker_loop(mut rx: Receiver<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>) {
    std::thread::spawn(move || {
        let runtime_ret = runtime::Builder::new_current_thread().enable_all().build();
        if let Ok(runtime) = runtime_ret {
            runtime.block_on(async move {
                while let Some(task) = rx.next().await {
                    task.await;
                }
            });
        };
    });
}

pub struct VibeDbWorkerNoop;

impl VibeDbWorkerNoop {
    pub fn new() -> Self {
        Self
    }

    pub async fn try_open(
        &mut self,
        _store_path: PathBuf,
        _user_id: String,
        _is_encrypt: bool,
    ) -> Result<(), DbError> {
        Ok(())
    }

    pub async fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    pub async fn insert_or_replace_key_val(&self, _table: VibeTableKeyVal) -> Result<(), DbError> {
        Ok(())
    }

    pub async fn get_key_val(
        &self,
        _user_id: String,
        _bucket: String,
        _key: String,
    ) -> Result<Option<VibeTableKeyVal>, DbError> {
        Ok(None)
    }

    pub async fn get_key_val_vec(
        &self,
        _user_id: String,
        _bucket: String,
        _keys: Vec<String>,
    ) -> Result<Vec<VibeTableKeyVal>, DbError> {
        Ok(Vec::new())
    }

    pub async fn remove_key_val(
        &self,
        _user_id: String,
        _bucket: String,
        _key: String,
    ) -> Result<bool, DbError> {
        Ok(false)
    }

    pub async fn contains_key_val(
        &self,
        _user_id: String,
        _bucket: String,
        _key: String,
    ) -> Result<bool, DbError> {
        Ok(false)
    }

    pub async fn list_key_vals(
        &self,
        _user_id: String,
        _bucket: String,
    ) -> Result<Vec<String>, DbError> {
        Ok(Vec::new())
    }

    pub async fn purge_expired(&self, _now_ms: i64) -> Result<usize, DbError> {
        Ok(0)
    }
}

#[cfg(test)]
mod strict_tests {
    use super::*;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test/unit/store/db_worker_noop_tests.rs"
    ));
}
