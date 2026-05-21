use crate::log::log_def::DESC;
use crate::store::db::db_impl::db_diesel::{DbKvOp, VibeDbSqlite};
use crate::store::db::enums::db_error::{DbError, VibeDbErrorInfo};
use crate::store::db::tables::key_val::VibeTableKeyVal;
use crate::{log_db_e, log_db_r, log_db_t, log_e};
use std::path::PathBuf;
use std::sync::Arc;

pub struct VibeDbWorkerDiesel {
    pub db_sqlite: Arc<Option<VibeDbSqlite>>,
}

impl VibeDbWorkerDiesel {
    pub fn new() -> Self {
        VibeDbWorkerDiesel {
            db_sqlite: Arc::new(None),
        }
    }

    pub async fn try_open(
        &mut self,
        store_path: PathBuf,
        user_id: String,
        is_encrypt: bool,
    ) -> Result<(), DbError> {
        let db_sqlite = VibeDbSqlite::try_open(store_path, user_id, is_encrypt).map_err(|ext| {
            log_e!(
                "try_open",
                DESC,
                format!("VibeDbSqlite::try_open: {:?}", ext)
            );
            ext.code()
        })?;

        self.db_sqlite = Arc::new(Some(db_sqlite));

        Ok(())
    }

    pub async fn close(&mut self) -> Result<(), DbError> {
        let method_name = "close";
        log_db_t!(method_name);
        let db_opt_ref = self.db_sqlite.as_ref();
        let Some(db) = db_opt_ref.as_ref() else {
            return Ok(());
        };
        db.close();

        self.db_sqlite = Arc::new(None);

        self.callback(method_name, Ok(()))
    }

    pub async fn insert_or_replace_key_val(&self, table: VibeTableKeyVal) -> Result<(), DbError> {
        let method_name = "insert_or_replace_key_val";
        let db = self.opened_db(method_name)?;
        self.callback(method_name, db.insert_or_replace_key_val(table))
    }

    pub async fn get_key_val(
        &self,
        user_id: String,
        bucket: String,
        key: String,
    ) -> Result<Option<VibeTableKeyVal>, DbError> {
        let method_name = "get_key_val";
        let db = self.opened_db(method_name)?;
        self.callback(method_name, db.get_key_val_in_bucket(&user_id, &bucket, &key))
    }

    pub async fn get_key_val_vec(
        &self,
        user_id: String,
        bucket: String,
        keys: Vec<String>,
    ) -> Result<Vec<VibeTableKeyVal>, DbError> {
        let method_name = "get_key_val_vec";
        let db = self.opened_db(method_name)?;
        self.callback(
            method_name,
            db.get_key_val_vec_in_bucket(&user_id, &bucket, keys),
        )
    }

    pub async fn remove_key_val(
        &self,
        user_id: String,
        bucket: String,
        key: String,
    ) -> Result<bool, DbError> {
        let method_name = "remove_key_val";
        let db = self.opened_db(method_name)?;
        self.callback(
            method_name,
            db.remove_key_val_in_bucket(&user_id, &bucket, &key),
        )
    }

    pub async fn contains_key_val(
        &self,
        user_id: String,
        bucket: String,
        key: String,
    ) -> Result<bool, DbError> {
        let method_name = "contains_key_val";
        let db = self.opened_db(method_name)?;
        self.callback(
            method_name,
            db.contains_key_val_in_bucket(&user_id, &bucket, &key),
        )
    }

    pub async fn list_key_vals(
        &self,
        user_id: String,
        bucket: String,
    ) -> Result<Vec<String>, DbError> {
        let method_name = "list_key_vals";
        let db = self.opened_db(method_name)?;
        self.callback(method_name, db.list_key_vals_in_bucket(&user_id, &bucket))
    }

    pub async fn transaction(&self, ops: Vec<DbKvOp>) -> Result<(), DbError> {
        let method_name = "transaction";
        let db = self.opened_db(method_name)?;
        self.callback(method_name, db.transaction(ops))
    }

    pub async fn purge_expired(&self, now_ms: i64) -> Result<usize, DbError> {
        let method_name = "purge_expired";
        let db = self.opened_db(method_name)?;
        self.callback(method_name, db.purge_expired(now_ms))
    }

    fn opened_db(&self, method_name: &str) -> Result<&VibeDbSqlite, DbError> {
        self.db_sqlite
            .as_ref()
            .as_ref()
            .ok_or_else(|| self.callback_error(method_name, DbError::NotOpen))
    }

    fn callback<T>(
        &self,
        method_name: &str,
        ret: Result<T, VibeDbErrorInfo>,
    ) -> Result<T, DbError> {
        match ret {
            Ok(v) => {
                log_db_r!(method_name, "code", 0);
                Ok(v)
            }
            Err(info) => {
                log_db_e!(
                    method_name,
                    "code|location|desc|sql",
                    info.code(),
                    info.location(),
                    info.desc(),
                    info.sql()
                );
                Err(info.code())
            }
        }
    }

    fn callback_error(&self, method_name: &str, err: DbError) -> DbError {
        log_e!(method_name, "code", err);
        err
    }
}
