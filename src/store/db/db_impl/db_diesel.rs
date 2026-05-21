use crate::log::log_def::DESC;
use crate::store::db::db_common;
use crate::store::db::db_common::{WORK_DB_NAME, WORK_ENC_DB_NAME};
use crate::store::db::db_impl::sql_exception_listener::{
    DbSqlExceptionListenerTrait, DB_EXCEPTION_LISTENER, GLOBAL_DB_SQL_EXCEPTION_LISTENER,
};
use crate::store::db::db_impl::sql_perf_listener::{
    DbSqlPerfListenerTrait, GLOBAL_DB_SQL_PERF_LISTENER,
};
use crate::store::db::enums::db_error::VibeDbErrorInfo;
use crate::store::db::tables::key_val::{
    vibe_ready_key_val, VibeKvValue, VibeTableKeyVal, DEFAULT_BUCKET, EXPIRES_AT_NEVER,
    TABLE_NAME_KEY_VAL, TABLE_NAME_KV_META,
};
use crate::{log_db_e, log_db_r, log_db_t};
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use diesel::Connection;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

pub type DbDatabaseMigrationListener = Box<dyn Fn(/*progress*/ i32) + Send + Sync + 'static>;

/// Latest schema version for the KV store. Bump together with a migration step
/// inside [`VibeDbSqlite::run_migrations`].
const KV_SCHEMA_VERSION: i32 = 1;

/// Single write/remove op accepted by [`VibeDbSqlite::transaction`].
#[derive(Debug, Clone)]
pub enum DbKvOp {
    Set(VibeTableKeyVal),
    Remove {
        user_id: String,
        bucket: String,
        key: String,
    },
}

#[derive(Clone)]
pub struct VibeDbSqlite {
    pub db_lock: Arc<Mutex<SqliteConnection>>,
    pub user_id: String,
}

impl VibeDbSqlite {
    pub fn try_open(
        store_path: PathBuf,
        user_id: String,
        is_encrypt: bool,
    ) -> Result<Self, VibeDbErrorInfo> {
        let method_name = "try_open";
        let store_path_str = store_path.to_str().unwrap_or("");
        log_db_t!(
            method_name,
            "user_id|is_encrypt|store_path",
            user_id,
            is_encrypt,
            store_path_str
        );

        let mut conn = Self::get_default_db(store_path, user_id.clone(), is_encrypt)?;
        Self::run_migrations(&mut conn)?;

        log_db_r!(method_name);
        Ok(Self {
            db_lock: Arc::new(Mutex::new(conn)),
            user_id,
        })
    }

    pub fn close(&self) {}

    pub fn register_sql_perf_listener<T>(listener_opt: Option<T>)
    where
        T: DbSqlPerfListenerTrait + 'static,
    {
        match GLOBAL_DB_SQL_PERF_LISTENER.try_lock() {
            Ok(mut my_listener) => match listener_opt {
                None => {
                    *my_listener = None;
                }
                Some(lsr) => {
                    *my_listener = Some(Box::new(lsr));
                }
            },
            Err(error) => {
                log_db_e!("register_sql_perf_listener", DESC, error.to_string());
            }
        }
    }

    pub fn register_sql_exception_listener<T>(listener_opt: Option<T>)
    where
        T: DbSqlExceptionListenerTrait + 'static,
    {
        match GLOBAL_DB_SQL_EXCEPTION_LISTENER.try_lock() {
            Ok(mut my_listener) => match listener_opt {
                None => {
                    *my_listener = None;
                }
                Some(lsr) => {
                    *my_listener = Some(Box::new(lsr));
                }
            },
            Err(error) => {
                log_db_e!("register_sql_exception_listener", DESC, error.to_string());
            }
        }
    }

    pub fn register_db_exception_listener<T>(listener_opt: Option<T>)
    where
        T: DbSqlExceptionListenerTrait + 'static,
    {
        match DB_EXCEPTION_LISTENER.try_lock() {
            Ok(mut my_listener) => match listener_opt {
                None => {
                    *my_listener = None;
                }
                Some(lsr) => {
                    *my_listener = Some(Box::new(lsr));
                }
            },
            Err(error) => {
                log_db_e!("register_db_exception_listener", DESC, error.to_string());
            }
        }
    }

    pub fn un_register_db_listener() {
        if let Ok(mut listener) = GLOBAL_DB_SQL_PERF_LISTENER.try_lock() {
            *listener = None;
        }
        if let Ok(mut listener) = GLOBAL_DB_SQL_EXCEPTION_LISTENER.try_lock() {
            *listener = None;
        }
        if let Ok(mut listener) = DB_EXCEPTION_LISTENER.try_lock() {
            *listener = None;
        }
    }

    fn run_migrations(conn: &mut SqliteConnection) -> Result<(), VibeDbErrorInfo> {
        // Always-on settings & meta table.
        let bootstrap = format!(
            "PRAGMA journal_mode = WAL;
             CREATE TABLE IF NOT EXISTS {TABLE_NAME_KV_META} (
                meta_key TEXT PRIMARY KEY NOT NULL,
                meta_val TEXT NOT NULL
             );"
        );
        conn.batch_execute(&bootstrap)
            .map_err(|error| VibeDbErrorInfo::from_diesel(error, Some(&bootstrap)))?;

        let current = Self::read_schema_version(conn)?;

        // Determine if we are dealing with a legacy schema (table exists but no
        // `bucket` column). Legacy databases have `current == 0` and no meta row.
        let legacy = Self::table_exists(conn, TABLE_NAME_KEY_VAL)?
            && !Self::column_exists(conn, TABLE_NAME_KEY_VAL, "bucket")?;

        if current >= KV_SCHEMA_VERSION && !legacy {
            return Ok(());
        }

        if legacy {
            Self::migrate_v0_to_v1(conn)?;
        } else if !Self::table_exists(conn, TABLE_NAME_KEY_VAL)? {
            Self::create_v1_table(conn)?;
        } else if !Self::column_exists(conn, TABLE_NAME_KEY_VAL, "expires_at_ms")? {
            // Half-migrated state defensive path: rebuild table.
            Self::migrate_v0_to_v1(conn)?;
        }

        Self::write_schema_version(conn, KV_SCHEMA_VERSION)?;
        Ok(())
    }

    fn create_v1_table(conn: &mut SqliteConnection) -> Result<(), VibeDbErrorInfo> {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {TABLE_NAME_KEY_VAL} (
                user_id TEXT NOT NULL,
                bucket TEXT NOT NULL DEFAULT '{DEFAULT_BUCKET}',
                \"key\" TEXT NOT NULL,
                value_type SMALLINT NOT NULL,
                value_str TEXT NOT NULL DEFAULT '',
                value_bool BOOLEAN NOT NULL DEFAULT 0,
                value_i32 INTEGER NOT NULL DEFAULT 0,
                value_i64 BIGINT NOT NULL DEFAULT 0,
                value_f64 DOUBLE NOT NULL DEFAULT 0,
                value_bytes BLOB NOT NULL DEFAULT (X''),
                value_json TEXT NOT NULL DEFAULT '',
                expires_at_ms BIGINT NOT NULL DEFAULT 0,
                PRIMARY KEY(user_id, bucket, \"key\")
            );
            CREATE INDEX IF NOT EXISTS index_key_val_user_id_bucket_key
                ON {TABLE_NAME_KEY_VAL} (user_id, bucket, \"key\");
            CREATE INDEX IF NOT EXISTS index_key_val_expires_at
                ON {TABLE_NAME_KEY_VAL} (expires_at_ms);"
        );
        conn.batch_execute(&sql)
            .map_err(|error| VibeDbErrorInfo::from_diesel(error, Some(&sql)))
    }

    fn migrate_v0_to_v1(conn: &mut SqliteConnection) -> Result<(), VibeDbErrorInfo> {
        // Rename old, create new, copy data, drop old, recreate indexes.
        let sql = format!(
            "ALTER TABLE {TABLE_NAME_KEY_VAL} RENAME TO {TABLE_NAME_KEY_VAL}_legacy_v0;
            CREATE TABLE {TABLE_NAME_KEY_VAL} (
                user_id TEXT NOT NULL,
                bucket TEXT NOT NULL DEFAULT '{DEFAULT_BUCKET}',
                \"key\" TEXT NOT NULL,
                value_type SMALLINT NOT NULL,
                value_str TEXT NOT NULL DEFAULT '',
                value_bool BOOLEAN NOT NULL DEFAULT 0,
                value_i32 INTEGER NOT NULL DEFAULT 0,
                value_i64 BIGINT NOT NULL DEFAULT 0,
                value_f64 DOUBLE NOT NULL DEFAULT 0,
                value_bytes BLOB NOT NULL DEFAULT (X''),
                value_json TEXT NOT NULL DEFAULT '',
                expires_at_ms BIGINT NOT NULL DEFAULT 0,
                PRIMARY KEY(user_id, bucket, \"key\")
            );
            INSERT INTO {TABLE_NAME_KEY_VAL}
                (user_id, bucket, \"key\", value_type, value_str, value_bool, value_i32,
                 value_i64, value_f64, value_bytes, value_json, expires_at_ms)
                SELECT user_id, '{DEFAULT_BUCKET}', \"key\", value_type, value_str, value_bool,
                       value_i32, 0, 0, X'', '', 0
                  FROM {TABLE_NAME_KEY_VAL}_legacy_v0;
            DROP TABLE {TABLE_NAME_KEY_VAL}_legacy_v0;
            CREATE INDEX IF NOT EXISTS index_key_val_user_id_bucket_key
                ON {TABLE_NAME_KEY_VAL} (user_id, bucket, \"key\");
            CREATE INDEX IF NOT EXISTS index_key_val_expires_at
                ON {TABLE_NAME_KEY_VAL} (expires_at_ms);"
        );
        conn.batch_execute(&sql)
            .map_err(|error| VibeDbErrorInfo::from_diesel(error, Some(&sql)))
    }

    fn read_schema_version(conn: &mut SqliteConnection) -> Result<i32, VibeDbErrorInfo> {
        let sql = format!(
            "SELECT meta_val FROM {TABLE_NAME_KV_META} WHERE meta_key = 'schema_version'"
        );
        match diesel::sql_query(&sql).load::<MetaValueRow>(conn) {
            Ok(rows) => {
                if let Some(row) = rows.into_iter().next() {
                    Ok(row.meta_val.parse().unwrap_or(0))
                } else {
                    Ok(0)
                }
            }
            Err(error) => Err(VibeDbErrorInfo::from_diesel(error, Some(&sql))),
        }
    }

    fn write_schema_version(
        conn: &mut SqliteConnection,
        version: i32,
    ) -> Result<(), VibeDbErrorInfo> {
        let sql = format!(
            "INSERT INTO {TABLE_NAME_KV_META} (meta_key, meta_val) VALUES ('schema_version', '{version}')
             ON CONFLICT(meta_key) DO UPDATE SET meta_val = excluded.meta_val;"
        );
        conn.batch_execute(&sql)
            .map_err(|error| VibeDbErrorInfo::from_diesel(error, Some(&sql)))
    }

    fn table_exists(conn: &mut SqliteConnection, name: &str) -> Result<bool, VibeDbErrorInfo> {
        let sql = format!(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='{name}'"
        );
        diesel::sql_query(&sql)
            .load::<NameRow>(conn)
            .map(|rows| !rows.is_empty())
            .map_err(|error| VibeDbErrorInfo::from_diesel(error, Some(&sql)))
    }

    fn column_exists(
        conn: &mut SqliteConnection,
        table: &str,
        column: &str,
    ) -> Result<bool, VibeDbErrorInfo> {
        let sql = format!("PRAGMA table_info({table})");
        let rows: Vec<PragmaInfoRow> = diesel::sql_query(&sql)
            .load(conn)
            .map_err(|error| VibeDbErrorInfo::from_diesel(error, Some(&sql)))?;
        Ok(rows.iter().any(|r| r.name == column))
    }
}

#[derive(QueryableByName)]
struct MetaValueRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    meta_val: String,
}

#[derive(QueryableByName)]
struct NameRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    #[allow(dead_code)]
    name: String,
}

#[derive(QueryableByName)]
struct PragmaInfoRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    name: String,
}

impl VibeDbSqlite {
    pub fn insert_or_replace_key_val(&self, table: VibeTableKeyVal) -> Result<(), VibeDbErrorInfo> {
        let sql = "replace key value";
        let mut db = self.db_lock.lock().map_err(VibeDbErrorInfo::from_lock)?;
        diesel::replace_into(vibe_ready_key_val::table)
            .values(&table)
            .execute(&mut *db)
            .map_err(|error| VibeDbErrorInfo::from_diesel(error, Some(sql)))?;
        Ok(())
    }

    /// Returns the row regardless of expiry; expiry filtering is applied by callers.
    pub fn get_key_val(
        &self,
        owner_user_id: &str,
        owner_key: &str,
    ) -> Result<Option<VibeTableKeyVal>, VibeDbErrorInfo> {
        self.get_key_val_in_bucket(owner_user_id, DEFAULT_BUCKET, owner_key)
    }

    pub fn get_key_val_in_bucket(
        &self,
        owner_user_id: &str,
        owner_bucket: &str,
        owner_key: &str,
    ) -> Result<Option<VibeTableKeyVal>, VibeDbErrorInfo> {
        let sql = "select key value";
        let mut db = self.db_lock.lock().map_err(VibeDbErrorInfo::from_lock)?;
        vibe_ready_key_val::table
            .filter(vibe_ready_key_val::user_id.eq(owner_user_id))
            .filter(vibe_ready_key_val::bucket.eq(owner_bucket))
            .filter(vibe_ready_key_val::key.eq(owner_key))
            .first::<VibeTableKeyVal>(&mut *db)
            .optional()
            .map_err(|error| VibeDbErrorInfo::from_diesel(error, Some(sql)))
    }

    pub fn get_key_val_vec(
        &self,
        owner_user_id: &str,
        keys: Vec<String>,
    ) -> Result<Vec<VibeTableKeyVal>, VibeDbErrorInfo> {
        self.get_key_val_vec_in_bucket(owner_user_id, DEFAULT_BUCKET, keys)
    }

    pub fn get_key_val_vec_in_bucket(
        &self,
        owner_user_id: &str,
        owner_bucket: &str,
        keys: Vec<String>,
    ) -> Result<Vec<VibeTableKeyVal>, VibeDbErrorInfo> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let sql = "select key values";
        let mut db = self.db_lock.lock().map_err(VibeDbErrorInfo::from_lock)?;
        vibe_ready_key_val::table
            .filter(vibe_ready_key_val::user_id.eq(owner_user_id))
            .filter(vibe_ready_key_val::bucket.eq(owner_bucket))
            .filter(vibe_ready_key_val::key.eq_any(keys))
            .order(vibe_ready_key_val::key.asc())
            .load::<VibeTableKeyVal>(&mut *db)
            .map_err(|error| VibeDbErrorInfo::from_diesel(error, Some(sql)))
    }

    pub fn remove_key_val(
        &self,
        owner_user_id: &str,
        owner_key: &str,
    ) -> Result<bool, VibeDbErrorInfo> {
        self.remove_key_val_in_bucket(owner_user_id, DEFAULT_BUCKET, owner_key)
    }

    pub fn remove_key_val_in_bucket(
        &self,
        owner_user_id: &str,
        owner_bucket: &str,
        owner_key: &str,
    ) -> Result<bool, VibeDbErrorInfo> {
        let sql = "delete key value";
        let mut db = self.db_lock.lock().map_err(VibeDbErrorInfo::from_lock)?;
        let deleted = diesel::delete(
            vibe_ready_key_val::table
                .filter(vibe_ready_key_val::user_id.eq(owner_user_id))
                .filter(vibe_ready_key_val::bucket.eq(owner_bucket))
                .filter(vibe_ready_key_val::key.eq(owner_key)),
        )
        .execute(&mut *db)
        .map_err(|error| VibeDbErrorInfo::from_diesel(error, Some(sql)))?;
        Ok(deleted > 0)
    }

    pub fn contains_key_val(
        &self,
        owner_user_id: &str,
        owner_key: &str,
    ) -> Result<bool, VibeDbErrorInfo> {
        self.contains_key_val_in_bucket(owner_user_id, DEFAULT_BUCKET, owner_key)
    }

    pub fn contains_key_val_in_bucket(
        &self,
        owner_user_id: &str,
        owner_bucket: &str,
        owner_key: &str,
    ) -> Result<bool, VibeDbErrorInfo> {
        let sql = "contains key value";
        let mut db = self.db_lock.lock().map_err(VibeDbErrorInfo::from_lock)?;
        let value = vibe_ready_key_val::table
            .select(vibe_ready_key_val::key)
            .filter(vibe_ready_key_val::user_id.eq(owner_user_id))
            .filter(vibe_ready_key_val::bucket.eq(owner_bucket))
            .filter(vibe_ready_key_val::key.eq(owner_key))
            .first::<String>(&mut *db)
            .optional()
            .map_err(|error| VibeDbErrorInfo::from_diesel(error, Some(sql)))?;
        Ok(value.is_some())
    }

    pub fn list_key_vals(&self, owner_user_id: &str) -> Result<Vec<String>, VibeDbErrorInfo> {
        self.list_key_vals_in_bucket(owner_user_id, DEFAULT_BUCKET)
    }

    pub fn list_key_vals_in_bucket(
        &self,
        owner_user_id: &str,
        owner_bucket: &str,
    ) -> Result<Vec<String>, VibeDbErrorInfo> {
        let sql = "list key values";
        let mut db = self.db_lock.lock().map_err(VibeDbErrorInfo::from_lock)?;
        vibe_ready_key_val::table
            .select(vibe_ready_key_val::key)
            .filter(vibe_ready_key_val::user_id.eq(owner_user_id))
            .filter(vibe_ready_key_val::bucket.eq(owner_bucket))
            .order(vibe_ready_key_val::key.asc())
            .load::<String>(&mut *db)
            .map_err(|error| VibeDbErrorInfo::from_diesel(error, Some(sql)))
    }

    /// Apply a list of writes/removes inside a single SQLite transaction. Any
    /// failure rolls the transaction back.
    pub fn transaction(&self, ops: Vec<DbKvOp>) -> Result<(), VibeDbErrorInfo> {
        let sql = "kv transaction";
        let mut db = self.db_lock.lock().map_err(VibeDbErrorInfo::from_lock)?;
        db.transaction::<(), diesel::result::Error, _>(|conn| {
            for op in ops {
                match op {
                    DbKvOp::Set(row) => {
                        diesel::replace_into(vibe_ready_key_val::table)
                            .values(&row)
                            .execute(conn)?;
                    }
                    DbKvOp::Remove {
                        user_id,
                        bucket,
                        key,
                    } => {
                        diesel::delete(
                            vibe_ready_key_val::table
                                .filter(vibe_ready_key_val::user_id.eq(user_id))
                                .filter(vibe_ready_key_val::bucket.eq(bucket))
                                .filter(vibe_ready_key_val::key.eq(key)),
                        )
                        .execute(conn)?;
                    }
                }
            }
            Ok(())
        })
        .map_err(|error| VibeDbErrorInfo::from_diesel(error, Some(sql)))
    }

    /// Delete all rows whose `expires_at_ms` is non-zero and `<= now_ms`.
    /// Returns the number of rows removed.
    pub fn purge_expired(&self, now_ms: i64) -> Result<usize, VibeDbErrorInfo> {
        let sql = "purge expired";
        let mut db = self.db_lock.lock().map_err(VibeDbErrorInfo::from_lock)?;
        diesel::delete(
            vibe_ready_key_val::table
                .filter(vibe_ready_key_val::expires_at_ms.ne(EXPIRES_AT_NEVER))
                .filter(vibe_ready_key_val::expires_at_ms.le(now_ms)),
        )
        .execute(&mut *db)
        .map_err(|error| VibeDbErrorInfo::from_diesel(error, Some(sql)))
    }
}

impl VibeDbSqlite {
    pub fn manual_backup(&self) -> Result<(), VibeDbErrorInfo> {
        Err(VibeDbErrorInfo::from_not_supported(
            "manual_backup is not supported by diesel sqlite yet".to_string(),
        ))
    }

    pub fn manual_retrieve(&self) -> Result<(), VibeDbErrorInfo> {
        Err(VibeDbErrorInfo::from_not_supported(
            "manual_retrieve is not supported by diesel sqlite yet".to_string(),
        ))
    }
}

impl VibeDbSqlite {
    fn get_default_db(
        store_path: PathBuf,
        user_id: String,
        is_encrypt: bool,
    ) -> Result<SqliteConnection, VibeDbErrorInfo> {
        let (store_path, _password) = Self::get_default_db_name(store_path, user_id, is_encrypt)?;

        let store_path_str = store_path.to_str().ok_or_else(|| {
            let err_msg = "db path is null".to_string();
            log_db_e!("get_default_db", DESC, err_msg);
            VibeDbErrorInfo::from_io(err_msg)
        })?;

        if is_encrypt {
            log_db_e!(
                "get_default_db",
                DESC,
                "diesel sqlite does not enable SQLCipher encryption by default"
            );
        }

        SqliteConnection::establish(store_path_str).map_err(VibeDbErrorInfo::from_connection)
    }

    fn get_default_db_name(
        store_path: PathBuf,
        user_id: String,
        is_encrypt: bool,
    ) -> Result<(/* db_path */ PathBuf, /* password */ String), VibeDbErrorInfo> {
        db_common::get_db_name_pwd(
            store_path,
            user_id,
            is_encrypt,
            WORK_DB_NAME,
            WORK_ENC_DB_NAME,
        )
    }
}

// Silence "unused" warnings on the helper enum from non-feature builds.
#[allow(dead_code)]
fn _touch(_: &VibeKvValue) {}
