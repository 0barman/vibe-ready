use crate::log::log_def::VibeLogInfo;
use crate::log::log_def::DESC;
use crate::log::table_log::{vibe_ready_log, VibeTableLog, TABLE_NAME_LOG};
use crate::log_e;
use crate::store::db::db_common;
use crate::store::db::db_common::{LOG_DB_NAME, LOG_ENC_DB_NAME};
use crate::store::db::enums::db_error::VibeDbErrorInfo;
use diesel::connection::SimpleConnection;
use diesel::dsl::max;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use log::{error, info};
use std::fs;
use std::path::PathBuf;
use std::sync::mpsc::Sender;
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;

enum ChannelData {
    _STOP,
    INSERT(VibeLogInfo),
}

pub struct VibeDbLog {
    #[allow(dead_code)]
    pub db_lock: Arc<Mutex<SqliteConnection>>,
    tx_4_log_writer: std::sync::mpsc::Sender<ChannelData>,
}

fn get_loop_log_id(db_lock: Arc<Mutex<SqliteConnection>>) -> i64 {
    let mut db = match db_lock.lock() {
        Ok(db) => db,
        Err(_) => return -1,
    };

    vibe_ready_log::table
        .select(max(vibe_ready_log::id))
        .first::<Option<i64>>(&mut *db)
        .ok()
        .flatten()
        .unwrap_or(-1)
}

fn insert_batch_log(db_lock: &Arc<Mutex<SqliteConnection>>, vec_log: Vec<VibeTableLog>) {
    if vec_log.is_empty() {
        return;
    }
    let mut db = match db_lock.lock() {
        Ok(db) => db,
        Err(e) => {
            error!("log_diesel -> lock db failed: {:?}", e);
            return;
        }
    };

    let ret = diesel::replace_into(vibe_ready_log::table)
        .values(&vec_log)
        .execute(&mut *db);
    if let Err(e) = ret {
        error!("log_diesel -> error inserting log: {:?}", e);
    }
}

fn start_log_writer(
    db_lock: Arc<Mutex<SqliteConnection>>,
    max_rows: usize,
) -> (Sender<ChannelData>, JoinHandle<()>) {
    let db_lock_clone = db_lock.clone();
    let (tx, rx) = mpsc::channel::<ChannelData>();
    let max_rows = max_rows as i64;
    let mut loop_id = get_loop_log_id(db_lock);
    let handle = std::thread::spawn(move || {
        for data in rx {
            let data = match data {
                ChannelData::_STOP => {
                    info!("log_diesel -> write thread stop");
                    break;
                }
                ChannelData::INSERT(log) => log,
            };
            let mut vec_log = vec![];

            for insert_log in data.slice() {
                loop_id = if loop_id + 1 >= max_rows {
                    0
                } else {
                    loop_id + 1
                };
                vec_log.push(VibeTableLog::new(loop_id, &insert_log));
            }

            insert_batch_log(&db_lock_clone, vec_log);
        }
    });
    (tx, handle)
}

impl VibeDbLog {
    pub fn try_open(
        store_path: PathBuf,
        is_encrypt: bool,
        user_id: String,
        max_rows: usize,
    ) -> Result<Self, VibeDbErrorInfo> {
        let store_path = store_path.clone();

        fs::create_dir_all(store_path.as_path())
            .map_err(|e| VibeDbErrorInfo::from_io(e.to_string()))?;

        let (store_path, _password) = db_common::get_db_name_pwd(
            store_path,
            user_id,
            is_encrypt,
            LOG_DB_NAME,
            LOG_ENC_DB_NAME,
        )?;

        let store_path_str = store_path.to_str().ok_or_else(|| {
            let err_msg = "log db path is null".to_string();
            log_e!("get_default_db", DESC, err_msg);
            VibeDbErrorInfo::from_io(err_msg)
        })?;

        if is_encrypt {
            log_e!(
                "log_try_open",
                DESC,
                "diesel sqlite does not enable SQLCipher encryption by default"
            );
        }

        let mut db = SqliteConnection::establish(store_path_str)
            .map_err(VibeDbErrorInfo::from_connection)?;
        Self::create_tables(&mut db)?;

        let db_lock = Arc::new(Mutex::new(db));
        let (tx_4_log_writer, _writer_handler) = start_log_writer(db_lock.clone(), max_rows);

        Ok(Self {
            db_lock,
            tx_4_log_writer,
        })
    }

    fn create_tables(db: &mut SqliteConnection) -> Result<(), VibeDbErrorInfo> {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {TABLE_NAME_LOG} (
                id INTEGER PRIMARY KEY NOT NULL,
                level SMALLINT NOT NULL,
                tag TEXT NOT NULL,
                content TEXT NOT NULL,
                create_time BIGINT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS index_create_time ON {TABLE_NAME_LOG} (create_time);"
        );
        db.batch_execute(&sql)
            .map_err(|error| VibeDbErrorInfo::from_diesel(error, Some(&sql)))
    }

    pub fn send_2_writer(&self, log: VibeLogInfo) {
        if let Err(e) = self.tx_4_log_writer.send(ChannelData::INSERT(log)) {
            error!(
                "log_diesel -> send_2_writer failed, log will be dropped. reason: {:?}",
                e.to_string()
            );
        }
    }

    pub fn close(&self) -> Result<(), VibeDbErrorInfo> {
        let _ = self.tx_4_log_writer.send(ChannelData::_STOP);
        Ok(())
    }
}

#[cfg(test)]
mod strict_tests {
    use super::*;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test/unit/log/log_diesel_tests.rs"
    ));
}
