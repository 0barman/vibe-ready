use crate::log_db_e;
use lazy_static::lazy_static;
use std::sync::Arc;
use tokio::sync::Mutex;

pub trait DbSqlExceptionListenerTrait: Fn(/* exception description */String, i32) + Send {

}

pub type DbSqlExceptionListener = Box<dyn DbSqlExceptionListenerTrait>;

impl<T> DbSqlExceptionListenerTrait for T where T: Fn(String, i32) + Send {}

lazy_static! {
    pub static ref GLOBAL_DB_SQL_EXCEPTION_LISTENER: Arc<Mutex<Option<DbSqlExceptionListener>>> =
        Arc::new(Mutex::new(None));
    pub static ref DB_EXCEPTION_LISTENER: Arc<Mutex<Option<DbSqlExceptionListener>>> =
        Arc::new(Mutex::new(None));
}

pub fn on_sql_exception(description: String, code: i32) {
    let listener_opt = GLOBAL_DB_SQL_EXCEPTION_LISTENER.try_lock();
    if let Ok(listener_opt) = listener_opt {
        if let Some(listener) = &*listener_opt {
            listener(description, code);
        }
    } else {
        log_db_e!(
            "on_sql_exception",
            "desc",
            "Error occurred while locking GLOBAL_DB_SQL_EXCEPTION_LISTENER"
        );
    }

    let listener_opt = DB_EXCEPTION_LISTENER.try_lock();
    if let Ok(listener_opt) = listener_opt {
        if let Some(listener) = &*listener_opt {
            listener(String::new(), code);
        }
    } else {
        log_db_e!(
            "on_sql_exception",
            "desc",
            "Error occurred while locking DB_EXCEPTION_LISTENER"
        );
    }
}
