use lazy_static::lazy_static;
use std::sync::Arc;
use tokio::sync::Mutex;

pub trait DbSqlPerfListenerTrait:
Fn(
    /*sql*/ String,
    /*table_page_read_count*/ i32,
    /*table_page_write_count*/ i32,
    /*index_page_read_count*/ i32,
    /*index_page_write_count*/ i32,
    /*overflow_page_read_count*/ i32,
    /*overflow_page_write_count*/ i32,
    /*cost_in_nanoseconds*/ u64,
) + Send
{

}

pub type DbSqlPerfListener = Box<dyn DbSqlPerfListenerTrait>;

impl<T> DbSqlPerfListenerTrait for T where T: Fn(String, i32, i32, i32, i32, i32, i32, u64) + Send {}

lazy_static! {
    pub static ref GLOBAL_DB_SQL_PERF_LISTENER: Arc<Mutex<Option<DbSqlPerfListener>>> =
        Arc::new(Mutex::new(None));
}

pub fn on_sql_perf(sql: String, cost_in_nanoseconds: u64) {
    if let Ok(listener_opt) = GLOBAL_DB_SQL_PERF_LISTENER.try_lock() {
        if let Some(listener) = &*listener_opt {
            listener(sql, 0, 0, 0, 0, 0, 0, cost_in_nanoseconds);
        }
    } else {
        println!("Error occurred while locking GLOBAL_DB_SQL_PERF_LISTENER");
    }
}
