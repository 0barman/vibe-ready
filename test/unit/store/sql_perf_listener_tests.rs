use std::sync::mpsc;
use std::time::Duration;

#[test]
fn sql_perf_dispatches_cost_and_zero_page_counts() {
    futures::executor::block_on(async {
        *GLOBAL_DB_SQL_PERF_LISTENER.lock().await = None;
    });

    let (tx, rx) = mpsc::channel();
    futures::executor::block_on(async {
        *GLOBAL_DB_SQL_PERF_LISTENER.lock().await = Some(Box::new(
            move |sql, table_read, table_write, index_read, index_write, overflow_read, overflow_write, cost| {
                tx.send((
                    sql,
                    table_read,
                    table_write,
                    index_read,
                    index_write,
                    overflow_read,
                    overflow_write,
                    cost,
                ))
                .expect("send perf event");
            },
        ));
    });

    on_sql_perf("select 1".to_string(), u64::MAX);
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1)).expect("perf event"),
        ("select 1".to_string(), 0, 0, 0, 0, 0, 0, u64::MAX)
    );

    futures::executor::block_on(async {
        *GLOBAL_DB_SQL_PERF_LISTENER.lock().await = None;
    });
    on_sql_perf("ignored".to_string(), 1);
    assert!(rx.recv_timeout(Duration::from_millis(80)).is_err());
}
