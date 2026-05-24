use std::sync::mpsc;
use std::time::Duration;

#[test]
fn sql_exception_dispatches_global_and_db_listeners_and_can_clear() {
    futures::executor::block_on(async {
        *GLOBAL_DB_SQL_EXCEPTION_LISTENER.lock().await = None;
        *DB_EXCEPTION_LISTENER.lock().await = None;
    });

    let (tx, rx) = mpsc::channel();
    let global_tx = tx.clone();
    futures::executor::block_on(async {
        *GLOBAL_DB_SQL_EXCEPTION_LISTENER.lock().await = Some(Box::new(move |description, code| {
            global_tx
                .send(("global", description, code))
                .expect("send global exception");
        }));
        *DB_EXCEPTION_LISTENER.lock().await = Some(Box::new(move |description, code| {
            tx.send(("db", description, code))
                .expect("send db exception");
        }));
    });

    on_sql_exception("disk full".to_string(), -7);
    let mut events = [
        rx.recv_timeout(Duration::from_secs(1)).expect("global event"),
        rx.recv_timeout(Duration::from_secs(1)).expect("db event"),
    ];
    events.sort_by_key(|event| event.0);
    assert_eq!(events[0], ("db", String::new(), -7));
    assert_eq!(events[1], ("global", "disk full".to_string(), -7));

    futures::executor::block_on(async {
        *GLOBAL_DB_SQL_EXCEPTION_LISTENER.lock().await = None;
        *DB_EXCEPTION_LISTENER.lock().await = None;
    });
    on_sql_exception("ignored".to_string(), 1);
    assert!(rx.recv_timeout(Duration::from_millis(80)).is_err());
}
