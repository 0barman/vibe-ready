use std::sync::mpsc;
use std::time::Duration;

#[test]
fn callback_executor_covers_boxed_and_string_wrappers() {
    let executor = VibeCallbackExecutor::new(ThreadPool::new(2));
    let (tx, rx) = mpsc::channel();

    executor.boxed_fn0({
        let tx = tx.clone();
        move || tx.send("zero".to_string()).expect("send zero")
    })();
    executor.boxed_fn({
        let tx = tx.clone();
        move |value: i32| tx.send(format!("one:{value}")).expect("send one")
    })(7);
    executor.boxed_fn2({
        let tx = tx.clone();
        move |a: i32, b: i32| tx.send(format!("two:{}", a + b)).expect("send two")
    })(2, 3);
    executor.boxed_ref_fn2({
        let tx = tx.clone();
        move |name: &String, value: i32| tx.send(format!("ref:{name}:{value}")).expect("send ref")
    })(&"name".to_string(), 9);
    executor.boxed_str_fn({
        let tx = tx.clone();
        move |value| tx.send(format!("str:{value}")).expect("send str")
    })("demo");
    executor.boxed_str_str4_fn({
        let tx = tx.clone();
        move |a, b: i32, c: i32, d| tx.send(format!("str4:{a}:{b}:{c}:{d}")).expect("send str4")
    })("left", 1, 2, "right");

    let mut values = Vec::new();
    for _ in 0..6 {
        values.push(rx.recv_timeout(Duration::from_secs(1)).expect("callback result"));
    }
    values.sort();
    assert_eq!(
        values,
        vec!["one:7", "ref:name:9", "str4:left:1:2:right", "str:demo", "two:5", "zero"]
    );
}

#[test]
fn run_blocking_on_engine_runtime_converts_panics_to_runtime_errors() {
    let runtime = Arc::new(Runtime::new().expect("runtime"));
    let (async_tx, _async_rx) = tokio::sync::mpsc::channel(1);
    let (sync_tx, _sync_rx) = tokio::sync::mpsc::channel(1);
    let (_shutdown_tx, shutdown_rx) = std::sync::mpsc::channel();
    let executor = VibeEngineExecutor::new(
        ThreadPool::new(1),
        async_tx,
        sync_tx,
        VibeRuntimeHandle::owned(runtime),
        shutdown_rx,
    );

    let error = executor
        .run_blocking_on_engine_rt(|| panic!("expected panic"))
        .expect_err("panic should be converted");
    assert_eq!(error.code(), VibeEngineErrorCode::RuntimeError.code());
}
