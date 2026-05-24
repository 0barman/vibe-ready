#[tokio::test]
async fn spawn_returns_join_handle_for_future_output() {
    let handle = spawn(async { 42 });
    assert_eq!(handle.await.expect("spawned task"), 42);
}
