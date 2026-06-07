//! End-to-end HTTP scenarios against an in-process localhost server.
#![cfg(feature = "net-http")]

use std::io::{Read, Write};
use std::net::TcpListener;
use std::time::Duration;

use vibe_ready::{VibeHttpClient, VibeRetryPolicy};

fn http_response(status: u16, reason: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

/// Serves each provided raw HTTP response once, in order, one per connection.
fn spawn_responses(responses: Vec<String>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    let base = format!("http://{addr}");
    std::thread::spawn(move || {
        for body in responses {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let mut buf = [0u8; 2048];
                    let _ = stream.read(&mut buf);
                    let _ = stream.write_all(body.as_bytes());
                    let _ = stream.flush();
                }
                Err(_) => break,
            }
        }
    });
    base
}

/// Accepts connections but never replies, to trigger client timeouts.
fn spawn_silent() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    let base = format!("http://{addr}");
    std::thread::spawn(move || {
        while let Ok((stream, _)) = listener.accept() {
            std::thread::sleep(Duration::from_secs(2));
            drop(stream);
        }
    });
    base
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
}

#[test]
fn get_reads_json_body() {
    let base = spawn_responses(vec![http_response(200, "OK", "{\"ok\":true}")]);
    let client = VibeHttpClient::new().expect("client");
    runtime().block_on(async move {
        let response = client.get(&base).await.expect("get");
        assert_eq!(response.status(), 200);
        let value: serde_json::Value = response.json().await.expect("json");
        assert_eq!(value["ok"], serde_json::json!(true));
    });
}

#[test]
fn post_json_sends_body_and_succeeds() {
    let base = spawn_responses(vec![http_response(200, "OK", "{\"created\":1}")]);
    let client = VibeHttpClient::new().expect("client");
    runtime().block_on(async move {
        let payload = serde_json::json!({ "name": "vibe" });
        let response = client.post_json(&base, &payload).await.expect("post");
        assert!(response.is_success());
    });
}

#[test]
fn error_for_status_maps_404() {
    let base = spawn_responses(vec![http_response(404, "Not Found", "{}")]);
    let client = VibeHttpClient::new().expect("client");
    runtime().block_on(async move {
        let response = client.get(&base).await.expect("get");
        let err = response.error_for_status().expect_err("expected error");
        // 404 maps to BadRequest in the network error model.
        assert_eq!(err.kind(), vibe_ready::VibeErrorKind::Network);
    });
}

#[test]
fn retries_then_succeeds_on_503() {
    let base = spawn_responses(vec![
        http_response(503, "Service Unavailable", "{}"),
        http_response(200, "OK", "{\"ok\":true}"),
    ]);
    let client = VibeHttpClient::builder()
        .retry(
            VibeRetryPolicy::default()
                .initial_backoff(Duration::from_millis(1))
                .jitter(false),
        )
        .build()
        .expect("client");
    runtime().block_on(async move {
        let response = client.get(&base).await.expect("get");
        assert_eq!(response.status(), 200);
    });
}

#[test]
fn timeout_returns_timeout_error() {
    let base = spawn_silent();
    let client = VibeHttpClient::builder()
        .timeout(Duration::from_millis(150))
        .retry(VibeRetryPolicy::none())
        .build()
        .expect("client");
    runtime().block_on(async move {
        let err = client.get(&base).await.expect_err("expected timeout");
        assert_eq!(
            err.code(),
            vibe_ready::VibeError::from_error_code(vibe_ready::VibeErrorCode::TimeoutError).code()
        );
    });
}
