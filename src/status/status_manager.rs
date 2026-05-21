use crate::api::connection_status::VibeConnectionStatus;
use crate::api::engine_error::VibeEngineError;
use crate::{log_e, log_s, platform};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Callback invoked when the connection status changes.
pub type ConnectionStatusListener = Box<dyn Fn(VibeConnectionStatus) + Send + Sync + 'static>;

#[derive(Default)]
/// Tracks and publishes connection status changes.
pub struct VibeStatusManager {
    conn_status: Mutex<VibeConnectionStatus>,
    conn_status_listener: Arc<Mutex<Option<ConnectionStatusListener>>>,
}

impl VibeStatusManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn get_connection_status(&self) -> VibeConnectionStatus {
        *self.conn_status.lock().await
    }

    pub async fn set_connection_status_listener(
        &self,
        listener_opt: Option<ConnectionStatusListener>,
    ) {
        match listener_opt {
            Some(listener) => {
                self.conn_status_listener.lock().await.replace(listener);
            }
            None => {
                self.conn_status_listener.lock().await.take();
            }
        }
    }

    pub async fn set_connection_status(
        &self,
        status: VibeConnectionStatus,
    ) -> Result<VibeConnectionStatus, VibeEngineError> {
        let method = "set_server_tcp_connection_status";
        let mut conn_status_lock = self.conn_status.lock().await;
        let last_status = *conn_status_lock;
        log_s!(
            method,
            "from_status|to_status",
            last_status.to_string(),
            status.to_string()
        );
        let ret = conn_status_lock.trans(status);
        if let Err(err) = ret {
            log_e!(method, "trans_ret", err.to_string());
            return Err(err);
        }
        log_s!(method, "trans_ret", "success");
        if last_status != status {
            let listener_clone = self.conn_status_listener.clone();
            platform::spawn(async move {
                let listener_opt_lock = listener_clone.lock().await;
                if let Some(listener) = listener_opt_lock.as_ref() {
                    listener(status);
                }
            });
        }
        Ok(status)
    }
}
