use crate::api::engine_config::VibeLogBackend;
use crate::log::log_backend::VibeLogBackendHandle;
use crate::log::log_def::{LogListener, VibeLogInfo};
use crate::log::log_level::LogLevel;
use crate::platform;
use crate::store::db::enums::db_error::VibeDbErrorInfo;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};

enum ChannelData {
    Print(VibeLogInfo),
    SetFilter(LogLevel),
    SetListener(Option<LogListener>),
}

/// Low-level logger that writes SDK log records and dispatches log listeners.
///
/// Most applications should use [`crate::VibeEngine::insert_log`] and
/// [`crate::VibeEngine::set_log_listener`] instead of constructing this type.
pub struct VibeLogger {
    db_log: Arc<VibeLogBackendHandle>,
    tx: Mutex<Option<Sender<ChannelData>>>,
    is_close: AtomicBool,
    write_to_store: AtomicBool,
    output_stdout: AtomicBool,
}

impl VibeLogger {
    /// Opens a logger using the selected backend and filtering behavior.
    ///
    /// # Returns
    ///
    /// `Ok(VibeLogger)` when the backend opens, or [`VibeDbErrorInfo`] if the
    /// log backend cannot be initialized.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::path::PathBuf;
    /// use vibe_ready::{VibeLogBackend, VibeLogLevel, VibeLogger};
    ///
    /// # fn demo() -> Result<(), vibe_ready::VibeDbErrorInfo> {
    /// let logger = VibeLogger::try_new(
    ///     VibeLogBackend::Noop,
    ///     PathBuf::from("/tmp/vibe-ready-log"),
    ///     false,
    ///     "demo:user".to_string(),
    ///     VibeLogLevel::Info,
    ///     false,
    ///     true,
    ///     1000,
    /// )?;
    /// logger.close()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn try_new(
        backend: VibeLogBackend,
        store_path: PathBuf,
        is_encrypt: bool,
        user_id: String,
        level: LogLevel,
        write_to_store: bool,
        output_stdout: bool,
        max_rows: usize,
    ) -> Result<Self, VibeDbErrorInfo> {
        let (tx, rx) = std::sync::mpsc::channel::<ChannelData>();
        std::thread::spawn(move || {
            let mut filter = level;
            let mut log_lsr: Option<Box<dyn Fn(VibeLogInfo) + Send + Sync>> = None;

            for data in rx {
                match data {
                    ChannelData::Print(log) => {
                        if log.level <= filter {
                            if let Some(lsr) = &log_lsr {
                                lsr(log);
                            }
                        }
                    }
                    ChannelData::SetFilter(level) => filter = level,
                    ChannelData::SetListener(lsr) => log_lsr = lsr,
                }
            }
        });
        let log =
            VibeLogBackendHandle::try_open(backend, store_path, is_encrypt, user_id, max_rows)?;
        Ok(VibeLogger {
            db_log: Arc::new(log),
            tx: Mutex::new(Some(tx)),
            is_close: AtomicBool::new(false),
            write_to_store: AtomicBool::new(write_to_store),
            output_stdout: AtomicBool::new(output_stdout),
        })
    }

    /// Registers this logger as the global receiver for the exported log macros.
    ///
    /// # Returns
    ///
    /// This method returns `()` after installing the listener when possible.
    pub fn register_listener(&self) {
        let db_log_clone = self.db_log.clone();
        let write_to_store = self.write_to_store.load(Ordering::SeqCst);
        let tx_clone = match self.tx.lock() {
            Ok(guard) => guard.clone(),
            Err(_) => None,
        };
        let Some(tx_clone) = tx_clone else {
            return;
        };
        VibeLogger::register_log_listener(Some(
            move |_location: String, level: LogLevel, tag: String, content: String| {
                let create_time = platform::now();
                let log_info =
                    VibeLogInfo::new(level, tag.to_string(), content.clone(), create_time);
                if write_to_store {
                    db_log_clone.send_2_writer(log_info.clone());
                }
                if let Err(e) = tx_clone.send(ChannelData::Print(log_info)) {
                    eprintln!("print log err: {:?}", e);
                }
            },
        ));
    }

    /// Inserts a log entry into the backend and listener pipeline.
    ///
    /// # Returns
    ///
    /// This method returns `()`; closed loggers or channel failures are reported
    /// to stderr.
    pub fn insert_log(
        &self,
        should_output_log: bool,
        level: i32,
        tag: String,
        content: String,
        create_time: i64,
    ) {
        if self.is_close.load(Ordering::SeqCst) {
            eprintln!("VibeLogger has been released");
            return;
        }
        let db_log_clone = self.db_log.clone();
        let log_info = VibeLogInfo::new(LogLevel::from(level), tag, content, create_time);
        if should_output_log && self.output_stdout.load(Ordering::SeqCst) {
            println!("log->log_info: {:?}", log_info);
        }
        if self.write_to_store.load(Ordering::SeqCst) {
            db_log_clone.send_2_writer(log_info.clone());
        }
        if let Ok(guard) = self.tx.lock() {
            if let Some(tx) = guard.as_ref() {
                if let Err(e) = tx.send(ChannelData::Print(log_info)) {
                    eprintln!("print log err: {:?}", e);
                }
            }
        }
    }

    /// Sets the minimum severity delivered to the in-process listener.
    ///
    /// # Returns
    ///
    /// This method returns `()` after queuing the filter change.
    pub fn set_filter(&self, level: LogLevel) {
        if let Ok(guard) = self.tx.lock() {
            if let Some(tx) = guard.as_ref() {
                if let Err(e) = tx.send(ChannelData::SetFilter(level)) {
                    eprintln!("set log filter err: {:?}", e);
                }
            }
        }
    }

    /// Sets or clears the in-process log listener.
    ///
    /// # Returns
    ///
    /// This method returns `()` after queuing the listener change.
    pub fn set_log_listener(&self, listener: Option<LogListener>) {
        if self.is_close.load(Ordering::SeqCst) {
            eprintln!("VibeLogger has been released");
            return;
        }
        if let Ok(guard) = self.tx.lock() {
            if let Some(tx) = guard.as_ref() {
                if let Err(e) = tx.send(ChannelData::SetListener(listener)) {
                    eprintln!("set log listener err: {:?}", e);
                }
            }
        }
    }

    /// Closes the logger and its backing log store.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the logger is closed or already closed; [`VibeDbErrorInfo`]
    /// if the backend close operation fails.
    pub fn close(&self) -> Result<(), VibeDbErrorInfo> {
        if self.is_close.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        VibeLogger::clear_global_log_listener();
        if let Ok(mut guard) = self.tx.lock() {
            if let Some(tx) = guard.take() {
                let _ = tx.send(ChannelData::SetListener(None));
            }
        }
        self.db_log.close()
    }
}

#[cfg(test)]
mod strict_tests {
    use super::*;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test/unit/log/logger_tests.rs"
    ));
}
