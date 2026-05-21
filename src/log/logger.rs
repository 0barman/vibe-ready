use crate::api::engine_config::VibeLogBackend;
use crate::log::log_backend::VibeLogBackendHandle;
use crate::log::log_def::{LogListener, LogType, VibeLogInfo};
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

pub struct VibeLogger {
    db_log: Arc<VibeLogBackendHandle>,
    tx: Mutex<Option<Sender<ChannelData>>>,
    is_close: AtomicBool,
    write_to_store: AtomicBool,
    output_stdout: AtomicBool,
}

impl VibeLogger {
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
            move |_location: String,
                  level: LogLevel,
                  _log_type: LogType,
                  tag: String,
                  content: String| {
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

    pub fn set_filter(&self, level: LogLevel) {
        if let Ok(guard) = self.tx.lock() {
            if let Some(tx) = guard.as_ref() {
                if let Err(e) = tx.send(ChannelData::SetFilter(level)) {
                    eprintln!("set log filter err: {:?}", e);
                }
            }
        }
    }

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
