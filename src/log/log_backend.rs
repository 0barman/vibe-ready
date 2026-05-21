use crate::api::engine_config::VibeLogBackend;
use crate::log::log_def::VibeLogInfo;
use crate::store::db::enums::db_error::VibeDbErrorInfo;
use std::path::PathBuf;

pub enum VibeLogBackendHandle {
    Noop,
    #[cfg(feature = "log-diesel")]
    DieselSqlite(crate::log::log_diesel::VibeDbLog),
}

impl VibeLogBackendHandle {
    pub fn try_open(
        backend: VibeLogBackend,
        store_path: PathBuf,
        is_encrypt: bool,
        user_id: String,
        max_rows: usize,
    ) -> Result<Self, VibeDbErrorInfo> {
        match backend {
            VibeLogBackend::Noop => Ok(Self::Noop),
            VibeLogBackend::DieselSqlite => {
                #[cfg(feature = "log-diesel")]
                {
                    crate::log::log_diesel::VibeDbLog::try_open(
                        store_path, is_encrypt, user_id, max_rows,
                    )
                    .map(Self::DieselSqlite)
                }
                #[cfg(not(feature = "log-diesel"))]
                {
                    let _ = (store_path, is_encrypt, user_id, max_rows);
                    Err(VibeDbErrorInfo::from_not_supported(
                        "log backend DieselSqlite requires feature log-diesel".to_string(),
                    ))
                }
            }
        }
    }

    pub fn send_2_writer(&self, log: VibeLogInfo) {
        match self {
            Self::Noop => drop(log),
            #[cfg(feature = "log-diesel")]
            Self::DieselSqlite(backend) => backend.send_2_writer(log),
        }
    }

    pub fn close(&self) -> Result<(), VibeDbErrorInfo> {
        match self {
            Self::Noop => Ok(()),
            #[cfg(feature = "log-diesel")]
            Self::DieselSqlite(backend) => backend.close(),
        }
    }
}
