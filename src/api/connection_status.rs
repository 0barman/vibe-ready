use crate::api::engine_error::{VibeEngineError, VibeEngineErrorCode};
use crate::log::log_def::DESC;
use crate::{err, log_e};
use serde::{Deserialize, Serialize};
use std::backtrace::Backtrace;
use std::fmt;

#[repr(i32)]
#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Serialize, Eq, Default)]
pub enum VibeConnectionStatus {
    #[default]
    Idle,

    Connecting,

    Connected,

    Disconnecting,

    Disconnected,
}

impl fmt::Display for VibeConnectionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            VibeConnectionStatus::Idle => "Idle",
            VibeConnectionStatus::Connecting => "Connecting",
            VibeConnectionStatus::Connected => "Connected",
            VibeConnectionStatus::Disconnecting => "Disconnecting",
            VibeConnectionStatus::Disconnected => "Disconnected",
        };
        write!(f, "{}", s)
    }
}

fn trans_unreachable(
    from: VibeConnectionStatus,
    to: VibeConnectionStatus,
) -> Result<VibeConnectionStatus, VibeEngineError> {
    log_e!(
        "trans_unreachable",
        DESC,
        format!(
            "trans_unreachable_test error , from: {:?},to: {:?}, -- {:?}",
            from,
            to,
            Backtrace::capture()
        )
    );
    Err(err!(VibeEngineError::from_internal_error()))
}

impl VibeConnectionStatus {
    pub fn trans(
        &mut self,
        new_status: VibeConnectionStatus,
    ) -> Result<VibeConnectionStatus, VibeEngineError> {
        let ret = match self {
            VibeConnectionStatus::Idle => match new_status {
                VibeConnectionStatus::Idle => Ok(VibeConnectionStatus::Idle),
                VibeConnectionStatus::Connecting => Ok(VibeConnectionStatus::Connecting),
                VibeConnectionStatus::Connected => trans_unreachable(self.clone(), new_status),
                VibeConnectionStatus::Disconnecting => Err(err!(VibeEngineError::from_error_code(
                    VibeEngineErrorCode::ConnectionClosed
                ))),
                VibeConnectionStatus::Disconnected => Ok(VibeConnectionStatus::Disconnected),
            },
            VibeConnectionStatus::Connecting => match new_status {
                VibeConnectionStatus::Idle => Ok(VibeConnectionStatus::Idle),
                VibeConnectionStatus::Connecting => trans_unreachable(self.clone(), new_status),
                VibeConnectionStatus::Connected => Ok(VibeConnectionStatus::Connected),
                VibeConnectionStatus::Disconnecting => Ok(VibeConnectionStatus::Disconnecting),
                VibeConnectionStatus::Disconnected => Ok(VibeConnectionStatus::Disconnected),
            },
            VibeConnectionStatus::Connected => match new_status {
                VibeConnectionStatus::Idle => Ok(VibeConnectionStatus::Idle),
                VibeConnectionStatus::Connecting => Err(VibeEngineError::from_error_code(
                    VibeEngineErrorCode::ConnectionExists,
                )),
                VibeConnectionStatus::Connected => trans_unreachable(self.clone(), new_status),
                VibeConnectionStatus::Disconnecting => Ok(VibeConnectionStatus::Disconnecting),
                VibeConnectionStatus::Disconnected => Ok(VibeConnectionStatus::Disconnected),
            },
            VibeConnectionStatus::Disconnecting => match new_status {
                VibeConnectionStatus::Idle => Ok(VibeConnectionStatus::Idle),
                VibeConnectionStatus::Connecting => Ok(VibeConnectionStatus::Connecting),
                VibeConnectionStatus::Connected => trans_unreachable(self.clone(), new_status),
                VibeConnectionStatus::Disconnecting => Err(err!(VibeEngineError::from_error_code(
                    VibeEngineErrorCode::ConnectionClosing
                ))),
                VibeConnectionStatus::Disconnected => Ok(VibeConnectionStatus::Disconnected),
            },
            VibeConnectionStatus::Disconnected => match new_status {
                VibeConnectionStatus::Idle => Ok(VibeConnectionStatus::Idle),
                VibeConnectionStatus::Connecting => Ok(VibeConnectionStatus::Connecting),
                VibeConnectionStatus::Connected => trans_unreachable(self.clone(), new_status),
                VibeConnectionStatus::Disconnecting => Err(err!(VibeEngineError::from_error_code(
                    VibeEngineErrorCode::ConnectionClosed,
                ))),
                VibeConnectionStatus::Disconnected => Ok(VibeConnectionStatus::Disconnected),
            },
        };
        *self = ret?;
        Ok(*self)
    }
}
