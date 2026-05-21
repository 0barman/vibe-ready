#[cfg(any(feature = "log-diesel", feature = "store-diesel-sqlite"))]
use diesel::result::Error as DieselError;
#[cfg(any(feature = "log-diesel", feature = "store-diesel-sqlite"))]
use diesel::ConnectionError;
use serde::Serialize;
use std::panic::Location;
use std::sync::PoisonError;
use tokio::task::JoinError;

#[repr(i32)]
#[derive(Debug, Clone, PartialEq, Serialize)]
/// Database error category used by storage backends.
pub enum DbError {
    OpenFailed,
    DatabaseIOError,
    NotOpen,
    TargetNotFound,

    DatabaseThreadError,

    DatabaseUnlockError,

    JoinError,

    NotSupportedYet,
    InvalidArgumentPageToken,
    GeneralBusinessError,
}

#[derive(Debug)]
/// Detailed database error information for diagnostics.
pub struct VibeDbErrorInfo {
    location: String,
    desc: String,
    code: DbError,
    sql: Option<String>,
}

impl VibeDbErrorInfo {
    #[track_caller]
    #[cfg(any(feature = "log-diesel", feature = "store-diesel-sqlite"))]
    pub fn from_connection(value: ConnectionError) -> Self {
        let location = Self::gen_location(Location::caller());
        VibeDbErrorInfo::new(location, value.to_string(), DbError::OpenFailed, None)
    }

    #[track_caller]
    #[cfg(any(feature = "log-diesel", feature = "store-diesel-sqlite"))]
    pub fn from_diesel(value: DieselError, sql: Option<&str>) -> Self {
        let location = Self::gen_location(Location::caller());
        let code = match value {
            DieselError::NotFound => DbError::TargetNotFound,
            DieselError::DatabaseError(_, _) => DbError::DatabaseIOError,
            _ => DbError::DatabaseIOError,
        };
        VibeDbErrorInfo::new(
            location,
            value.to_string(),
            code,
            sql.map(std::string::ToString::to_string),
        )
    }

    #[track_caller]
    pub fn from_lock<T>(error: PoisonError<T>) -> Self {
        let location = Self::gen_location(Location::caller());
        let ext = VibeDbErrorInfo::new(
            location.clone(),
            error.to_string(),
            DbError::DatabaseUnlockError,
            None,
        );
        ext
    }

    #[track_caller]
    pub fn from_thread(desc: String) -> Self {
        let location = Self::gen_location(Location::caller());
        let ext = VibeDbErrorInfo::new(
            location.clone(),
            desc.clone(),
            DbError::DatabaseThreadError,
            None,
        );
        ext
    }

    #[track_caller]
    pub fn from_join_error(db_error: JoinError) -> Self {
        let location = Self::gen_location(Location::caller());
        let ext = VibeDbErrorInfo::new(
            location.clone(),
            db_error.to_string(),
            DbError::JoinError,
            None,
        );
        ext
    }

    #[track_caller]
    pub fn from_io(desc: String) -> Self {
        let location = Self::gen_location(Location::caller());
        let ext = VibeDbErrorInfo::new(
            location.clone(),
            desc.to_string(),
            DbError::OpenFailed,
            None,
        );
        ext
    }

    #[track_caller]
    pub fn from_not_found() -> Self {
        let location = Self::gen_location(Location::caller());
        let ext = VibeDbErrorInfo::new(
            location.clone(),
            "Target Not Found".to_string(),
            DbError::TargetNotFound,
            None,
        );
        ext
    }

    #[track_caller]
    pub fn from_not_supported(desc: String) -> Self {
        let location = Self::gen_location(Location::caller());
        VibeDbErrorInfo::new(location, desc, DbError::NotSupportedYet, None)
    }

    fn gen_location(location: &'static Location<'static>) -> String {
        let location_str = format!(
            "{}:{}:{}",
            location.file(),
            location.line(),
            location.column()
        );
        location_str
    }
}

impl VibeDbErrorInfo {
    pub fn new(location: String, desc: String, code: DbError, sql: Option<String>) -> Self {
        Self {
            location,
            desc,
            code,
            sql,
        }
    }

    pub fn location(&self) -> String {
        self.location.clone()
    }

    pub fn desc(&self) -> String {
        self.desc.clone()
    }

    pub fn code(&self) -> DbError {
        self.code.clone()
    }

    pub fn sql(&self) -> String {
        match &self.sql {
            None => String::from(""),
            Some(val) => val.clone(),
        }
    }
}

impl std::fmt::Display for VibeDbErrorInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DbError[{:?}] at {}: {}{}",
            self.code,
            self.location,
            self.desc,
            if let Some(sql) = &self.sql {
                format!(" (SQL: {})", sql)
            } else {
                String::new()
            }
        )
    }
}
