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
    /// Backend open operation failed.
    OpenFailed,
    /// Backend reported an I/O error.
    DatabaseIOError,
    /// Operation requires an opened database.
    NotOpen,
    /// Requested row was not found.
    TargetNotFound,

    /// Database worker thread or channel failed.
    DatabaseThreadError,

    /// Database lock was poisoned or could not be acquired.
    DatabaseUnlockError,

    /// Worker join operation failed.
    JoinError,

    /// Backend does not support the requested operation yet.
    NotSupportedYet,
    /// Page token argument is invalid.
    InvalidArgumentPageToken,
    /// Backend returned a business-level error.
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
    /// Creates error info from a Diesel connection error.
    ///
    /// # Returns
    ///
    /// A [`VibeDbErrorInfo`] with [`DbError::OpenFailed`].
    pub fn from_connection(value: ConnectionError) -> Self {
        let location = Self::gen_location(Location::caller());
        VibeDbErrorInfo::new(location, value.to_string(), DbError::OpenFailed, None)
    }

    #[track_caller]
    #[cfg(any(feature = "log-diesel", feature = "store-diesel-sqlite"))]
    /// Creates error info from a Diesel query error.
    ///
    /// # Returns
    ///
    /// A [`VibeDbErrorInfo`] mapped to [`DbError::TargetNotFound`] or
    /// [`DbError::DatabaseIOError`] depending on the Diesel error kind.
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

    /// Creates error info from a poisoned lock.
    ///
    /// # Returns
    ///
    /// A [`VibeDbErrorInfo`] with [`DbError::DatabaseUnlockError`].
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

    /// Creates error info for a database worker thread failure.
    ///
    /// # Returns
    ///
    /// A [`VibeDbErrorInfo`] with [`DbError::DatabaseThreadError`].
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

    /// Creates error info from a Tokio join error.
    ///
    /// # Returns
    ///
    /// A [`VibeDbErrorInfo`] with [`DbError::JoinError`].
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

    /// Creates error info for an I/O or open failure.
    ///
    /// # Returns
    ///
    /// A [`VibeDbErrorInfo`] with [`DbError::OpenFailed`].
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

    /// Creates error info for a missing target.
    ///
    /// # Returns
    ///
    /// A [`VibeDbErrorInfo`] with [`DbError::TargetNotFound`].
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

    /// Creates error info for an unsupported operation.
    ///
    /// # Returns
    ///
    /// A [`VibeDbErrorInfo`] with [`DbError::NotSupportedYet`].
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
    /// Creates detailed database error information.
    ///
    /// # Returns
    ///
    /// A [`VibeDbErrorInfo`] containing location, description, code, and SQL text.
    ///
    /// # Examples
    ///
    /// ```
    /// use vibe_ready::{DbError, VibeDbErrorInfo};
    ///
    /// let info = VibeDbErrorInfo::new("file.rs:1:1".into(), "failed".into(), DbError::OpenFailed, None);
    /// assert_eq!(info.code(), DbError::OpenFailed);
    /// ```
    pub fn new(location: String, desc: String, code: DbError, sql: Option<String>) -> Self {
        Self {
            location,
            desc,
            code,
            sql,
        }
    }

    /// Returns the source-code location captured for this error.
    ///
    /// # Returns
    ///
    /// A cloned location string.
    pub fn location(&self) -> String {
        self.location.clone()
    }

    /// Returns the database error description.
    ///
    /// # Returns
    ///
    /// A cloned description string.
    pub fn desc(&self) -> String {
        self.desc.clone()
    }

    /// Returns the database error category.
    ///
    /// # Returns
    ///
    /// The [`DbError`] variant stored in this error info.
    pub fn code(&self) -> DbError {
        self.code.clone()
    }

    /// Returns the SQL associated with this error, if any.
    ///
    /// # Returns
    ///
    /// A cloned SQL string, or an empty string when no SQL was captured.
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

#[cfg(test)]
mod strict_tests {
    use super::*;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test/unit/store/db_error_tests.rs"
    ));
}
