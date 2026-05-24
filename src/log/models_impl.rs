use crate::log::log_def::VibeLogInfo;
use crate::log::log_level::LogLevel;

impl VibeLogInfo {
    /// Creates a structured log record.
    ///
    /// # Returns
    ///
    /// A [`VibeLogInfo`] containing the provided level, tag, content, and timestamp.
    ///
    /// # Examples
    ///
    /// ```
    /// use vibe_ready::{VibeLogInfo, VibeLogLevel};
    ///
    /// let info = VibeLogInfo::new(VibeLogLevel::Info, "startup".into(), "ready".into(), 1);
    /// assert_eq!(info.tag(), "startup");
    /// ```
    pub fn new(level: LogLevel, tag: String, content: String, create_time: i64) -> VibeLogInfo {
        VibeLogInfo {
            level,
            tag,
            content,
            create_time,
        }
    }

    /// Returns the log severity.
    ///
    /// # Returns
    ///
    /// The [`LogLevel`] stored in the record.
    pub fn level(&self) -> LogLevel {
        self.level
    }

    /// Returns the log tag.
    ///
    /// # Returns
    ///
    /// A cloned tag string.
    pub fn tag(&self) -> String {
        self.tag.clone()
    }

    /// Returns the log content.
    ///
    /// # Returns
    ///
    /// A cloned content string.
    pub fn content(&self) -> String {
        self.content.clone()
    }

    /// Returns the creation timestamp.
    ///
    /// # Returns
    ///
    /// Unix timestamp in milliseconds.
    pub fn create_time(&self) -> i64 {
        self.create_time
    }

    /// Splits oversized log content into smaller log records.
    ///
    /// # Returns
    ///
    /// One record for normal content, or multiple records for content larger
    /// than the internal CSV chunk size.
    pub fn slice(&self) -> Vec<VibeLogInfo> {
        let size = self.to_csv().len();
        if size <= 1024 * 1024 {
            return vec![self.clone()];
        }

        let mut ret = vec![];
        let chars = self.content.chars().collect::<Vec<_>>();
        for (_, chunk) in chars.chunks(1024 * 1024 / 4).enumerate() {
            let content = chunk.iter().collect::<String>();
            ret.push(Self {
                content,
                ..self.clone()
            })
        }
        ret
    }

    /// Serializes the log record as a CSV row.
    ///
    /// # Returns
    ///
    /// A CSV-formatted string ending with a newline.
    pub fn to_csv(&self) -> String {
        format!(
            "{},{},{},\"{}\"\n",
            self.create_time,
            self.level as i16,
            self.tag,
            self.content.replace('"', "\"\"")
        )
    }
}

#[cfg(test)]
mod strict_tests {
    use super::*;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test/unit/log/models_impl_tests.rs"
    ));
}
