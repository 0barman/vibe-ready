#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq)]
#[allow(dead_code)]
pub enum DBLogLevel {
    None = 0,
    Error = 1,
    Warn = 2,
    Info = 3,
    Debug = 4,
}

#[cfg(test)]
mod strict_tests {
    use super::*;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test/unit/store/db_log_level_tests.rs"
    ));
}
