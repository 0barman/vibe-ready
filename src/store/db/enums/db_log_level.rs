#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum DBLogLevel {
    None = 0,
    Error = 1,
    Warn = 2,
    Info = 3,
    Debug = 4,
}
