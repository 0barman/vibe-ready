use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Formatter;

#[repr(i32)]
#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Serialize, Eq, Default)]
/// Platform where the SDK is running.
pub enum VibePlatformType {
    /// Android application runtime.
    Android,
    /// iOS application runtime.
    IOS,
    /// HarmonyOS application runtime.
    HarmonyOS,
    /// Windows desktop runtime.
    Windows,
    /// macOS desktop runtime.
    MacOS,
    /// Linux desktop or server runtime.
    Linux,
    /// Electron desktop runtime.
    Electron,
    /// Web runtime.
    Web,
    /// HarmonyOS PC runtime.
    HarmonyOSPC,
    /// Mini-program web runtime.
    MiniWeb,
    /// Generic PC runtime.
    PC,
    /// iPad runtime.
    IPad,
    /// Android tablet runtime.
    APad,
    /// HarmonyOS tablet runtime.
    HPad,
    /// Unknown or unsupported runtime.
    #[default]
    Unknown = 127,
}

impl VibePlatformType {
    /// Converts the platform variant to its stable integer representation.
    ///
    /// # Returns
    ///
    /// The `i32` value associated with this platform.
    ///
    /// # Examples
    ///
    /// ```
    /// use vibe_ready::VibePlatformType;
    ///
    /// assert_eq!(VibePlatformType::MacOS.to_i32(), VibePlatformType::MacOS as i32);
    /// ```
    pub fn to_i32(&self) -> i32 {
        *self as i32
    }
}

impl fmt::Display for VibePlatformType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            VibePlatformType::Android => write!(f, "Android"),
            VibePlatformType::IOS => write!(f, "iOS"),
            VibePlatformType::HarmonyOS => write!(f, "HarmonyOS"),
            VibePlatformType::Windows => write!(f, "PC"),
            VibePlatformType::MacOS => write!(f, "PC"),
            VibePlatformType::Linux => write!(f, "PC"),
            VibePlatformType::Electron => write!(f, "PC"),
            VibePlatformType::HarmonyOSPC => write!(f, "HarmonyOSPC"),
            VibePlatformType::Web => write!(f, "Websocket"),
            VibePlatformType::PC => write!(f, "PC"),
            VibePlatformType::MiniWeb => write!(f, "MiniProgram"),
            VibePlatformType::IPad => write!(f, "iPad"),
            VibePlatformType::APad => write!(f, "aPad"),
            VibePlatformType::HPad => write!(f, "hPad"),
            VibePlatformType::Unknown => write!(f, "Unknown"),
        }
    }
}

#[cfg(test)]
mod strict_tests {
    use super::*;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test/unit/api/platform_type_tests.rs"
    ));
}
