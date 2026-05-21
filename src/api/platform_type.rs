use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Formatter;

#[repr(i32)]
#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Serialize, Eq, Default)]
/// Platform where the SDK is running.
pub enum VibePlatformType {
    Android,
    IOS,
    HarmonyOS,
    Windows,
    MacOS,
    Linux,
    Electron,
    Web,
    HarmonyOSPC,
    MiniWeb,
    PC,
    IPad, // ios pad
    APad, // android pad
    HPad, // harmony pad
    #[default]
    Unknown = 127,
}

impl VibePlatformType {
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
