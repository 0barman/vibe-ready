#[test]
fn platform_type_numeric_values_and_display_are_stable() {
    let cases = [
        (VibePlatformType::Android, "Android"),
        (VibePlatformType::IOS, "iOS"),
        (VibePlatformType::HarmonyOS, "HarmonyOS"),
        (VibePlatformType::Windows, "PC"),
        (VibePlatformType::MacOS, "PC"),
        (VibePlatformType::Linux, "PC"),
        (VibePlatformType::Electron, "PC"),
        (VibePlatformType::Web, "Websocket"),
        (VibePlatformType::HarmonyOSPC, "HarmonyOSPC"),
        (VibePlatformType::MiniWeb, "MiniProgram"),
        (VibePlatformType::PC, "PC"),
        (VibePlatformType::IPad, "iPad"),
        (VibePlatformType::APad, "aPad"),
        (VibePlatformType::HPad, "hPad"),
        (VibePlatformType::Unknown, "Unknown"),
    ];

    for (platform, display) in cases {
        assert_eq!(platform.to_i32(), platform as i32);
        assert_eq!(platform.to_string(), display);
    }
    assert_eq!(VibePlatformType::default(), VibePlatformType::Unknown);
}
