#[test]
fn log_level_from_i32_covers_negative_valid_and_extreme_values() {
    assert_eq!(LogLevel::from(-1), LogLevel::None);
    assert_eq!(LogLevel::from(0), LogLevel::None);
    assert_eq!(LogLevel::from(1), LogLevel::Error);
    assert_eq!(LogLevel::from(2), LogLevel::Warn);
    assert_eq!(LogLevel::from(3), LogLevel::Info);
    assert_eq!(LogLevel::from(4), LogLevel::Debug);
    assert_eq!(LogLevel::from(5), LogLevel::None);
    assert_eq!(LogLevel::from(i32::MAX), LogLevel::None);
    assert_eq!(i32::from(LogLevel::Warn), 2);
}

#[test]
fn log_level_ordering_and_default_are_stable() {
    assert_eq!(LogLevel::default(), LogLevel::Info);
    assert!(LogLevel::None < LogLevel::Error);
    assert!(LogLevel::Error < LogLevel::Warn);
    assert!(LogLevel::Warn < LogLevel::Info);
    assert!(LogLevel::Info < LogLevel::Debug);
}
