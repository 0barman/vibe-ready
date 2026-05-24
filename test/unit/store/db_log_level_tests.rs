#[test]
fn db_log_level_repr_clone_copy_and_debug_are_stable() {
    let cases = [
        (DBLogLevel::None, 0, "None"),
        (DBLogLevel::Error, 1, "Error"),
        (DBLogLevel::Warn, 2, "Warn"),
        (DBLogLevel::Info, 3, "Info"),
        (DBLogLevel::Debug, 4, "Debug"),
    ];

    for (level, value, debug) in cases {
        let copied = level;
        assert_eq!(copied, level);
        assert_eq!(level as i32, value);
        assert_eq!(format!("{level:?}"), debug);
    }
}
