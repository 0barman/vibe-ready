#[test]
fn get_db_name_pwd_creates_directory_and_returns_password_only_when_encrypted() {
    let root = std::env::temp_dir().join(format!("strict-db-common-{}", crate::platform::now()));
    let (plain_path, plain_pwd) = get_db_name_pwd(
        root.join("plain"),
        "user".to_string(),
        false,
        WORK_DB_NAME,
        WORK_ENC_DB_NAME,
    )
    .expect("plain path");
    assert!(plain_path.ends_with(WORK_DB_NAME));
    assert_eq!(plain_pwd, "");
    assert!(plain_path.parent().expect("parent").exists());

    let (enc_path, enc_pwd) = get_db_name_pwd(
        root.join("enc"),
        "user".to_string(),
        true,
        LOG_DB_NAME,
        LOG_ENC_DB_NAME,
    )
    .expect("encrypted path");
    assert!(enc_path.ends_with(LOG_DB_NAME));
    assert_eq!(enc_pwd, "pwduseruser");
}
