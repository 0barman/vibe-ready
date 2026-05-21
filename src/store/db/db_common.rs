use crate::log_db_s;
use crate::store::db::enums::db_error::VibeDbErrorInfo;
use std::fs;
use std::path::PathBuf;

pub const WORK_DB_NAME: &str = "loona_desktop_storage.db";
pub const WORK_ENC_DB_NAME: &str = "loona_desktop_storage.enc.db";

pub const LOG_DB_NAME: &str = "loona_desktop_log_storage.db";
pub const LOG_ENC_DB_NAME: &str = "loona_desktop_log_storage.enc.db";

pub fn get_db_name_pwd(
    store_path: PathBuf,
    user_id: String,
    is_encrypt: bool,
    db_name: &str,      // xxx.db
    _enc_db_name: &str, // xxx.enc.db
) -> Result<(/* db_path */ PathBuf, /* password */ String), VibeDbErrorInfo> {
    let mut store_path = store_path.clone();

    fs::create_dir_all(store_path.as_path())
        .map_err(|e| VibeDbErrorInfo::from_io(e.to_string()))?;

    if is_encrypt {
        store_path.push(db_name);
        // let password = format!("{:x}", md5::compute(key.as_bytes()));
        let password = format!("pwd{}{}", user_id, user_id);
        log_db_s!("db_common", "store_path_str", store_path.clone());
        Ok((store_path, password.to_string()))
    } else {
        store_path.push(db_name);
        log_db_s!("db_common", "store_path_str", store_path.clone());
        Ok((store_path, "".to_string()))
    }
}
