use crate::log::log_def::VibeLogInfo;
use diesel::{AsChangeset, Insertable, Queryable, Selectable};
use serde::{Deserialize, Serialize};

pub static TABLE_NAME_LOG: &str = "vibe_ready_log";

diesel::table! {
    vibe_ready_log (id) {
        id -> BigInt,
        level -> SmallInt,
        tag -> Text,
        content -> Text,
        create_time -> BigInt,
    }
}

#[derive(Queryable, Selectable, Insertable, AsChangeset, Debug, Clone, Serialize, Deserialize)]
#[diesel(table_name = vibe_ready_log)]
pub struct VibeTableLog {
    pub id: i64,
    pub level: i16,
    pub tag: String,
    pub content: String,
    pub create_time: i64,
}

impl VibeTableLog {
    pub fn new(id: i64, log: &VibeLogInfo) -> Self {
        VibeTableLog {
            id,
            level: log.level as i16,
            tag: log.tag.clone(),
            content: log.content.clone(),
            create_time: log.create_time,
        }
    }

    pub fn to_csv(&self) -> String {
        format!(
            "{},{},{},\"{}\"\n",
            self.create_time,
            self.level,
            self.tag,
            self.content.replace('"', "\"\"")
        )
    }
}
