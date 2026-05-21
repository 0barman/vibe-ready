use crate::log::log_def::VibeLogInfo;
use crate::log::log_level::LogLevel;

impl VibeLogInfo {
    pub fn new(level: LogLevel, tag: String, content: String, create_time: i64) -> VibeLogInfo {
        VibeLogInfo {
            level,
            tag,
            content,
            create_time,
        }
    }

    pub fn level(&self) -> LogLevel {
        self.level
    }

    pub fn tag(&self) -> String {
        self.tag.clone()
    }

    pub fn content(&self) -> String {
        self.content.clone()
    }

    pub fn create_time(&self) -> i64 {
        self.create_time
    }

    pub fn slice(&self) -> Vec<VibeLogInfo> {
        let size = self.to_csv().len();
        if size <= 1024 * 1024 {
            return vec![self.clone()];
        }

        let mut ret = vec![];
        let chars = self.content.chars().collect::<Vec<_>>();
        for (_, chunk) in chars.chunks(1024 * 1024 / 4).enumerate() {
            let content = chunk.iter().collect::<String>();
            ret.push(Self {
                content,
                ..self.clone()
            })
        }
        ret
    }

    pub fn to_csv(&self) -> String {
        format!(
            "{},{},{},\"{}\"\n",
            self.create_time,
            self.level as i16,
            self.tag,
            self.content.replace('"', "\"\"")
        )
    }
}
