use serde::{Deserialize, Serialize};

#[cfg(feature = "store-diesel-sqlite")]
pub static TABLE_NAME_KEY_VAL: &str = "vibe_ready_key_val";
#[cfg(feature = "store-diesel-sqlite")]
pub static TABLE_NAME_KV_META: &str = "vibe_ready_kv_meta";

pub const DEFAULT_BUCKET: &str = "default";

pub(crate) const VALUE_TYPE_STR: i16 = 1;
pub(crate) const VALUE_TYPE_BOOL: i16 = 2;
pub(crate) const VALUE_TYPE_I32: i16 = 3;
pub(crate) const VALUE_TYPE_I64: i16 = 4;
pub(crate) const VALUE_TYPE_F64: i16 = 5;
pub(crate) const VALUE_TYPE_BYTES: i16 = 6;
pub(crate) const VALUE_TYPE_JSON: i16 = 7;

/// Sentinel meaning "no expiry".
pub(crate) const EXPIRES_AT_NEVER: i64 = 0;

#[cfg(feature = "store-diesel-sqlite")]
diesel::table! {
    vibe_ready_key_val (user_id, bucket, key) {
        user_id -> Text,
        bucket -> Text,
        key -> Text,
        value_type -> SmallInt,
        value_str -> Text,
        value_bool -> Bool,
        value_i32 -> Integer,
        value_i64 -> BigInt,
        value_f64 -> Double,
        value_bytes -> Binary,
        value_json -> Text,
        expires_at_ms -> BigInt,
    }
}

/// Value stored in the high-level key-value store.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum VibeKvValue {
    String(String),
    Bool(bool),
    I32(i32),
    I64(i64),
    F64(f64),
    Bytes(Vec<u8>),
    Json(serde_json::Value),
}

impl VibeKvValue {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value.as_str()),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_i32(&self) -> Option<i32> {
        match self {
            Self::I32(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::I64(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::F64(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Bytes(value) => Some(value.as_slice()),
            _ => None,
        }
    }

    pub fn as_json(&self) -> Option<&serde_json::Value> {
        match self {
            Self::Json(value) => Some(value),
            _ => None,
        }
    }
}

impl From<String> for VibeKvValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for VibeKvValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<bool> for VibeKvValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<i32> for VibeKvValue {
    fn from(value: i32) -> Self {
        Self::I32(value)
    }
}

impl From<i64> for VibeKvValue {
    fn from(value: i64) -> Self {
        Self::I64(value)
    }
}

impl From<f64> for VibeKvValue {
    fn from(value: f64) -> Self {
        Self::F64(value)
    }
}

impl From<Vec<u8>> for VibeKvValue {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(value)
    }
}

impl From<&[u8]> for VibeKvValue {
    fn from(value: &[u8]) -> Self {
        Self::Bytes(value.to_vec())
    }
}

impl From<serde_json::Value> for VibeKvValue {
    fn from(value: serde_json::Value) -> Self {
        Self::Json(value)
    }
}

#[cfg_attr(
    feature = "store-diesel-sqlite",
    derive(diesel::Queryable, diesel::Selectable, diesel::Insertable)
)]
#[cfg_attr(feature = "store-diesel-sqlite", diesel(table_name = vibe_ready_key_val))]
#[cfg_attr(feature = "store-diesel-sqlite", diesel(primary_key(user_id, bucket, key)))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VibeTableKeyVal {
    pub(crate) user_id: String,
    pub(crate) bucket: String,
    pub(crate) key: String,
    pub(crate) value_type: i16,
    pub(crate) value_str: String,
    pub(crate) value_bool: bool,
    pub(crate) value_i32: i32,
    pub(crate) value_i64: i64,
    pub(crate) value_f64: f64,
    pub(crate) value_bytes: Vec<u8>,
    pub(crate) value_json: String,
    pub(crate) expires_at_ms: i64,
}

impl VibeTableKeyVal {
    /// Build a row using the [`DEFAULT_BUCKET`] and no TTL.
    pub fn new(user_id: &str, key: &str, value: VibeKvValue) -> Self {
        Self::new_in_bucket(user_id, DEFAULT_BUCKET, key, value, EXPIRES_AT_NEVER)
    }

    /// Build a row with explicit bucket and optional `expires_at_ms`
    /// (`0` means no expiry).
    pub fn new_in_bucket(
        user_id: &str,
        bucket: &str,
        key: &str,
        value: VibeKvValue,
        expires_at_ms: i64,
    ) -> Self {
        let mut row = Self::empty(user_id, bucket, key);
        row.expires_at_ms = expires_at_ms;
        match value {
            VibeKvValue::String(v) => {
                row.value_type = VALUE_TYPE_STR;
                row.value_str = v;
            }
            VibeKvValue::Bool(v) => {
                row.value_type = VALUE_TYPE_BOOL;
                row.value_bool = v;
            }
            VibeKvValue::I32(v) => {
                row.value_type = VALUE_TYPE_I32;
                row.value_i32 = v;
            }
            VibeKvValue::I64(v) => {
                row.value_type = VALUE_TYPE_I64;
                row.value_i64 = v;
            }
            VibeKvValue::F64(v) => {
                row.value_type = VALUE_TYPE_F64;
                row.value_f64 = v;
            }
            VibeKvValue::Bytes(v) => {
                row.value_type = VALUE_TYPE_BYTES;
                row.value_bytes = v;
            }
            VibeKvValue::Json(v) => {
                row.value_type = VALUE_TYPE_JSON;
                row.value_json = v.to_string();
            }
        }
        row
    }

    fn empty(user_id: &str, bucket: &str, key: &str) -> Self {
        Self {
            user_id: user_id.to_string(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            value_type: 0,
            value_str: String::new(),
            value_bool: false,
            value_i32: 0,
            value_i64: 0,
            value_f64: 0.0,
            value_bytes: Vec::new(),
            value_json: String::new(),
            expires_at_ms: EXPIRES_AT_NEVER,
        }
    }

    pub fn new_with_str(user_id: &str, key: &str, val: &str) -> Self {
        Self::new(user_id, key, VibeKvValue::String(val.to_string()))
    }

    pub fn new_with_bool(user_id: &str, key: &str, val: bool) -> Self {
        Self::new(user_id, key, VibeKvValue::Bool(val))
    }

    pub fn new_with_i32(user_id: &str, key: &str, val: i32) -> Self {
        Self::new(user_id, key, VibeKvValue::I32(val))
    }

    pub fn key(&self) -> &str {
        self.key.as_str()
    }

    pub fn user_id(&self) -> &str {
        self.user_id.as_str()
    }

    pub fn bucket(&self) -> &str {
        self.bucket.as_str()
    }

    pub fn expires_at_ms(&self) -> i64 {
        self.expires_at_ms
    }

    pub fn is_expired(&self, now_ms: i64) -> bool {
        self.expires_at_ms != EXPIRES_AT_NEVER && now_ms >= self.expires_at_ms
    }

    pub fn value(&self) -> Option<VibeKvValue> {
        match self.value_type {
            VALUE_TYPE_STR => Some(VibeKvValue::String(self.value_str.clone())),
            VALUE_TYPE_BOOL => Some(VibeKvValue::Bool(self.value_bool)),
            VALUE_TYPE_I32 => Some(VibeKvValue::I32(self.value_i32)),
            VALUE_TYPE_I64 => Some(VibeKvValue::I64(self.value_i64)),
            VALUE_TYPE_F64 => Some(VibeKvValue::F64(self.value_f64)),
            VALUE_TYPE_BYTES => Some(VibeKvValue::Bytes(self.value_bytes.clone())),
            VALUE_TYPE_JSON => serde_json::from_str(&self.value_json)
                .ok()
                .map(VibeKvValue::Json),
            _ => None,
        }
    }

    pub fn get_value_str(&self) -> Option<&str> {
        if self.value_type == VALUE_TYPE_STR {
            Some(self.value_str.as_str())
        } else {
            None
        }
    }

    pub fn get_value_bool(&self) -> Option<bool> {
        if self.value_type == VALUE_TYPE_BOOL {
            Some(self.value_bool)
        } else {
            None
        }
    }

    pub fn get_value_i32(&self) -> Option<i32> {
        if self.value_type == VALUE_TYPE_I32 {
            Some(self.value_i32)
        } else {
            None
        }
    }
}
