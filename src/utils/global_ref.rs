pub const ENGINE_CHANNEL_BUFFER_SIZE: usize = 512;
pub const ENGINE_SYNC_CHANNEL_BUFFER_SIZE: usize = 128;

pub const DB_CHANNEL_BUFFER_SIZE: usize = 1024;

#[cfg(test)]
mod strict_tests {
    use super::*;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test/unit/utils/global_ref_tests.rs"
    ));
}
