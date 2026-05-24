/// Build-time capabilities available in the current vibe-ready crate build.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct VibeCapabilities {
    /// Persistent log store support.
    pub log_store: bool,
    /// Persistent work/key-value store support.
    pub work_store: bool,
    /// Network API support.
    pub network: bool,
    /// Encrypted store support.
    pub encryption: bool,
    /// WebAssembly target support for the current build.
    pub wasm: bool,
    /// Tracing integration support.
    pub tracing: bool,
    /// Metrics integration support.
    pub metrics: bool,
}

impl VibeCapabilities {
    /// Capabilities compiled into the current build.
    pub const CURRENT: Self = Self::current();

    /// Returns the capabilities compiled into the current build.
    pub const fn current() -> Self {
        Self {
            log_store: cfg!(feature = "log-diesel"),
            work_store: cfg!(feature = "store-diesel-sqlite"),
            network: false,
            encryption: false,
            wasm: cfg!(target_arch = "wasm32"),
            tracing: false,
            metrics: false,
        }
    }
}

impl Default for VibeCapabilities {
    fn default() -> Self {
        Self::current()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_reflects_compiled_features() {
        let capabilities = VibeCapabilities::current();

        assert_eq!(capabilities, VibeCapabilities::CURRENT);
        assert_eq!(capabilities.log_store, cfg!(feature = "log-diesel"));
        assert_eq!(
            capabilities.work_store,
            cfg!(feature = "store-diesel-sqlite")
        );
        assert!(!capabilities.network);
        assert_eq!(capabilities.wasm, cfg!(target_arch = "wasm32"));
        assert!(!capabilities.encryption);
        assert!(!capabilities.tracing);
        assert!(!capabilities.metrics);
    }
}

#[cfg(test)]
mod strict_tests {
    use super::*;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test/unit/api/capabilities_tests.rs"
    ));
}
