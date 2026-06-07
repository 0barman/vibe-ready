use crate::api::capabilities::VibeCapabilities;
use crate::api::engine_config::VibeEngineConfig;
use crate::api::engine_context::VibeEngineContext;
use crate::api::engine_error::{VibeEngineError, VibeEngineErrorCode};
use crate::api::engine_executor::{VibeEngineExecutor, VibeEngineTask, VibeRuntimeHandle};
use crate::api::scheduler::{
    VibeCancellationToken, VibeTaskHandle, VibeTaskPanel, VibeTaskPriority, VibeTaskScheduler,
};
use crate::log::log_def::{LogListener, DESC};
use crate::log::log_level::LogLevel;
use crate::store::kv_store::VibeKvStore;
use crate::{log_e, log_t, platform};
use std::future::Future;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use threadpool::ThreadPool;
use tokio::runtime::Handle;
use tokio::sync::mpsc::channel;

const DEFAULT_DESTROY_TIMEOUT: Duration = Duration::from_secs(5);

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Lifecycle state of a [`VibeEngine`].
pub enum VibeEngineState {
    /// The engine value has been constructed but is not accepting work yet.
    Created = 0,
    /// The engine is ready to accept tasks and storage operations.
    Running = 1,
    /// The engine is shutting down resources and no longer accepts new work.
    Closing = 2,
    /// The engine has released its runtime-owned resources.
    Closed = 3,
}

impl VibeEngineState {
    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Created,
            1 => Self::Running,
            2 => Self::Closing,
            3 => Self::Closed,
            _ => Self::Closed,
        }
    }
}

/// Main runtime facade for task execution, logging, and SDK context access.
pub struct VibeEngine {
    executor: VibeEngineExecutor,
    /// Shared engine context for advanced integrations that need low-level clients.
    pub ctx: Arc<VibeEngineContext>,
    state: Arc<AtomicU8>,
    destroy_lock: Arc<Mutex<()>>,
    scheduler: Arc<VibeTaskScheduler>,
    #[cfg(feature = "net-http")]
    http: Arc<std::sync::OnceLock<crate::net::VibeHttpClient>>,
}

impl VibeEngine {
    /// Returns compile-time capabilities enabled for this crate build.
    ///
    /// # Returns
    ///
    /// A [`VibeCapabilities`] snapshot describing enabled storage, logging,
    /// and platform capabilities.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use vibe_ready::{VibeEngine, VibeEngineConfig, VibeResult};
    ///
    /// # fn demo() -> VibeResult<()> {
    /// let engine = VibeEngine::create(VibeEngineConfig::builder().build())?;
    /// let capabilities = engine.capabilities();
    /// assert_eq!(capabilities.log_store, cfg!(feature = "log-diesel"));
    /// # engine.destroy_with_timeout(std::time::Duration::from_secs(1))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn capabilities(&self) -> VibeCapabilities {
        VibeCapabilities::current()
    }

    /// Returns the current engine lifecycle state.
    ///
    /// # Returns
    ///
    /// A [`VibeEngineState`] value such as [`VibeEngineState::Running`] or
    /// [`VibeEngineState::Closed`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use vibe_ready::{VibeEngine, VibeEngineConfig, VibeEngineState, VibeResult};
    ///
    /// # fn demo() -> VibeResult<()> {
    /// let engine = VibeEngine::create(VibeEngineConfig::builder().build())?;
    /// assert_eq!(engine.state(), VibeEngineState::Running);
    /// # engine.destroy_with_timeout(std::time::Duration::from_secs(1))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn state(&self) -> VibeEngineState {
        VibeEngineState::from_u8(self.state.load(Ordering::SeqCst))
    }

    /// Clones the engine executor for advanced task and callback integrations.
    ///
    /// # Returns
    ///
    /// A cheap clone of [`VibeEngineExecutor`] sharing the engine runtime.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use vibe_ready::{VibeEngine, VibeEngineConfig, VibeResult};
    ///
    /// # fn demo() -> VibeResult<()> {
    /// let engine = VibeEngine::create(VibeEngineConfig::builder().build())?;
    /// let executor = engine.executor();
    /// executor.post(async {})?;
    /// # engine.destroy_with_timeout(std::time::Duration::from_secs(1))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn executor(&self) -> VibeEngineExecutor {
        self.executor.clone()
    }

    /// Creates a high-level key-value store facade bound to this engine.
    ///
    /// # Returns
    ///
    /// A [`VibeKvStore`] that performs blocking-friendly operations through
    /// the engine executor.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use vibe_ready::{VibeEngine, VibeEngineConfig, VibeResult};
    ///
    /// # fn demo() -> VibeResult<()> {
    /// let engine = VibeEngine::create(VibeEngineConfig::builder().build())?;
    /// let store = engine.store();
    /// store.set_str("theme", "dark")?;
    /// # engine.destroy_with_timeout(std::time::Duration::from_secs(1))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn store(&self) -> VibeKvStore {
        VibeKvStore::new(self.ctx.db_client().clone(), self.executor.clone())
    }

    /// Returns a shared HTTP client bound to this engine, building it on first use.
    ///
    /// The client is created once with default configuration and cached; later
    /// calls return cheap clones that share the same connection pool. Requires
    /// the `net-http` feature.
    ///
    /// # Returns
    ///
    /// `Ok(VibeHttpClient)` on success, or [`VibeEngineError`] if the client
    /// could not be constructed.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(feature = "net-http")]
    /// # async fn demo(engine: &vibe_ready::VibeEngine) -> vibe_ready::VibeResult<()> {
    /// let client = engine.http()?;
    /// let response = client.get("https://example.com").await?;
    /// assert!(response.status() > 0);
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "net-http")]
    pub fn http(&self) -> Result<crate::net::VibeHttpClient, VibeEngineError> {
        if let Some(client) = self.http.get() {
            return Ok(client.clone());
        }
        let client = crate::net::VibeHttpClient::new()?;
        let _ = self.http.set(client.clone());
        Ok(self.http.get().cloned().unwrap_or(client))
    }

    /// Runs a future on the engine runtime and waits for its result.
    ///
    /// Use this for short async operations where the caller needs the return
    /// value synchronously.
    ///
    /// # Returns
    ///
    /// `Ok(F)` with the future output, or [`VibeEngineError`] if the engine is
    /// not running or the task cannot be delivered.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use vibe_ready::{VibeEngine, VibeEngineConfig, VibeResult};
    ///
    /// # fn demo() -> VibeResult<()> {
    /// let engine = VibeEngine::create(VibeEngineConfig::builder().build())?;
    /// let answer = engine.invoke(async { 42 })?;
    /// assert_eq!(answer, 42);
    /// # engine.destroy_with_timeout(std::time::Duration::from_secs(1))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn invoke<T, F>(&self, future: T) -> Result<F, VibeEngineError>
    where
        T: Future<Output = F> + Send + 'static,
        F: Send + 'static,
    {
        if self.state() != VibeEngineState::Running {
            return Err(VibeEngineError::from_error_code(
                VibeEngineErrorCode::PostError,
            ));
        }
        self.executor.invoke(future)
    }

    /// Posts a fire-and-forget future to the engine runtime.
    ///
    /// The method logs failures instead of returning them, making it suitable
    /// for background work where the caller does not need a result.
    ///
    /// # Returns
    ///
    /// This method returns `()`; delivery errors are written to the SDK log.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use vibe_ready::{VibeEngine, VibeEngineConfig, VibeResult};
    ///
    /// # fn demo() -> VibeResult<()> {
    /// let engine = VibeEngine::create(VibeEngineConfig::builder().build())?;
    /// engine.post(async {
    ///     // perform background work here
    /// });
    /// # engine.destroy_with_timeout(std::time::Duration::from_secs(1))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn post<T>(&self, future: T)
    where
        T: Future<Output = ()> + Send + 'static,
    {
        if self.state() != VibeEngineState::Running {
            log_e!("post", DESC, "engine is not running");
            return;
        }
        if let Err(error) = self.executor.post(future) {
            log_e!("post", DESC, format!("executor post error: {}", error));
        }
    }

    /// Wraps a one-argument callback so it runs on the engine callback pool.
    ///
    /// # Returns
    ///
    /// A `FnOnce(R)` wrapper that schedules `cb` on the callback thread pool.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use vibe_ready::{VibeEngine, VibeEngineConfig, VibeResult};
    ///
    /// # fn demo() -> VibeResult<()> {
    /// let engine = VibeEngine::create(VibeEngineConfig::builder().build())?;
    /// let callback = engine.cb_pool_once(|value: i32| assert_eq!(value, 7));
    /// callback(7);
    /// # engine.destroy_with_timeout(std::time::Duration::from_secs(1))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn cb_pool_once<F, R>(&self, cb: F) -> impl FnOnce(R)
    where
        F: FnOnce(R) + Send + 'static,
        R: Send + 'static,
    {
        self.executor.callback().once(cb)
    }

    /// Wraps a two-argument callback so it runs on the engine callback pool.
    ///
    /// # Returns
    ///
    /// A `FnOnce(R1, R2)` wrapper that schedules `cb` on the callback thread pool.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use vibe_ready::{VibeEngine, VibeEngineConfig, VibeResult};
    ///
    /// # fn demo() -> VibeResult<()> {
    /// let engine = VibeEngine::create(VibeEngineConfig::builder().build())?;
    /// let callback = engine.cb_pool_once2(|left: i32, right: i32| assert_eq!(left + right, 3));
    /// callback(1, 2);
    /// # engine.destroy_with_timeout(std::time::Duration::from_secs(1))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn cb_pool_once2<F, R1, R2>(&self, cb: F) -> impl FnOnce(R1, R2)
    where
        F: FnOnce(R1, R2) + Send + 'static,
        R1: Send + 'static,
        R2: Send + 'static,
    {
        self.executor.callback().once2(cb)
    }
}

impl VibeEngine {
    /// Posts a future to a dedicated priority lane.
    ///
    /// Tasks submitted to a higher priority lane run before lower-priority
    /// tasks queued at the same time. Returns a [`VibeTaskHandle`] that can
    /// be cancelled or `await`ed.
    pub fn post_with_priority<F>(
        &self,
        name: impl Into<String>,
        priority: VibeTaskPriority,
        future: F,
    ) -> Result<VibeTaskHandle, VibeEngineError>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        if self.state() != VibeEngineState::Running {
            return Err(VibeEngineError::from_error_code(
                VibeEngineErrorCode::PostError,
            ));
        }
        self.scheduler.post_with_priority(name, priority, future)
    }

    /// Schedule a one-shot task to run after `delay`.
    ///
    /// The builder receives a [`VibeCancellationToken`] so the user task can
    /// abort cooperatively when the handle is cancelled.
    pub fn schedule_after<F, Fut>(
        &self,
        name: impl Into<String>,
        delay: Duration,
        builder: F,
    ) -> Result<VibeTaskHandle, VibeEngineError>
    where
        F: FnOnce(VibeCancellationToken) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        if self.state() != VibeEngineState::Running {
            return Err(VibeEngineError::from_error_code(
                VibeEngineErrorCode::PostError,
            ));
        }
        self.scheduler.schedule_after(name, delay, builder)
    }

    /// Schedule a periodic task. The builder is invoked once every `period`
    /// until the returned handle is cancelled or the engine is destroyed.
    pub fn schedule_every<F, Fut>(
        &self,
        name: impl Into<String>,
        period: Duration,
        builder: F,
    ) -> Result<VibeTaskHandle, VibeEngineError>
    where
        F: FnMut(VibeCancellationToken) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        if self.state() != VibeEngineState::Running {
            return Err(VibeEngineError::from_error_code(
                VibeEngineErrorCode::PostError,
            ));
        }
        self.scheduler.schedule_every(name, period, builder)
    }

    /// Diagnostic panel exposing live snapshots of scheduler-tracked tasks.
    pub fn tasks(&self) -> VibeTaskPanel {
        self.scheduler.panel()
    }
}

impl VibeEngine {
    /// Creates an engine with a Tokio runtime owned by vibe-ready.
    ///
    /// # Returns
    ///
    /// `Ok(VibeEngine)` when configuration is valid and storage/logging
    /// backends open successfully, otherwise [`VibeEngineError`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use vibe_ready::{VibeEngine, VibeEngineConfig, VibeResult};
    ///
    /// # fn demo() -> VibeResult<()> {
    /// let engine = VibeEngine::create(VibeEngineConfig::builder().app_name("demo").build())?;
    /// engine.destroy_with_timeout(std::time::Duration::from_secs(1))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn create(config: VibeEngineConfig) -> Result<Self, VibeEngineError> {
        config.validate()?;
        let runtime_config = config.runtime_config().clone();
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(runtime_config.worker_threads)
                .enable_all()
                .build()
                .map_err(|_| VibeEngineError::from_error_code(VibeEngineErrorCode::RuntimeError))?,
        );
        let handle = runtime.handle().clone();

        Self::create_with_runtime(config, VibeRuntimeHandle::owned(runtime), handle)
    }

    /// Creates an engine using a Tokio runtime owned by the host application.
    ///
    /// The host runtime must stay alive for the lifetime of the engine. Destroying
    /// the engine closes vibe-ready resources, but does not shut down this runtime.
    ///
    /// # Returns
    ///
    /// `Ok(VibeEngine)` bound to `runtime_handle`, or [`VibeEngineError`] if
    /// validation or backend initialization fails.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use vibe_ready::{VibeEngine, VibeEngineConfig, VibeResult};
    ///
    /// # fn demo() -> VibeResult<()> {
    /// let runtime = tokio::runtime::Runtime::new().expect("create runtime");
    /// let engine = VibeEngine::create_with_runtime_handle(
    ///     VibeEngineConfig::builder().app_name("hosted").build(),
    ///     runtime.handle().clone(),
    /// )?;
    /// engine.destroy_with_timeout(std::time::Duration::from_secs(1))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn create_with_runtime_handle(
        config: VibeEngineConfig,
        runtime_handle: Handle,
    ) -> Result<Self, VibeEngineError> {
        config.validate()?;

        Self::create_with_runtime(
            config,
            VibeRuntimeHandle::external(runtime_handle.clone()),
            runtime_handle,
        )
    }

    fn create_with_runtime(
        config: VibeEngineConfig,
        runtime: VibeRuntimeHandle,
        runtime_handle: Handle,
    ) -> Result<Self, VibeEngineError> {
        let runtime_config = config.runtime_config().clone();
        let (async_tx, mut async_rx) =
            channel::<VibeEngineTask>(runtime_config.async_queue_capacity);
        let (sync_tx, mut sync_rx) = channel::<VibeEngineTask>(runtime_config.sync_queue_capacity);
        let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel();

        runtime_handle.spawn(async move {
            let sync_handle = tokio::spawn(async move {
                while let Some(future) = sync_rx.recv().await {
                    future.await;
                }
            });

            let async_handle = tokio::spawn(async move {
                while let Some(future) = async_rx.recv().await {
                    future.await;
                }
            });

            let (sync_ret, async_ret) = tokio::join!(sync_handle, async_handle);
            if let Err(e) = sync_ret {
                log_e!("create", DESC, format!("sync queue worker failed: {}", e));
            }
            if let Err(e) = async_ret {
                log_e!("create", DESC, format!("async queue worker failed: {}", e));
            }
            let _ = shutdown_tx.send(());
        });

        let ctx = VibeEngineContext::new(config)?;
        let ctx_arc = Arc::new(ctx);

        let scheduler = VibeTaskScheduler::new(
            runtime_handle.clone(),
            runtime_config.priority_queue_capacity,
        );

        Ok(Self {
            executor: VibeEngineExecutor::new(
                ThreadPool::new(runtime_config.callback_threads),
                async_tx,
                sync_tx,
                runtime,
                shutdown_rx,
            ),
            ctx: ctx_arc,
            state: Arc::new(AtomicU8::new(VibeEngineState::Running as u8)),
            destroy_lock: Arc::new(Mutex::new(())),
            scheduler,
            #[cfg(feature = "net-http")]
            http: Arc::new(std::sync::OnceLock::new()),
        })
    }

    /// Destroys the engine and waits up to `timeout` for resources to close.
    ///
    /// # Returns
    ///
    /// `Ok(())` when shutdown finishes or the engine is already closed;
    /// [`VibeEngineError`] on timeout, runtime, or backend close failures.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::time::Duration;
    /// use vibe_ready::{VibeEngine, VibeEngineConfig, VibeResult};
    ///
    /// # fn demo() -> VibeResult<()> {
    /// let engine = VibeEngine::create(VibeEngineConfig::builder().build())?;
    /// engine.destroy_with_timeout(Duration::from_secs(2))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn destroy_with_timeout(&self, timeout: Duration) -> Result<(), VibeEngineError> {
        let _guard = self
            .destroy_lock
            .lock()
            .map_err(|_| VibeEngineError::from_error_code(VibeEngineErrorCode::RuntimeError))?;

        if self.state() == VibeEngineState::Closed {
            return Ok(());
        }

        self.state
            .store(VibeEngineState::Closing as u8, Ordering::SeqCst);
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| VibeEngineError::from_error_code(VibeEngineErrorCode::TimeoutError))?;

        // Cancel and drop the scheduler's priority lanes first so periodic
        // tasks observe their cancellation tokens before we wait on the
        // executor's queues. This satisfies the B9 acceptance criterion that
        // periodic tasks are cancelled cleanly during destroy.
        self.scheduler.shutdown();

        self.executor
            .shutdown_queues(Self::remaining_timeout(deadline)?)?;
        let ctx = Arc::clone(&self.ctx);
        self.executor.block_on_with_timeout(
            async move { ctx.close().await },
            Self::remaining_timeout(deadline)?,
        )?;

        self.state
            .store(VibeEngineState::Closed as u8, Ordering::SeqCst);
        Ok(())
    }

    fn remaining_timeout(deadline: Instant) -> Result<Duration, VibeEngineError> {
        deadline
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or_else(|| VibeEngineError::from_error_code(VibeEngineErrorCode::TimeoutError))
    }

    /// Destroys the engine using the default timeout and reports through a callback.
    ///
    /// # Returns
    ///
    /// This method returns `()` immediately after invoking `cb` on the callback
    /// pool with `Result<(), VibeEngineError>`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use vibe_ready::{VibeEngine, VibeEngineConfig, VibeResult};
    ///
    /// # fn demo() -> VibeResult<()> {
    /// let engine = VibeEngine::create(VibeEngineConfig::builder().build())?;
    /// engine.destroy(|result| {
    ///     let _ = result;
    /// });
    /// # Ok(())
    /// # }
    /// ```
    pub fn destroy<CB>(&self, cb: CB)
    where
        CB: FnOnce(Result<(), VibeEngineError>) + Send + 'static,
    {
        let method_name = "destroy";
        log_t!(method_name);
        let cb = self.cb_pool_once(cb);
        let result = self.destroy_with_timeout(DEFAULT_DESTROY_TIMEOUT);
        cb(result);
    }
}

impl VibeEngine {
    /// Inserts a log record into the configured log backend.
    ///
    /// # Returns
    ///
    /// This method returns `()`; backend write failures are handled by the log
    /// subsystem.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use vibe_ready::{VibeEngine, VibeEngineConfig, VibeLogLevel, VibeResult};
    ///
    /// # fn demo() -> VibeResult<()> {
    /// let engine = VibeEngine::create(VibeEngineConfig::builder().build())?;
    /// engine.insert_log(true, VibeLogLevel::Info, "startup".into(), "ready".into());
    /// # engine.destroy_with_timeout(std::time::Duration::from_secs(1))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn insert_log(
        &self,
        should_output_log: bool,
        level: LogLevel,
        tag: String,
        content: String,
    ) {
        let create_time = platform::now();
        let ctx = self.ctx.clone();
        ctx.log_db_client()
            .insert_log(should_output_log, level as i32, tag, content, create_time);
    }
}

impl VibeEngine {
    /// Sets or clears the listener that receives emitted log entries.
    ///
    /// # Returns
    ///
    /// This method returns `()` and schedules listener installation on the
    /// engine runtime.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use vibe_ready::{VibeEngine, VibeEngineConfig, VibeResult};
    ///
    /// # fn demo() -> VibeResult<()> {
    /// let engine = VibeEngine::create(VibeEngineConfig::builder().build())?;
    /// engine.set_log_listener(Some(Box::new(|info| {
    ///     let _ = info;
    /// })));
    /// engine.set_log_listener(None);
    /// # engine.destroy_with_timeout(std::time::Duration::from_secs(1))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_log_listener(&self, listener: Option<LogListener>) {
        let ctx = self.ctx.clone();
        self.post(async move {
            ctx.log_db_client().set_log_listener(listener);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::engine_config::{VibeLogBackend, VibeStoreBackend};
    use crate::api::platform_type::VibePlatformType;

    #[test]
    fn destroy_is_idempotent_and_closes_engine() -> Result<(), VibeEngineError> {
        let store_path = std::env::temp_dir().join(format!(
            "vibe-ready-engine-lifecycle-{}",
            crate::platform::now()
        ));
        let config = VibeEngineConfig::builder()
            .platform(VibePlatformType::MacOS)
            .app_name("lifecycle-test")
            .namespace("tests")
            .runtime_worker_threads(1)
            .callback_threads(1)
            .queue_capacity(8, 4)
            .store_root_path(store_path)
            .build();

        let engine = VibeEngine::create(config)?;
        assert_eq!(engine.capabilities(), VibeCapabilities::current());
        assert_eq!(engine.state(), VibeEngineState::Running);

        engine.destroy_with_timeout(Duration::from_secs(2))?;
        assert_eq!(engine.state(), VibeEngineState::Closed);

        engine.destroy_with_timeout(Duration::from_secs(2))?;
        assert_eq!(engine.state(), VibeEngineState::Closed);
        Ok(())
    }

    #[test]
    fn create_with_runtime_handle_uses_host_runtime() -> Result<(), VibeEngineError> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|_| VibeEngineError::from_error_code(VibeEngineErrorCode::RuntimeError))?;
        let store_path = std::env::temp_dir().join(format!(
            "vibe-ready-engine-external-runtime-{}",
            crate::platform::now()
        ));
        let config = VibeEngineConfig::builder()
            .platform(VibePlatformType::MacOS)
            .app_name("external-runtime-test")
            .namespace("tests")
            .log_backend(VibeLogBackend::Noop)
            .store_backend(VibeStoreBackend::Noop)
            .callback_threads(1)
            .queue_capacity(8, 4)
            .store_root_path(store_path)
            .build();

        let engine = VibeEngine::create_with_runtime_handle(config, runtime.handle().clone())?;
        assert_eq!(engine.invoke(async { 42 })?, 42);

        let (tx, rx) = std::sync::mpsc::channel();
        engine.post(async move {
            let _ = tx.send(7);
        });
        let received = rx.recv_timeout(Duration::from_secs(2)).map_err(|err| {
            VibeEngineError::from_error_code(VibeEngineErrorCode::TimeoutError)
                .with_source(err.to_string())
        })?;
        assert_eq!(received, 7);

        engine.destroy_with_timeout(Duration::from_secs(2))?;
        assert_eq!(runtime.block_on(async { 9 }), 9);
        Ok(())
    }

    fn build_scheduler_config(suffix: &str) -> VibeEngineConfig {
        let store_path = std::env::temp_dir().join(format!(
            "vibe-ready-scheduler-{}-{}",
            suffix,
            crate::platform::now()
        ));
        VibeEngineConfig::builder()
            .platform(VibePlatformType::MacOS)
            .app_name("scheduler-test")
            .namespace("tests")
            .log_backend(VibeLogBackend::Noop)
            .store_backend(VibeStoreBackend::Noop)
            .runtime_worker_threads(1)
            .callback_threads(1)
            .queue_capacity(16, 8)
            .priority_queue_capacity(256)
            .store_root_path(store_path)
            .build()
    }

    /// Acceptance #1: 周期任务在 destroy 时被正确取消。
    #[test]
    fn periodic_task_is_cancelled_on_destroy() -> Result<(), VibeEngineError> {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let engine = VibeEngine::create(build_scheduler_config("periodic-cancel"))?;
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);
        let handle =
            engine.schedule_every("periodic.tick", Duration::from_millis(20), move |_token| {
                let c = Arc::clone(&counter_clone);
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                }
            })?;
        std::thread::sleep(Duration::from_millis(120));
        let runs_before_destroy = counter.load(Ordering::SeqCst);
        assert!(runs_before_destroy >= 2, "periodic should have ticked");

        engine.destroy_with_timeout(Duration::from_secs(2))?;

        // After destroy the handle must report a terminal state and the
        // counter must stop growing.
        assert!(handle.is_finished()?, "handle finished after destroy");
        let after = counter.load(Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(80));
        assert_eq!(
            after,
            counter.load(Ordering::SeqCst),
            "no further ticks after destroy"
        );
        Ok(())
    }

    /// Acceptance #2: 高优先级任务在拥塞时延迟显著低于普通任务。
    #[test]
    fn high_priority_task_runs_before_queued_normal_tasks() -> Result<(), VibeEngineError> {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let engine = VibeEngine::create(build_scheduler_config("priority"))?;
        let order = Arc::new(Mutex::new(Vec::<u32>::new()));
        let next_idx = Arc::new(AtomicUsize::new(0));
        // Saturate the normal lane with 30 long-ish tasks (sequential
        // dispatcher → ~30 * 30ms = 900ms of work) so the high-priority task,
        // posted shortly after, wins the next dispatch cycle.
        for _ in 0..30 {
            let order = Arc::clone(&order);
            let next_idx = Arc::clone(&next_idx);
            engine.post_with_priority("normal", VibeTaskPriority::Normal, async move {
                tokio::time::sleep(Duration::from_millis(30)).await;
                let idx = next_idx.fetch_add(1, Ordering::SeqCst) as u32;
                if let Ok(mut order) = order.lock() {
                    order.push(idx);
                }
            })?;
        }

        // Give the dispatcher a moment to begin the first normal task, then
        // enqueue a high-priority marker.
        std::thread::sleep(Duration::from_millis(40));
        let high_marker = Arc::new(Mutex::new(None::<u32>));
        let marker_clone = Arc::clone(&high_marker);
        let next_idx_clone = Arc::clone(&next_idx);
        engine.post_with_priority("high", VibeTaskPriority::High, async move {
            let idx = next_idx_clone.fetch_add(1, Ordering::SeqCst) as u32;
            if let Ok(mut marker) = marker_clone.lock() {
                *marker = Some(idx);
            }
        })?;

        // Wait long enough for the high-priority task to run but far less
        // than the time required to drain all normal tasks (~900ms).
        std::thread::sleep(Duration::from_millis(200));
        let high_idx = high_marker
            .lock()
            .map_err(|_| VibeEngineError::from_error_code(VibeEngineErrorCode::RuntimeError))?
            .ok_or_else(|| {
                VibeEngineError::from_error_code_msg(
                    VibeEngineErrorCode::TimeoutError,
                    "high task did not run".to_string(),
                )
            })?;
        assert!(
            (high_idx as usize) < 15,
            "high-priority task ran at index {high_idx}, expected to overtake majority of normal tasks"
        );

        engine.destroy_with_timeout(Duration::from_secs(5))?;
        Ok(())
    }

    /// Acceptance #3: 取消后的任务不再产生副作用且 join 返回 Cancelled。
    #[test]
    fn cancelled_task_join_returns_cancelled_error() -> Result<(), VibeEngineError> {
        use std::sync::atomic::{AtomicBool, Ordering};
        let engine = VibeEngine::create(build_scheduler_config("cancel"))?;
        let ran = Arc::new(AtomicBool::new(false));
        let ran_clone = Arc::clone(&ran);
        let handle = engine.schedule_after(
            "delayed",
            Duration::from_millis(200),
            move |token| async move {
                // Should never fire its side-effect because the cancellation
                // is requested before the delay elapses; but if it does start,
                // it bails out immediately on the token.
                if token.is_cancelled() {
                    return;
                }
                ran_clone.store(true, Ordering::SeqCst);
            },
        )?;

        // Cancel before the delay elapses.
        std::thread::sleep(Duration::from_millis(40));
        handle.cancel();

        // Join via a host runtime since we are on the test thread.
        let join_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| VibeEngineError::from_error_code(VibeEngineErrorCode::RuntimeError))?;
        let join_handle = handle.clone();
        let result = join_runtime.block_on(async move {
            tokio::time::timeout(Duration::from_secs(2), join_handle.join()).await
        });
        let join_result = result.map_err(|_| {
            VibeEngineError::from_error_code_msg(
                VibeEngineErrorCode::TimeoutError,
                "join did not time out".to_string(),
            )
        })?;
        assert_eq!(
            join_result.unwrap_err().code(),
            VibeEngineErrorCode::Cancelled.code()
        );
        assert!(!ran.load(Ordering::SeqCst), "cancelled task did not run");

        engine.destroy_with_timeout(Duration::from_secs(2))?;
        Ok(())
    }

    /// Sanity: tasks() panel exposes scheduler activity.
    #[test]
    fn task_panel_lists_pending_tasks() -> Result<(), VibeEngineError> {
        let engine = VibeEngine::create(build_scheduler_config("panel"))?;
        let _h = engine.schedule_after(
            "long-delay",
            Duration::from_secs(30),
            |_token| async move {},
        )?;
        let snapshot = engine.tasks().list()?;
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].name, "long-delay");
        engine.destroy_with_timeout(Duration::from_secs(2))?;
        Ok(())
    }
}

#[cfg(test)]
mod strict_tests {
    use super::*;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test/unit/api/engine_tests.rs"
    ));
}
