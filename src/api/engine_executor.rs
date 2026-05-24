use crate::api::engine_error::{VibeEngineError, VibeEngineErrorCode};
use crate::log::log_def::DESC;
use crate::log_e;
use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use threadpool::ThreadPool;
use tokio::runtime::{Handle, Runtime};
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;

pub(crate) type VibeEngineTask = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

fn panic_payload_message(payload: &Box<dyn Any + Send + 'static>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

#[derive(Clone)]
/// Executes user callbacks on the engine callback thread pool.
///
/// Use this when an integration receives events on an async/runtime thread but
/// wants user callbacks to run on the configured callback pool.
///
/// # Examples
///
/// ```no_run
/// use vibe_ready::{VibeEngine, VibeEngineConfig, VibeResult};
///
/// # fn demo() -> VibeResult<()> {
/// let engine = VibeEngine::create(VibeEngineConfig::builder().build())?;
/// let callbacks = engine.executor().callback();
/// callbacks.execute(|| println!("callback ran"));
/// # engine.destroy_with_timeout(std::time::Duration::from_secs(1))?;
/// # Ok(())
/// # }
/// ```
pub struct VibeCallbackExecutor {
    cb_pool: ThreadPool,
}

impl VibeCallbackExecutor {
    pub(crate) fn new(cb_pool: ThreadPool) -> Self {
        Self { cb_pool }
    }

    /// Executes a zero-argument callback on the callback pool.
    ///
    /// # Returns
    ///
    /// This method returns `()` after queuing the callback.
    pub fn execute<F>(&self, cb: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.cb_pool.execute(cb);
    }

    /// Converts a one-shot single-argument callback into a pool-dispatched callback.
    ///
    /// # Returns
    ///
    /// A `FnOnce(R)` wrapper that moves the argument into the callback pool.
    pub fn once<F, R>(&self, cb: F) -> impl FnOnce(R) + Send + 'static
    where
        F: FnOnce(R) + Send + 'static,
        R: Send + 'static,
    {
        let executor = self.clone();
        move |value| executor.execute(move || cb(value))
    }

    /// Converts a one-shot two-argument callback into a pool-dispatched callback.
    ///
    /// # Returns
    ///
    /// A `FnOnce(R1, R2)` wrapper that moves both arguments into the callback pool.
    pub fn once2<F, R1, R2>(&self, cb: F) -> impl FnOnce(R1, R2) + Send + 'static
    where
        F: FnOnce(R1, R2) + Send + 'static,
        R1: Send + 'static,
        R2: Send + 'static,
    {
        let executor = self.clone();
        move |value1, value2| executor.execute(move || cb(value1, value2))
    }

    /// Converts a one-shot three-argument callback into a pool-dispatched callback.
    ///
    /// # Returns
    ///
    /// A `FnOnce(R1, R2, R3)` wrapper that moves all arguments into the callback pool.
    pub fn once3<F, R1, R2, R3>(&self, cb: F) -> impl FnOnce(R1, R2, R3) + Send + 'static
    where
        F: FnOnce(R1, R2, R3) + Send + 'static,
        R1: Send + 'static,
        R2: Send + 'static,
        R3: Send + 'static,
    {
        let executor = self.clone();
        move |value1, value2, value3| executor.execute(move || cb(value1, value2, value3))
    }

    /// Returns a boxed one-shot three-argument callback wrapper.
    ///
    /// # Returns
    ///
    /// A boxed `FnOnce` that dispatches the original callback on the pool.
    pub fn once3_boxed<F, R1, R2, R3>(
        &self,
        cb: F,
    ) -> Box<dyn FnOnce(R1, R2, R3) + Send + Sync + 'static>
    where
        F: FnOnce(R1, R2, R3) + Send + Sync + 'static,
        R1: Send + 'static,
        R2: Send + 'static,
        R3: Send + 'static,
    {
        let executor = self.clone();
        Box::new(move |value1, value2, value3| {
            executor.execute(move || cb(value1, value2, value3));
        })
    }

    /// Returns a boxed zero-argument callback wrapper.
    ///
    /// # Returns
    ///
    /// A boxed `Fn` that can be cloned or stored by APIs expecting trait objects.
    pub fn boxed_fn0<F>(&self, cb: F) -> Box<dyn Fn() + Send + Sync + 'static>
    where
        F: Fn() + Send + Sync + 'static,
    {
        let executor = self.clone();
        let cb = Arc::new(cb);
        Box::new(move || {
            let cb_clone = Arc::clone(&cb);
            executor.execute(move || cb_clone());
        })
    }

    /// Returns a boxed one-argument callback wrapper.
    ///
    /// # Returns
    ///
    /// A boxed `Fn(R)` that dispatches each invocation on the callback pool.
    pub fn boxed_fn<F, R>(&self, cb: F) -> Box<dyn Fn(R) + Send + Sync + 'static>
    where
        F: Fn(R) + Send + Sync + 'static,
        R: Send + 'static,
    {
        let executor = self.clone();
        let cb = Arc::new(cb);
        Box::new(move |value| {
            let cb_clone = Arc::clone(&cb);
            executor.execute(move || cb_clone(value));
        })
    }

    /// Returns a boxed two-argument callback wrapper.
    ///
    /// # Returns
    ///
    /// A boxed `Fn(R1, R2)` that dispatches each invocation on the callback pool.
    pub fn boxed_fn2<F, R1, R2>(&self, cb: F) -> Box<dyn Fn(R1, R2) + Send + Sync + 'static>
    where
        F: Fn(R1, R2) + Send + Sync + 'static,
        R1: Send + 'static,
        R2: Send + 'static,
    {
        let executor = self.clone();
        let cb = Arc::new(cb);
        Box::new(move |value1, value2| {
            let cb_clone = Arc::clone(&cb);
            executor.execute(move || cb_clone(value1, value2));
        })
    }

    /// Returns an unboxed two-argument callback wrapper.
    ///
    /// # Returns
    ///
    /// An `impl Fn(R1, R2)` wrapper that dispatches each invocation on the pool.
    pub fn fn2<F, R1, R2>(&self, cb: F) -> impl Fn(R1, R2) + Send + Sync + 'static
    where
        F: Fn(R1, R2) + Send + Sync + 'static,
        R1: Send + 'static,
        R2: Send + 'static,
    {
        let executor = self.clone();
        let cb = Arc::new(cb);
        move |value1, value2| {
            let cb_clone = Arc::clone(&cb);
            executor.execute(move || cb_clone(value1, value2));
        }
    }

    /// Returns a cloneable unboxed two-argument callback wrapper.
    ///
    /// # Returns
    ///
    /// A cloneable `impl Fn(R1, R2)` wrapper for APIs that duplicate callbacks.
    pub fn fn2_cloneable<F, R1, R2>(&self, cb: F) -> impl Fn(R1, R2) + Clone + Send + Sync + 'static
    where
        F: Fn(R1, R2) + Clone + Send + Sync + 'static,
        R1: Send + 'static,
        R2: Send + 'static,
    {
        let executor = self.clone();
        let cb = Arc::new(cb);
        move |value1, value2| {
            let cb_clone = Arc::clone(&cb);
            executor.execute(move || cb_clone(value1, value2));
        }
    }

    /// Returns a boxed three-argument callback wrapper.
    ///
    /// # Returns
    ///
    /// A boxed `Fn(R1, R2, R3)` that dispatches each invocation on the pool.
    pub fn boxed_fn3<F, R1, R2, R3>(&self, cb: F) -> Box<dyn Fn(R1, R2, R3) + Send + Sync + 'static>
    where
        F: Fn(R1, R2, R3) + Send + Sync + 'static,
        R1: Send + 'static,
        R2: Send + 'static,
        R3: Send + 'static,
    {
        let executor = self.clone();
        let cb = Arc::new(cb);
        Box::new(move |value1, value2, value3| {
            let cb_clone = Arc::clone(&cb);
            executor.execute(move || cb_clone(value1, value2, value3));
        })
    }

    /// Returns a boxed four-argument callback wrapper.
    ///
    /// # Returns
    ///
    /// A boxed `Fn(R1, R2, R3, R4)` dispatched on the callback pool.
    pub fn boxed_fn4<F, R1, R2, R3, R4>(
        &self,
        cb: F,
    ) -> Box<dyn Fn(R1, R2, R3, R4) + Send + Sync + 'static>
    where
        F: Fn(R1, R2, R3, R4) + Send + Sync + 'static,
        R1: Send + 'static,
        R2: Send + 'static,
        R3: Send + 'static,
        R4: Send + 'static,
    {
        let executor = self.clone();
        let cb = Arc::new(cb);
        Box::new(move |value1, value2, value3, value4| {
            let cb_clone = Arc::clone(&cb);
            executor.execute(move || cb_clone(value1, value2, value3, value4));
        })
    }

    /// Returns a boxed five-argument callback wrapper.
    ///
    /// # Returns
    ///
    /// A boxed `Fn(R1, R2, R3, R4, R5)` dispatched on the callback pool.
    pub fn boxed_fn5<F, R1, R2, R3, R4, R5>(
        &self,
        cb: F,
    ) -> Box<dyn Fn(R1, R2, R3, R4, R5) + Send + Sync + 'static>
    where
        F: Fn(R1, R2, R3, R4, R5) + Send + Sync + 'static,
        R1: Send + 'static,
        R2: Send + 'static,
        R3: Send + 'static,
        R4: Send + 'static,
        R5: Send + 'static,
    {
        let executor = self.clone();
        let cb = Arc::new(cb);
        Box::new(move |value1, value2, value3, value4, value5| {
            let cb_clone = Arc::clone(&cb);
            executor.execute(move || cb_clone(value1, value2, value3, value4, value5));
        })
    }

    /// Returns a boxed callback wrapper that accepts a borrowed first argument.
    ///
    /// # Returns
    ///
    /// A boxed `Fn(&R1, R2)` wrapper. The borrowed value is cloned before the
    /// callback is moved to the pool.
    pub fn boxed_ref_fn2<F, R1, R2>(&self, cb: F) -> Box<dyn Fn(&R1, R2) + Send + Sync + 'static>
    where
        F: Fn(&R1, R2) + Send + Sync + 'static,
        R1: Clone + Send + 'static,
        R2: Send + 'static,
    {
        let executor = self.clone();
        let cb = Arc::new(cb);
        Box::new(move |value1, value2| {
            let owned_value1 = value1.clone();
            let cb_clone = Arc::clone(&cb);
            executor.execute(move || cb_clone(&owned_value1, value2));
        })
    }

    /// Returns a boxed string-slice callback wrapper.
    ///
    /// # Returns
    ///
    /// A boxed `Fn(&str)` wrapper that owns the string before dispatching.
    pub fn boxed_str_fn<F>(&self, cb: F) -> Box<dyn Fn(&str) + Send + Sync + 'static>
    where
        F: Fn(&str) + Send + Sync + 'static,
    {
        let executor = self.clone();
        let cb = Arc::new(cb);
        Box::new(move |value| {
            let owned = value.to_string();
            let cb_clone = Arc::clone(&cb);
            executor.execute(move || cb_clone(&owned));
        })
    }

    /// Returns a boxed `&str` plus one-argument callback wrapper.
    ///
    /// # Returns
    ///
    /// A boxed `Fn(&str, R2)` wrapper that owns the string before dispatching.
    pub fn boxed_str_fn2<F, R2>(&self, cb: F) -> Box<dyn Fn(&str, R2) + Send + Sync + 'static>
    where
        F: Fn(&str, R2) + Send + Sync + 'static,
        R2: Send + 'static,
    {
        let executor = self.clone();
        let cb = Arc::new(cb);
        Box::new(move |value, value2| {
            let owned = value.to_string();
            let cb_clone = Arc::clone(&cb);
            executor.execute(move || cb_clone(&owned, value2));
        })
    }

    /// Returns a boxed `&str` plus two-argument callback wrapper.
    ///
    /// # Returns
    ///
    /// A boxed `Fn(&str, R2, R3)` wrapper that owns the string before dispatching.
    pub fn boxed_str_fn3<F, R2, R3>(
        &self,
        cb: F,
    ) -> Box<dyn Fn(&str, R2, R3) + Send + Sync + 'static>
    where
        F: Fn(&str, R2, R3) + Send + Sync + 'static,
        R2: Send + 'static,
        R3: Send + 'static,
    {
        let executor = self.clone();
        let cb = Arc::new(cb);
        Box::new(move |value, value2, value3| {
            let owned = value.to_string();
            let cb_clone = Arc::clone(&cb);
            executor.execute(move || cb_clone(&owned, value2, value3));
        })
    }

    /// Returns a boxed `&str` plus three-argument callback wrapper.
    ///
    /// # Returns
    ///
    /// A boxed `Fn(&str, R2, R3, R4)` wrapper that owns the string before dispatching.
    pub fn boxed_str_fn4<F, R2, R3, R4>(
        &self,
        cb: F,
    ) -> Box<dyn Fn(&str, R2, R3, R4) + Send + Sync + 'static>
    where
        F: Fn(&str, R2, R3, R4) + Send + Sync + 'static,
        R2: Send + 'static,
        R3: Send + 'static,
        R4: Send + 'static,
    {
        let executor = self.clone();
        let cb = Arc::new(cb);
        Box::new(move |value, value2, value3, value4| {
            let owned = value.to_string();
            let cb_clone = Arc::clone(&cb);
            executor.execute(move || cb_clone(&owned, value2, value3, value4));
        })
    }

    /// Returns a boxed callback wrapper with first and fourth string arguments.
    ///
    /// # Returns
    ///
    /// A boxed `Fn(&str, R2, R3, &str)` wrapper that owns both string slices
    /// before dispatching.
    pub fn boxed_str_str4_fn<F, R2, R3>(
        &self,
        cb: F,
    ) -> Box<dyn Fn(&str, R2, R3, &str) + Send + Sync + 'static>
    where
        F: Fn(&str, R2, R3, &str) + Send + Sync + 'static,
        R2: Send + 'static,
        R3: Send + 'static,
    {
        let executor = self.clone();
        let cb = Arc::new(cb);
        Box::new(move |value1, value2, value3, value4| {
            let owned_value1 = value1.to_string();
            let owned_value4 = value4.to_string();
            let cb_clone = Arc::clone(&cb);
            executor.execute(move || cb_clone(&owned_value1, value2, value3, &owned_value4));
        })
    }
}

#[derive(Clone)]
/// Executes futures and callbacks on the engine-owned runtime infrastructure.
///
/// # Examples
///
/// ```no_run
/// use vibe_ready::{VibeEngine, VibeEngineConfig, VibeResult};
///
/// # fn demo() -> VibeResult<()> {
/// let engine = VibeEngine::create(VibeEngineConfig::builder().build())?;
/// let executor = engine.executor();
/// let value = executor.invoke(async { "ready" })?;
/// assert_eq!(value, "ready");
/// # engine.destroy_with_timeout(std::time::Duration::from_secs(1))?;
/// # Ok(())
/// # }
/// ```
pub struct VibeEngineExecutor {
    inner: Arc<VibeEngineExecutorInner>,
}

pub(crate) struct VibeRuntimeHandle {
    handle: Handle,
    _runtime: Option<Arc<Runtime>>,
}

impl VibeRuntimeHandle {
    pub(crate) fn owned(runtime: Arc<Runtime>) -> Self {
        Self {
            handle: runtime.handle().clone(),
            _runtime: Some(runtime),
        }
    }

    pub(crate) fn external(handle: Handle) -> Self {
        Self {
            handle,
            _runtime: None,
        }
    }

    fn handle(&self) -> &Handle {
        &self.handle
    }
}

struct VibeEngineExecutorInner {
    callback_executor: VibeCallbackExecutor,
    async_tx: Mutex<Option<Sender<VibeEngineTask>>>,
    sync_tx: Mutex<Option<Sender<VibeEngineTask>>>,
    rt: VibeRuntimeHandle,
    shutdown_rx: Mutex<Receiver<()>>,
}

impl VibeEngineExecutor {
    pub(crate) fn new(
        cb_pool: ThreadPool,
        async_tx: Sender<VibeEngineTask>,
        sync_tx: Sender<VibeEngineTask>,
        rt: VibeRuntimeHandle,
        shutdown_rx: Receiver<()>,
    ) -> Self {
        Self {
            inner: Arc::new(VibeEngineExecutorInner {
                callback_executor: VibeCallbackExecutor::new(cb_pool),
                async_tx: Mutex::new(Some(async_tx)),
                sync_tx: Mutex::new(Some(sync_tx)),
                rt,
                shutdown_rx: Mutex::new(shutdown_rx),
            }),
        }
    }

    /// Returns the callback executor associated with this engine.
    ///
    /// # Returns
    ///
    /// A cheap clone of [`VibeCallbackExecutor`].
    pub fn callback(&self) -> VibeCallbackExecutor {
        self.inner.callback_executor.clone()
    }

    pub(crate) fn block_on_with_timeout<T, Fut>(
        &self,
        future: Fut,
        timeout: Duration,
    ) -> Result<T, VibeEngineError>
    where
        T: Send + 'static,
        Fut: Future<Output = Result<T, VibeEngineError>> + Send + 'static,
    {
        let handle = self.inner.rt.handle().clone();
        self.run_blocking_on_engine_rt(move || {
            handle.block_on(async move {
                tokio::time::timeout(timeout, future).await.map_err(|_| {
                    VibeEngineError::from_error_code(VibeEngineErrorCode::TimeoutError)
                })?
            })
        })?
    }

    pub(crate) fn shutdown_queues(&self, timeout: Duration) -> Result<(), VibeEngineError> {
        let async_tx = self
            .inner
            .async_tx
            .lock()
            .map_err(|_| VibeEngineError::from_error_code(VibeEngineErrorCode::RuntimeError))?
            .take();
        let sync_tx = self
            .inner
            .sync_tx
            .lock()
            .map_err(|_| VibeEngineError::from_error_code(VibeEngineErrorCode::RuntimeError))?
            .take();

        if async_tx.is_none() && sync_tx.is_none() {
            return Ok(());
        }

        drop(async_tx);
        drop(sync_tx);

        let shutdown_rx = self
            .inner
            .shutdown_rx
            .lock()
            .map_err(|_| VibeEngineError::from_error_code(VibeEngineErrorCode::RuntimeError))?;
        shutdown_rx
            .recv_timeout(timeout)
            .map_err(|_| VibeEngineError::from_error_code(VibeEngineErrorCode::TimeoutError))
    }

    fn run_blocking_on_engine_rt<R>(
        &self,
        f: impl FnOnce() -> R + Send + 'static,
    ) -> Result<R, VibeEngineError>
    where
        R: Send + 'static,
    {
        let engine_rt_id = self.inner.rt.handle().id();
        match Handle::try_current() {
            Ok(current) if current.id() == engine_rt_id => {
                std::panic::catch_unwind(AssertUnwindSafe(|| tokio::task::block_in_place(f)))
                    .map_err(|payload| {
                        log_e!(
                            "run_blocking_on_engine_rt",
                            DESC,
                            format!(
                                "engine runtime task panicked: {}",
                                panic_payload_message(&payload)
                            )
                        );
                        VibeEngineError::from_error_code(VibeEngineErrorCode::RuntimeError)
                    })
            }
            Ok(_) => {
                let handle =
                    std::thread::spawn(move || std::panic::catch_unwind(AssertUnwindSafe(f)));
                match handle.join() {
                    Ok(Ok(v)) => Ok(v),
                    Ok(Err(payload)) => {
                        log_e!(
                            "run_blocking_on_engine_rt",
                            DESC,
                            format!(
                                "helper thread task panicked: {}",
                                panic_payload_message(&payload)
                            )
                        );
                        Err(VibeEngineError::from_error_code(
                            VibeEngineErrorCode::RuntimeError,
                        ))
                    }
                    Err(payload) => {
                        log_e!(
                            "run_blocking_on_engine_rt",
                            DESC,
                            format!(
                                "helper thread panicked: {}",
                                panic_payload_message(&payload)
                            )
                        );
                        Err(VibeEngineError::from_error_code(
                            VibeEngineErrorCode::RuntimeError,
                        ))
                    }
                }
            }
            Err(_) => std::panic::catch_unwind(AssertUnwindSafe(f)).map_err(|payload| {
                log_e!(
                    "run_blocking_on_engine_rt",
                    DESC,
                    format!(
                        "non-runtime thread task panicked: {}",
                        panic_payload_message(&payload)
                    )
                );
                VibeEngineError::from_error_code(VibeEngineErrorCode::RuntimeError)
            }),
        }
    }

    /// Runs a future on the engine synchronous queue and waits for its output.
    ///
    /// # Returns
    ///
    /// `Ok(F)` with the future output, or [`VibeEngineError`] if the task could
    /// not be queued or the runtime failed while waiting.
    pub fn invoke<T, F>(&self, future: T) -> Result<F, VibeEngineError>
    where
        T: Future<Output = F> + Send + 'static,
        F: Send + 'static,
    {
        let (result_tx, result_rx) = oneshot::channel();
        let invoke_future = async move {
            let result = future.await;
            let _ = result_tx.send(result);
        };

        let handle = self.inner.rt.handle().clone();
        let sync_tx = self
            .inner
            .sync_tx
            .lock()
            .map_err(|_| VibeEngineError::from_error_code(VibeEngineErrorCode::RuntimeError))?
            .clone()
            .ok_or_else(|| VibeEngineError::from_code(VibeEngineErrorCode::PostError))?;
        self.run_blocking_on_engine_rt(move || {
            let task: VibeEngineTask = Box::pin(invoke_future);
            if handle.block_on(sync_tx.send(task)).is_err() {
                log_e!("invoke", DESC, "runtime handle block_on error");
                return Err(VibeEngineError::from_code(VibeEngineErrorCode::PostError));
            }
            result_rx.blocking_recv().map_err(|e| {
                log_e!("invoke", DESC, format!("blocking_recv error {}", e));
                VibeEngineError::from_error_code(VibeEngineErrorCode::RuntimeError)
            })
        })?
    }

    /// Posts a fire-and-forget future to the engine asynchronous queue.
    ///
    /// # Returns
    ///
    /// `Ok(())` after the future is queued, or [`VibeEngineError`] if queuing fails.
    pub fn post<T>(&self, future: T) -> Result<(), VibeEngineError>
    where
        T: Future<Output = ()> + Send + 'static,
    {
        let handle = self.inner.rt.handle().clone();
        let async_tx = self
            .inner
            .async_tx
            .lock()
            .map_err(|_| VibeEngineError::from_error_code(VibeEngineErrorCode::RuntimeError))?
            .clone()
            .ok_or_else(|| VibeEngineError::from_code(VibeEngineErrorCode::PostError))?;
        self.run_blocking_on_engine_rt(move || {
            let task: VibeEngineTask = Box::pin(future);
            handle.block_on(async_tx.send(task)).map_err(|error| {
                log_e!("post", DESC, format!("send async task error: {}", error));
                VibeEngineError::from_code(VibeEngineErrorCode::PostError)
            })
        })?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    #[test]
    fn callback_executor_supports_multi_argument_wrappers() {
        let executor = VibeCallbackExecutor::new(ThreadPool::new(1));
        let (tx, rx) = mpsc::channel();
        let callback = executor.once3(move |a: i32, b: i32, c: i32| {
            tx.send(a + b + c).expect("send callback result");
        });

        callback(1, 2, 3);

        assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), 6);
    }

    #[test]
    fn callback_executor_supports_boxed_string_wrappers() {
        let executor = VibeCallbackExecutor::new(ThreadPool::new(1));
        let (tx, rx) = mpsc::channel();
        let callback = executor.boxed_str_fn4(move |name, a: i32, b: i32, c: i32| {
            tx.send(format!("{name}:{a}:{b}:{c}"))
                .expect("send callback result");
        });

        callback("demo", 1, 2, 3);

        assert_eq!(
            rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            "demo:1:2:3"
        );
    }
}

#[cfg(test)]
mod strict_tests {
    use super::*;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test/unit/api/engine_executor_tests.rs"
    ));
}
