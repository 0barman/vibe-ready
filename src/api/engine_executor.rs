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
pub struct VibeCallbackExecutor {
    cb_pool: ThreadPool,
}

impl VibeCallbackExecutor {
    pub(crate) fn new(cb_pool: ThreadPool) -> Self {
        Self { cb_pool }
    }

    pub fn execute<F>(&self, cb: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.cb_pool.execute(cb);
    }

    pub fn once<F, R>(&self, cb: F) -> impl FnOnce(R) + Send + 'static
    where
        F: FnOnce(R) + Send + 'static,
        R: Send + 'static,
    {
        let executor = self.clone();
        move |value| executor.execute(move || cb(value))
    }

    pub fn once2<F, R1, R2>(&self, cb: F) -> impl FnOnce(R1, R2) + Send + 'static
    where
        F: FnOnce(R1, R2) + Send + 'static,
        R1: Send + 'static,
        R2: Send + 'static,
    {
        let executor = self.clone();
        move |value1, value2| executor.execute(move || cb(value1, value2))
    }

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
}

#[derive(Clone)]
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
