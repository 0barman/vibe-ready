//! Task scheduler primitives (B9 capability).
//!
//! Provides delayed/periodic execution, cancellation tokens, priority lanes,
//! and an inspection panel returning live snapshots of in-flight tasks.

use crate::api::engine_error::{VibeEngineError, VibeEngineErrorCode};
use crate::api::engine_executor::VibeEngineTask;
use crate::log::log_def::DESC;
use crate::{log_e, platform};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::Notify;

fn scheduler_lock_error(context: impl Into<String>) -> VibeEngineError {
    VibeEngineError::from_error_code(VibeEngineErrorCode::RuntimeError).with_context(context)
}

/// Priority lane used by [`VibeEngine::post_with_priority`] and friends.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum VibeTaskPriority {
    High = 0,
    Normal = 1,
    Low = 2,
}

impl VibeTaskPriority {
    fn as_str(self) -> &'static str {
        match self {
            VibeTaskPriority::High => "high",
            VibeTaskPriority::Normal => "normal",
            VibeTaskPriority::Low => "low",
        }
    }
}

/// Static category describing how a task was scheduled.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VibeTaskKind {
    /// One-shot task launched on a priority lane via `post_with_priority`.
    Once,
    /// One-shot task scheduled via `schedule_after`.
    Delayed,
    /// Repeating task scheduled via `schedule_every`.
    Periodic,
}

/// Lifecycle state of a scheduler-tracked task.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VibeTaskState {
    Pending,
    Running,
    Completed,
    Cancelled,
    Failed,
}

/// Cooperative cancellation token passed to scheduled tasks.
///
/// Tasks may poll [`is_cancelled`] before/after async points or `await`
/// [`cancelled`] to be notified the moment cancellation is requested.
#[derive(Clone)]
pub struct VibeCancellationToken {
    flag: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl VibeCancellationToken {
    pub fn new() -> Self {
        Self {
            flag: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Trip the token; idempotent.
    pub fn cancel(&self) {
        if !self.flag.swap(true, Ordering::AcqRel) {
            self.notify.notify_waiters();
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::Acquire)
    }

    /// Resolves once the token has been tripped.
    pub async fn cancelled(&self) {
        loop {
            if self.is_cancelled() {
                return;
            }
            let waiter = self.notify.notified();
            if self.is_cancelled() {
                return;
            }
            waiter.await;
            if self.is_cancelled() {
                return;
            }
        }
    }
}

impl Default for VibeCancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
struct TaskTimestamps {
    created_at_ms: i64,
    started_at_ms: Option<i64>,
    finished_at_ms: Option<i64>,
}

struct TaskInner {
    id: u64,
    name: String,
    kind: VibeTaskKind,
    priority: VibeTaskPriority,
    token: VibeCancellationToken,
    state: Mutex<VibeTaskState>,
    timestamps: Mutex<TaskTimestamps>,
    finished: Notify,
}

impl TaskInner {
    fn snapshot(&self) -> Result<VibeTaskInfo, VibeEngineError> {
        let state = *self
            .state
            .lock()
            .map_err(|_| scheduler_lock_error("task state lock poisoned"))?;
        let ts = self
            .timestamps
            .lock()
            .map_err(|_| scheduler_lock_error("task timestamps lock poisoned"))?
            .clone();
        Ok(VibeTaskInfo {
            id: self.id,
            name: self.name.clone(),
            kind: self.kind,
            priority: self.priority,
            state,
            created_at_ms: ts.created_at_ms,
            started_at_ms: ts.started_at_ms,
            finished_at_ms: ts.finished_at_ms,
        })
    }

    fn set_state(&self, new_state: VibeTaskState) -> Result<(), VibeEngineError> {
        let mut guard = self
            .state
            .lock()
            .map_err(|_| scheduler_lock_error("task state lock poisoned"))?;
        if matches!(
            *guard,
            VibeTaskState::Completed | VibeTaskState::Cancelled | VibeTaskState::Failed
        ) {
            return Ok(());
        }
        *guard = new_state;
        Ok(())
    }

    fn mark_started(&self) -> Result<(), VibeEngineError> {
        self.set_state(VibeTaskState::Running)?;
        let mut ts = self
            .timestamps
            .lock()
            .map_err(|_| scheduler_lock_error("task timestamps lock poisoned"))?;
        if ts.started_at_ms.is_none() {
            ts.started_at_ms = Some(platform::now());
        }
        Ok(())
    }

    fn finish(&self, final_state: VibeTaskState) -> Result<(), VibeEngineError> {
        {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| scheduler_lock_error("task state lock poisoned"))?;
            *guard = final_state;
        }
        {
            let mut ts = self
                .timestamps
                .lock()
                .map_err(|_| scheduler_lock_error("task timestamps lock poisoned"))?;
            if ts.finished_at_ms.is_none() {
                ts.finished_at_ms = Some(platform::now());
            }
        }
        self.finished.notify_waiters();
        Ok(())
    }
}

/// Snapshot of a scheduler-tracked task, returned by [`VibeTaskPanel::list`].
#[derive(Clone, Debug)]
pub struct VibeTaskInfo {
    pub id: u64,
    pub name: String,
    pub kind: VibeTaskKind,
    pub priority: VibeTaskPriority,
    pub state: VibeTaskState,
    pub created_at_ms: i64,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
}

/// Handle returned by every scheduling API.
///
/// Cloning is cheap: all clones share the same task state.
#[derive(Clone)]
pub struct VibeTaskHandle {
    inner: Arc<TaskInner>,
}

impl VibeTaskHandle {
    pub fn id(&self) -> u64 {
        self.inner.id
    }

    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub fn kind(&self) -> VibeTaskKind {
        self.inner.kind
    }

    pub fn priority(&self) -> VibeTaskPriority {
        self.inner.priority
    }

    pub fn state(&self) -> Result<VibeTaskState, VibeEngineError> {
        self.inner
            .state
            .lock()
            .map(|guard| *guard)
            .map_err(|_| scheduler_lock_error("task state lock poisoned"))
    }

    pub fn token(&self) -> VibeCancellationToken {
        self.inner.token.clone()
    }

    pub fn info(&self) -> Result<VibeTaskInfo, VibeEngineError> {
        self.inner.snapshot()
    }

    /// Trips the cancellation token. The task itself decides when to bail.
    /// Periodic tasks observe the token before scheduling the next iteration
    /// and `await` callers of [`join`] eventually return `Err(Cancelled)`.
    pub fn cancel(&self) {
        self.inner.token.cancel();
    }

    pub fn is_finished(&self) -> Result<bool, VibeEngineError> {
        Ok(matches!(
            self.state()?,
            VibeTaskState::Completed | VibeTaskState::Cancelled | VibeTaskState::Failed
        ))
    }

    /// Async join: resolves to `Ok(())` on completion, `Err(Cancelled)` on cancellation,
    /// or `Err(InternalError)` on panic.
    pub async fn join(&self) -> Result<(), VibeEngineError> {
        loop {
            match self.state()? {
                VibeTaskState::Completed => return Ok(()),
                VibeTaskState::Cancelled => {
                    return Err(VibeEngineError::from_error_code(
                        VibeEngineErrorCode::Cancelled,
                    ));
                }
                VibeTaskState::Failed => {
                    return Err(VibeEngineError::from_error_code(
                        VibeEngineErrorCode::InternalError,
                    ));
                }
                VibeTaskState::Pending | VibeTaskState::Running => {}
            }
            let notified = self.inner.finished.notified();
            if self.is_finished()? {
                continue;
            }
            notified.await;
        }
    }
}

/// Diagnostic snapshot of all live tasks owned by the engine scheduler.
#[derive(Clone)]
pub struct VibeTaskPanel {
    registry: Arc<TaskRegistry>,
}

impl VibeTaskPanel {
    pub fn list(&self) -> Result<Vec<VibeTaskInfo>, VibeEngineError> {
        self.registry.snapshot()
    }

    pub fn count(&self) -> Result<usize, VibeEngineError> {
        self.registry.len()
    }
}

struct TaskRegistry {
    tasks: Mutex<HashMap<u64, Arc<TaskInner>>>,
}

impl TaskRegistry {
    fn new() -> Self {
        Self {
            tasks: Mutex::new(HashMap::new()),
        }
    }

    fn insert(&self, task: Arc<TaskInner>) -> Result<(), VibeEngineError> {
        self.tasks
            .lock()
            .map_err(|_| scheduler_lock_error("task registry lock poisoned"))?
            .insert(task.id, task);
        Ok(())
    }

    fn remove(&self, id: u64) -> Result<(), VibeEngineError> {
        self.tasks
            .lock()
            .map_err(|_| scheduler_lock_error("task registry lock poisoned"))?
            .remove(&id);
        Ok(())
    }

    fn snapshot(&self) -> Result<Vec<VibeTaskInfo>, VibeEngineError> {
        let guard = self
            .tasks
            .lock()
            .map_err(|_| scheduler_lock_error("task registry lock poisoned"))?;
        guard.values().map(|t| t.snapshot()).collect()
    }

    fn len(&self) -> Result<usize, VibeEngineError> {
        Ok(self
            .tasks
            .lock()
            .map_err(|_| scheduler_lock_error("task registry lock poisoned"))?
            .len())
    }

    fn cancel_all(&self) -> Result<Vec<Arc<TaskInner>>, VibeEngineError> {
        let guard = self
            .tasks
            .lock()
            .map_err(|_| scheduler_lock_error("task registry lock poisoned"))?;
        let snapshot: Vec<Arc<TaskInner>> = guard.values().cloned().collect();
        for task in &snapshot {
            task.token.cancel();
        }
        Ok(snapshot)
    }
}

/// Internal scheduler driving priority lanes and tracking task state.
pub(crate) struct VibeTaskScheduler {
    handle: Handle,
    registry: Arc<TaskRegistry>,
    next_id: AtomicU64,
    senders: Mutex<Option<[Sender<VibeEngineTask>; 3]>>,
}

impl VibeTaskScheduler {
    pub(crate) fn new(handle: Handle, capacity: usize) -> Arc<Self> {
        let cap = capacity.max(1);
        let (high_tx, mut high_rx) = channel::<VibeEngineTask>(cap);
        let (normal_tx, mut normal_rx) = channel::<VibeEngineTask>(cap);
        let (low_tx, mut low_rx) = channel::<VibeEngineTask>(cap);

        // Single dispatcher that drains the three priority channels with a
        // biased select: when multiple lanes have work ready, the high lane
        // wins, then normal, then low. Each pulled task is awaited inline so
        // ordering of dispatch is preserved (priority cannot be circumvented
        // by tokio's scheduler once we've handed work to it).
        handle.spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    maybe = high_rx.recv() => {
                        match maybe {
                            Some(task) => task.await,
                            None => {
                                // High lane closed: drain remaining lanes
                                // before exiting so already-queued work runs.
                                while let Some(task) = normal_rx.recv().await {
                                    task.await;
                                }
                                while let Some(task) = low_rx.recv().await {
                                    task.await;
                                }
                                break;
                            }
                        }
                    }
                    maybe = normal_rx.recv() => {
                        if let Some(task) = maybe {
                            task.await;
                        }
                    }
                    maybe = low_rx.recv() => {
                        if let Some(task) = maybe {
                            task.await;
                        }
                    }
                }
            }
        });

        Arc::new(Self {
            handle,
            registry: Arc::new(TaskRegistry::new()),
            next_id: AtomicU64::new(1),
            senders: Mutex::new(Some([high_tx, normal_tx, low_tx])),
        })
    }

    pub(crate) fn handle(&self) -> &Handle {
        &self.handle
    }

    pub(crate) fn panel(self: &Arc<Self>) -> VibeTaskPanel {
        VibeTaskPanel {
            registry: Arc::clone(&self.registry),
        }
    }

    fn make_task(
        &self,
        name: String,
        kind: VibeTaskKind,
        priority: VibeTaskPriority,
    ) -> Result<Arc<TaskInner>, VibeEngineError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let inner = Arc::new(TaskInner {
            id,
            name,
            kind,
            priority,
            token: VibeCancellationToken::new(),
            state: Mutex::new(VibeTaskState::Pending),
            timestamps: Mutex::new(TaskTimestamps {
                created_at_ms: platform::now(),
                started_at_ms: None,
                finished_at_ms: None,
            }),
            finished: Notify::new(),
        });
        self.registry.insert(Arc::clone(&inner))?;
        Ok(inner)
    }

    fn priority_sender(
        &self,
        priority: VibeTaskPriority,
    ) -> Result<Sender<VibeEngineTask>, VibeEngineError> {
        let guard = self
            .senders
            .lock()
            .map_err(|_| VibeEngineError::from_error_code(VibeEngineErrorCode::RuntimeError))?;
        let array = guard
            .as_ref()
            .ok_or_else(|| VibeEngineError::from_error_code(VibeEngineErrorCode::PostError))?;
        Ok(array[priority as usize].clone())
    }

    /// Send a future to the priority dispatcher and track it in the registry.
    pub(crate) fn post_with_priority<F>(
        &self,
        name: impl Into<String>,
        priority: VibeTaskPriority,
        future: F,
    ) -> Result<VibeTaskHandle, VibeEngineError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let task = self.make_task(name.into(), VibeTaskKind::Once, priority)?;
        let registry = Arc::clone(&self.registry);
        let task_for_run = Arc::clone(&task);
        let token = task.token.clone();
        let wrapped: VibeEngineTask = Box::pin(async move {
            if token.is_cancelled() {
                if let Err(error) = task_for_run.finish(VibeTaskState::Cancelled) {
                    log_e!("scheduler.post_with_priority", DESC, format!("finish failed: {error}"));
                }
                if let Err(error) = registry.remove(task_for_run.id) {
                    log_e!("scheduler.post_with_priority", DESC, format!("registry remove failed: {error}"));
                }
                return;
            }
            if let Err(error) = task_for_run.mark_started() {
                log_e!("scheduler.post_with_priority", DESC, format!("mark started failed: {error}"));
                return;
            }
            let final_state = run_user_future(Box::pin(future), &token).await;
            if let Err(error) = task_for_run.finish(final_state) {
                log_e!("scheduler.post_with_priority", DESC, format!("finish failed: {error}"));
            }
            if let Err(error) = registry.remove(task_for_run.id) {
                log_e!("scheduler.post_with_priority", DESC, format!("registry remove failed: {error}"));
            }
        });

        let sender = self.priority_sender(priority)?;
        let task_for_send = Arc::clone(&task);
        let registry_for_send = Arc::clone(&self.registry);
        // Non-blocking send so callers running on the tokio runtime thread
        // never deadlock. The capacity is configured high enough that this
        // path should virtually never fail under normal load.
        if let Err(err) = sender.try_send(wrapped) {
            log_e!(
                "scheduler.post_with_priority",
                DESC,
                format!("send to priority lane {} failed: {err}", priority.as_str())
            );
            if let Err(error) = task_for_send.finish(VibeTaskState::Failed) {
                log_e!("scheduler.post_with_priority", DESC, format!("finish failed: {error}"));
            }
            if let Err(error) = registry_for_send.remove(task_for_send.id) {
                log_e!("scheduler.post_with_priority", DESC, format!("registry remove failed: {error}"));
            }
            return Err(VibeEngineError::from_error_code(
                VibeEngineErrorCode::PostError,
            ));
        }
        Ok(VibeTaskHandle { inner: task })
    }

    /// One-shot delayed task. The future is built lazily after the delay so
    /// callers do not pay the cost of the work until it is actually due.
    pub(crate) fn schedule_after<F, Fut>(
        &self,
        name: impl Into<String>,
        delay: Duration,
        builder: F,
    ) -> Result<VibeTaskHandle, VibeEngineError>
    where
        F: FnOnce(VibeCancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let task = self.make_task(name.into(), VibeTaskKind::Delayed, VibeTaskPriority::Normal)?;
        let registry = Arc::clone(&self.registry);
        let task_for_run = Arc::clone(&task);
        let token = task.token.clone();
        self.handle.spawn(async move {
            tokio::select! {
                _ = token.cancelled() => {
                    if let Err(error) = task_for_run.finish(VibeTaskState::Cancelled) {
                        log_e!("scheduler.schedule_after", DESC, format!("finish failed: {error}"));
                    }
                    if let Err(error) = registry.remove(task_for_run.id) {
                        log_e!("scheduler.schedule_after", DESC, format!("registry remove failed: {error}"));
                    }
                    return;
                }
                _ = tokio::time::sleep(delay) => {}
            }
            if token.is_cancelled() {
                if let Err(error) = task_for_run.finish(VibeTaskState::Cancelled) {
                    log_e!("scheduler.schedule_after", DESC, format!("finish failed: {error}"));
                }
                if let Err(error) = registry.remove(task_for_run.id) {
                    log_e!("scheduler.schedule_after", DESC, format!("registry remove failed: {error}"));
                }
                return;
            }
            if let Err(error) = task_for_run.mark_started() {
                log_e!("scheduler.schedule_after", DESC, format!("mark started failed: {error}"));
                return;
            }
            let fut = Box::pin(builder(token.clone()));
            let final_state = run_user_future(fut, &token).await;
            if let Err(error) = task_for_run.finish(final_state) {
                log_e!("scheduler.schedule_after", DESC, format!("finish failed: {error}"));
            }
            if let Err(error) = registry.remove(task_for_run.id) {
                log_e!("scheduler.schedule_after", DESC, format!("registry remove failed: {error}"));
            }
        });
        Ok(VibeTaskHandle { inner: task })
    }

    /// Periodic task. The builder is invoked every `period` until cancelled.
    /// The first invocation occurs after waiting one period; this matches
    /// `tokio::time::interval` semantics with `MissedTickBehavior::Delay`.
    pub(crate) fn schedule_every<F, Fut>(
        &self,
        name: impl Into<String>,
        period: Duration,
        mut builder: F,
    ) -> Result<VibeTaskHandle, VibeEngineError>
    where
        F: FnMut(VibeCancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let task = self.make_task(name.into(), VibeTaskKind::Periodic, VibeTaskPriority::Normal)?;
        let registry = Arc::clone(&self.registry);
        let task_for_run = Arc::clone(&task);
        let token = task.token.clone();
        self.handle.spawn(async move {
            if let Err(error) = task_for_run.mark_started() {
                log_e!("scheduler.schedule_every", DESC, format!("mark started failed: {error}"));
                return;
            }
            loop {
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = tokio::time::sleep(period) => {}
                }
                if token.is_cancelled() {
                    break;
                }
                let fut = Box::pin(builder(token.clone()));
                let state = run_user_future(fut, &token).await;
                if !matches!(state, VibeTaskState::Completed) {
                    // Cancellation or panic terminates the periodic loop.
                    if let Err(error) = task_for_run.finish(state) {
                        log_e!("scheduler.schedule_every", DESC, format!("finish failed: {error}"));
                    }
                    if let Err(error) = registry.remove(task_for_run.id) {
                        log_e!("scheduler.schedule_every", DESC, format!("registry remove failed: {error}"));
                    }
                    return;
                }
            }
            if let Err(error) = task_for_run.finish(VibeTaskState::Cancelled) {
                log_e!("scheduler.schedule_every", DESC, format!("finish failed: {error}"));
            }
            if let Err(error) = registry.remove(task_for_run.id) {
                log_e!("scheduler.schedule_every", DESC, format!("registry remove failed: {error}"));
            }
        });
        Ok(VibeTaskHandle { inner: task })
    }

    /// Trip every tracked task and drop the priority senders so the
    /// dispatcher exits once already-queued work drains.
    pub(crate) fn shutdown(&self) {
        if let Err(error) = self.registry.cancel_all() {
            log_e!("scheduler.shutdown", DESC, format!("cancel all failed: {error}"));
        }
        if let Ok(mut guard) = self.senders.lock() {
            *guard = None;
        }
    }
}

async fn run_user_future(
    fut: Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
    token: &VibeCancellationToken,
) -> VibeTaskState {
    use futures::future::FutureExt;
    use std::panic::AssertUnwindSafe;
    let outcome = AssertUnwindSafe(fut).catch_unwind().await;
    match outcome {
        Ok(()) => {
            if token.is_cancelled() {
                VibeTaskState::Cancelled
            } else {
                VibeTaskState::Completed
            }
        }
        Err(payload) => {
            let msg = if let Some(s) = payload.downcast_ref::<&str>() {
                (*s).to_string()
            } else if let Some(s) = payload.downcast_ref::<String>() {
                s.clone()
            } else {
                "unknown panic payload".to_string()
            };
            log_e!(
                "scheduler.run_user_future",
                DESC,
                format!("scheduled task panicked: {msg}")
            );
            VibeTaskState::Failed
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancellation_token_resolves_for_concurrent_waiters() {
        let token = VibeCancellationToken::new();
        let t1 = token.clone();
        let t2 = token.clone();
        let h1 = tokio::spawn(async move { t1.cancelled().await });
        let h2 = tokio::spawn(async move { t2.cancelled().await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        token.cancel();
        assert!(h1.await.is_ok());
        assert!(h2.await.is_ok());
        assert!(token.is_cancelled());
    }
}
