use std::error::Error as StdError;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

#[cfg(test)]
use std::sync::atomic::AtomicUsize;

use rmcp::model::{
    CallToolRequestParams, CallToolResult, ClientCapabilities, ClientInfo, Implementation,
};
use rmcp::service::{QuitReason, RoleClient, RunningService};
use rmcp::transport::IntoTransport;
use rmcp::{Peer, ServiceError, ServiceExt};
use serde_json::Value;
use tokio::sync::{Mutex, Notify};
use tokio_util::sync::CancellationToken;

use crate::{
    McpAdapterError, McpAdapterErrorKind, McpServerConfig, McpServerId,
    config::{McpStdioParts, validate_remote_name},
};

const ACTIVE: u8 = 0;
const CLOSING: u8 = 1;
const CLOSED: u8 = 2;

/// A cheaply shared, initialized MCP client session.
#[derive(Clone)]
pub struct McpClientSession {
    inner: Arc<SessionInner>,
}

struct SessionInner {
    server_id: McpServerId,
    peer: Peer<RoleClient>,
    lifecycle: Mutex<SessionLifecycle>,
    cancellation: CancellationToken,
    state: Arc<AtomicU8>,
    child_process_id: Option<u32>,
}

struct SessionLifecycle {
    running: Option<RunningService<RoleClient, ClientInfo>>,
    child: Option<StdioChildGuard>,
    completion: Option<Arc<ShutdownCompletion>>,
    cleanup_task: Option<tokio::task::JoinHandle<()>>,
}

type ServiceCloseFuture =
    Pin<Box<dyn Future<Output = Result<QuitReason, tokio::task::JoinError>> + Send + 'static>>;
type ChildCleanupFuture =
    Pin<Box<dyn Future<Output = Result<(), McpAdapterError>> + Send + 'static>>;

struct ShutdownCompletion {
    slot: StdMutex<ShutdownCompletionSlot>,
    notify: Notify,
}

struct ShutdownCompletionSlot {
    result: Option<Result<(), McpAdapterError>>,
    published: bool,
}

impl ShutdownCompletion {
    fn new() -> Self {
        Self {
            slot: StdMutex::new(ShutdownCompletionSlot {
                result: None,
                published: false,
            }),
            notify: Notify::new(),
        }
    }

    fn complete_after_closed(&self, result: Result<(), McpAdapterError>, state: &AtomicU8) {
        let mut slot = self
            .slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if slot.result.is_none() {
            slot.result = Some(result);
            state.store(CLOSED, Ordering::Release);
            slot.published = true;
            drop(slot);
            self.notify.notify_waiters();
        }
    }

    async fn wait(&self) -> Result<(), McpAdapterError> {
        loop {
            let notified = self.notify.notified();
            {
                let slot = self
                    .slot
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if slot.published {
                    return slot
                        .result
                        .as_ref()
                        .expect("published shutdown completion has a result")
                        .clone();
                }
            }
            notified.await;
        }
    }
}

struct StdioChildGuard {
    child: Option<Child>,
    shutdown_grace: Duration,
    #[cfg(test)]
    panic_on_terminate: bool,
}

#[cfg(test)]
static DROP_REAPER_SPAWNS: AtomicUsize = AtomicUsize::new(0);

impl StdioChildGuard {
    fn new(child: Child, shutdown_grace: Duration) -> Self {
        Self {
            child: Some(child),
            shutdown_grace,
            #[cfg(test)]
            panic_on_terminate: false,
        }
    }

    fn id(&self) -> u32 {
        self.child.as_ref().expect("child guard is armed").id()
    }

    fn take_stdout(&mut self) -> Option<std::process::ChildStdout> {
        self.child
            .as_mut()
            .expect("child guard is armed")
            .stdout
            .take()
    }

    fn take_stdin(&mut self) -> Option<std::process::ChildStdin> {
        self.child
            .as_mut()
            .expect("child guard is armed")
            .stdin
            .take()
    }

    #[cfg(test)]
    fn arm_termination_panic(&mut self) {
        self.panic_on_terminate = true;
    }

    fn terminate(&mut self) -> std::io::Result<()> {
        #[cfg(test)]
        if std::mem::take(&mut self.panic_on_terminate) {
            panic!("SECRET_REAL_CHILD_CLEANUP_PANIC");
        }

        let deadline = Instant::now().checked_add(self.shutdown_grace);
        let mut first_error = None;
        loop {
            let Some(child) = self.child.as_mut() else {
                return Ok(());
            };
            match child.try_wait() {
                Ok(Some(_status)) => {
                    self.child = None;
                    return Ok(());
                }
                Ok(None) => {}
                Err(source) => {
                    first_error = Some(source);
                    break;
                }
            }
            let Some(deadline) = deadline else {
                break;
            };
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            std::thread::sleep(
                deadline
                    .saturating_duration_since(now)
                    .min(Duration::from_millis(10)),
            );
        }

        self.kill_and_wait(first_error)
    }

    fn kill_and_wait(&mut self, mut first_error: Option<std::io::Error>) -> std::io::Result<()> {
        let Some(child) = self.child.as_mut() else {
            return first_error.map_or(Ok(()), Err);
        };
        if let Err(source) = child.kill()
            && first_error.is_none()
        {
            first_error = Some(source);
        }
        match child.wait() {
            Ok(_) => self.child = None,
            Err(source) if first_error.is_none() => first_error = Some(source),
            Err(_) => {}
        }
        first_error.map_or(Ok(()), Err)
    }
}

impl Drop for StdioChildGuard {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        let _ = child.kill();
        let reaper_spawned = std::thread::Builder::new()
            .name("group-mcp-child-reaper".to_owned())
            .spawn(move || {
                let _ = child.wait();
            })
            .is_ok();
        #[cfg(test)]
        if reaper_spawned {
            DROP_REAPER_SPAWNS.fetch_add(1, Ordering::Relaxed);
        }
        #[cfg(not(test))]
        let _ = reaper_spawned;
    }
}

impl Drop for SessionInner {
    fn drop(&mut self) {
        self.state.store(CLOSING, Ordering::Release);
        self.cancellation.cancel();
    }
}

impl McpClientSession {
    /// Initializes a client over an arbitrary async read/write transport.
    pub async fn connect<T, E, A>(
        server_id: McpServerId,
        transport: T,
    ) -> Result<Self, McpAdapterError>
    where
        T: IntoTransport<RoleClient, E, A>,
        E: StdError + Send + Sync + 'static,
    {
        Self::connect_inner(server_id, transport, None, None).await
    }

    /// Spawns and initializes a child-process stdio MCP server.
    pub async fn connect_stdio(config: McpServerConfig) -> Result<Self, McpAdapterError> {
        let McpStdioParts {
            server_id,
            executable,
            args,
            environment,
            inherit_environment,
            current_dir,
            shutdown_grace,
        } = config.into_parts();
        let mut command = Command::new(executable);
        command.args(args);
        if !inherit_environment {
            command.env_clear();
        }
        command.envs(environment);
        if let Some(current_dir) = current_dir {
            command.current_dir(current_dir);
        }
        command.stdin(Stdio::piped()).stdout(Stdio::piped());
        let child = command.spawn().map_err(|source| {
            McpAdapterError::with_source(McpAdapterErrorKind::InitializationFailed, source)
                .with_server(server_id.clone())
        })?;
        let mut child = StdioChildGuard::new(child, shutdown_grace);
        let child_process_id = child.id();
        let child_stdout = child.take_stdout().ok_or_else(|| {
            McpAdapterError::with_source(
                McpAdapterErrorKind::InitializationFailed,
                std::io::Error::other("stdio child stdout was unavailable"),
            )
            .with_server(server_id.clone())
        })?;
        let child_stdin = child.take_stdin().ok_or_else(|| {
            McpAdapterError::with_source(
                McpAdapterErrorKind::InitializationFailed,
                std::io::Error::other("stdio child stdin was unavailable"),
            )
            .with_server(server_id.clone())
        })?;
        let child_stdout =
            tokio::process::ChildStdout::from_std(child_stdout).map_err(|source| {
                McpAdapterError::with_source(McpAdapterErrorKind::InitializationFailed, source)
                    .with_server(server_id.clone())
            })?;
        let child_stdin = tokio::process::ChildStdin::from_std(child_stdin).map_err(|source| {
            McpAdapterError::with_source(McpAdapterErrorKind::InitializationFailed, source)
                .with_server(server_id.clone())
        })?;
        Self::connect_inner(
            server_id,
            (child_stdout, child_stdin),
            Some(child_process_id),
            Some(child),
        )
        .await
    }

    async fn connect_inner<T, E, A>(
        server_id: McpServerId,
        transport: T,
        child_process_id: Option<u32>,
        child: Option<StdioChildGuard>,
    ) -> Result<Self, McpAdapterError>
    where
        T: IntoTransport<RoleClient, E, A>,
        E: StdError + Send + Sync + 'static,
    {
        let cancellation = CancellationToken::new();
        let client_info = ClientInfo::new(
            ClientCapabilities::default(),
            Implementation::new("group-agent-mcp", env!("CARGO_PKG_VERSION")),
        );
        let running = client_info
            .serve_with_ct(transport, cancellation.clone())
            .await
            .map_err(|source| {
                let kind = initialization_kind(&source);
                McpAdapterError::with_source(kind, source).with_server(server_id.clone())
            })?;
        let peer = running.peer().clone();
        Ok(Self {
            inner: Arc::new(SessionInner {
                server_id,
                peer,
                lifecycle: Mutex::new(SessionLifecycle {
                    running: Some(running),
                    child,
                    completion: None,
                    cleanup_task: None,
                }),
                cancellation,
                state: Arc::new(AtomicU8::new(ACTIVE)),
                child_process_id,
            }),
        })
    }

    /// Returns the server identity.
    #[must_use]
    pub fn server_id(&self) -> &McpServerId {
        &self.inner.server_id
    }

    /// Returns whether shutdown has begun or the rmcp transport has closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.inner.state.load(Ordering::Acquire) != ACTIVE || self.inner.peer.is_transport_closed()
    }

    /// Returns the child process ID for a stdio session.
    #[must_use]
    pub fn child_process_id(&self) -> Option<u32> {
        self.inner.child_process_id
    }

    /// Explicitly closes rmcp, then waits for or terminates the direct child.
    ///
    /// For stdio, this method closes child stdin through the rmcp service,
    /// waits for the configured grace period, then kills and reaps the direct
    /// child if needed. Zero grace performs one non-blocking exit check before
    /// immediate kill/wait. It does not claim process-tree cleanup. Concurrent
    /// and repeated callers observe the same stored final result. Cancelling a
    /// caller's shutdown Future does not cancel the Session-owned cleanup.
    /// Service close and child cleanup run independently, so an rmcp error or
    /// worker panic cannot skip direct-child cleanup. Both success and failure
    /// are published only after that cleanup completes; `CLOSED` is visible
    /// before waiters wake. If service and child cleanup both fail, the service
    /// failure is primary. rmcp task JoinErrors and worker panics are
    /// `ShutdownFailed`; rmcp 2.2.0 does not expose errors returned by
    /// `transport.close()`. Prefer this method over the best-effort Drop
    /// fallback whenever lifecycle completion matters.
    pub async fn shutdown(&self) -> Result<(), McpAdapterError> {
        let mut lifecycle = self.inner.lifecycle.lock().await;
        let completion = if let Some(completion) = &lifecycle.completion {
            Arc::clone(completion)
        } else {
            self.inner.state.store(CLOSING, Ordering::Release);
            let server_id = self.inner.server_id.clone();
            let service: ServiceCloseFuture = if let Some(mut running) = lifecycle.running.take() {
                Box::pin(async move { running.close().await })
            } else {
                Box::pin(async { Ok(QuitReason::Closed) })
            };
            let child: ChildCleanupFuture = if let Some(child) = lifecycle.child.take() {
                child_cleanup_future(server_id.clone(), child)
            } else {
                Box::pin(async { Ok(()) })
            };
            let (completion, task) =
                spawn_shutdown_cleanup(server_id, Arc::clone(&self.inner.state), service, child);
            lifecycle.completion = Some(Arc::clone(&completion));
            lifecycle.cleanup_task = Some(task);
            completion
        };
        drop(lifecycle);
        completion.wait().await
    }

    pub(crate) fn peer(&self) -> Result<&Peer<RoleClient>, McpAdapterError> {
        if self.inner.state.load(Ordering::Acquire) != ACTIVE {
            Err(McpAdapterError::new(McpAdapterErrorKind::SessionClosed)
                .with_server(self.inner.server_id.clone()))
        } else if self.inner.peer.is_transport_closed() {
            Err(McpAdapterError::with_source(
                McpAdapterErrorKind::Transport,
                ServiceError::TransportClosed,
            )
            .with_server(self.inner.server_id.clone()))
        } else {
            Ok(&self.inner.peer)
        }
    }

    pub(crate) async fn call_tool(
        &self,
        remote_name: &str,
        arguments: &Value,
    ) -> Result<CallToolResult, McpAdapterError> {
        validate_remote_name(remote_name).map_err(McpAdapterError::from)?;
        let peer = self.peer()?;
        let mut params = CallToolRequestParams::new(remote_name.to_owned());
        match arguments {
            Value::Object(arguments) => {
                params = params.with_arguments(arguments.clone());
            }
            Value::Null => {}
            _ => {
                return Err(McpAdapterError::new(McpAdapterErrorKind::Protocol)
                    .with_server(self.inner.server_id.clone()));
            }
        }
        peer.call_tool(params).await.map_err(|source| {
            let kind = service_error_kind(&source);
            McpAdapterError::with_source(kind, source).with_server(self.inner.server_id.clone())
        })
    }
}

fn spawn_shutdown_cleanup(
    server_id: McpServerId,
    state: Arc<AtomicU8>,
    service: ServiceCloseFuture,
    child: ChildCleanupFuture,
) -> (Arc<ShutdownCompletion>, tokio::task::JoinHandle<()>) {
    let completion = Arc::new(ShutdownCompletion::new());
    let service_server_id = server_id.clone();
    let service_task =
        tokio::spawn(async move { map_quit_reason(service.await, &service_server_id) });
    let child_task = tokio::spawn(child);
    let task_completion = Arc::clone(&completion);
    let task = tokio::spawn(async move {
        let (service_result, child_result) = tokio::join!(service_task, child_task);
        let service_result = map_cleanup_task_result(service_result, &server_id);
        let child_result = map_cleanup_task_result(child_result, &server_id);
        let result = combine_shutdown_results(service_result, child_result);
        task_completion.complete_after_closed(result, &state);
    });
    (completion, task)
}

fn child_cleanup_future(server_id: McpServerId, mut child: StdioChildGuard) -> ChildCleanupFuture {
    Box::pin(async move {
        tokio::task::spawn_blocking(move || {
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| child.terminate()));
            match result {
                Ok(result) => result,
                Err(payload) => {
                    let _ = child.kill_and_wait(None);
                    std::panic::resume_unwind(payload);
                }
            }
        })
        .await
        .map_err(|source| {
            McpAdapterError::with_source(McpAdapterErrorKind::ShutdownFailed, source)
                .with_server(server_id.clone())
        })?
        .map_err(|source| {
            McpAdapterError::with_source(McpAdapterErrorKind::ShutdownFailed, source)
                .with_server(server_id)
        })
    })
}

fn map_cleanup_task_result(
    result: Result<Result<(), McpAdapterError>, tokio::task::JoinError>,
    server_id: &McpServerId,
) -> Result<(), McpAdapterError> {
    result.unwrap_or_else(|source| {
        Err(
            McpAdapterError::with_source(McpAdapterErrorKind::ShutdownFailed, source)
                .with_server(server_id.clone()),
        )
    })
}

fn combine_shutdown_results(
    service_result: Result<(), McpAdapterError>,
    child_result: Result<(), McpAdapterError>,
) -> Result<(), McpAdapterError> {
    match service_result {
        Err(service_error) => Err(service_error),
        Ok(()) => child_result,
    }
}

fn map_quit_reason(
    result: Result<QuitReason, tokio::task::JoinError>,
    server_id: &McpServerId,
) -> Result<(), McpAdapterError> {
    match result {
        Ok(QuitReason::Cancelled | QuitReason::Closed) => Ok(()),
        Ok(QuitReason::JoinError(source)) | Err(source) => Err(McpAdapterError::with_source(
            McpAdapterErrorKind::ShutdownFailed,
            source,
        )
        .with_server(server_id.clone())),
        _ => Err(McpAdapterError::new(McpAdapterErrorKind::ShutdownFailed)
            .with_server(server_id.clone())),
    }
}

impl fmt::Debug for McpClientSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpClientSession")
            .field("server_id", &self.inner.server_id)
            .field("closed", &self.is_closed())
            .field("has_child_process", &self.inner.child_process_id.is_some())
            .finish()
    }
}

fn initialization_kind(error: &rmcp::service::ClientInitializeError) -> McpAdapterErrorKind {
    match error {
        rmcp::service::ClientInitializeError::TransportError { .. }
        | rmcp::service::ClientInitializeError::ConnectionClosed(_) => {
            McpAdapterErrorKind::Transport
        }
        rmcp::service::ClientInitializeError::JsonRpcError(_)
        | rmcp::service::ClientInitializeError::ExpectedInitResponse(_)
        | rmcp::service::ClientInitializeError::ExpectedInitResult(_)
        | rmcp::service::ClientInitializeError::ConflictInitResponseId(_, _) => {
            McpAdapterErrorKind::Protocol
        }
        rmcp::service::ClientInitializeError::Cancelled => {
            McpAdapterErrorKind::InitializationFailed
        }
        _ => McpAdapterErrorKind::InitializationFailed,
    }
}

pub(crate) fn service_error_kind(error: &ServiceError) -> McpAdapterErrorKind {
    match error {
        ServiceError::TransportSend(_) => McpAdapterErrorKind::Transport,
        ServiceError::TransportClosed => McpAdapterErrorKind::Transport,
        ServiceError::UnexpectedResponse | ServiceError::McpError(_) => {
            McpAdapterErrorKind::Protocol
        }
        ServiceError::Cancelled { .. } | ServiceError::Timeout { .. } => {
            McpAdapterErrorKind::CallFailed
        }
        _ => McpAdapterErrorKind::Other,
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;
    use std::path::{Path, PathBuf};
    use std::process::Stdio;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::sync::Semaphore;

    use super::*;

    async fn join_error(message: &'static str) -> tokio::task::JoinError {
        tokio::spawn(async move { panic!("{message}") })
            .await
            .expect_err("fixture task panics")
    }

    const STUBBORN_FIXTURE_MARKER: &str = "GROUP_AGENT_MCP_STUBBORN_FIXTURE_MARKER";

    #[test]
    fn stubborn_stdio_child_fixture() {
        let Some(marker) = std::env::var_os(STUBBORN_FIXTURE_MARKER) else {
            return;
        };
        std::fs::write(marker, std::process::id().to_string())
            .expect("stubborn fixture writes its startup marker");
        loop {
            std::thread::park();
        }
    }

    fn fixture_marker_path() -> PathBuf {
        static NEXT_MARKER: AtomicUsize = AtomicUsize::new(0);

        std::env::temp_dir().join(format!(
            "group-agent-mcp-shutdown-panic-{}-{}",
            std::process::id(),
            NEXT_MARKER.fetch_add(1, Ordering::Relaxed)
        ))
    }

    async fn spawn_stubborn_stdio_child(
        shutdown_grace: Duration,
    ) -> (StdioChildGuard, u32, PathBuf) {
        let marker = fixture_marker_path();
        let executable = std::env::current_exe().expect("current test executable is available");
        let child = Command::new(&executable)
            .arg("--exact")
            .arg("session::tests::stubborn_stdio_child_fixture")
            .arg("--nocapture")
            .env(STUBBORN_FIXTURE_MARKER, &marker)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("real stubborn stdio child starts");
        let guard = StdioChildGuard::new(child, shutdown_grace);
        let pid = guard.id();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !marker.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("stubborn child startup marker appears");
        (guard, pid, marker)
    }

    #[cfg(unix)]
    fn process_exists(pid: u32) -> bool {
        Command::new("kill")
            .arg("-0")
            .arg(pid.to_string())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
    }

    fn remove_marker(marker: &Path) {
        if let Err(source) = std::fs::remove_file(marker) {
            assert_eq!(
                source.kind(),
                std::io::ErrorKind::NotFound,
                "fixture marker cleanup failed"
            );
        }
    }

    #[tokio::test]
    async fn quit_reason_join_error_is_shutdown_failed_source_preserving_and_redacted() {
        let server_id = McpServerId::new("quit-reason").expect("valid id");
        assert!(map_quit_reason(Ok(QuitReason::Closed), &server_id).is_ok());
        assert!(map_quit_reason(Ok(QuitReason::Cancelled), &server_id).is_ok());

        let error = map_quit_reason(
            Ok(QuitReason::JoinError(
                join_error("SECRET_RMCP_JOIN_ERROR").await,
            )),
            &server_id,
        )
        .expect_err("inner JoinError fails shutdown");
        assert_eq!(error.kind(), McpAdapterErrorKind::ShutdownFailed);
        assert!(
            error
                .source()
                .is_some_and(|source| source.is::<tokio::task::JoinError>())
        );
        assert!(!format!("{error}").contains("SECRET_RMCP_JOIN_ERROR"));
        assert!(!format!("{error:?}").contains("SECRET_RMCP_JOIN_ERROR"));

        let outer = map_quit_reason(Err(join_error("SECRET_OUTER_JOIN_ERROR").await), &server_id)
            .expect_err("outer JoinError fails shutdown");
        assert_eq!(outer.kind(), McpAdapterErrorKind::ShutdownFailed);
        assert!(
            outer
                .source()
                .is_some_and(|source| source.is::<tokio::task::JoinError>())
        );
        assert!(!format!("{outer:?}").contains("SECRET_OUTER_JOIN_ERROR"));
    }

    #[tokio::test]
    async fn join_error_still_runs_child_cleanup_before_publishing_failure() {
        let child_calls = Arc::new(AtomicUsize::new(0));
        let child_calls_for_future = Arc::clone(&child_calls);
        let service: ServiceCloseFuture = Box::pin(async {
            Ok(QuitReason::JoinError(
                join_error("SECRET_SERVICE_TASK_FAILURE").await,
            ))
        });
        let child: ChildCleanupFuture = Box::pin(async move {
            child_calls_for_future.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });
        let state = Arc::new(AtomicU8::new(CLOSING));
        let (completion, _task) = spawn_shutdown_cleanup(
            McpServerId::new("join-child").expect("valid id"),
            Arc::clone(&state),
            service,
            child,
        );

        let error = completion
            .wait()
            .await
            .expect_err("JoinError is not shutdown success");
        assert_eq!(error.kind(), McpAdapterErrorKind::ShutdownFailed);
        assert_eq!(child_calls.load(Ordering::SeqCst), 1);
        assert_eq!(state.load(Ordering::Acquire), CLOSED);
    }

    #[tokio::test]
    async fn cancelled_waiter_does_not_cancel_shared_failing_cleanup() {
        let started = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let child_calls = Arc::new(AtomicUsize::new(0));
        let service_started = Arc::clone(&started);
        let service_release = Arc::clone(&release);
        let service: ServiceCloseFuture = Box::pin(async move {
            service_started.add_permits(1);
            let permit = service_release.acquire().await.expect("gate remains open");
            permit.forget();
            Ok(QuitReason::JoinError(
                join_error("SECRET_DELAYED_JOIN_ERROR").await,
            ))
        });
        let child_calls_for_future = Arc::clone(&child_calls);
        let child: ChildCleanupFuture = Box::pin(async move {
            child_calls_for_future.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });
        let state = Arc::new(AtomicU8::new(CLOSING));
        let (completion, _task) = spawn_shutdown_cleanup(
            McpServerId::new("cancelled-waiter").expect("valid id"),
            Arc::clone(&state),
            service,
            child,
        );
        let first_completion = Arc::clone(&completion);
        let first = tokio::spawn(async move { first_completion.wait().await });
        let started_permit = started.acquire().await.expect("gate remains open");
        started_permit.forget();
        first.abort();
        first.await.expect_err("first shutdown waiter is cancelled");
        release.add_permits(1);

        let (second, third) = tokio::join!(completion.wait(), completion.wait());
        let second = second.expect_err("shared cleanup failure is retained");
        let third = third.expect_err("all callers observe failure");
        assert_eq!(second.kind(), McpAdapterErrorKind::ShutdownFailed);
        assert_eq!(third.kind(), McpAdapterErrorKind::ShutdownFailed);
        assert_eq!(child_calls.load(Ordering::SeqCst), 1);
        assert_eq!(state.load(Ordering::Acquire), CLOSED);
        assert!(std::ptr::eq(
            second.source().expect("shared source"),
            third.source().expect("shared source")
        ));
    }

    #[tokio::test]
    async fn service_worker_panic_is_mapped_to_shutdown_failed() {
        let service: ServiceCloseFuture = Box::pin(async { panic!("SECRET_CLEANUP_TASK_PANIC") });
        let child: ChildCleanupFuture = Box::pin(async { Ok(()) });
        let state = Arc::new(AtomicU8::new(CLOSING));
        let (completion, _task) = spawn_shutdown_cleanup(
            McpServerId::new("cleanup-panic").expect("valid id"),
            Arc::clone(&state),
            service,
            child,
        );

        let error = completion
            .wait()
            .await
            .expect_err("cleanup task panic is structured");
        assert_eq!(error.kind(), McpAdapterErrorKind::ShutdownFailed);
        assert!(
            error
                .source()
                .is_some_and(|source| source.is::<tokio::task::JoinError>())
        );
        assert!(!format!("{error}").contains("SECRET_CLEANUP_TASK_PANIC"));
        assert!(!format!("{error:?}").contains("SECRET_CLEANUP_TASK_PANIC"));
        assert_eq!(state.load(Ordering::Acquire), CLOSED);
    }

    #[tokio::test]
    async fn service_failure_has_priority_after_child_failure_is_observed() {
        let server_id = McpServerId::new("combined-failure").expect("valid id");
        let service_error =
            map_cleanup_task_result(Err(join_error("SECRET_SERVICE_PRIMARY").await), &server_id)
                .expect_err("service task fails");
        let child_error = McpAdapterError::with_source(
            McpAdapterErrorKind::ShutdownFailed,
            std::io::Error::other("SECRET_CHILD_SECONDARY"),
        )
        .with_server(server_id);

        let combined = combine_shutdown_results(Err(service_error), Err(child_error.clone()))
            .expect_err("service failure is primary");
        assert!(
            combined
                .source()
                .is_some_and(|source| source.is::<tokio::task::JoinError>())
        );
        assert!(!format!("{combined:?}").contains("SECRET_SERVICE_PRIMARY"));
        assert!(!format!("{combined:?}").contains("SECRET_CHILD_SECONDARY"));

        let child_only = combine_shutdown_results(Ok(()), Err(child_error))
            .expect_err("child failure is retained when service succeeds");
        assert!(
            child_only
                .source()
                .is_some_and(|source| source.is::<std::io::Error>())
        );
    }

    #[tokio::test]
    async fn closed_state_is_visible_before_completion_waiters_return() {
        let state = Arc::new(AtomicU8::new(CLOSING));
        let completion = Arc::new(ShutdownCompletion::new());
        let waiter_state = Arc::clone(&state);
        let waiter_completion = Arc::clone(&completion);
        let waiter = tokio::spawn(async move {
            let result = waiter_completion.wait().await;
            assert_eq!(waiter_state.load(Ordering::Acquire), CLOSED);
            result
        });

        completion.complete_after_closed(Ok(()), &state);
        waiter
            .await
            .expect("waiter task completes")
            .expect("completion succeeds");
        assert_eq!(state.load(Ordering::Acquire), CLOSED);
        completion
            .wait()
            .await
            .expect("repeated waiter sees the same completion");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn real_stubborn_child_is_reaped_before_panicked_service_failure_is_published() {
        DROP_REAPER_SPAWNS.store(0, Ordering::Relaxed);
        let (child, pid, marker) = spawn_stubborn_stdio_child(Duration::from_millis(250)).await;
        assert!(process_exists(pid), "stubborn child is running");

        let service: ServiceCloseFuture =
            Box::pin(async { panic!("SECRET_REAL_SERVICE_WORKER_PANIC") });
        let server_id = McpServerId::new("real-panic-child").expect("valid id");
        let child = child_cleanup_future(server_id.clone(), child);
        let state = Arc::new(AtomicU8::new(CLOSING));
        let (completion, _supervisor) =
            spawn_shutdown_cleanup(server_id, Arc::clone(&state), service, child);

        let first_completion = Arc::clone(&completion);
        let first = tokio::spawn(async move { first_completion.wait().await });
        assert!(
            !first.is_finished(),
            "real child cleanup keeps the completion pending"
        );
        first.abort();
        first.await.expect_err("first waiter is cancelled");

        let (second, concurrent) = tokio::join!(completion.wait(), completion.wait());
        let second = second.expect_err("service panic fails shutdown");
        let concurrent = concurrent.expect_err("concurrent waiter sees the same failure");
        assert_eq!(second.kind(), McpAdapterErrorKind::ShutdownFailed);
        assert_eq!(concurrent.kind(), McpAdapterErrorKind::ShutdownFailed);
        assert!(
            second
                .source()
                .is_some_and(|source| source.is::<tokio::task::JoinError>())
        );
        assert!(std::ptr::eq(
            second.source().expect("shared service source"),
            concurrent.source().expect("shared service source")
        ));
        assert_eq!(state.load(Ordering::Acquire), CLOSED);
        assert!(
            !process_exists(pid),
            "failure is published only after the direct child is reaped"
        );
        assert_eq!(
            DROP_REAPER_SPAWNS.load(Ordering::Relaxed),
            0,
            "explicit cleanup disarms the Drop reaper"
        );

        let executable = std::env::current_exe().expect("current executable is available");
        for rendered in [format!("{second}"), format!("{second:?}")] {
            assert!(!rendered.contains("SECRET_REAL_SERVICE_WORKER_PANIC"));
            assert!(!rendered.contains(&executable.display().to_string()));
            assert!(!rendered.contains("stubborn_stdio_child_fixture"));
        }
        let repeated = completion
            .wait()
            .await
            .expect_err("repeated waiter sees the stored failure");
        assert!(std::ptr::eq(
            second.source().expect("shared service source"),
            repeated.source().expect("shared service source")
        ));
        remove_marker(&marker);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn real_stubborn_child_is_reaped_before_child_worker_panic_is_published() {
        DROP_REAPER_SPAWNS.store(0, Ordering::Relaxed);
        let (mut child, pid, marker) = spawn_stubborn_stdio_child(Duration::ZERO).await;
        child.arm_termination_panic();
        assert!(process_exists(pid), "stubborn child is running");

        let server_id = McpServerId::new("real-child-panic").expect("valid id");
        let child = child_cleanup_future(server_id.clone(), child);
        let state = Arc::new(AtomicU8::new(CLOSING));
        let service: ServiceCloseFuture = Box::pin(async { Ok(QuitReason::Closed) });
        let (completion, _supervisor) =
            spawn_shutdown_cleanup(server_id, Arc::clone(&state), service, child);

        let error = completion
            .wait()
            .await
            .expect_err("child cleanup worker panic fails shutdown");
        assert_eq!(error.kind(), McpAdapterErrorKind::ShutdownFailed);
        assert!(
            error
                .source()
                .is_some_and(|source| source.is::<tokio::task::JoinError>())
        );
        assert_eq!(state.load(Ordering::Acquire), CLOSED);
        assert!(
            !process_exists(pid),
            "child worker panic is published only after kill/wait/reap"
        );
        assert_eq!(
            DROP_REAPER_SPAWNS.load(Ordering::Relaxed),
            0,
            "panic recovery reaps directly without the Drop reaper"
        );
        assert!(!format!("{error}").contains("SECRET_REAL_CHILD_CLEANUP_PANIC"));
        assert!(!format!("{error:?}").contains("SECRET_REAL_CHILD_CLEANUP_PANIC"));
        remove_marker(&marker);
    }
}
