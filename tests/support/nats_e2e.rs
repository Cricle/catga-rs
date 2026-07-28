//! Shared Docker-backed NATS JetStream fixture support.

use std::{env, ops::Deref, thread};

use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::sync::{Mutex, MutexGuard};

static TEST_LOCK: Mutex<()> = Mutex::const_new(());

/// A NATS E2E lease that owns its Testcontainers guard until the test returns.
pub struct NatsTestLease {
    container: Option<ContainerAsync<GenericImage>>,
    _lock: MutexGuard<'static, ()>,
    url: Box<str>,
}

impl NatsTestLease {
    /// Returns the external or Testcontainers-provided NATS URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Explicitly removes the managed container before releasing the test lock.
    ///
    /// External `CATGA_NATS_URL` endpoints are never removed. Dropping this lease also
    /// performs the same best-effort synchronous cleanup for tests that return early or panic.
    pub async fn close(mut self) -> Result<(), testcontainers::TestcontainersError> {
        match self.container.take() {
            Some(container) => container.rm().await,
            None => Ok(()),
        }
    }
}

impl Drop for NatsTestLease {
    fn drop(&mut self) {
        if let Some(container) = self.container.take() {
            // `ContainerAsync` queues removal during drop. Use a short-lived runtime on a
            // dedicated thread so an unwinding Tokio test still waits for Docker cleanup.
            if let Ok(cleanup) = thread::Builder::new()
                .name("catga-nats-e2e-cleanup".into())
                .spawn(move || {
                    if let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        let _ = runtime.block_on(container.rm());
                    }
                })
            {
                let _ = cleanup.join();
            }
        }
    }
}

impl Deref for NatsTestLease {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.url()
    }
}

/// Returns an externally configured NATS URL or starts one isolated JetStream container.
pub async fn server_url() -> NatsTestLease {
    let lock = TEST_LOCK.lock().await;
    if let Ok(url) = env::var("CATGA_NATS_URL")
        && !url.trim().is_empty()
    {
        return NatsTestLease {
            container: None,
            _lock: lock,
            url: url.into(),
        };
    }
    let container = GenericImage::new("nats", "2.10-alpine")
        .with_exposed_port(4222.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
        .with_cmd(["-js"])
        .start()
        .await
        .expect("Docker must start the NATS JetStream E2E container");
    let port = container
        .get_host_port_ipv4(4222.tcp())
        .await
        .expect("NATS JetStream E2E container must expose port 4222");
    NatsTestLease {
        container: Some(container),
        _lock: lock,
        url: format!("nats://127.0.0.1:{port}").into(),
    }
}
