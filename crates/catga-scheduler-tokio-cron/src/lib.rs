#![forbid(unsafe_code)]

//! Opt-in adapter for running Catga bounded due-flow sweeps from cron jobs.
//!
//! [`CronRuntime`] is a narrow wrapper around
//! [`tokio_cron_scheduler::JobScheduler`]. It does not replace Catga's durable
//! [`FlowDueService`]: durable flow state, due-work claiming, lease renewal, and retry handling
//! remain owned by the configured Catga stores and scheduler backend.
//!
//! # Lifecycle
//!
//! Constructing a runtime and adding jobs do not start work. Call [`CronRuntime::start`] at an
//! application-owned lifecycle boundary, and call [`CronRuntime::shutdown`] before dropping the
//! runtime. Catga creates no background task, polling loop, signal handler, or automatic
//! shutdown hook. The wrapped scheduler creates its own task only when the caller explicitly
//! invokes `start`.
//!
//! # Flow due sweeps
//!
//! [`flow_due_job`] creates an upstream async cron job whose callback makes exactly one bounded
//! [`FlowDueService::check_at`] call. It deliberately does not call [`FlowDueService::run`], so
//! cron frequency remains an application-level policy and every callback has the normal
//! `DueFlowOptions::batch_size` bound. Failures are logged and left for the next scheduled sweep;
//! applications that need different supervision can create their own [`Job`].

use std::{
    collections::HashSet,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard},
    time::SystemTime,
};

use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, ErrorCode, ScheduledTask, ScheduledTaskId, TaskSchedule, TaskScheduler,
};
use catga_flow::{DueFlowScheduler, FlowDueService, SuspendedFlowStore};
use tokio_cron_scheduler::JobSchedulerError;

/// The upstream, caller-configured cron job type.
pub use tokio_cron_scheduler::Job;
/// The upstream scheduler type passed to async job callbacks.
pub use tokio_cron_scheduler::JobScheduler;
/// The identifier assigned by the upstream scheduler when a job is registered.
pub type JobId = uuid::Uuid;

/// An explicitly lifecycle-managed in-memory cron runtime.
///
/// This adapter owns only the third-party scheduler. It does not persist jobs, install a signal
/// handler, or infer when an application should start or stop. Use a storage-enabled upstream
/// scheduler directly when cron job persistence is required.
pub struct CronRuntime {
    scheduler: JobScheduler,
    task_registration: Mutex<TaskRegistration>,
}

#[derive(Default)]
struct TaskRegistration {
    core_task_ids: HashSet<JobId>,
    removing_task_ids: HashSet<JobId>,
}

impl CronRuntime {
    /// Builds an idle cron runtime using the upstream scheduler's default in-memory stores.
    ///
    /// The returned runtime is inactive until [`Self::start`] is called.
    pub async fn new() -> CatgaResult<Self> {
        JobScheduler::new()
            .await
            .map(|scheduler| Self {
                scheduler,
                task_registration: Mutex::new(TaskRegistration::default()),
            })
            .map_err(map_scheduler_error)
    }

    /// Constructs an upstream asynchronous cron job with Catga error mapping.
    ///
    /// This is the only cron construction helper in this crate; schedule parsing is delegated to
    /// `tokio-cron-scheduler`. An invalid expression returns [`ErrorCode::Validation`] and no
    /// job exists to register.
    pub fn new_async_job<S, T>(schedule: S, run: T) -> CatgaResult<Job>
    where
        S: ToString,
        T: FnMut(JobId, JobScheduler) -> Pin<Box<dyn Future<Output = ()> + Send>>
            + Send
            + Sync
            + 'static,
    {
        Job::new_async(schedule, run).map_err(map_scheduler_error)
    }

    /// Registers `job` with this idle or running runtime and returns its assigned identity.
    ///
    /// Registering does not start the scheduler. Jobs added after [`Self::start`] use the
    /// upstream scheduler's normal registration behavior.
    pub async fn add(&self, job: Job) -> CatgaResult<JobId> {
        self.scheduler.add(job).await.map_err(map_scheduler_error)
    }

    /// Removes a previously registered cron job by its [`JobId`].
    ///
    /// Removing a job does not stop the runtime. The remaining registered jobs continue only
    /// after [`Self::start`] has been explicitly called. If the job was registered through the
    /// Core [`TaskScheduler`] contract, this also invalidates its [`ScheduledTaskId`].
    pub async fn remove(&self, job_id: &JobId) -> CatgaResult<()> {
        self.remove_registered_job(job_id, false).await
    }

    /// Explicitly starts the upstream scheduler.
    ///
    /// This is the only method in this adapter that can cause scheduled callbacks to run.
    pub async fn start(&self) -> CatgaResult<()> {
        self.scheduler.start().await.map_err(map_scheduler_error)
    }

    /// Explicitly shuts down the upstream scheduler and its task.
    ///
    /// The mutable receiver mirrors `tokio-cron-scheduler`'s lifecycle API and makes ownership of
    /// shutdown visible to the application.
    pub async fn shutdown(&mut self) -> CatgaResult<()> {
        self.scheduler.shutdown().await.map_err(map_scheduler_error)
    }

    fn task_registration(&self) -> CatgaResult<MutexGuard<'_, TaskRegistration>> {
        self.task_registration.lock().map_err(|_| {
            CatgaError::new(
                ErrorCode::Internal,
                "tokio cron scheduled task identity tracking lock is poisoned",
            )
        })
    }

    fn begin_removal(&self, job_id: JobId, require_core_task: bool) -> CatgaResult<()> {
        let mut registration = self.task_registration()?;
        if require_core_task && !registration.core_task_ids.contains(&job_id) {
            return Err(CatgaError::new(
                ErrorCode::NotFound,
                "scheduled task identifier is not registered by this scheduler",
            ));
        }
        if !registration.removing_task_ids.insert(job_id) {
            return Err(CatgaError::new(
                ErrorCode::Conflict,
                "scheduled task removal is already in progress",
            ));
        }
        Ok(())
    }

    fn finish_removal(&self, job_id: JobId, removed: bool) -> CatgaResult<()> {
        let mut registration = self.task_registration()?;
        registration.removing_task_ids.remove(&job_id);
        if removed {
            registration.core_task_ids.remove(&job_id);
        }
        Ok(())
    }

    async fn remove_registered_job(
        &self,
        job_id: &JobId,
        require_core_task: bool,
    ) -> CatgaResult<()> {
        self.begin_removal(*job_id, require_core_task)?;
        let result = self
            .scheduler
            .remove(job_id)
            .await
            .map_err(map_scheduler_error);
        self.finish_removal(*job_id, result.is_ok())?;
        result
    }
}

#[async_trait]
impl TaskScheduler for CronRuntime {
    async fn schedule(
        &self,
        schedule: TaskSchedule,
        task: Arc<dyn ScheduledTask>,
    ) -> CatgaResult<ScheduledTaskId> {
        let job = Self::new_async_job(schedule.as_cron(), move |_job_id, _scheduler| {
            let task = Arc::clone(&task);
            Box::pin(async move {
                if let Err(error) = task.execute().await {
                    tracing::warn!(error = ?error, "scheduled task failed");
                }
            })
        })?;
        let job_id = self.add(job).await?;
        self.task_registration()?.core_task_ids.insert(job_id);
        ScheduledTaskId::new(job_id.to_string())
    }

    async fn cancel(&self, task_id: &ScheduledTaskId) -> CatgaResult<()> {
        let job_id = uuid::Uuid::parse_str(task_id.as_str()).map_err(|error| {
            CatgaError::new(
                ErrorCode::Validation,
                format!("invalid tokio cron scheduled task identifier: {error}"),
            )
        })?;
        self.remove_registered_job(&job_id, true).await
    }
}

/// Builds an async upstream cron job that performs one bounded due-flow sweep per callback.
///
/// The callback calls [`FlowDueService::check_at`] with [`SystemTime::now`] and never invokes
/// [`FlowDueService::run`]. A failed sweep is logged with [`tracing::warn!`] rather than escaping
/// the upstream callback, which has no result channel. No metric labels are created.
///
/// The job is not registered or started by this function. Register it with [`CronRuntime::add`]
/// and then explicitly call [`CronRuntime::start`].
pub fn flow_due_job<S, H>(
    schedule: impl ToString,
    service: Arc<FlowDueService<S, H>>,
) -> CatgaResult<Job>
where
    S: SuspendedFlowStore + ?Sized + 'static,
    H: DueFlowScheduler + ?Sized + 'static,
{
    CronRuntime::new_async_job(schedule, move |_job_id, _scheduler| {
        let service = Arc::clone(&service);
        Box::pin(async move {
            if let Err(error) = service.check_at(SystemTime::now()).await {
                tracing::warn!(error = ?error, "cron flow due sweep failed");
            }
        })
    })
}

fn map_scheduler_error(error: JobSchedulerError) -> CatgaError {
    let code = match error {
        JobSchedulerError::ParseSchedule
        | JobSchedulerError::JobTypeNotSet
        | JobSchedulerError::RunOrRunAsyncNotSet
        | JobSchedulerError::ScheduleNotSet => ErrorCode::Validation,
        JobSchedulerError::CantInit
        | JobSchedulerError::StartScheduler
        | JobSchedulerError::Shutdown
        | JobSchedulerError::ShutdownNotifier => ErrorCode::Unavailable,
        _ => ErrorCode::Internal,
    };
    CatgaError::new(code, format!("tokio cron scheduler error: {error}"))
}
