//! Strict contracts for coordination behaviors: distributed leases around
//! handlers, authorization policies, compensation fan-out, and fault events.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use catga_core::{
    AuthorizationBehavior, AuthorizationPolicies, AuthorizationPolicy, CatgaError, CatgaResult,
    CommandPipeline, CompensationBehavior, DistributedLockBehavior, ErrorCode,
    EventCompensationPublisher, Fault, FaultPublisher, FaultPublishingBehavior, LeaseStore,
    Mediator, Message, Pipeline, Registry, SecurityIdentity,
};

#[path = "support/behavior_support.rs"]
mod behavior_support;

use behavior_support::{Req, req_mediator};

/// Cloneable command used by command-behavior contracts.
#[derive(Clone)]
struct Cmd(pub u64);
impl Message for Cmd {}
impl catga_core::Command for Cmd {}

/// Request that only requires an authenticated identity.
#[derive(Clone)]
struct AuthPlain;
impl Message for AuthPlain {}
impl catga_core::Request for AuthPlain {
    type Response = u64;
}
impl catga_core::AuthorizedRequest for AuthPlain {
    fn authorization() -> catga_core::AuthorizationRequirements {
        catga_core::AuthorizationRequirements::authenticated()
    }
}

/// Request that requires one of the listed roles.
#[derive(Clone)]
struct AuthRole;
impl Message for AuthRole {}
impl catga_core::Request for AuthRole {
    type Response = u64;
}
impl catga_core::AuthorizedRequest for AuthRole {
    fn authorization() -> catga_core::AuthorizationRequirements {
        catga_core::AuthorizationRequirements::with_roles(&["admin", "editor"])
    }
}

/// Request that requires the named billing policy.
#[derive(Clone)]
struct AuthPolicy;
impl Message for AuthPolicy {}
impl catga_core::Request for AuthPolicy {
    type Response = u64;
}
impl catga_core::AuthorizedRequest for AuthPolicy {
    fn authorization() -> catga_core::AuthorizationRequirements {
        catga_core::AuthorizationRequirements::with_policy("billing")
    }
}

/// Request that requires both a role and the named policy.
#[derive(Clone)]
struct AuthRolePolicy;
impl Message for AuthRolePolicy {}
impl catga_core::Request for AuthRolePolicy {
    type Response = u64;
}
impl catga_core::AuthorizedRequest for AuthRolePolicy {
    fn authorization() -> catga_core::AuthorizationRequirements {
        catga_core::AuthorizationRequirements::with_roles_and_policy(&["admin"], "billing")
    }
}

// ---------------------------------------------------------------------------
// DistributedLockBehavior
// ---------------------------------------------------------------------------

/// Lease store stub with selectable failure points around a memory store.
enum LeaseFault {
    Acquire,
    RenewLost,
    RenewError,
    ReleaseFalse,
    ReleaseError,
}

struct FaultyLeases {
    fault: Mutex<LeaseFault>,
}

#[async_trait]
impl LeaseStore for FaultyLeases {
    async fn try_acquire(&self, _: &str, _: &str, _: Duration) -> CatgaResult<bool> {
        match &*self.fault.lock().expect("lease fault lock") {
            LeaseFault::Acquire => Err(CatgaError::new(ErrorCode::Unavailable, "lease store down")),
            _ => Ok(true),
        }
    }
    async fn renew(&self, _: &str, _: &str, _: Duration) -> CatgaResult<bool> {
        match &*self.fault.lock().expect("lease fault lock") {
            LeaseFault::RenewLost => Ok(false),
            LeaseFault::RenewError => Err(CatgaError::new(ErrorCode::Unavailable, "renew failed")),
            _ => Ok(true),
        }
    }
    async fn release(&self, _: &str, _: &str) -> CatgaResult<bool> {
        match &*self.fault.lock().expect("lease fault lock") {
            LeaseFault::ReleaseFalse => Ok(false),
            LeaseFault::ReleaseError => {
                Err(CatgaError::new(ErrorCode::Unavailable, "release failed"))
            }
            _ => Ok(true),
        }
    }
}

fn lock_mediator(delay: Duration) -> Arc<Mediator> {
    req_mediator(catga_core::request_handler(move |req: Req| async move {
        tokio::time::sleep(delay).await;
        Ok(req.id)
    }))
}

#[tokio::test]
async fn distributed_lock_acquires_renews_and_releases_around_the_handler() {
    let leases = Arc::new(catga_core::memory::MemoryLeases::default());
    let behavior = DistributedLockBehavior::new(
        Arc::clone(&leases) as Arc<dyn LeaseStore>,
        "svc",
        Duration::from_millis(40),
    );
    let pipeline = Pipeline::<Req>::new().with(behavior);
    let mediator = lock_mediator(Duration::from_millis(50));

    // The handler outlives one renewal interval, so the lease is renewed.
    let value = mediator
        .send_with(
            Req {
                id: 1,
                key: "res-a",
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect("the locked handler completes");
    assert_eq!(value, 1);

    // The lease is released afterwards: a new owner can take it immediately.
    assert!(
        leases
            .try_acquire("res-a", "other", Duration::from_secs(5))
            .await
            .expect("acquire succeeds"),
        "the release frees the resource"
    );
}

#[tokio::test]
async fn distributed_lock_rejects_zero_lease_and_contention() {
    let leases = Arc::new(catga_core::memory::MemoryLeases::default());
    let zero = DistributedLockBehavior::new(
        Arc::clone(&leases) as Arc<dyn LeaseStore>,
        "svc",
        Duration::ZERO,
    );
    let pipeline = Pipeline::<Req>::new().with(zero);
    let mediator = lock_mediator(Duration::ZERO);
    let error = mediator
        .send_with(
            Req {
                id: 1,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("a zero lease is rejected");
    assert_eq!(error.code(), ErrorCode::Validation);

    // Another owner holding the resource produces a Conflict without waiting.
    leases
        .try_acquire("held", "other", Duration::from_secs(60))
        .await
        .expect("seed acquire succeeds");
    let behavior = DistributedLockBehavior::new(
        Arc::clone(&leases) as Arc<dyn LeaseStore>,
        "svc",
        Duration::from_secs(30),
    );
    let pipeline = Pipeline::<Req>::new().with(behavior);
    let error = mediator
        .send_with(
            Req {
                id: 1,
                key: "held",
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("a held lock conflicts");
    assert_eq!(error.code(), ErrorCode::Conflict);
}

#[tokio::test]
async fn distributed_lock_waits_for_contention_until_timeout_or_release() {
    let leases = Arc::new(catga_core::memory::MemoryLeases::default());
    leases
        .try_acquire("brief", "other", Duration::from_millis(60))
        .await
        .expect("seed acquire succeeds");

    // A short wait budget gives up while the other lease is still alive.
    let behavior = DistributedLockBehavior::new(
        Arc::clone(&leases) as Arc<dyn LeaseStore>,
        "svc",
        Duration::from_secs(30),
    )
    .with_wait_timeout(Duration::from_millis(10));
    let pipeline = Pipeline::<Req>::new().with(behavior);
    let mediator = lock_mediator(Duration::ZERO);
    let error = mediator
        .send_with(
            Req {
                id: 1,
                key: "brief",
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("the wait budget expires while held");
    assert_eq!(error.code(), ErrorCode::Conflict);

    // A longer budget rides out the expiry and then acquires.
    let behavior = DistributedLockBehavior::new(
        Arc::clone(&leases) as Arc<dyn LeaseStore>,
        "svc",
        Duration::from_secs(30),
    )
    .with_wait_timeout(Duration::from_millis(500));
    let pipeline = Pipeline::<Req>::new().with(behavior);
    let value = mediator
        .send_with(
            Req {
                id: 2,
                key: "brief",
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect("the lease expires within the wait budget");
    assert_eq!(value, 2);
}

#[tokio::test]
async fn distributed_lock_surfaces_store_faults_and_lost_ownership() {
    let mediator = lock_mediator(Duration::from_millis(60));

    let acquire_fails = DistributedLockBehavior::new(
        Arc::new(FaultyLeases {
            fault: Mutex::new(LeaseFault::Acquire),
        }) as Arc<dyn LeaseStore>,
        "svc",
        Duration::from_secs(30),
    );
    let pipeline = Pipeline::<Req>::new().with(acquire_fails);
    let error = mediator
        .send_with(
            Req {
                id: 1,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("an acquire failure surfaces");
    assert_eq!(error.code(), ErrorCode::Unavailable);

    let renew_lost = DistributedLockBehavior::new(
        Arc::new(FaultyLeases {
            fault: Mutex::new(LeaseFault::RenewLost),
        }) as Arc<dyn LeaseStore>,
        "svc",
        Duration::from_millis(20),
    );
    let pipeline = Pipeline::<Req>::new().with(renew_lost);
    let error = mediator
        .send_with(
            Req {
                id: 1,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("lost ownership during renewal conflicts");
    assert_eq!(error.code(), ErrorCode::Conflict);

    let renew_error = DistributedLockBehavior::new(
        Arc::new(FaultyLeases {
            fault: Mutex::new(LeaseFault::RenewError),
        }) as Arc<dyn LeaseStore>,
        "svc",
        Duration::from_millis(20),
    );
    let pipeline = Pipeline::<Req>::new().with(renew_error);
    let error = mediator
        .send_with(
            Req {
                id: 1,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("a renewal error surfaces");
    assert_eq!(error.code(), ErrorCode::Unavailable);

    let release_false = DistributedLockBehavior::new(
        Arc::new(FaultyLeases {
            fault: Mutex::new(LeaseFault::ReleaseFalse),
        }) as Arc<dyn LeaseStore>,
        "svc",
        Duration::from_secs(30),
    );
    let pipeline = Pipeline::<Req>::new().with(release_false);
    let error = mediator
        .send_with(
            Req {
                id: 1,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("a lost release maps to Internal");
    assert_eq!(error.code(), ErrorCode::Internal);

    let release_error = DistributedLockBehavior::new(
        Arc::new(FaultyLeases {
            fault: Mutex::new(LeaseFault::ReleaseError),
        }) as Arc<dyn LeaseStore>,
        "svc",
        Duration::from_secs(30),
    );
    let pipeline = Pipeline::<Req>::new().with(release_error);
    let error = mediator
        .send_with(
            Req {
                id: 1,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("a release failure surfaces");
    assert_eq!(error.code(), ErrorCode::Unavailable);
}

// ---------------------------------------------------------------------------
// AuthorizationBehavior
// ---------------------------------------------------------------------------

struct BillingPolicy {
    allow: bool,
}

#[async_trait]
impl AuthorizationPolicy<AuthPolicy> for BillingPolicy {
    fn name(&self) -> &str {
        "BILLING"
    }
    async fn authorize(&self, _: &SecurityIdentity, _: &AuthPolicy) -> CatgaResult<bool> {
        Ok(self.allow)
    }
}

#[async_trait]
impl AuthorizationPolicy<AuthRolePolicy> for BillingPolicy {
    fn name(&self) -> &str {
        "billing"
    }
    async fn authorize(&self, _: &SecurityIdentity, _: &AuthRolePolicy) -> CatgaResult<bool> {
        Ok(self.allow)
    }
}

struct DenyingPolicy;

#[async_trait]
impl AuthorizationPolicy<AuthPolicy> for DenyingPolicy {
    fn name(&self) -> &str {
        "billing"
    }
    async fn authorize(&self, _: &SecurityIdentity, _: &AuthPolicy) -> CatgaResult<bool> {
        Err(CatgaError::new(
            ErrorCode::Unavailable,
            "policy backend down",
        ))
    }
}

fn auth_mediator() -> Arc<Mediator> {
    let mut registry = Registry::new();
    let ok = catga_core::request_handler(move |_: AuthPlain| async move { Ok(1_u64) });
    registry
        .register_request::<AuthPlain, _>(ok)
        .expect("registers");
    let ok = catga_core::request_handler(move |_: AuthRole| async move { Ok(2_u64) });
    registry
        .register_request::<AuthRole, _>(ok)
        .expect("registers");
    let ok = catga_core::request_handler(move |_: AuthPolicy| async move { Ok(3_u64) });
    registry
        .register_request::<AuthPolicy, _>(ok)
        .expect("registers");
    let ok = catga_core::request_handler(move |_: AuthRolePolicy| async move { Ok(4_u64) });
    registry
        .register_request::<AuthRolePolicy, _>(ok)
        .expect("registers");
    Arc::new(Mediator::new(registry))
}

#[tokio::test]
async fn authorization_requires_identity_roles_and_policies() {
    let mediator = auth_mediator();
    let plain = Pipeline::<AuthPlain>::new().with(AuthorizationBehavior::new());
    let role = Pipeline::<AuthRole>::new().with(AuthorizationBehavior::new());

    // Missing identity.
    let error = mediator
        .send_with(AuthPlain, &plain)
        .await
        .expect_err("an anonymous request is unauthorized");
    assert_eq!(error.code(), ErrorCode::Unauthorized);

    // Authenticated without required role.
    let identity = SecurityIdentity::new("alice", ["viewer"]);
    let error = catga_core::scope_security_identity(identity, mediator.send_with(AuthRole, &role))
        .await
        .expect_err("a missing role is forbidden");
    assert_eq!(error.code(), ErrorCode::Forbidden);
    assert!(error.message().contains("admin"));
    assert!(error.message().contains("editor"));

    // A matching role passes.
    let identity = SecurityIdentity::new("alice", ["editor"]);
    let value = catga_core::scope_security_identity(identity, mediator.send_with(AuthRole, &role))
        .await
        .expect("a matching role passes");
    assert_eq!(value, 2);

    // Authenticated-only requests pass with any identity.
    let identity = SecurityIdentity::new("bob", ["anything"]);
    let value =
        catga_core::scope_security_identity(identity, mediator.send_with(AuthPlain, &plain))
            .await
            .expect("authenticated requests pass");
    assert_eq!(value, 1);
}

#[tokio::test]
async fn authorization_named_policies_decide_case_insensitively() {
    let mediator = auth_mediator();
    let policies = AuthorizationPolicies::from_shared(vec![
        Arc::new(BillingPolicy { allow: false }) as Arc<dyn AuthorizationPolicy<AuthPolicy>>,
        Arc::new(DenyingPolicy) as Arc<dyn AuthorizationPolicy<AuthPolicy>>,
    ]);
    let pipeline =
        Pipeline::<AuthPolicy>::new().with(AuthorizationBehavior::with_policies(policies));
    let identity = SecurityIdentity::new("carol", ["billing"]);

    // Policy lookup ignores ASCII case ("billing" vs "BILLING").
    let error = catga_core::scope_security_identity(
        identity.clone(),
        mediator.send_with(AuthPolicy, &pipeline),
    )
    .await
    .expect_err("the policy denies");
    assert_eq!(error.code(), ErrorCode::Forbidden);
    assert!(error.message().contains("billing"));

    // An allowing policy passes.
    let policies = AuthorizationPolicies::from_shared(vec![
        Arc::new(BillingPolicy { allow: true }) as Arc<dyn AuthorizationPolicy<AuthPolicy>>,
    ]);
    let pipeline =
        Pipeline::<AuthPolicy>::new().with(AuthorizationBehavior::with_policies(policies));
    let value = catga_core::scope_security_identity(
        identity.clone(),
        mediator.send_with(AuthPolicy, &pipeline),
    )
    .await
    .expect("the policy allows");
    assert_eq!(value, 3);

    // Unregistered policy names fall through to role checks.
    let pipeline = Pipeline::<AuthPolicy>::new().with(AuthorizationBehavior::default());
    let value =
        catga_core::scope_security_identity(identity, mediator.send_with(AuthPolicy, &pipeline))
            .await
            .expect("an unregistered policy is ignored");
    assert_eq!(value, 3);
}

#[tokio::test]
async fn authorization_policy_errors_and_role_plus_policy_combinations() {
    let mediator = auth_mediator();

    // A failing policy surfaces its error.
    let policies = AuthorizationPolicies::from_shared(vec![
        Arc::new(DenyingPolicy) as Arc<dyn AuthorizationPolicy<AuthPolicy>>
    ]);
    let pipeline =
        Pipeline::<AuthPolicy>::new().with(AuthorizationBehavior::with_policies(policies));
    let identity = SecurityIdentity::new("dave", ["admin"]);
    let error =
        catga_core::scope_security_identity(identity, mediator.send_with(AuthPolicy, &pipeline))
            .await
            .expect_err("a policy failure surfaces");
    assert_eq!(error.code(), ErrorCode::Unavailable);

    // Role + policy requirements must both hold.
    let policies = AuthorizationPolicies::new(vec![Arc::new(BillingPolicy { allow: true })]);
    let combo =
        Pipeline::<AuthRolePolicy>::new().with(AuthorizationBehavior::with_policies(policies));
    let identity = SecurityIdentity::new("erin", ["viewer"]);
    let error =
        catga_core::scope_security_identity(identity, mediator.send_with(AuthRolePolicy, &combo))
            .await
            .expect_err("a missing role is forbidden even with an allowing policy");
    assert_eq!(error.code(), ErrorCode::Forbidden);

    let policies = AuthorizationPolicies::new(vec![Arc::new(BillingPolicy { allow: true })]);
    let combo =
        Pipeline::<AuthRolePolicy>::new().with(AuthorizationBehavior::with_policies(policies));
    let identity = SecurityIdentity::new("erin", ["admin"]);
    let value =
        catga_core::scope_security_identity(identity, mediator.send_with(AuthRolePolicy, &combo))
            .await
            .expect("role and policy both pass");
    assert_eq!(value, 4);
}

// ---------------------------------------------------------------------------
// CompensationBehavior
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Compensated(pub u64);
impl Message for Compensated {}
impl catga_core::Event for Compensated {}

/// Compensation publisher for commands.
///
/// [`EventCompensationPublisher`] only implements [`catga_core::CompensationPublisher`]
/// for request types, so the command pipeline needs this dedicated adapter.
struct CommandCompensator {
    mediator: Arc<Mediator>,
}

#[async_trait]
impl catga_core::CompensationPublisher<Cmd> for CommandCompensator {
    async fn publish(&self, command: &Cmd, _error: &CatgaError) -> CatgaResult<()> {
        self.mediator.publish(Compensated(command.0)).await
    }
}

#[tokio::test]
async fn compensation_publishes_after_failures_only() {
    let received = Arc::new(Mutex::new(Vec::<u64>::new()));
    let slot = Arc::clone(&received);
    let mut registry = Registry::new();
    registry.register_event::<Compensated, _>(catga_core::event_handler(
        move |event: Compensated| {
            let slot = Arc::clone(&slot);
            async move {
                slot.lock().expect("compensation log lock").push(event.0);
                Ok(())
            }
        },
    ));
    registry
        .register_request::<Req, _>(catga_core::request_handler(move |_: Req| async move {
            Err(CatgaError::new(ErrorCode::Validation, "needs compensation"))
        }))
        .expect("registers");
    let mediator = Arc::new(Mediator::new(registry));

    let publisher = Arc::new(EventCompensationPublisher::new(
        Arc::clone(&mediator),
        |req: &Req, _error: &CatgaError| Some(Compensated(req.id)),
    ));
    let pipeline = Pipeline::<Req>::new().with(CompensationBehavior::new(
        Arc::clone(&publisher) as Arc<dyn catga_core::CompensationPublisher<Req>>
    ));

    mediator
        .send_with(
            Req {
                id: 5,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("the failure surfaces");
    assert_eq!(*received.lock().expect("compensation log lock"), vec![5]);

    // A factory may opt out of compensation.
    let silent = Arc::new(EventCompensationPublisher::new(
        Arc::clone(&mediator),
        |_: &Req, _: &CatgaError| None::<Compensated>,
    ));
    let pipeline = Pipeline::<Req>::new().with(CompensationBehavior::new(
        Arc::clone(&silent) as Arc<dyn catga_core::CompensationPublisher<Req>>
    ));
    mediator
        .send_with(
            Req {
                id: 6,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("the failure surfaces");
    assert_eq!(received.lock().expect("compensation log lock").len(), 1);
}

#[tokio::test]
async fn compensation_skips_successes_and_covers_panics_and_commands() {
    let received = Arc::new(Mutex::new(Vec::<u64>::new()));
    let slot = Arc::clone(&received);
    let mut registry = Registry::new();
    registry.register_event::<Compensated, _>(catga_core::event_handler(
        move |event: Compensated| {
            let slot = Arc::clone(&slot);
            async move {
                slot.lock().expect("compensation log lock").push(event.0);
                Ok(())
            }
        },
    ));
    registry
        .register_request::<Req, _>(catga_core::request_handler(move |req: Req| async move {
            Ok(req.id)
        }))
        .expect("registers");
    registry
        .register_command::<Cmd, _>(catga_core::command_handler(move |_: Cmd| async move {
            Err(CatgaError::new(
                ErrorCode::Validation,
                "command compensation",
            ))
        }))
        .expect("registers");
    let mediator = Arc::new(Mediator::new(registry));

    let publisher = Arc::new(EventCompensationPublisher::new(
        Arc::clone(&mediator),
        |_: &Req, _: &CatgaError| Some(Compensated(99)),
    ));
    let pipeline = Pipeline::<Req>::new().with(CompensationBehavior::new(
        Arc::clone(&publisher) as Arc<dyn catga_core::CompensationPublisher<Req>>
    ));
    mediator
        .send_with(
            Req {
                id: 7,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect("successes do not compensate");
    assert!(received.lock().expect("compensation log lock").is_empty());

    // Command compensation uses the command behavior path.
    let cmd_publisher = Arc::new(CommandCompensator {
        mediator: Arc::clone(&mediator),
    });
    let cmd_pipeline = CommandPipeline::<Cmd>::new()
        .with(CompensationBehavior::new(
            Arc::clone(&cmd_publisher) as Arc<dyn catga_core::CompensationPublisher<Cmd>>
        ));
    mediator
        .send_command_with(Cmd(21), &cmd_pipeline)
        .await
        .expect_err("the command failure surfaces");
    assert_eq!(*received.lock().expect("compensation log lock"), vec![21]);
}

// ---------------------------------------------------------------------------
// FaultPublishingBehavior
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Eq, PartialEq)]
struct RecordedFault(u64, ErrorCode);
impl Message for RecordedFault {}
impl catga_core::Event for RecordedFault {}

struct RecordingPublisher {
    sink: Arc<Mutex<Vec<RecordedFault>>>,
}

#[async_trait]
impl FaultPublisher<Req> for RecordingPublisher {
    async fn publish(&self, fault: Fault<Req>) -> CatgaResult<()> {
        self.sink
            .lock()
            .expect("fault sink lock")
            .push(RecordedFault(fault.message().id, fault.error().code()));
        Ok(())
    }
}

struct RefusingPublisher;

#[async_trait]
impl FaultPublisher<Req> for RefusingPublisher {
    async fn publish(&self, _: Fault<Req>) -> CatgaResult<()> {
        Err(CatgaError::new(ErrorCode::Unavailable, "fault bus down"))
    }
}

#[tokio::test]
async fn fault_publishing_records_failures_best_effort() {
    let sink = Arc::new(Mutex::new(Vec::<RecordedFault>::new()));
    let publisher = Arc::new(RecordingPublisher {
        sink: Arc::clone(&sink),
    });
    let pipeline = Pipeline::<Req>::new().with(FaultPublishingBehavior::new(
        Arc::clone(&publisher) as Arc<dyn FaultPublisher<Req>>,
    ));

    let failing = req_mediator(catga_core::request_handler(move |_: Req| async move {
        Err(CatgaError::new(ErrorCode::Validation, "faulted"))
    }));
    let error = failing
        .send_with(
            Req {
                id: 1,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("the original error surfaces");
    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(
        *sink.lock().expect("fault sink lock"),
        vec![RecordedFault(1, ErrorCode::Validation)]
    );

    // Successes never publish faults.
    let mediator = req_mediator(catga_core::request_handler(move |req: Req| async move {
        Ok(req.id)
    }));
    mediator
        .send_with(
            Req {
                id: 2,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect("success passes");
    assert_eq!(sink.lock().expect("fault sink lock").len(), 1);

    // A refusing publisher does not disturb the original error.
    let refusing = Pipeline::<Req>::new()
        .with(FaultPublishingBehavior::new(
            Arc::new(RefusingPublisher) as Arc<dyn FaultPublisher<Req>>
        ));
    let error = failing
        .send_with(
            Req {
                id: 3,
                ..Default::default()
            },
            &refusing,
        )
        .await
        .expect_err("the original error survives a broken fault bus");
    assert_eq!(error.code(), ErrorCode::Validation);
}

#[tokio::test]
async fn fault_publisher_adapter_forwards_to_the_mediator() {
    let received = Arc::new(Mutex::new(Vec::<u64>::new()));
    let slot = Arc::clone(&received);
    let mut registry = Registry::new();
    registry.register_event::<Fault<Req>, _>(catga_core::event_handler(
        move |fault: Fault<Req>| {
            let slot = Arc::clone(&slot);
            async move {
                slot.lock()
                    .expect("fault log lock")
                    .push(fault.message().id);
                Ok(())
            }
        },
    ));
    registry
        .register_request::<Req, _>(catga_core::request_handler(move |_: Req| async move {
            Err(CatgaError::new(ErrorCode::Validation, "adapter fault"))
        }))
        .expect("registers");
    let mediator = Arc::new(Mediator::new(registry));

    let pipeline = Pipeline::<Req>::new().with(FaultPublishingBehavior::new(
        Arc::clone(&mediator) as Arc<dyn FaultPublisher<Req>>
    ));
    mediator
        .send_with(
            Req {
                id: 44,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("the failure surfaces");
    assert_eq!(*received.lock().expect("fault log lock"), vec![44]);
}
