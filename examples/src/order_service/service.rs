//! Public service composition and its HTTP-facing observability helpers.

use std::sync::{Arc, atomic::Ordering};

use axum::{Json, Router, routing::get};
use catga_axum::CatgaApplication;
use catga_cluster::{MemoryCluster, MemoryClusterNode, cluster_health};
use catga_core::{
    CatgaError, CatgaResult, ErrorCode, EventStore, MessageTransport, command_handler_with,
    event_handler_with, request_handler_with,
};
use serde::{Deserialize, Serialize};

use super::{
    domain::{GetOrder, OrderAccepted, OrderCompleted, PlaceOrder, RecordOrder},
    in_memory::OrderRuntime,
};

/// Startup options for the runnable in-memory order service.
///
/// [`Default::default`] starts `node-a` as the sole leader and accepts payment captures. Use
/// [`Self::with_members`] to demonstrate a follower rejecting writes, and
/// [`Self::with_declined_payments`] to exercise Flow compensation.
pub struct OrderServiceOptions {
    node_id: Box<str>,
    members: Vec<Box<str>>,
    accepts_payments: bool,
}

impl Default for OrderServiceOptions {
    fn default() -> Self {
        Self {
            node_id: "node-a".into(),
            members: vec!["http://cluster/node-a".into()],
            accepts_payments: true,
        }
    }
}

impl OrderServiceOptions {
    /// Returns options whose payment gateway declines every capture for compensation demos.
    #[must_use]
    pub fn with_declined_payments() -> Self {
        Self {
            accepts_payments: false,
            ..Self::default()
        }
    }

    /// Returns options for `node-a` with the supplied cluster member endpoints.
    ///
    /// Each endpoint must end with its stable node identifier, such as
    /// `http://cluster/node-a`. The supplied members must include `node-a` because it is the
    /// in-memory application's local node.
    #[must_use]
    pub fn with_members<I, E>(members: I) -> Self
    where
        I: IntoIterator<Item = E>,
        E: Into<Box<str>>,
    {
        Self {
            members: members.into_iter().map(Into::into).collect(),
            ..Self::default()
        }
    }
}

/// An immutable health document exposed by `GET /healthz`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OrderServiceHealth {
    /// The node serving the health document.
    pub node_id: Box<str>,
    /// Whether this node currently owns leadership.
    pub is_leader: bool,
    /// The elected leader endpoint, when the coordinator knows one.
    pub leader_endpoint: Option<Box<str>>,
    /// The configured cluster-member count.
    pub cluster_size: usize,
}

/// A complete in-memory checkout application.
///
/// The application demonstrates explicit startup registration and no hidden background tasks.
/// Successful commands append an event and enqueue it in the outbox; this sample immediately
/// invokes a single outbox flush to make its local delivery observable. Production applications
/// should supervise an [`catga_core::OutboxProcessor`] worker and replace memory adapters with
/// durable ones.
#[derive(Clone)]
pub struct OrderService {
    application: CatgaApplication,
    cluster: Arc<MemoryCluster>,
    node: Arc<MemoryClusterNode>,
    runtime: Arc<OrderRuntime>,
}

impl OrderService {
    /// Constructs the complete application with in-memory cluster, event, outbox, and transport adapters.
    ///
    /// This validates topology before registering handlers, so configuration errors occur before
    /// any listener starts or command executes.
    pub fn in_memory(options: OrderServiceOptions) -> CatgaResult<Self> {
        if options.members.is_empty() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "the order-service cluster requires at least one member",
            ));
        }
        if !options
            .members
            .iter()
            .any(|member| node_id(member) == options.node_id.as_ref())
        {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "the order-service local node must be a configured cluster member",
            ));
        }

        let endpoints = options
            .members
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let cluster = Arc::new(MemoryCluster::new(options.node_id.to_string(), endpoints));
        let node = cluster.node(&options.node_id).ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Validation,
                "the order-service local node is not addressable in the configured cluster",
            )
        })?;
        let runtime = Arc::new(OrderRuntime::new(
            Arc::clone(&node),
            options.accepts_payments,
        )?);
        let application = catga_axum::catga_application! {
            mediator_handle = runtime.mediator;
            handlers {
                request PlaceOrder => request_handler_with(Arc::clone(&runtime), super::checkout::place_order);
                request GetOrder => request_handler_with(Arc::clone(&runtime), super::checkout::get_order);
                command RecordOrder => command_handler_with(Arc::clone(&runtime), super::checkout::record_order);
                event OrderCompleted => [event_handler_with(Arc::clone(&runtime), super::checkout::project_completed)];
            }
            routes {
                requests { @post "/orders" => PlaceOrder }
                events {}
            }
        }?;
        Ok(Self {
            application,
            cluster,
            node,
            runtime,
        })
    }

    /// Builds the typed Axum API and the cluster readiness endpoint.
    pub fn router(&self) -> CatgaResult<Router> {
        let service = self.clone();
        Ok(self.application.router().route(
            "/healthz",
            get(move || {
                let service = service.clone();
                async move { Json(service.health()) }
            }),
        ))
    }

    /// Captures the local cluster-health snapshot used by `GET /healthz`.
    #[must_use]
    pub fn health(&self) -> OrderServiceHealth {
        let health = cluster_health(self.node.as_ref());
        OrderServiceHealth {
            node_id: health.node_id().into(),
            is_leader: health.is_leader(),
            leader_endpoint: health.leader_endpoint().map(Into::into),
            cluster_size: health.cluster_size(),
        }
    }

    /// Moves the in-memory cluster's leader for deterministic local demonstrations and tests.
    ///
    /// Real deployments receive leadership from the Raft runtime rather than selecting it through
    /// an application method.
    pub fn elect_leader(&self, node_id: &str) -> CatgaResult<()> {
        self.cluster.elect(node_id).ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Validation,
                "the requested in-memory leader is not a configured cluster member",
            )
        })
    }

    /// Receives, decodes, and acknowledges one completed-order outbox delivery.
    ///
    /// The acknowledgement occurs even when decoding fails so this runnable sample does not leave
    /// an in-flight memory delivery behind. Durable consumers should instead route malformed
    /// records to a dead-letter policy before acknowledging their broker delivery.
    pub async fn receive_completed_order(&self) -> CatgaResult<OrderAccepted> {
        let delivery = self.runtime.transport.receive().await?;
        let decoded = if delivery.envelope().message_type() != "order.completed" {
            Err(CatgaError::new(
                ErrorCode::SerializationFailed,
                "order-service received an unexpected outbound event type",
            ))
        } else {
            serde_json::from_slice::<OrderCompleted>(delivery.envelope().payload())
                .map(|event| OrderAccepted {
                    order_id: event.order_id,
                    quantity: 0,
                    total_cents: event.total_cents,
                })
                .map_err(|error| {
                    CatgaError::new(
                        ErrorCode::SerializationFailed,
                        "decode completed-order outbox event",
                    )
                    .with_details(error.to_string())
                })
        };
        self.runtime.transport.ack(delivery).await?;
        decoded
    }

    /// Returns how many completed events the event store has durably recorded in this process.
    pub async fn completed_event_count(&self) -> CatgaResult<u64> {
        let version = self.runtime.event_store.version("orders").await?;
        u64::try_from(version.saturating_add(1)).map_err(|_| {
            CatgaError::new(
                ErrorCode::Internal,
                "order-service event-store version cannot fit in u64",
            )
        })
    }

    /// Returns the number of active inventory reservations in the example's in-memory gateway.
    pub fn reserved_inventory_count(&self) -> usize {
        self.runtime.inventory_len()
    }

    /// Returns the number of captured payments in the example's in-memory gateway.
    pub fn captured_payment_count(&self) -> usize {
        self.runtime.payment_len()
    }

    /// Returns how many `OrderCompleted` events passed through the CQRS event handler.
    pub fn handled_completion_count(&self) -> usize {
        self.runtime.completed_handlers.load(Ordering::Acquire)
    }
}

fn node_id(endpoint: &str) -> &str {
    endpoint.rsplit('/').next().unwrap_or(endpoint)
}
