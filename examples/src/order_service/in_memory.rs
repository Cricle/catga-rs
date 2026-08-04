//! In-memory infrastructure used only by the runnable order-service example.
//!
//! This module deliberately keeps process-local storage and gateway doubles out of the business
//! checkout workflow. Production applications replace these adapters with durable boundaries.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, AtomicUsize},
    },
};

use catga_cluster::MemoryClusterNode;
use catga_core::memory::{MemoryEventStore, MemoryOutbox, MemoryTransport};
use catga_core::{CatgaError, CatgaResult, ErrorCode, MediatorHandle};

use super::domain::OrderAccepted;

/// Process-local adapters and state for the runnable example.
pub(super) struct OrderRuntime {
    pub(super) mediator: MediatorHandle,
    pub(super) node: Arc<MemoryClusterNode>,
    pub(super) event_store: Arc<MemoryEventStore>,
    pub(super) outbox: Arc<MemoryOutbox>,
    pub(super) transport: Arc<MemoryTransport>,
    orders: Mutex<HashMap<Box<str>, OrderAccepted>>,
    inventory: Mutex<HashSet<Box<str>>>,
    payments: Mutex<HashSet<Box<str>>>,
    accepts_payments: bool,
    pub(super) next_order_id: AtomicU64,
    pub(super) completed_handlers: AtomicUsize,
}

impl OrderRuntime {
    pub(super) fn new(node: Arc<MemoryClusterNode>, accepts_payments: bool) -> CatgaResult<Self> {
        Ok(Self {
            mediator: MediatorHandle::new(),
            node,
            event_store: Arc::new(MemoryEventStore::default()),
            outbox: Arc::new(MemoryOutbox::default()),
            transport: Arc::new(MemoryTransport::new(32)?),
            orders: Mutex::new(HashMap::new()),
            inventory: Mutex::new(HashSet::new()),
            payments: Mutex::new(HashSet::new()),
            accepts_payments,
            next_order_id: AtomicU64::new(0),
            completed_handlers: AtomicUsize::new(0),
        })
    }

    pub(super) fn lock_orders(
        &self,
    ) -> CatgaResult<MutexGuard<'_, HashMap<Box<str>, OrderAccepted>>> {
        self.orders.lock().map_err(|_| {
            CatgaError::new(
                ErrorCode::Internal,
                "order-service read model lock is poisoned",
            )
        })
    }

    pub(super) fn reserve_inventory(&self, order_id: &str) -> CatgaResult<()> {
        let inserted = self
            .inventory
            .lock()
            .map_err(|_| {
                CatgaError::new(
                    ErrorCode::Internal,
                    "order-service inventory lock is poisoned",
                )
            })?
            .insert(order_id.into());
        if inserted {
            Ok(())
        } else {
            Err(CatgaError::new(
                ErrorCode::Conflict,
                "inventory is already reserved for this order",
            ))
        }
    }

    pub(super) fn release_inventory(&self, order_id: &str) -> CatgaResult<()> {
        self.inventory
            .lock()
            .map_err(|_| {
                CatgaError::new(
                    ErrorCode::Internal,
                    "order-service inventory lock is poisoned",
                )
            })?
            .remove(order_id);
        Ok(())
    }

    pub(super) fn capture_payment(&self, order_id: &str) -> CatgaResult<()> {
        if !self.accepts_payments {
            return Err(CatgaError::new(
                ErrorCode::Unavailable,
                "payment provider declined the charge",
            ));
        }
        self.payments
            .lock()
            .map_err(|_| {
                CatgaError::new(
                    ErrorCode::Internal,
                    "order-service payment lock is poisoned",
                )
            })?
            .insert(order_id.into());
        Ok(())
    }

    pub(super) fn refund_payment(&self, order_id: &str) -> CatgaResult<()> {
        self.payments
            .lock()
            .map_err(|_| {
                CatgaError::new(
                    ErrorCode::Internal,
                    "order-service payment lock is poisoned",
                )
            })?
            .remove(order_id);
        Ok(())
    }

    pub(super) fn inventory_len(&self) -> usize {
        self.inventory.lock().map_or(0, |inventory| inventory.len())
    }

    pub(super) fn payment_len(&self) -> usize {
        self.payments.lock().map_or(0, |payments| payments.len())
    }
}
