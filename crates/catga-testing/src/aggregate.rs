//! Typed, in-memory scenarios for event-sourced aggregate tests.

use std::marker::PhantomData;

use catga_core::{
    Aggregate, CatgaError, CatgaResult, Envelope, ErrorCode, EventStore, MAX_EVENT_STORE_PAGE_SIZE,
};
use catga_memory::MemoryEventStore;

/// A caller-configured aggregate history that is replayed through [`MemoryEventStore`].
///
/// This test-only helper borrows caller-owned seed vectors and never mutates their envelopes.
/// Each [`Self::replay`] call creates a fresh real memory store, appends copies of that history,
/// and replays the persisted envelopes into a newly constructed aggregate. It is not a production
/// repository, registry, or global aggregate lookup mechanism.
pub struct AggregateScenario<A> {
    id: Box<str>,
    aggregate: PhantomData<fn() -> A>,
}

impl<A> AggregateScenario<A>
where
    A: Aggregate,
{
    /// Creates a scenario for `id`.
    ///
    /// Empty histories are valid and replay to the aggregate's initial version. An empty
    /// identifier is rejected because it cannot name a stable aggregate stream.
    pub fn new(id: impl Into<Box<str>>) -> CatgaResult<Self> {
        let id = id.into();
        if id.is_empty() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "aggregate test scenario id must not be empty",
            ));
        }
        Ok(Self {
            id,
            aggregate: PhantomData,
        })
    }

    /// Replays this scenario's seed history through a fresh [`MemoryEventStore`].
    ///
    /// The returned value retains the replayed aggregate and immutable persisted envelopes for
    /// explicit version and history assertions.
    pub async fn replay(&self, seeded_events: &[Envelope]) -> CatgaResult<ReplayedAggregate<A>> {
        let store = MemoryEventStore::default();
        let stream_id = A::stream_id(&self.id);
        if !seeded_events.is_empty() {
            store
                .append(&stream_id, seeded_events.to_vec(), Some(-1))
                .await?;
        }
        let mut aggregate = A::new(&self.id);
        let mut events = Vec::with_capacity(seeded_events.len());
        let mut stream_version = -1;
        let mut cursor = 0;
        loop {
            let page = store
                .read_page(&stream_id, cursor, MAX_EVENT_STORE_PAGE_SIZE)
                .await?;
            for stored in page.stream().events() {
                aggregate.apply(stored.envelope())?;
                if aggregate.version() != stored.version() {
                    return Err(CatgaError::new(
                        ErrorCode::Validation,
                        "aggregate apply did not advance to the persisted event version",
                    ));
                }
                stream_version = stored.version();
                events.push(stored.envelope().as_ref().clone());
            }
            let Some(next) = page.next_version() else {
                break;
            };
            cursor = next;
        }
        Ok(ReplayedAggregate {
            aggregate,
            events,
            stream_version,
        })
    }
}

/// The result of replaying one [`AggregateScenario`].
///
/// Assertion methods return [`CatgaResult`] so invalid test expectations remain structured
/// errors rather than panics in helper implementation code.
pub struct ReplayedAggregate<A> {
    aggregate: A,
    events: Vec<Envelope>,
    stream_version: i64,
}

impl<A> ReplayedAggregate<A>
where
    A: Aggregate,
{
    /// Returns the aggregate built from the persisted scenario history.
    pub fn aggregate(&self) -> &A {
        &self.aggregate
    }

    /// Consumes the replay result and returns its aggregate.
    pub fn into_aggregate(self) -> A {
        self.aggregate
    }

    /// Returns the immutable envelopes read from the real memory event store.
    pub fn events(&self) -> &[Envelope] {
        &self.events
    }

    /// Verifies both the aggregate and persisted stream reached `expected`.
    pub fn assert_version(&self, expected: i64) -> CatgaResult<()> {
        if self.aggregate.version() == expected && self.stream_version == expected {
            return Ok(());
        }
        Err(CatgaError::new(
            ErrorCode::Validation,
            format!(
                "aggregate scenario version mismatch: aggregate={}, stream={}, expected={expected}",
                self.aggregate.version(),
                self.stream_version,
            ),
        ))
    }

    /// Verifies the replayed immutable envelope history equals `expected` in stream order.
    pub fn assert_events(&self, expected: &[Envelope]) -> CatgaResult<()> {
        if self.events == expected {
            return Ok(());
        }
        Err(CatgaError::new(
            ErrorCode::Validation,
            format!(
                "aggregate scenario event history mismatch: actual={} expected={}",
                self.events.len(),
                expected.len(),
            ),
        ))
    }
}
