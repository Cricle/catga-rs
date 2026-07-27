# Source Compatibility Matrix

This matrix tracks semantic migration from the checked-in C# reference to the
pure-Rust workspace.  It records Rust replacements rather than preserving
.NET DI, reflection, or package names where they are not idiomatic in Rust.

| Upstream area | Rust replacement | Evidence | Status |
| --- | --- | --- | --- |
| Core CQRS, pipeline, reliability, event sourcing (144 C# files) | `catga-core`, `catga-macros`, `catga-codec-memorypack` | `tests/{mediator,pipeline,resilience,reliability_contracts,event_sourcing,projections,transport,distributed_id,time_travel,event_store}` | Migrated; public Rustdoc is checked with warnings denied, event-store reads use validated cursor pages of at most 1,024 records, Snowflake IDs support zero-allocation caller-buffer formatting, `ResilienceExecutor` provides bounded reusable transport/persistence resilience with rolling failure-ratio circuits and atomic full jitter, and `SnapshotTimeTravelService` reconstructs historical aggregates from immutable version-matched snapshots plus only later events |
| HTTP integration (11 C# files) | `catga-axum` | `tests/axum.rs` | Migrated with typed and arbitrary-signature static Axum routes instead of reflection discovery; leader forwarding and the opt-in `CorrelationHttpClient` retain ambient correlation and W3C trace context across HTTP hops without a global client wrapper, `EndpointValidation` maps input errors to the stable Catga validation result, `IntoCatgaHttpResponse` replaces overlapping mutable C# result builders with one allocation-conscious result-to-response trait, and opt-in `endpoint_panic_middleware` replaces endpoint exception handling without exposing panic payloads |
| Cluster coordination (10 C# files) | `catga-cluster`, Axum raft transport | `tests/{cluster,raft_cluster,raft_runtime,raft_state_machine_runtime}.rs` | Migrated; committed application entries use a bounded in-memory page and resume from durable Raft storage without discarding overflow |
| Flow and state machines (67 C# files) | `catga-flow`, Memory/Redis/NATS flow stores | `tests/flow`, `tests/state_machine*.rs`, `tests/observability.rs` | Migrated; durable DSL recovery uses bounded nested conditional paths, explicitly replayable ForEach item snapshots, branch-local parallel cursors, and a persisted `when_any` winner. Durable child fan-out first persists up to 1,024 caller-supplied stable child identities, then uses expiring CAS launch claims and an application-owned idempotent launcher; it retains no child task, no unbounded result list, and no result payload larger than 64 KiB. Tagged durable steps select caller-owned timeouts and bounded retries only for typed `Transient` errors; no detached timer or retry task is created, and every durable transition remains persisted regardless of a source-style persist marker. Discovery summaries and state-machine snapshots preserve exact creation and last-successful-update timestamps in their first Rust durable format, with no backend-specific schema expansion. Async step lifecycle hooks are serial, caller-owned, and emitted before ordinary checkpoint persistence, preserving at-least-once replay. `FlowSucceeded` is emitted only after an atomically created bounded terminal state, so later invocations restore the terminal state without replaying steps or that success hook. The same recovery contract is exercised in memory and compiled through explicit Redis/NATS service-gated tests |
| SQL Flow persistence | `catga-flow-store` | `crates/catga-flow-store/tests/{sqlite,mysql,postgres,mssql}.rs`, `tests/redis.rs` | Migrated as feature-gated SQLite, MySQL, PostgreSQL, and SQL Server adapters. The Redis feature re-exports `RedisFlows` and `RedisSuspendedFlows`; plain state uses Lua atomic create/version CAS/heartbeat transitions and a 32-candidate per-type stale-claim index. All SQL values use bound parameters, continuation discovery has physical scan indexes, summaries preserve exact sub-millisecond creation times, claims use bounded revision fencing, and SQL Server holds skip-locked selection plus CAS in one transaction. No worker, RabbitMQ/AMQP adapter, or HTTP health route is introduced |
| In-memory persistence (17 C# files) | `catga-memory` | `tests/{memory_reliability,event_sourcing,flow}` | Migrated |
| NATS persistence and transport (24 C# files) | `catga-nats` | `tests/{nats,nats_request}.rs` | Migrated; real Core NATS and JetStream regressions include explicit QoS separation and native redelivery-attempt reporting |
| Redis persistence and transport (26 C# files) | `catga-redis` | `tests/redis.rs`, `tests/state_machine_persistence.rs` | Migrated; real Redis regression covers Streams queues, ephemeral Pub/Sub broadcasts, stores, historical enhanced snapshots, durable DSL step progress, native redelivery-attempt reporting, and bounded group-wide idle-delivery recovery |
| External job schedulers (6 C# files) | `FlowDueService`, `DueFlowScheduler`, Memory/SQL/Redis/NATS schedulers, `catga-scheduler-tokio-cron` | `tests/flow`, `crates/catga-flow-store/tests/{sqlite,mssql}.rs`, `tests/{redis,nats}.rs`, `crates/catga-scheduler-tokio-cron/tests/cron_runtime.rs` | Replaced with lease-aware pure-Rust scheduling. `SqlFlowScheduler` provides feature-gated SQLite, MySQL, PostgreSQL, and SQL Server persistence with target uniqueness, indexed bounded lease claims, and no worker. The NATS scheduler uses JetStream KV CAS, generation-fenced schedule identities, and a 32-entry paged discovery bound. `FlowDueService` remains caller-owned; the opt-in `CronRuntime` only drives explicitly registered cron callbacks, and `flow_due_job` runs one bounded `check_at` sweep per callback. |
| Binary serialization (4 C# files) | `catga-codec-memorypack` | `tests/{codec,memorypack}.rs`, codec crate tests | Replaced with bounded MemoryPack framing, reusable direct encode buffers, and Core `TypedTransport`/`TypedDelivery` generic transport contracts; application payloads use explicit static MemoryPack schemas rather than runtime reflection |
| Source generation (15 C# files) | `catga-macros` | `tests/macros.rs`, `docs/source-generator-mapping.md` | Replaced with proc macros, trait bounds, and compile-time handler validation |
| Testing helpers (5 C# files) | `catga-testing` | `tests/testing/{helpers,harness,aggregate,flow}.rs` | Migrated; spies cover concrete handlers, async actions, explicit missing-handler failures, ordered captures, and assertion helpers, while typed aggregate scenarios and Flow contexts use real bounded in-memory dependencies |
| In-memory transport (3 C# files) | `catga-memory::{MemoryTransport, MemoryPubSubTransport}` | `tests/{memory_transport,delivery_ack,observability}.rs` | Migrated with distinct bounded queue and broadcast adapters plus drain tracking; real queue and Pub/Sub publications emit bounded producer metrics |
| Destination send/subscribe transport contract | `DestinationTransport` plus Memory/Redis/NATS adapters | `tests/transport/destination.rs`, `tests/{redis,nats}.rs` | Migrated; Memory, Redis, and JetStream paths verified |
| Transport-context metadata and header destination routing | `catga-core::{EnvelopeHeaders, MessageRouter}` and `catga-codec-memorypack` | `tests/{message,codec,routing}.rs` | Migrated with bounded immutable headers, allocation-free routing, and strict MemoryPack propagation |
| Cross-service request client factory | `MemoryPackRequestClientFactory` | `tests/transport/request_client.rs` | Migrated with `Arc` sharing, typed default destinations, timeout validation, and codec-independent Core request traits |
| RabbitMQ/AMQP broker adapter | None by project constraint | workspace dependency and source audit | Intentionally excluded; no RabbitMQ, AMQP, `lapin`, or `amqprs` dependency is present |
| ASP.NET HTTP health routes | None by project constraint | route and source audit | Intentionally excluded; internal Rust lifecycle probes are not HTTP health-check endpoints |

## Verification Boundaries

The workspace's deterministic tests, formatting, Clippy, and Rustdoc gates run
without services. `cargo test -p catga-tests --test nats --test nats_request`
starts one isolated JetStream container per test when `CATGA_NATS_URL` is
absent, waits for it to be removed before returning, and uses a configured URL
without deleting that external service. Redis and RobustMQ integration tests remain ignored by
default and require `CATGA_REDIS_URL` or `CATGA_ROBUSTMQ_URL`; run them
explicitly with `cargo test -- --ignored` when those services are available.
The optional mailbox-protocol request/server test intentionally requires
`CATGA_ROBUSTMQ_URL`, because a plain NATS server does not implement the
protocol's mailbox-control endpoint. Manual, host-neutral performance baselines
are ignored by default: NATS durable round trips
(`cargo test -p catga-tests --test nats_performance -- --ignored --nocapture`),
bounded automatic batching
(`cargo test --manifest-path tests/Cargo.toml --test transport_batch transport_batcher_throughput_benchmark -- --ignored --nocapture`),
and SQLite state-machine/DSL progress lifecycles
(`cargo test -p catga-flow-store --features sqlite --test sqlite_state_machine_performance --test sqlite_dsl_progress_performance -- --ignored --nocapture`).
Each prints throughput while retaining correctness checks and intentionally has
no machine-dependent timing threshold.

## Migration Rules

* Public Rust APIs use `CatgaResult` for invalid input and operational errors;
  production paths do not use panic-prone `unwrap` or `expect` calls.
* Source `ErrorInfo` maps to a typed `CatgaError` category, bounded optional
  diagnostic details (at most 1 KiB without splitting UTF-8), and explicit
  retryability. `Transient`, `Timeout`, and `Unavailable` derive retryability;
  source `TRANSPORT_FAILED` and `SERIALIZATION_FAILED` are accepted as typed
  input aliases. MemoryPack RPC errors preserve the supported fields and every
  decode consumes one exact bounded frame; legacy binary layouts are rejected
  instead of remaining a runtime dependency.
* Public API documentation is compiled with warnings denied, so broken links and
  documentation warnings fail the quality gate instead of reaching consumers.
* EventStore has no whole-history or whole-catalog read API. Consumers follow
  validated event, metadata, and lexical stream-ID cursors, applying a page before
  requesting the next one. Redis uses bounded stream ranges and a sorted index;
  JetStream collects no more than one page; the in-memory implementation retains
  only the page's selected IDs while traversing its map.
* Raft retains only its configured number of unapplied application commands in
  memory. A peer that commits more commands leaves the overflow in the durable
  Raft log; callers consume a page and resume without data loss. State-machine
  acknowledgement advances only through commands successfully applied to business
  state.
* `MemoryPackValueCodec<T>` is an explicit, caller-defined application schema
  boundary. Its exact-frame helpers preserve the reader and writer allocation,
  nesting, and trailing-input checks without runtime type lookup or reflection.
* Transport operations consume caller-owned envelopes and use bounded futures
  or queues rather than task collections sized by input batches.
* Source generic transport calls map to Core `TypedTransport` with an explicit
  payload codec: ordinary messages
  use `AtLeastOnce`, while explicit `publish_event`/`send_event_to` operations
  use the source event default of `AtMostOnce` and reliable-event operations
  select `AtLeastOnce`. `#[catga(version = N)]` implements `Message::schema_version`
  and typed publication writes that value into the outgoing `Envelope`, replacing
  the source transport's reflection-based `TransportContext.SchemaVersion` enrichment.
  `Message::priority()` similarly replaces `IPrioritizedMessage.Priority` with
  a typed `Envelope` metadata field; `#[catga(priority = low | normal | high |
  critical)]` emits a static zero-allocation selection, while manual
  implementations may select from message values. Rust intentionally does not
  encode priority as the source's untyped `x-priority` header. Scoped inbound
  transport context carries the received `Copy` priority through nested typed
  publication, scheduling, and requests ahead of a nested message's default.
  Typed receive methods return `TypedDelivery<T>`,
  which retains the original owned acknowledgement token; decode failures
  request redelivery before returning a structured error. Typed batches lazily
  retain only the configured number of encoded messages and pending futures;
  convenience methods use the core default bounded concurrency. The typed
  scheduled outbox applies the same explicit ordinary-event-reliable-event QoS
  policy before durable insertion and preserves `Message::schema_version()` so
  delayed versioned messages remain upgradeable after persistence.
* Source `IDelayedMessage`, `IDelayedRequest`, and `IDelayedEvent` map to the
  runtime-neutral `DelayedMessage`, `DelayedRequest`, and `DelayedEvent` traits.
  A message may declare an absolute deadline or relative delay, with the absolute
  deadline taking precedence. `MemoryPackScheduledOutbox` resolves that declaration
  once and persists the existing `not_before` boundary; direct transport send and
  publish deliberately do not promise broker-specific delay and no `x-delay` header,
  timer, or hidden worker is introduced.
* Runtime-neutral traits live in `catga-core`; broker crates depend on core,
  never the reverse.
* The source competing subscription's single-event operation maps to
  `CompetingSubscriptionRunner::try_process_next`. It acquires the durable
  owner lease, retains only one bounded event-store page, advances filtered
  events through their per-stream checkpoints, and releases the lease after
  one handled event. The `None`/`Some(bool)` result distinguishes a busy lease
  from an idle subscription without an allocation or polling task.
* The source continuous subscription runner maps to
  `SubscriptionRunner::run_until_cancelled` and validated
  `SubscriptionLoopOptions`. Its first pass runs immediately, cancellation
  interrupts only the inter-pass wait, and the application owns the returned
  future rather than Catga spawning an unsupervised Tokio task. Completed pass
  counts saturate in the return value and each pass keeps the existing bounded
  event-store page.
* Subscription stream versions remain signed for source compatibility. Rust
  treats `i64::MAX` as a terminal persisted checkpoint or event version rather
  than adding one and panicking or wrapping to an invalid read position.
* Existing source abstractions that solely configure .NET DI are represented
  by explicit Rust construction and typed composition, not a global registry.
* The source recovery hosted service maps to caller-owned
  `RecoveryManager::run_auto_recovery`. It performs its first sweep
  immediately, keeps retries and cancellation bounded, and isolates a panic
  from one `RecoverableComponent` as that component's failed attempt so later
  components and future sweeps remain available. This is the only unwind
  boundary around third-party recovery code; no panic payload is retained or
  exposed. `recover_unhealthy_until` also selects cancellation against an
  in-progress component recovery and clears the exclusive sweep state before
  returning a structured cancellation error, rather than waiting for an
  uncooperative component forever.
* C# source-generation responsibilities are split by Rust's compile-time
  boundaries: `#[derive(Message)]` emits message identity, authorization,
  batch and trace-tag metadata; `catga_handlers!` emits typed request/event
  registration; `catga_routes!` emits static Catga routes and `axum_routes!`
  emits static native Axum-handler routes; and payload codec bounds make serializer
  availability a compile-time requirement. This replaces
  reflection-based registration analyzers without runtime scanning. MemoryPack
  bounds stay in `catga-codec-memorypack`; Core payload and request traits are
  format-independent so future codecs do not require a Core dependency change.
* The source hosted transport lifecycle maps to owning
  `TransportLifecycle<T>`. It initializes in the caller's task, permanently
  stops accepting work, waits for one bounded drain future, and consumes the
  transport so Rust `Drop` releases resources before shutdown returns. No
  framework-owned worker or HTTP health endpoint is introduced.
* Source identity claims map to bounded, immutable `SecurityClaims`: entries
  use sorted `Arc<[SecurityClaim]>` storage and binary lookup, with fixed
  count/key/value budgets. Claims remain data and never grant a role or policy
  by themselves. Portable dead letters retain bounded error text plus stable
  error code, UTC timestamp, and processing stage; memory, Redis, and NATS
  readers accept their legacy records as explicitly marked legacy diagnostics.
* Source Polly-style circuit and retry policy maps to a caller-owned rolling
  bounded outcome window with explicit minimum throughput and exact failure
  ratio. `RetryJitter::Full` advances one atomic state per retry and retains no
  task or waiter; the compatibility default remains deterministic no-jitter.
* .NET cancellation-token parameters map to cancellation by dropping Rust
  futures or to explicit `CancellationToken` arguments on long-lived workers.
  Short-lived mediator dispatch also provides opt-in
  `*_with_cancellation` methods; handlers and pipeline behavior can
  cooperatively inspect the task-scoped token through `current_cancellation()`.
  Ordinary dispatch stays token-free, preserving the minimal Rust API.
* Source request and no-response command pipeline behaviors map to distinct,
  typed `Pipeline` and `CommandPipeline` contracts. Both enforce the same
  startup depth bound, compose only explicitly supplied behaviors, carry the
  task-scoped cancellation token, and isolate recoverable handler or behavior
  panics as `Internal` errors without representing a command as a synthetic
  `Request<Response = ()>`.
* The source automatic batching behavior maps to the paired
  `AutoBatchingBehavior` and `AutoBatchingRunner` types. Construction returns
  the bounded sender and its single-use receiver together; the application
  explicitly supervises `run_until_cancelled` with a `CancellationToken`.
  This deliberate lifecycle difference removes hidden framework-owned Tokio
  tasks. Cancellation rejects unstarted work as `Unavailable`, then drains
  started batches, while keyed queues and active shard batches remain bounded.
* The source acknowledgement mode and terminal-reject APIs are implemented
  only by the excluded upstream adapter. In-scope adapters use owned
  [`Delivery`](../crates/catga-core/src/transport.rs) values with explicit
  acknowledgement or redelivery, while permanent failure is recorded through
  the portable dead-letter contract rather than silently discarded.
  `AckOptions`' delayed and exponential-redelivery fields have no upstream
  production consumer: the NATS adapter issues an immediate native NAK and
  the Redis adapter leaves an unacknowledged stream entry pending. Rust
  consequently keeps `Delivery::negative_acknowledge` immediate and
  backend-neutral instead of exposing a delay API that Redis and memory cannot
  faithfully implement. Applications that need scheduled retry compose the
  durable retry or flow scheduler explicitly; a future NATS-only capability
  can expose its native delayed NAK without weakening this portable contract.
* Source inbox requests with a zero message identifier bypass deduplication.
  Rust treats zero as an absent stable transport identity and runs the request
  directly, rather than allowing unrelated requests to share a sentinel inbox
  cache record.
* Source inbox `LockDuration` maps to the Rust five-minute
  `DEFAULT_INBOX_CLAIM_LEASE`, `InboxBehavior::with_claim_lease`, and the
  explicit `InboxStore::try_claim_for` operation. Memory keeps the deadline
  in its per-key atomic record, Redis compares the persisted deadline in one
  Lua transition, and JetStream KV retries revision-CAS updates. An expired
  claim is recovered only when a caller requests it, so crash recovery needs
  no polling task or unbounded in-process lease map.
* `CompetingConsumer` applies its optional terminal-attempt policy from the
  broker-maintained `Delivery::attempts()` value. JetStream reads the delivery
  count from its acknowledgement metadata without allocation; Redis inspects
  only the recovered entry's bounded `XPENDING` record. A terminal failure is
  acknowledged only after `DeadLetterStore::enqueue` succeeds. A dead-letter
  store error instead negatively acknowledges the original delivery, retaining
  it for a later retry rather than silently losing work.
* The source Redis adapter has two distinct delivery modes. Rust retains them
  as separate types: `RedisTransport` is the acknowledgement-backed Redis
  Streams queue, while `RedisPubSubTransport` is the intentionally ephemeral
  Redis Pub/Sub broadcast adapter. This prevents a configuration flag from
  accidentally changing durability, acknowledgement, or recovery semantics.
  For `ExactlyOnce` Pub/Sub envelopes, a Lua script atomically claims the
  bounded publisher identity and broadcasts once; each subscriber instance
  independently claims received identities with the same TTL, so duplicate
  broadcasts are suppressed without an unbounded local cache or cross-
  subscriber message loss.
* The source NATS adapter likewise has separate Core NATS and JetStream
  semantics. `NatsPubSubTransport` explicitly supports only ephemeral
  `AtMostOnce` broadcasts and requires no JetStream server; `NatsTransport`
  owns JetStream resource provisioning and is the sole adapter for durable
  `AtLeastOnce` and deduplicated `ExactlyOnce` deliveries. This makes the
  durability boundary visible in the Rust type system.
* The source in-memory transport's handler fan-out maps to
  `MemoryPubSubTransport`; its bounded Tokio broadcast ring gives every clone
  an independent subscriber cursor. `MemoryTransport` remains the separate
  bounded FIFO queue for acknowledgement and drain tests. Both broadcast
  adapters reject durable QoS values rather than implying recovery that an
  ephemeral channel cannot provide.
* The source diagnostics counters and activities map to `catga_core::telemetry`.
  It records bounded `backend`, `component`, `operation`, and `outcome` labels
  through the configured Rust `metrics` recorder and creates child `tracing`
  spans without a framework-owned exporter or background task. Core, memory,
  Redis, and JetStream durable operations use the same cancellation-safe async
  guard, so validation and CAS errors retain their original `CatgaResult` while
  producing a failure observation. Typed conflicts additionally increment
  `catga.persistence.conflicts`, while successful-but-unowned claim and lease
  attempts increment `catga.persistence.contention`. Inbox behavior reports the
  fixed outcomes `processed`, `hit`, `conflict`, `failure`, and `bypassed`, and
  retry backoff uses a cancellation-safe `catga.resilience.retry.pending` gauge.
  Distributed locking records acquisition latency and the fixed `success`,
  `contention`, and `failure` acquisition outcomes, a cancellation-safe
  `catga.distributed_lock.held` gauge, and fixed release outcomes for success,
  failure, and ownership loss; resource keys and owner identifiers are never
  metric labels.
  Queue, destination, broadcast, Core NATS,
  and JetStream publishers additionally emit the bounded
  `catga.messages.{published,failed,aborted}` counters and publish-duration
  histogram through a caller-owned future, without a framework task or
  dynamic destination labels. The Redis and JetStream regression targets remain
  endpoint-gated; without their environment variables they compile and execute
  the deterministic skip path rather than claiming a live service verification.
* Source Flow lifecycle counters map to the durable `FlowRuntime` transition
  boundaries. `catga.flow.active` counts claimed drives currently executing in
  this process rather than persisted suspended flows, so it is restored through
  cancellation-safe RAII without polling a store or retaining flow ids. Flow
  IDs, flow types, and step names are tracing-only fields; Flow metric labels
  are static outcomes where a histogram needs one.
* Cluster coordination publishes low-cardinality Raft leader, role, term,
  commit/apply, pending-commit, inbound-queue, and command-queue gauges plus
  transition and fixed-kind failure counters. Numeric member identities are
  gauge values rather than labels. Envelope and HTTP boundaries propagate
  validated, bounded W3C `traceparent` and `tracestate` headers, and consumer
  processing keeps one tracing span active through handler, dead-letter, and
  acknowledgement work. Libraries install neither an OpenTelemetry provider
  nor a health endpoint; applications connect the `metrics` recorder and
  `tracing` subscriber to their chosen OpenTelemetry SDK/exporter.
* NATS ExactlyOnce deduplication is broker-owned. The adapter records
  `catga.nats.dedup.drops` only when a JetStream publish acknowledgement marks
  the message as duplicate, including explicit destination publication. It
  intentionally does not retain the source adapter's local deduplication map
  or expose cache-eviction metrics, avoiding an in-process memory budget that
  can diverge from the broker's deduplication window.
* Source `TransportContext.Metadata` dictionaries map to optional immutable
  `EnvelopeHeaders`. Header-free envelopes retain no header allocation; a
  nonempty header set uses one shared `Arc` slice with bounded, validated
  key/value bytes and unique keys. `TypedDelivery::with_transport_context`
  explicitly scopes a received envelope's correlation, `Copy` priority, and
  shared headers while Rust owns the delivery; nested typed publication, typed
  request/reply, and delayed outbox insertion inherit them without a payload
  copy or background task. Explicit typed headers override matching scoped
  keys and retain other scoped keys, using the same bounded merge validation.
  `MessageRouter::resolve_envelope_headers` retains first-match routing without
  rebuilding a map, and the envelope header field carries the context
  through codec-backed adapters with strict bounded MemoryPack frames.
* Source `TransportContext.SentAt` maps to optional UTC epoch milliseconds on
  `Envelope`. New Rust envelopes capture the wall clock without coupling to a
  Snowflake ID, callers can supply an exact replay time, and MemoryPack preserves
  it across codec-backed adapters.
* Source outbox `RetryCount`, `MaxRetries`, `LastError`, and terminal `Failed`
  status map to per-message Rust state with a default three-failure limit.
  Failure text is UTF-8-safe and capped at 1 KiB. Memory uses an entry-local
  mutation and selects source-ordered `CreatedAt` candidates through a bounded
  heap, Redis uses one owner-checking Lua transition, and JetStream uses a
  revision-CAS record update; terminal records remain inspectable but cannot be
  reclaimed. All enqueue boundaries reject the source-reserved identifier zero
  before encoding, allocation, or backend I/O. This intentionally does not
  silently repurpose the portable dead-letter store, matching the source
  outbox contract.
* Outbox claims use an explicit 1,024-message maximum instead of trusting an
  arbitrary caller `usize`. Oversized requests return validation errors before
  allocation or I/O; a caller drains larger backlogs through repeated bounded
  claims. Redis additionally scans at most four times the requested candidates
  per Lua invocation, so a large due backlog cannot expand script memory or
  runtime proportionally to every due record.
* RobustMQ mailbox requests and replies consume `Envelope` priority rather
  than an adapter-local default. Typed MemoryPack replies preserve the request
  priority while keeping response QoS, delivery mode, and scheduling metadata
  independent. RobustMQ has only low, normal, and high broker levels, so both
  Catga `High` and `Critical` intentionally map to its high level without a
  cache or header side channel.
* The source Flow DSL is intentionally split by Rust execution lifetime:
  `DslFlow` covers typed in-process composition (`send_into` replaces
  result-producing query steps and `match_on` replaces switch branches), while
  `FlowDefinition` plus `FlowRuntime` owns restart-safe work. Its
  `FlowStepOutcome::delay` and `suspend_until` are the durable `Delay` and
  `ScheduleAt` counterparts and persist a named continuation before delegating
  wake-up to a scheduler. This avoids a hidden `sleep` task that would lose
  ownership and recovery semantics on restart.
* The source nested `Throttle` builder creates a fresh semaphore for each
  sequential inner branch execution, so it does not impose a shared limit
  across reusable flows. Rust replaces it with the explicit, cloneable
  `FlowThrottle`: callers share one validated semaphore budget across the
  particular `DslFlow::throttle` actions that need it. This is intentionally
  stronger, avoids a no-op nested builder, and remains in-process only; durable
  concurrency ownership belongs to `FlowRuntime` and its lease-aware stores.
* The source's internal `ArrayPool`-backed buffer writer maps to explicit
  caller-owned Rust buffers. `MemoryPackCodec::encode_into` and
  `encode_value_into` clear and reuse a supplied `Vec<u8>` without replacing
  its capacity; fixed-output compression APIs cover callers that need a hard
  allocation ceiling. This retains the pool's allocation benefit without a
  shared return lifecycle, reference-clearing policy, or global allocator
  contention in the public API.
* The source `IResiliencePipelineProvider` maps to the explicit,
  caller-owned `ResilienceExecutor` and `ResilienceOptions`. One executor can
  be shared by the particular mediator, transport, or persistence adapters
  that require the same admission budget and circuit, while independent
  executors prevent an overloaded transport from consuming persistence
  capacity. It bounds executing and queued calls, cancels timed-out attempts,
  retries only structured `Transient` and `Timeout` errors, and opens its
  circuit only for those recoverable failures. Unlike a DI-injected Polly
  provider, Rust does not apply hidden retries to every store call: an
  application explicitly composes the executor around operations known to be
  idempotent, retaining optimistic-concurrency and lease transitions as
  single-attempt operations.
* Source per-step modifiers map to composable `DslStep` and `DslQueryStep`
  builders. `only_when` skips the action before request construction,
  `optional` suppresses only non-cancellation `CatgaError` values returned by
  the underlying action or request (not a later `fail_if` predicate), and
  `fail_if` / `fail_if_response` return explicit structured errors before a
  later step or a response-to-state write can occur. This retains the useful
  fluent composition without catching panics or weakening Rust's cooperative
  cancellation semantics. Conditional routing remains explicit through
  `if_else` and `match_on`.
* The source flow-state change-tracking generator targets mutable C# POCO
  fields. Rust durable flows instead advance immutable, versioned `FlowState`
  revisions. `DslFlow::run_checkpointed` persists bounded nested conditional
  paths, explicit replayable ForEach item snapshots, per-branch local progress
  for parallel work, and one completed `when_any` winner before merging it.
  The legacy generic `for_each` remains available for in-process execution but
  returns validation from checkpointed execution; callers opt into the
  serialization-bounded replayable form when restart recovery is required.
  Generic streaming and concurrent ForEach operations likewise return
  validation because they have no stable replay/result cursor. This makes
  write ownership and restart recovery explicit, so no reflection-driven field
  mask or hidden mutable dirty flag is needed.
* Source `ForEachFailureHandling.ContinueOnFailure` maps to the explicitly
  named `DslFlow::for_each_continue_on_error` and
  `DslFlow::for_each_replayable_continue_on_error` operations. Rust requires
  the caller to receive every failed item index and structured error through a
  callback, rather than silently returning success after dropped failures.
  The replayable form atomically persists callback-updated state and its next
  item cursor, so a completed failure is not replayed after restart; a callback
  error leaves that item unresolved for a deliberate retry.
* The source Redis enhanced snapshot and DSL-flow stores map to
  `RedisEnhancedSnapshots` and `RedisDslStepProgress`. Snapshot versions use
  lexicographically ordered, signed-version encodings instead of lossy Redis
  floating-point scores; record and index changes are Lua-atomic, and history
  cleanup is bounded in small batches. DSL progress keeps its version in Redis
  so create and update use server-side compare-and-set rather than a client
  read-modify-write race.
