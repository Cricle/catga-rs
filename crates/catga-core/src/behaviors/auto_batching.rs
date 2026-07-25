//! Bounded, keyed request batching with caller-owned execution.

use std::{
    collections::{HashMap, HashSet, VecDeque, hash_map::Entry as HashMapEntry},
    future::Future,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use futures::{
    FutureExt,
    stream::{self, FuturesUnordered, StreamExt},
};
use tokio::{
    sync::{mpsc, oneshot},
    time::{Instant, sleep_until},
};
use tokio_util::sync::CancellationToken;

use crate::{
    BatchKeyProvider, BatchOptionsProvider, Behavior, CatgaError, CatgaResult, ErrorCode, Next,
    Request,
};

const DEFAULT_BATCH_KEY: &str = "_";

type KeySelector<M> = Arc<dyn Fn(&M) -> Box<str> + Send + Sync>;
type FlushFuture = Pin<Box<dyn Future<Output = Box<str>> + Send>>;

/// Runtime limits for [`AutoBatchingBehavior`].
///
/// The behavior is active when `max_batch_size` is greater than one. Every
/// waiting shard is bounded by `max_queue_length`, and the runner keeps at
/// most `max_shards` waiting shards in memory.
#[derive(Clone, Debug)]
pub struct BatchOptions {
    /// Number of requests that immediately makes one shard ready to flush.
    pub max_batch_size: usize,
    /// Maximum time the oldest request waits before its shard is ready to flush.
    pub batch_timeout: Duration,
    /// Maximum number of pending requests in one shard.
    pub max_queue_length: usize,
    /// Maximum number of distinct pending keys.
    pub max_shards: usize,
    /// Maximum number of request handlers run concurrently within one batch.
    ///
    /// Shards retain FIFO order. The runner independently bounds active shard
    /// batches by `max_shards`, avoiding an unbounded task collection.
    pub flush_concurrency: usize,
}

impl Default for BatchOptions {
    fn default() -> Self {
        Self {
            max_batch_size: 100,
            batch_timeout: Duration::from_millis(100),
            max_queue_length: 10_000,
            max_shards: 2_048,
            flush_concurrency: 1,
        }
    }
}

impl BatchOptions {
    fn validate(&self) -> CatgaResult<()> {
        if self.max_batch_size == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "batch max size must be greater than zero",
            ));
        }
        if self.batch_timeout.is_zero() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "batch timeout must be greater than zero",
            ));
        }
        if self.max_queue_length == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "batch queue length must be greater than zero",
            ));
        }
        if self.max_shards == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "batch shard limit must be greater than zero",
            ));
        }
        if self.flush_concurrency == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "batch flush concurrency must be greater than zero",
            ));
        }
        Ok(())
    }
}

/// Defers compatible requests into bounded batches without changing handlers.
///
/// Construction returns this behavior together with an [`AutoBatchingRunner`].
/// The application owns and supervises the runner future; this behavior never
/// starts a background task. Concurrent calls enqueue through a bounded
/// channel, so a full queue applies Tokio backpressure to the caller.
pub struct AutoBatchingBehavior<M: Request> {
    options: BatchOptions,
    key_selector: KeySelector<M>,
    sender: mpsc::Sender<Queued<M>>,
}

/// Executes the queued work for one [`AutoBatchingBehavior`].
///
/// A runner is returned exactly once by the behavior constructor and is
/// consumed by [`Self::run_until_cancelled`]. Applications should run that
/// future inside a task they own and supervise. Cancellation stops new work,
/// rejects queued but unstarted requests with [`ErrorCode::Unavailable`], and
/// waits for already-started batches to resolve their reply tokens.
pub struct AutoBatchingRunner<M: Request> {
    receiver: mpsc::Receiver<Queued<M>>,
    options: BatchOptions,
}

impl<M: Request> AutoBatchingBehavior<M> {
    /// Creates a behavior and caller-owned runner that use one shared shard.
    pub fn new(options: BatchOptions) -> CatgaResult<(Self, AutoBatchingRunner<M>)> {
        Self::with_key(options, |_| Box::<str>::from(DEFAULT_BATCH_KEY))
    }

    /// Creates a behavior and runner from `M`'s compile-time batch configuration.
    pub fn from_message_options() -> CatgaResult<(Self, AutoBatchingRunner<M>)>
    where
        M: BatchOptionsProvider,
    {
        Self::new(M::batch_options())
    }

    /// Creates a keyed behavior and runner from `M`'s compile-time batch configuration.
    pub fn from_message_options_with_key() -> CatgaResult<(Self, AutoBatchingRunner<M>)>
    where
        M: BatchKeyProvider + BatchOptionsProvider,
    {
        Self::with_message_key(M::batch_options())
    }

    /// Creates a behavior and runner that aggregate requests by `key_selector`.
    ///
    /// Insert the behavior into a [`crate::Pipeline`] and start the returned
    /// runner. Dropping the runner before it runs makes batched requests fail
    /// with [`ErrorCode::Unavailable`] instead of implicitly starting work.
    pub fn with_key(
        options: BatchOptions,
        key_selector: impl Fn(&M) -> Box<str> + Send + Sync + 'static,
    ) -> CatgaResult<(Self, AutoBatchingRunner<M>)> {
        options.validate()?;
        let (sender, receiver) = mpsc::channel(options.max_queue_length);
        Ok((
            Self {
                options: options.clone(),
                key_selector: Arc::new(key_selector),
                sender,
            },
            AutoBatchingRunner { receiver, options },
        ))
    }

    /// Creates a behavior and runner that use [`BatchKeyProvider`] for sharding.
    pub fn with_message_key(options: BatchOptions) -> CatgaResult<(Self, AutoBatchingRunner<M>)>
    where
        M: BatchKeyProvider,
    {
        Self::with_key(options, |message| {
            message
                .batch_key()
                .unwrap_or_else(|| Box::<str>::from(DEFAULT_BATCH_KEY))
        })
    }
}

#[async_trait]
impl<M: Request> Behavior<M> for AutoBatchingBehavior<M> {
    async fn handle(&self, message: M, next: Next<M>) -> CatgaResult<M::Response> {
        if self.options.max_batch_size == 1 {
            return next.run(message).await;
        }

        let (reply, response) = oneshot::channel();
        let queued = Queued {
            key: (self.key_selector)(&message),
            entry: Pending {
                message,
                next,
                reply,
                enqueued_at: Instant::now(),
            },
        };
        self.sender.send(queued).await.map_err(|_| {
            CatgaError::new(
                ErrorCode::Unavailable,
                "automatic batch runner is unavailable",
            )
        })?;
        response.await.map_err(|_| {
            CatgaError::new(
                ErrorCode::Unavailable,
                "automatic batch runner stopped before replying",
            )
        })?
    }
}

struct Queued<M: Request> {
    key: Box<str>,
    entry: Pending<M>,
}

struct Pending<M: Request> {
    message: M,
    next: Next<M>,
    reply: oneshot::Sender<CatgaResult<M::Response>>,
    enqueued_at: Instant,
}

impl<M: Request> AutoBatchingRunner<M> {
    /// Processes batches until `shutdown` is cancelled.
    ///
    /// This future owns every mutable shard queue and starts no detached work.
    /// At most [`BatchOptions::max_shards`] batches execute at once, and each
    /// batch starts no more than [`BatchOptions::flush_concurrency`] handlers.
    /// Cancellation rejects unstarted queued requests with
    /// [`ErrorCode::Unavailable`] and then drains started batches.
    pub async fn run_until_cancelled(mut self, shutdown: CancellationToken) -> CatgaResult<()> {
        let mut shards = HashMap::<Box<str>, VecDeque<Pending<M>>>::new();
        let mut active_keys = HashSet::<Box<str>>::new();
        let mut flushes = FuturesUnordered::<FlushFuture>::new();

        loop {
            while flushes.len() < self.options.max_shards {
                let Some((key, batch)) = take_ready_batch(&mut shards, &self.options, &active_keys)
                else {
                    break;
                };
                active_keys.insert(key.clone());
                flushes.push(Box::pin(flush_batch(
                    batch,
                    self.options.flush_concurrency,
                    key,
                )));
            }

            if shutdown.is_cancelled() {
                break;
            }

            match next_deadline(&shards, self.options.batch_timeout, &active_keys) {
                Some(deadline) => {
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        queued = self.receiver.recv() => match queued {
                            Some(queued) => enqueue(&mut shards, queued, &self.options),
                            None => break,
                        },
                        () = sleep_until(deadline) => {},
                        completed = flushes.next(), if !flushes.is_empty() => {
                            if let Some(key) = completed {
                                active_keys.remove(&key);
                            }
                        },
                    }
                }
                None => {
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        queued = self.receiver.recv() => match queued {
                            Some(queued) => enqueue(&mut shards, queued, &self.options),
                            None => break,
                        },
                        completed = flushes.next(), if !flushes.is_empty() => {
                            if let Some(key) = completed {
                                active_keys.remove(&key);
                            }
                        },
                    }
                }
            }
        }

        reject_queued(&mut self.receiver, &mut shards);
        while flushes.next().await.is_some() {}
        Ok(())
    }
}

fn take_ready_batch<M: Request>(
    shards: &mut HashMap<Box<str>, VecDeque<Pending<M>>>,
    options: &BatchOptions,
    active_keys: &HashSet<Box<str>>,
) -> Option<(Box<str>, VecDeque<Pending<M>>)> {
    let now = Instant::now();
    let key = shards
        .iter()
        .find(|(key, queue)| {
            !active_keys.contains(*key)
                && (queue.len() >= options.max_batch_size
                    || queue
                        .front()
                        .and_then(|entry| entry.enqueued_at.checked_add(options.batch_timeout))
                        .is_some_and(|deadline| deadline <= now))
        })
        .map(|(key, _)| key.clone())?;
    let mut batch = shards.remove(&key)?;
    if batch.len() > options.max_batch_size {
        let pending = batch.split_off(options.max_batch_size);
        shards.insert(key.clone(), pending);
    }
    Some((key, batch))
}

fn next_deadline<M: Request>(
    shards: &HashMap<Box<str>, VecDeque<Pending<M>>>,
    timeout: Duration,
    active_keys: &HashSet<Box<str>>,
) -> Option<Instant> {
    shards
        .iter()
        .filter(|(key, _)| !active_keys.contains(*key))
        .filter_map(|(_, queue)| queue.front()?.enqueued_at.checked_add(timeout))
        .min()
}

fn enqueue<M: Request>(
    shards: &mut HashMap<Box<str>, VecDeque<Pending<M>>>,
    queued: Queued<M>,
    options: &BatchOptions,
) {
    let Queued { key, entry } = queued;
    let at_shard_capacity = shards.len() >= options.max_shards;
    match shards.entry(key) {
        HashMapEntry::Occupied(mut occupied) => {
            let queue = occupied.get_mut();
            queue.push_back(entry);
            reject_overflow(queue, options.max_queue_length);
        }
        HashMapEntry::Vacant(vacant) => {
            if at_shard_capacity {
                reject_entry(entry, "automatic batch shard capacity has been reached");
            } else {
                let mut queue = VecDeque::new();
                queue.push_back(entry);
                vacant.insert(queue);
            }
        }
    }
}

fn reject_queued<M: Request>(
    receiver: &mut mpsc::Receiver<Queued<M>>,
    shards: &mut HashMap<Box<str>, VecDeque<Pending<M>>>,
) {
    while let Ok(queued) = receiver.try_recv() {
        reject_unavailable(queued.entry);
    }
    for (_, queue) in shards.drain() {
        for entry in queue {
            reject_unavailable(entry);
        }
    }
}

fn reject_overflow<M: Request>(queue: &mut VecDeque<Pending<M>>, limit: usize) {
    while queue.len() > limit {
        if let Some(entry) = queue.pop_front() {
            reject_entry(entry, "automatic batch queue is full");
        }
    }
}

fn reject_entry<M: Request>(entry: Pending<M>, message: &'static str) {
    let _ = entry
        .reply
        .send(Err(CatgaError::new(ErrorCode::Transient, message)));
}

fn reject_unavailable<M: Request>(entry: Pending<M>) {
    let _ = entry.reply.send(Err(CatgaError::new(
        ErrorCode::Unavailable,
        "automatic batch runner is unavailable",
    )));
}

async fn flush_batch<M: Request>(
    batch: VecDeque<Pending<M>>,
    concurrency: usize,
    key: Box<str>,
) -> Box<str> {
    stream::iter(batch)
        .for_each_concurrent(Some(concurrency), execute_entry)
        .await;
    key
}

async fn execute_entry<M: Request>(entry: Pending<M>) {
    let Pending {
        message,
        next,
        reply,
        ..
    } = entry;
    let result = AssertUnwindSafe(next.run(message))
        .catch_unwind()
        .await
        .unwrap_or_else(|_| {
            Err(CatgaError::new(
                ErrorCode::Internal,
                "automatic batch request handler panicked",
            ))
        });
    let _ = reply.send(result);
}
