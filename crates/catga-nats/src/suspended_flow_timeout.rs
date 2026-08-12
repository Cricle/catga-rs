use std::time::Duration;

use async_nats::{
    Client,
    jetstream::{AckKind, consumer},
};
use catga_core::flow::{
    TimedOutFlowPoll, TimedOutFlowReceipt, decode_continuation, flow_timeout_deadline_unix_ms,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use futures::StreamExt;

use crate::record::decode_record;

pub(crate) async fn poll(
    consumer: &consumer::PullConsumer,
    poll: &TimedOutFlowPoll,
) -> CatgaResult<Vec<TimedOutFlowReceipt>> {
    let now = unix_millis(poll.now())?;
    let mut messages = consumer
        .batch()
        .max_messages(poll.scan_limit())
        .expires(Duration::from_millis(100))
        .messages()
        .await
        .map_err(CatgaError::transient)?;
    let mut receipts = Vec::with_capacity(poll.limit());
    while let Some(message) = messages.next().await {
        let message = message.map_err(CatgaError::transient)?;
        if message.payload.is_empty() {
            message.ack().await.map_err(CatgaError::transient)?;
            continue;
        }
        let continuation = decode_continuation(decode_record(&message.payload)?.payload())?;
        let Some(deadline) = flow_timeout_deadline_unix_ms(&continuation)? else {
            message.ack().await.map_err(CatgaError::transient)?;
            continue;
        };
        if deadline > now {
            message
                .ack_with(AckKind::Nak(Some(Duration::from_millis(deadline - now))))
                .await
                .map_err(CatgaError::transient)?;
            continue;
        }
        if receipts.len() == poll.limit() {
            message
                .ack_with(AckKind::Nak(None))
                .await
                .map_err(CatgaError::transient)?;
            continue;
        }
        let reply = message.reply.as_ref().ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Internal,
                "NATS timeout delivery has no acknowledgement subject",
            )
        })?;
        receipts.push(TimedOutFlowReceipt::new(
            continuation.state().id(),
            reply.as_bytes().to_vec(),
        ));
    }
    Ok(receipts)
}

pub(crate) async fn ack(client: &Client, receipt: &TimedOutFlowReceipt) -> CatgaResult<()> {
    settle(client, receipt, AckKind::Ack).await
}

pub(crate) async fn release(client: &Client, receipt: &TimedOutFlowReceipt) -> CatgaResult<()> {
    settle(client, receipt, AckKind::Nak(None)).await
}

async fn settle(client: &Client, receipt: &TimedOutFlowReceipt, kind: AckKind) -> CatgaResult<()> {
    let subject = std::str::from_utf8(receipt.token())
        .map_err(|_| CatgaError::new(ErrorCode::Validation, "NATS timeout receipt is invalid"))?;
    client
        .publish(subject.to_owned(), kind.into())
        .await
        .map_err(CatgaError::transient)
}

fn unix_millis(time: std::time::SystemTime) -> CatgaResult<u64> {
    catga_core::time::checked_unix_millis(time).map_err(|error| {
        CatgaError::new(
            ErrorCode::Validation,
            match error {
                catga_core::time::UnixMillisError::BeforeEpoch => {
                    "NATS timeout poll precedes the Unix epoch"
                }
                catga_core::time::UnixMillisError::ExceedsRange => {
                    "NATS timeout poll exceeds the supported millisecond range"
                }
            },
        )
    })
}
