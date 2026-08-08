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
        .map_err(map_error)?;
    let mut receipts = Vec::with_capacity(poll.limit());
    while let Some(message) = messages.next().await {
        let message = message.map_err(map_error)?;
        if message.payload.is_empty() {
            message.ack().await.map_err(map_error)?;
            continue;
        }
        let continuation = decode_continuation(decode_record(&message.payload)?.payload())?;
        let Some(deadline) = flow_timeout_deadline_unix_ms(&continuation)? else {
            message.ack().await.map_err(map_error)?;
            continue;
        };
        if deadline > now {
            message
                .ack_with(AckKind::Nak(Some(Duration::from_millis(deadline - now))))
                .await
                .map_err(map_error)?;
            continue;
        }
        if receipts.len() == poll.limit() {
            message
                .ack_with(AckKind::Nak(None))
                .await
                .map_err(map_error)?;
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
        .map_err(map_error)
}

fn unix_millis(time: std::time::SystemTime) -> CatgaResult<u64> {
    let elapsed = time
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "NATS timeout poll precedes the Unix epoch",
            )
        })?;
    u64::try_from(elapsed.as_millis()).map_err(|_| {
        CatgaError::new(
            ErrorCode::Validation,
            "NATS timeout poll exceeds the supported millisecond range",
        )
    })
}

fn map_error(error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Transient, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unix_millis_handles_unix_epoch() {
        let result = unix_millis(std::time::UNIX_EPOCH);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn unix_millis_handles_reasonable_time() {
        let time = std::time::UNIX_EPOCH + Duration::from_secs(1700000000);
        let result = unix_millis(time);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1700000000000);
    }

    #[test]
    fn unix_millis_rejects_time_before_unix_epoch() {
        let before_epoch = std::time::UNIX_EPOCH - Duration::from_secs(1);
        let result = unix_millis(before_epoch);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), ErrorCode::Validation);
    }

    #[test]
    fn unix_millis_handles_max_u64_millis() {
        let max_time = std::time::UNIX_EPOCH + Duration::from_millis(u64::MAX);
        let result = unix_millis(max_time);
        assert!(result.is_ok());
    }

    #[test]
    fn map_error_creates_transient_error() {
        let error = map_error("timeout");
        assert_eq!(error.code(), ErrorCode::Transient);
        assert!(error.to_string().contains("timeout"));
    }
}
