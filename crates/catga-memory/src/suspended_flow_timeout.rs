use std::{
    collections::{BTreeMap, HashMap},
    time::SystemTime,
};

use catga_flow::{
    FlowContinuation, TimedOutFlowPoll, TimedOutFlowReceipt, flow_timeout_deadline_unix_ms,
};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DueKey {
    deadline: SystemTime,
    flow_id: Box<str>,
}

#[derive(Default)]
pub(crate) struct DueIndex {
    available: BTreeMap<DueKey, u64>,
    claimed: HashMap<u64, DueKey>,
    by_flow: HashMap<Box<str>, (u64, DueKey)>,
    next_token: u64,
}

impl DueIndex {
    pub(crate) fn replace(&mut self, continuation: &FlowContinuation) {
        self.remove(continuation.state().id());
        let Some(deadline) = timeout_deadline(continuation) else {
            return;
        };
        self.next_token = self.next_token.wrapping_add(1).max(1);
        let token = self.next_token;
        let key = DueKey {
            deadline,
            flow_id: continuation.state().id().into(),
        };
        self.by_flow
            .insert(key.flow_id.clone(), (token, key.clone()));
        self.available.insert(key, token);
    }

    pub(crate) fn remove(&mut self, flow_id: &str) {
        let Some((token, key)) = self.by_flow.remove(flow_id) else {
            return;
        };
        if self.claimed.remove(&token).is_some() {
            return;
        }
        self.available.remove(&key);
    }

    pub(crate) fn poll(&mut self, poll: &TimedOutFlowPoll) -> Vec<TimedOutFlowReceipt> {
        let mut receipts = Vec::with_capacity(poll.limit());
        let mut inspected = 0_usize;
        while inspected < poll.scan_limit() && receipts.len() < poll.limit() {
            let Some((key, token)) = self.available.first_key_value() else {
                break;
            };
            if key.deadline > poll.now() {
                break;
            }
            inspected = inspected.saturating_add(1);
            let key = key.clone();
            let token = *token;
            self.available.remove(&key);
            self.claimed.insert(token, key.clone());
            receipts.push(TimedOutFlowReceipt::new(
                key.flow_id,
                token.to_be_bytes().to_vec(),
            ));
        }
        receipts
    }

    pub(crate) fn ack(&mut self, token: u64) {
        if let Some(key) = self.claimed.remove(&token) {
            self.by_flow.remove(&key.flow_id);
        }
    }

    pub(crate) fn release(&mut self, token: u64, continuation: Option<&FlowContinuation>) {
        let Some(key) = self.claimed.remove(&token) else {
            return;
        };
        let current_matches = continuation.is_some_and(|continuation| {
            continuation.state().id() == key.flow_id.as_ref()
                && timeout_deadline(continuation) == Some(key.deadline)
        });
        if current_matches
            && self
                .by_flow
                .get(&key.flow_id)
                .is_some_and(|(current_token, _)| *current_token == token)
        {
            self.available.insert(key, token);
        } else {
            self.by_flow.remove(&key.flow_id);
        }
    }
}

pub(crate) fn receipt_token(receipt: &TimedOutFlowReceipt) -> Option<u64> {
    receipt.token().try_into().ok().map(u64::from_be_bytes)
}

fn timeout_deadline(continuation: &FlowContinuation) -> Option<SystemTime> {
    flow_timeout_deadline_unix_ms(continuation)
        .ok()
        .flatten()
        .and_then(|deadline| {
            SystemTime::UNIX_EPOCH.checked_add(std::time::Duration::from_millis(deadline))
        })
}
