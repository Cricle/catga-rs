#![forbid(unsafe_code)]
//! RobustMQ mq9 mailbox extensions for Catga.

mod client;
mod priority;

pub use client::{MailboxClient, MailboxConfig};
pub use priority::MailboxPriority;
