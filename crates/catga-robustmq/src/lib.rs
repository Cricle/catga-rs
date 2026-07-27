#![forbid(unsafe_code)]
//! RobustMQ mq9 mailbox extensions for Catga.

mod client;
mod priority;

pub use catga_codec_memorypack::{MemoryPackRequestClient, MemoryPackRpcResponse};
pub use client::{MailboxClient, MailboxConfig, MailboxRequest, MailboxRequestServer};
pub use priority::MailboxPriority;
