#![forbid(unsafe_code)]
//! RobustMQ mq9 mailbox extensions for Catga.
//!
//! [`MailboxClient`] sends complete Catga envelopes through RobustMQ's mq9 mailbox API, while
//! [`MailboxRequestServer`] and [`MailboxRequest`] provide explicit request/reply handling. The
//! default codec is bounded MemoryPack; use the `*_with_codec` constructors when both peers agree
//! on another [`catga_core::EnvelopeCodec`]. The adapter opens connections only when the caller
//! explicitly calls `connect` and owns no worker, signal handler, or global client.
//!
//! Mailbox configuration is ordinary application data and can be validated before any network
//! connection is attempted:
//!
//! ```
//! use catga_robustmq::MailboxConfig;
//!
//! let replies = MailboxConfig {
//!     server: "nats://127.0.0.1:4222".into(),
//!     ttl_seconds: 60,
//!     public: false,
//!     name: "order-replies".into(),
//!     description: "private request replies".into(),
//! };
//! assert!(!replies.public);
//! ```
//!
//! Keep reply mailboxes private and set a finite TTL. A caller must authenticate the server URL
//! and validate the identity and authorization of every received envelope before applying its
//! payload; mailbox visibility and correlation alone are not an authorization boundary.

mod client;
mod priority;

pub use catga_core::codec::memorypack::{MemoryPackRequestClient, MemoryPackRpcResponse};
pub use client::{MailboxClient, MailboxConfig, MailboxRequest, MailboxRequestServer};
pub use priority::MailboxPriority;
