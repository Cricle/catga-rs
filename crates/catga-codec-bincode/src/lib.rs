#![forbid(unsafe_code)]
//! Bincode-next payload codec for Catga typed transports.
//!
//! This crate implements Catga's format-neutral payload traits with the native
//! [`bincode_next::Encode`] and [`bincode_next::Decode`] contracts. Transport envelope codecs
//! remain independent, so applications can select this payload codec without coupling Core to a
//! serialization format.
//!
//! ```
//! use catga_codec_bincode::BincodeCodec;
//! use catga_core::{PayloadDecoder, PayloadEncoder};
//!
//! let codec = BincodeCodec;
//! let frame = <BincodeCodec as PayloadEncoder<u64>>::encode_payload(&codec, &42)?;
//! let value = <BincodeCodec as PayloadDecoder<u64>>::decode_payload(&codec, &frame)?;
//! assert_eq!(value, 42);
//! # Ok::<(), catga_core::CatgaError>(())
//! ```

mod codec;

pub use bincode_next::{Decode, Encode};
pub use codec::{BincodeCodec, MAX_BINCODE_FRAME_BYTES};
