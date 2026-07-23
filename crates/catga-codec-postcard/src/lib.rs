#![forbid(unsafe_code)]
//! Postcard envelope codec for Catga transports.

mod wire;

use std::marker::PhantomData;

use catga_core::{CatgaError, CatgaResult, Envelope, EnvelopeCodec, ErrorCode, SnapshotCodec};
use serde::{Serialize, de::DeserializeOwned};
use wire::EnvelopeWire;

/// A compact binary envelope codec backed by Postcard.
#[derive(Clone, Copy, Debug, Default)]
pub struct PostcardCodec;

impl EnvelopeCodec for PostcardCodec {
    fn encode(&self, envelope: &Envelope) -> CatgaResult<Vec<u8>> {
        postcard::to_allocvec(&EnvelopeWire::from(envelope)).map_err(map_error)
    }

    fn decode(&self, bytes: &[u8]) -> CatgaResult<Envelope> {
        postcard::from_bytes::<EnvelopeWire>(bytes)
            .map(Envelope::from)
            .map_err(map_error)
    }
}

/// Compact Postcard codec for one explicit persistent snapshot state type.
#[derive(Clone, Copy, Debug)]
pub struct PostcardSnapshotCodec<S> {
    state: PhantomData<fn() -> S>,
}

impl<S> Default for PostcardSnapshotCodec<S> {
    fn default() -> Self {
        Self { state: PhantomData }
    }
}

impl<S> SnapshotCodec<S> for PostcardSnapshotCodec<S>
where
    S: DeserializeOwned + Serialize + Send + Sync,
{
    fn encode_state(&self, state: &S) -> CatgaResult<Vec<u8>> {
        postcard::to_allocvec(state).map_err(map_error)
    }

    fn decode_state(&self, bytes: &[u8]) -> CatgaResult<S> {
        postcard::from_bytes(bytes).map_err(map_error)
    }
}

fn map_error(error: postcard::Error) -> CatgaError {
    CatgaError::new(ErrorCode::Internal, error.to_string())
}
