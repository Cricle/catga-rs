#![forbid(unsafe_code)]
//! Postcard envelope codec for Catga transports.

mod wire;

use catga_core::{CatgaError, CatgaResult, Envelope, EnvelopeCodec, ErrorCode};
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

fn map_error(error: postcard::Error) -> CatgaError {
    CatgaError::new(ErrorCode::Internal, error.to_string())
}
