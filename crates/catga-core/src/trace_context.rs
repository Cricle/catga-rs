//! Bounded W3C trace-context propagation for envelope headers.

use std::collections::HashSet;

use crate::{CatgaResult, EnvelopeHeaders};

/// Lowercase W3C header name carrying the parent trace identity.
pub const TRACEPARENT_HEADER: &str = "traceparent";

/// Lowercase W3C header name carrying vendor-specific trace state.
pub const TRACESTATE_HEADER: &str = "tracestate";

/// Maximum number of bytes retained for a W3C `tracestate` value.
pub const MAX_TRACESTATE_BYTES: usize = 512;

/// Maximum number of bytes retained for a W3C `traceparent` value.
pub const MAX_TRACEPARENT_BYTES: usize = 512;

/// A validated, bounded W3C trace context suitable for transport propagation.
///
/// The context intentionally retains only the W3C header values. It neither
/// installs an OpenTelemetry provider nor uses mutable global state, allowing
/// applications to choose their own tracing/exporter integration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TraceContext {
    traceparent: Box<str>,
    tracestate: Option<Box<str>>,
}

impl TraceContext {
    /// Parses a W3C `traceparent` and optional bounded `tracestate`.
    ///
    /// An invalid, all-zero, non-canonical, or oversized `traceparent` is rejected with `None`.
    /// `tracestate` is an independent companion header: invalid values are discarded while the
    /// valid parent context remains usable. Empty, whitespace-only, or malformed `tracestate`
    /// values are discarded so they are not propagated again.
    pub fn parse(traceparent: &str, tracestate: Option<&str>) -> Option<Self> {
        if !valid_traceparent(traceparent) {
            return None;
        }
        Some(Self {
            traceparent: traceparent.into(),
            tracestate: tracestate
                .filter(|value| {
                    valid_tracestate(value) && !value.trim_matches([' ', '\t']).is_empty()
                })
                .map(Into::into),
        })
    }

    /// Extracts a valid W3C context from case-insensitive envelope headers.
    ///
    /// A missing or invalid `traceparent` yields `None`. An invalid `tracestate` is discarded so
    /// a malformed remote value is never propagated onward, while the valid parent is retained.
    pub fn from_envelope_headers(headers: &EnvelopeHeaders) -> Option<Self> {
        Self::parse(
            envelope_header(headers, TRACEPARENT_HEADER)?,
            envelope_header(headers, TRACESTATE_HEADER),
        )
    }

    /// Returns the validated W3C `traceparent` value.
    pub fn traceparent(&self) -> &str {
        &self.traceparent
    }

    /// Returns the validated W3C `tracestate` value, when present.
    pub fn tracestate(&self) -> Option<&str> {
        self.tracestate.as_deref()
    }

    /// Merges this context into optional envelope headers without discarding application headers.
    ///
    /// Existing W3C values are replaced atomically. The returned value is revalidated against the
    /// normal immutable envelope-header count and byte limits.
    pub fn inject_into_envelope_headers(
        &self,
        headers: Option<&EnvelopeHeaders>,
    ) -> CatgaResult<EnvelopeHeaders> {
        let mut entries: Vec<(&str, &str)> = headers
            .into_iter()
            .flat_map(EnvelopeHeaders::iter)
            .filter(|(key, _)| {
                !key.eq_ignore_ascii_case(TRACEPARENT_HEADER)
                    && !key.eq_ignore_ascii_case(TRACESTATE_HEADER)
            })
            .collect();
        entries.push((TRACEPARENT_HEADER, self.traceparent()));
        if let Some(tracestate) = self.tracestate() {
            entries.push((TRACESTATE_HEADER, tracestate));
        }
        EnvelopeHeaders::try_new(entries)
    }
}

fn envelope_header<'a>(headers: &'a EnvelopeHeaders, name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value)
}

fn valid_traceparent(value: &str) -> bool {
    let bytes = value.as_bytes();
    if !(55..=MAX_TRACEPARENT_BYTES).contains(&bytes.len())
        || bytes[2] != b'-'
        || bytes[35] != b'-'
        || bytes[52] != b'-'
    {
        return false;
    }
    let version = &bytes[0..2];
    version
        .iter()
        .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        && version != b"ff"
        && valid_nonzero_hex(&bytes[3..35])
        && valid_nonzero_hex(&bytes[36..52])
        && bytes[53..55]
            .iter()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        && (version != b"00" || bytes.len() == 55)
        && (bytes.len() == 55
            || (bytes[55] == b'-'
                && bytes.len() > 56
                && bytes[56..]
                    .iter()
                    .all(|byte| byte.is_ascii_graphic() && *byte != b'-')))
}

fn valid_nonzero_hex(bytes: &[u8]) -> bool {
    bytes.iter().any(|byte| *byte != b'0')
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn valid_tracestate(value: &str) -> bool {
    if value.len() > MAX_TRACESTATE_BYTES || !value.is_ascii() || value.split(',').count() > 32 {
        return false;
    }
    let mut keys = HashSet::new();
    value
        .split(',')
        .all(|member| valid_tracestate_member(member, &mut keys))
}

fn valid_tracestate_member<'a>(member: &'a str, keys: &mut HashSet<&'a str>) -> bool {
    let member = member.trim_matches([' ', '\t']);
    if member.is_empty() {
        return false;
    }
    let Some((key, value)) = member.split_once('=') else {
        return false;
    };
    valid_tracestate_key(key) && valid_tracestate_value(value) && keys.insert(key)
}

fn valid_tracestate_key(key: &str) -> bool {
    let mut parts = key.split('@');
    let Some(tenant) = parts.next() else {
        return false;
    };
    let system = parts.next();
    if parts.next().is_some() {
        return false;
    }
    match system {
        Some(system) => {
            valid_tracestate_key_part(tenant, 241, true)
                && valid_tracestate_key_part(system, 14, false)
        }
        None => valid_tracestate_key_part(tenant, 256, false),
    }
}

fn valid_tracestate_key_part(value: &str, max_len: usize, digit_prefix: bool) -> bool {
    let Some((first, remaining)) = value.as_bytes().split_first() else {
        return false;
    };
    value.len() <= max_len
        && (first.is_ascii_lowercase() || (digit_prefix && first.is_ascii_digit()))
        && remaining.iter().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(*byte, b'_' | b'-' | b'*' | b'/')
        })
}

fn valid_tracestate_value(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && !value.ends_with([' ', '\t'])
        && value
            .bytes()
            .all(|byte| matches!(byte, b' '..=b'+' | b'-'..=b'<' | b'>'..=b'~'))
}
