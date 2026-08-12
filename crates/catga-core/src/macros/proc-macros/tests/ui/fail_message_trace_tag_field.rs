//! `#[derive(Message)]` rejects invalid field-level `trace_tag` options at expansion time.

#[derive(catga_core_macros::Message)]
struct TraceTagNotAString {
    #[catga(trace_tag = 1)]
    value: u64,
}

#[derive(catga_core_macros::Message)]
struct TraceTagEmpty {
    #[catga(trace_tag = "")]
    value: u64,
}

#[derive(catga_core_macros::Message)]
struct TraceTagUnknownOption {
    #[catga(bogus)]
    value: u64,
}

#[derive(catga_core_macros::Message)]
struct TraceTagTwice {
    #[catga(trace_tag, trace_tag)]
    value: u64,
}

fn main() {}
