//! `#[derive(Message)]` rejects invalid bulk `trace_tags(...)` configuration at expansion time.

#[derive(catga_core_macros::Message)]
#[catga(trace_tags(all_public = true), trace_tags(all_public = false))]
struct TraceTagsTwice {
    a: u64,
}

#[derive(catga_core_macros::Message)]
#[catga(trace_tags(all_public))]
struct TraceTagsOptionNotNamedValue {
    a: u64,
}

#[derive(catga_core_macros::Message)]
#[catga(trace_tags(a::b = true))]
struct TraceTagsOptionNotAnIdentifier {
    a: u64,
}

#[derive(catga_core_macros::Message)]
#[catga(trace_tags(bogus = 1))]
struct TraceTagsUnknownOption {
    a: u64,
}

#[derive(catga_core_macros::Message)]
#[catga(trace_tags(prefix = "a.", prefix = "b."))]
struct TraceTagsPrefixTwice {
    a: u64,
}

#[derive(catga_core_macros::Message)]
#[catga(trace_tags(prefix = 1))]
struct TraceTagsPrefixNotAString {
    a: u64,
}

#[derive(catga_core_macros::Message)]
#[catga(trace_tags(prefix = ""))]
struct TraceTagsPrefixEmpty {
    a: u64,
}

#[derive(catga_core_macros::Message)]
#[catga(trace_tags(all_public = true, all_public = false))]
struct TraceTagsAllPublicTwice {
    a: u64,
}

#[derive(catga_core_macros::Message)]
#[catga(trace_tags(all_public = 1))]
struct TraceTagsAllPublicNotABool {
    a: u64,
}

fn main() {}
