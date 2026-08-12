//! `#[derive(Message)]` rejects invalid bulk trace tag `include`/`exclude` lists.

#[derive(catga_core_macros::Message)]
#[catga(trace_tags(include = ["a"], include = ["b"]))]
struct IncludeTwice {
    a: u64,
}

#[derive(catga_core_macros::Message)]
#[catga(trace_tags(exclude = ["a"], exclude = ["b"]))]
struct ExcludeTwice {
    a: u64,
}

#[derive(catga_core_macros::Message)]
#[catga(trace_tags(include = "a"))]
struct IncludeNotAnArray {
    a: u64,
}

#[derive(catga_core_macros::Message)]
#[catga(trace_tags(exclude = "a"))]
struct ExcludeNotAnArray {
    a: u64,
}

#[derive(catga_core_macros::Message)]
#[catga(trace_tags(include = [1]))]
struct IncludeEntryNotAString {
    a: u64,
}

#[derive(catga_core_macros::Message)]
#[catga(trace_tags(include = [""]))]
struct IncludeEntryEmpty {
    a: u64,
}

fn main() {}
