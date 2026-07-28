use catga_macros::Message;

#[derive(Message)]
#[catga(priority = priority::high)]
struct QualifiedPriority;

#[derive(Message)]
#[catga(batch(max_batch_size = 1), batch(timeout_ms = 2))]
struct DuplicateBatch;

#[derive(Message)]
#[catga(batch(max_batch_size))]
struct MissingBatchValue;

#[derive(Message)]
#[catga(batch(max_batch_size = "many"))]
struct NonIntegerBatchValue;

#[derive(Message)]
#[catga(batch(max_batch_size = 18446744073709551616))]
struct OverflowingBatchValue;

#[derive(Message)]
#[catga(batch_key = 1)]
struct NonStringBatchKey {
    field: u64,
}

#[derive(Message)]
#[catga(batch_key = "first", batch_key = "second")]
struct DuplicateBatchKey {
    first: u64,
    second: u64,
}

#[derive(Message)]
#[catga(trace_tags(prefix = "first.", prefix = "second."))]
struct DuplicateTracePrefix {
    value: u64,
}

#[derive(Message)]
#[catga(trace_tags(include = ["first"], include = ["second"]))]
struct DuplicateTraceInclude {
    value: u64,
}

#[derive(Message)]
#[catga(trace_tags(exclude = ["first"], exclude = ["second"]))]
struct DuplicateTraceExclude {
    value: u64,
}

#[derive(Message)]
#[catga(trace_tags(all_public = true, all_public = false))]
struct DuplicateAllPublic {
    value: u64,
}

#[derive(Message)]
#[catga(trace_tags(all_public = "yes"))]
struct NonBooleanAllPublic {
    value: u64,
}

#[derive(Message)]
#[catga(trace_tags(unknown = true))]
struct UnknownTraceOption {
    value: u64,
}

#[derive(Message)]
#[catga(trace_tags(include = "field"))]
struct NonArrayTraceInclude {
    value: u64,
}

#[derive(Message)]
#[catga(trace_tags(exclude = [""]))]
struct EmptyTraceExclude {
    value: u64,
}

#[derive(Message)]
struct NonStringFieldTraceTag {
    #[catga(trace_tag = 1)]
    value: u64,
}

fn main() {}
