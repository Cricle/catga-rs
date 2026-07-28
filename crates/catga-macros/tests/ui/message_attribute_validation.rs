use catga_macros::Message;

#[derive(Message)]
#[catga(version = 0)]
struct ZeroVersion;

#[derive(Message)]
#[catga(version = 1, version = 2)]
struct DuplicateVersion;

#[derive(Message)]
#[catga(version = "one")]
struct NonIntegerVersion;

#[derive(Message)]
#[catga(priority = "high")]
struct NonPathPriority;

#[derive(Message)]
#[catga(priority = low, priority = high)]
struct DuplicatePriority;

#[derive(Message)]
#[catga(policy("first"), policy("second"))]
struct DuplicatePolicy;

#[derive(Message)]
#[catga(unknown)]
struct UnknownAuthorizationOption;

#[derive(Message)]
#[catga(batch(max_batch_size = 0))]
struct ZeroBatchOption;

#[derive(Message)]
#[catga(batch(unrecognized = 4))]
struct UnknownBatchOption;

#[derive(Message)]
#[catga(batch_key = "missing")]
struct MissingBatchKey {
    present: u64,
}

#[derive(Message)]
#[catga(batch_key = "")]
struct EmptyBatchKey {
    present: u64,
}

#[derive(Message)]
#[catga(batch_key = "field")]
struct TupleBatchKey(u64);

#[derive(Message)]
#[catga(trace_tags(prefix = ""))]
struct EmptyTracePrefix {
    value: u64,
}

#[derive(Message)]
#[catga(trace_tags(include = [1]))]
struct NonStringTraceInclude {
    value: u64,
}

#[derive(Message)]
struct InvalidFieldTraceTag {
    #[catga(trace_tag = "")]
    value: u64,
}

#[derive(Message)]
struct DuplicateFieldTraceTag {
    #[catga(trace_tag, trace_tag)]
    value: u64,
}

#[derive(Message)]
struct UnexpectedFieldTraceTag {
    #[catga(other)]
    value: u64,
}

fn main() {}
