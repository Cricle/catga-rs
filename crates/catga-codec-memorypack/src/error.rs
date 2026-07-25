use catga_core::{CatgaError, ErrorCode};

pub(crate) fn invalid(message: impl Into<Box<str>>) -> CatgaError {
    CatgaError::new(ErrorCode::Validation, message)
}

pub(crate) fn limit(message: impl Into<Box<str>>) -> CatgaError {
    CatgaError::new(ErrorCode::Validation, message)
}

pub(crate) fn truncated() -> CatgaError {
    CatgaError::new(ErrorCode::Validation, "truncated MemoryPack frame")
}

pub(crate) fn allocation() -> CatgaError {
    CatgaError::new(
        ErrorCode::Internal,
        "cannot reserve bounded MemoryPack allocation",
    )
}
