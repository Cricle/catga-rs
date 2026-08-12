//! Compile-pass and runtime test for the `#[catga_main]` entry-point wrapper.
//!
//! The expansion must reference the real application facade at
//! `::catga_core::auto::AutoApp`; a regression emitted `use catga_auto::AutoApp`, and no
//! `catga_auto` crate exists.

use catga_core::catga_main;

#[catga_main]
async fn app_entry() -> catga_core::CatgaResult<()> {
    Ok(())
}

#[tokio::test]
async fn catga_main_builds_auto_app_and_runs_body() -> catga_core::CatgaResult<()> {
    app_entry().await
}
