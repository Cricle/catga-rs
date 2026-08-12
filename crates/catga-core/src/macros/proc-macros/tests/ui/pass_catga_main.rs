//! A bare `#[catga_main]` entry point expands against the real AutoApp facade.

#[catga_core_macros::catga_main]
async fn entry() -> catga_core::CatgaResult<()> {
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    entry().await.expect("entry point runs");
}
